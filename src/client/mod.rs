//! The Gearman client role: submitting jobs to one or more job servers and
//! tracking their progress. See [`Client`] and [`ClientBuilder`].

mod job;
mod multi;

pub use job::{
    JobEvent, JobEventStream, JobHandle, JobStatus, Priority, SubmitOptions, SubmittedJob,
};

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};

use crate::error::{GearmanError, Result};
use crate::net::{BoxedStream, Transport};
use crate::protocol::{Packet, PacketType};
use crate::Connection;

use multi::{ServerPool, ServerSlot};

/// Builds a [`Client`], configuring which servers it connects to and how.
pub struct ClientBuilder {
    servers: Vec<String>,
    with_exceptions: bool,
    transport: Transport,
}

impl ClientBuilder {
    /// Starts a builder with no servers configured; call [`Self::servers`]
    /// before [`Self::connect`].
    pub fn new() -> Self {
        Self {
            servers: Vec::new(),
            with_exceptions: false,
            transport: Transport::default(),
        }
    }

    /// Sets the job server addresses (`host:port`) to connect to. The client
    /// spawns one persistent connection per server.
    pub fn servers<I, S>(mut self, servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.servers = servers.into_iter().map(Into::into).collect();
        self
    }

    /// Sends `OPTION_REQ "exceptions"` on connect, opting in to receiving
    /// `WORK_EXCEPTION` events for foreground jobs.
    pub fn with_exceptions(mut self, enabled: bool) -> Self {
        self.with_exceptions = enabled;
        self
    }

    /// Connect to every server over TLS instead of plain TCP. gearmand
    /// wraps the raw stream in TLS before any Gearman framing begins.
    #[cfg(feature = "tls")]
    pub fn tls(mut self, config: crate::tls::TlsConfig) -> Self {
        self.transport = Transport::Tls(config);
        self
    }

    /// Spawns one persistent connection per server and waits (up to 5s) for
    /// at least one to come up before returning.
    pub async fn connect(self) -> Result<Client> {
        if self.servers.is_empty() {
            return Err(GearmanError::NoServersAvailable);
        }

        let mut slots = Vec::with_capacity(self.servers.len());
        for addr in &self.servers {
            let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
            let connected = Arc::new(AtomicBool::new(false));
            tokio::spawn(run_server_actor(
                addr.clone(),
                cmd_rx,
                connected.clone(),
                self.with_exceptions,
                self.transport.clone(),
            ));
            slots.push(ServerSlot { cmd_tx, connected });
        }
        let pool = Arc::new(ServerPool::new(slots));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !pool.any_healthy() {
            if tokio::time::Instant::now() >= deadline {
                return Err(GearmanError::NoServersAvailable);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        Ok(Client { pool })
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A handle to one or more Gearman job servers, used to submit jobs and
/// query their status. Cheap to clone; clones share the same underlying
/// connections.
#[derive(Clone)]
pub struct Client {
    pool: Arc<ServerPool>,
}

impl Client {
    /// Starts a [`ClientBuilder`].
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Submits a job with explicit [`SubmitOptions`] (priority and
    /// foreground/background).
    pub async fn submit(
        &self,
        function: &str,
        unique: Option<&str>,
        payload: impl Into<Bytes>,
        opts: SubmitOptions,
    ) -> Result<SubmittedJob> {
        let ptype = submit_packet_type(opts.priority, opts.background);
        let args = vec![
            Bytes::copy_from_slice(function.as_bytes()),
            Bytes::from(unique.unwrap_or("").to_string()),
            payload.into(),
        ];
        self.submit_packet(ptype, args, unique, opts.background)
            .await
    }

    /// Schedules a job to become eligible for a worker to grab at or after
    /// `epoch` (Unix seconds). Like `submit_bg`, this returns as soon as the
    /// server acknowledges the job with `JOB_CREATED` — there is no
    /// foreground/event-stream variant, since a client waiting on a
    /// possibly-distant future completion isn't a sensible default. Poll
    /// `get_status`/`get_status_unique` for completion.
    pub async fn submit_epoch(
        &self,
        function: &str,
        unique: Option<&str>,
        epoch: u64,
        payload: impl Into<Bytes>,
    ) -> Result<JobHandle> {
        let args = vec![
            Bytes::copy_from_slice(function.as_bytes()),
            Bytes::from(unique.unwrap_or("").to_string()),
            Bytes::from(epoch.to_string()),
            payload.into(),
        ];
        let submitted = self
            .submit_packet(PacketType::SubmitJobEpoch, args, unique, true)
            .await?;
        Ok(submitted.handle)
    }

    /// Submits a job with a reducer name attached (`SUBMIT_REDUCE_JOB[_BACKGROUND]`).
    /// gearmand only forwards the reducer name to workers that grab with
    /// `GrabMode::All` (`WorkerJob::reducer`) — it implements no reduction
    /// logic itself, this is purely an opaque passthrough field.
    pub async fn submit_reduce_job(
        &self,
        function: &str,
        unique: Option<&str>,
        reducer: &str,
        payload: impl Into<Bytes>,
        background: bool,
    ) -> Result<SubmittedJob> {
        let ptype = if background {
            PacketType::SubmitReduceJobBackground
        } else {
            PacketType::SubmitReduceJob
        };
        let args = vec![
            Bytes::copy_from_slice(function.as_bytes()),
            Bytes::from(unique.unwrap_or("").to_string()),
            Bytes::copy_from_slice(reducer.as_bytes()),
            Bytes::new(), // UNUSED: a real field on the wire, ignored by the server
            payload.into(),
        ];
        self.submit_packet(ptype, args, unique, background).await
    }

    async fn submit_packet(
        &self,
        ptype: PacketType,
        args: Vec<Bytes>,
        unique: Option<&str>,
        background: bool,
    ) -> Result<SubmittedJob> {
        let server_index = self
            .pool
            .next_healthy()
            .ok_or(GearmanError::NoServersAvailable)?;
        let slot = self.pool.get(server_index);

        let packet = Packet::request(ptype, args);

        let (reply_tx, reply_rx) = oneshot::channel();
        let (events_tx, events_rx) = if background {
            (None, None)
        } else {
            let (tx, rx) = mpsc::unbounded_channel();
            (Some(tx), Some(rx))
        };

        slot.cmd_tx
            .send(ClientCommand::Submit {
                packet,
                reply: reply_tx,
                events: events_tx,
            })
            .map_err(|_| GearmanError::ConnectionClosed)?;

        let handle_str = reply_rx
            .await
            .map_err(|_| GearmanError::ConnectionClosed)??;
        let handle = JobHandle {
            server_index,
            handle: handle_str,
            unique: unique.map(str::to_string),
        };

        Ok(SubmittedJob {
            handle,
            events: events_rx.map(|rx| JobEventStream { rx }),
        })
    }

    /// Submits a job and waits for it to complete, returning its result
    /// payload. `WORK_STATUS`/`WORK_DATA`/`WORK_WARNING` events along the way
    /// are discarded; use [`Client::submit`] directly to observe them.
    pub async fn submit_fg(&self, function: &str, payload: impl Into<Bytes>) -> Result<Bytes> {
        self.submit_fg_unique(function, None, payload).await
    }

    /// Like [`Client::submit_fg`], but with an explicit unique id for
    /// deduplication against other jobs already queued for `function`.
    pub async fn submit_fg_unique(
        &self,
        function: &str,
        unique: Option<&str>,
        payload: impl Into<Bytes>,
    ) -> Result<Bytes> {
        let submitted = self
            .submit(function, unique, payload, SubmitOptions::foreground())
            .await?;
        let mut events = submitted
            .events
            .expect("foreground submit always registers an event stream");

        loop {
            match events.next().await {
                Some(JobEvent::Complete(result)) => return Ok(result),
                Some(JobEvent::Fail) => {
                    return Err(GearmanError::JobFailed {
                        handle: submitted.handle.handle,
                    })
                }
                Some(JobEvent::Exception(payload)) => {
                    return Err(GearmanError::JobException {
                        handle: submitted.handle.handle,
                        payload,
                    })
                }
                Some(_) => continue,
                None => return Err(GearmanError::ConnectionClosed),
            }
        }
    }

    /// Submits a background job, returning its handle as soon as the server
    /// acknowledges it with `JOB_CREATED`. No `WORK_*` events are ever
    /// delivered for a background job; poll [`Client::get_status`] instead.
    pub async fn submit_bg(&self, function: &str, payload: impl Into<Bytes>) -> Result<JobHandle> {
        let submitted = self
            .submit(function, None, payload, SubmitOptions::background())
            .await?;
        Ok(submitted.handle)
    }

    /// Queries a job's status with `GET_STATUS`. Must be sent to the same
    /// server that created the job (see [`JobHandle`]).
    pub async fn get_status(&self, handle: &JobHandle) -> Result<JobStatus> {
        let slot = self.pool.get(handle.server_index);
        let packet = Packet::request(
            PacketType::GetStatus,
            vec![Bytes::copy_from_slice(handle.handle.as_bytes())],
        );
        let (tx, rx) = oneshot::channel();
        slot.cmd_tx
            .send(ClientCommand::Status { packet, reply: tx })
            .map_err(|_| GearmanError::ConnectionClosed)?;
        rx.await.map_err(|_| GearmanError::ConnectionClosed)?
    }

    /// Like [`Client::get_status`], but via `GET_STATUS_UNIQUE`, which also
    /// reports `client_count`. Fails with [`GearmanError::NoUniqueId`] if
    /// `handle` was created without a unique id.
    pub async fn get_status_unique(&self, handle: &JobHandle) -> Result<JobStatus> {
        let unique = handle
            .unique
            .clone()
            .ok_or_else(|| GearmanError::NoUniqueId {
                handle: handle.handle.clone(),
            })?;
        let slot = self.pool.get(handle.server_index);
        let packet = Packet::request(PacketType::GetStatusUnique, vec![Bytes::from(unique)]);
        let (tx, rx) = oneshot::channel();
        slot.cmd_tx
            .send(ClientCommand::StatusUnique { packet, reply: tx })
            .map_err(|_| GearmanError::ConnectionClosed)?;
        rx.await.map_err(|_| GearmanError::ConnectionClosed)?
    }
}

fn submit_packet_type(priority: Priority, background: bool) -> PacketType {
    use Priority::*;
    match (priority, background) {
        (Normal, false) => PacketType::SubmitJob,
        (Normal, true) => PacketType::SubmitJobBg,
        (High, false) => PacketType::SubmitJobHigh,
        (High, true) => PacketType::SubmitJobHighBg,
        (Low, false) => PacketType::SubmitJobLow,
        (Low, true) => PacketType::SubmitJobLowBg,
    }
}

pub(crate) enum ClientCommand {
    Submit {
        packet: Packet,
        reply: oneshot::Sender<Result<String>>,
        events: Option<mpsc::UnboundedSender<JobEvent>>,
    },
    Status {
        packet: Packet,
        reply: oneshot::Sender<Result<JobStatus>>,
    },
    StatusUnique {
        packet: Packet,
        reply: oneshot::Sender<Result<JobStatus>>,
    },
}

enum PendingReply {
    JobCreated {
        reply: oneshot::Sender<Result<String>>,
        events: Option<mpsc::UnboundedSender<JobEvent>>,
    },
    Status(oneshot::Sender<Result<JobStatus>>),
    StatusUnique(oneshot::Sender<Result<JobStatus>>),
}

enum ActorOutcome {
    ConnectionLost,
    ClientDropped,
}

/// Owns one job server connection for its whole lifetime: reconnects with
/// exponential backoff on drop, and in between runs [`actor_loop`], which is
/// the only place that touches the socket, so ordering between outgoing
/// requests and their (handle-less) replies is trivially preserved.
async fn run_server_actor(
    addr: String,
    mut cmd_rx: mpsc::UnboundedReceiver<ClientCommand>,
    connected: Arc<AtomicBool>,
    with_exceptions: bool,
    transport: Transport,
) {
    let mut backoff = Duration::from_millis(100);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    loop {
        match crate::net::connect(&addr, &transport).await {
            Ok(mut conn) => {
                let ready = if with_exceptions {
                    match negotiate_exceptions(&mut conn).await {
                        Ok(supported) => {
                            if !supported {
                                tracing::warn!(server = %addr, "server rejected OPTION_REQ exceptions; continuing without exception events");
                            }
                            true
                        }
                        Err(e) => {
                            tracing::warn!(server = %addr, error = %e, "failed negotiating OPTION_REQ exceptions");
                            false
                        }
                    }
                } else {
                    true
                };

                if ready {
                    connected.store(true, Ordering::Relaxed);
                    backoff = Duration::from_millis(100);

                    let outcome = actor_loop(&mut conn, &mut cmd_rx).await;
                    connected.store(false, Ordering::Relaxed);

                    if matches!(outcome, ActorOutcome::ClientDropped) {
                        return;
                    }
                }
            }
            Err(e) => {
                tracing::warn!(server = %addr, error = %e, "failed to connect to job server");
            }
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Sends `OPTION_REQ "exceptions"` and consumes its reply before the
/// connection is handed to [`actor_loop`], so the reply never has a chance
/// to be misattributed to an unrelated request via `pending_replies`.
/// Returns whether the server acknowledged the option.
async fn negotiate_exceptions(conn: &mut Connection<BoxedStream>) -> Result<bool> {
    let opt_pkt = Packet::request(
        PacketType::OptionReq,
        vec![Bytes::from_static(b"exceptions")],
    );
    conn.send(opt_pkt).await?;

    match conn.recv().await? {
        Some(packet) if packet.ptype == PacketType::OptionRes => Ok(true),
        Some(packet) if packet.ptype == PacketType::Error => {
            let code = packet.arg_str(0).unwrap_or_default().to_string();
            let text = packet.arg_str(1).unwrap_or_default().to_string();
            tracing::debug!(code, text, "OPTION_REQ exceptions rejected by server");
            Ok(false)
        }
        Some(other) => {
            tracing::debug!(ptype = ?other.ptype, "unexpected reply to OPTION_REQ");
            Ok(false)
        }
        None => Err(GearmanError::ConnectionClosed),
    }
}

/// How often to sweep `job_events` for entries whose `JobEventStream` was
/// dropped without the job ever reaching a terminal state on this
/// connection (e.g. the caller timed out waiting) — otherwise those entries
/// and their senders live for the life of the connection.
const JOB_EVENTS_PRUNE_INTERVAL: Duration = Duration::from_secs(30);

async fn actor_loop(
    conn: &mut Connection<BoxedStream>,
    cmd_rx: &mut mpsc::UnboundedReceiver<ClientCommand>,
) -> ActorOutcome {
    let mut pending_replies: VecDeque<PendingReply> = VecDeque::new();
    let mut job_events: HashMap<String, mpsc::UnboundedSender<JobEvent>> = HashMap::new();
    let mut prune_interval = tokio::time::interval(JOB_EVENTS_PRUNE_INTERVAL);
    prune_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let outcome = loop {
        tokio::select! {
            _ = prune_interval.tick() => {
                job_events.retain(|_, sender| !sender.is_closed());
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    None => break ActorOutcome::ClientDropped,
                    Some(ClientCommand::Submit { packet, reply, events }) => {
                        if let Err(e) = conn.send(packet).await {
                            let _ = reply.send(Err(e));
                            break ActorOutcome::ConnectionLost;
                        }
                        pending_replies.push_back(PendingReply::JobCreated { reply, events });
                    }
                    Some(ClientCommand::Status { packet, reply }) => {
                        if let Err(e) = conn.send(packet).await {
                            let _ = reply.send(Err(e));
                            break ActorOutcome::ConnectionLost;
                        }
                        pending_replies.push_back(PendingReply::Status(reply));
                    }
                    Some(ClientCommand::StatusUnique { packet, reply }) => {
                        if let Err(e) = conn.send(packet).await {
                            let _ = reply.send(Err(e));
                            break ActorOutcome::ConnectionLost;
                        }
                        pending_replies.push_back(PendingReply::StatusUnique(reply));
                    }
                }
            }
            pkt = conn.recv() => {
                match pkt {
                    Ok(Some(packet)) => handle_incoming(packet, &mut pending_replies, &mut job_events),
                    Ok(None) | Err(_) => break ActorOutcome::ConnectionLost,
                }
            }
        }
    };

    fail_all_pending(pending_replies);
    outcome
}

fn fail_all_pending(pending_replies: VecDeque<PendingReply>) {
    for pending in pending_replies {
        match pending {
            PendingReply::JobCreated { reply, .. } => {
                let _ = reply.send(Err(GearmanError::ConnectionClosed));
            }
            PendingReply::Status(reply) | PendingReply::StatusUnique(reply) => {
                let _ = reply.send(Err(GearmanError::ConnectionClosed));
            }
        }
    }
    // job_events senders are simply dropped by going out of scope in
    // actor_loop, which closes each JobEventStream for its caller.
}

fn handle_incoming(
    packet: Packet,
    pending_replies: &mut VecDeque<PendingReply>,
    job_events: &mut HashMap<String, mpsc::UnboundedSender<JobEvent>>,
) {
    match packet.ptype {
        PacketType::JobCreated => match pending_replies.pop_front() {
            Some(PendingReply::JobCreated { reply, events }) => {
                let handle = packet.arg_str(0).unwrap_or_default().to_string();
                if let Some(sender) = events {
                    job_events.insert(handle.clone(), sender);
                }
                let _ = reply.send(Ok(handle));
            }
            _ => {
                tracing::warn!("unexpected JOB_CREATED with no pending submit")
            }
        },
        PacketType::StatusRes => match pending_replies.pop_front() {
            Some(PendingReply::Status(reply)) => {
                let _ = reply.send(Ok(parse_status_res(&packet)));
            }
            _ => {
                tracing::warn!("unexpected STATUS_RES with no pending request")
            }
        },
        PacketType::StatusResUnique => match pending_replies.pop_front() {
            Some(PendingReply::StatusUnique(reply)) => {
                let _ = reply.send(Ok(parse_status_res_unique(&packet)));
            }
            _ => tracing::warn!("unexpected STATUS_RES_UNIQUE with no pending request"),
        },
        PacketType::Error => {
            let code = packet.arg_str(0).unwrap_or_default().to_string();
            let text = packet.arg_str(1).unwrap_or_default().to_string();
            match pending_replies.pop_front() {
                Some(PendingReply::JobCreated { reply, .. }) => {
                    let _ = reply.send(Err(GearmanError::ServerError { code, text }));
                }
                Some(PendingReply::Status(reply)) | Some(PendingReply::StatusUnique(reply)) => {
                    let _ = reply.send(Err(GearmanError::ServerError { code, text }));
                }
                None => tracing::warn!(code, text, "unsolicited ERROR from job server"),
            }
        }
        PacketType::WorkStatus
        | PacketType::WorkComplete
        | PacketType::WorkFail
        | PacketType::WorkData
        | PacketType::WorkWarning
        | PacketType::WorkException => {
            dispatch_work_event(packet, job_events);
        }
        other => {
            tracing::debug!(ptype = ?other, "ignoring unhandled packet on client connection");
        }
    }
}

fn dispatch_work_event(
    packet: Packet,
    job_events: &mut HashMap<String, mpsc::UnboundedSender<JobEvent>>,
) {
    let Some(handle) = packet.arg_str(0).map(str::to_string) else {
        return;
    };
    let terminal = matches!(
        packet.ptype,
        PacketType::WorkComplete | PacketType::WorkFail | PacketType::WorkException
    );

    let event = match packet.ptype {
        PacketType::WorkStatus => {
            let numerator = packet.arg_str(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            let denominator = packet.arg_str(2).and_then(|s| s.parse().ok()).unwrap_or(0);
            JobEvent::Status {
                numerator,
                denominator,
            }
        }
        PacketType::WorkComplete => {
            JobEvent::Complete(packet.args.get(1).cloned().unwrap_or_default())
        }
        PacketType::WorkFail => JobEvent::Fail,
        PacketType::WorkData => JobEvent::Data(packet.args.get(1).cloned().unwrap_or_default()),
        PacketType::WorkWarning => {
            JobEvent::Warning(packet.args.get(1).cloned().unwrap_or_default())
        }
        PacketType::WorkException => {
            JobEvent::Exception(packet.args.get(1).cloned().unwrap_or_default())
        }
        _ => unreachable!("filtered by caller"),
    };

    if let Some(sender) = job_events.get(&handle) {
        let _ = sender.send(event);
    }
    if terminal {
        job_events.remove(&handle);
    }
}

fn parse_status_res(packet: &Packet) -> JobStatus {
    JobStatus {
        known: packet.arg_str(1) == Some("1"),
        running: packet.arg_str(2) == Some("1"),
        numerator: packet.arg_str(3).and_then(|s| s.parse().ok()).unwrap_or(0),
        denominator: packet.arg_str(4).and_then(|s| s.parse().ok()).unwrap_or(0),
        client_count: None,
    }
}

fn parse_status_res_unique(packet: &Packet) -> JobStatus {
    JobStatus {
        known: packet.arg_str(1) == Some("1"),
        running: packet.arg_str(2) == Some("1"),
        numerator: packet.arg_str(3).and_then(|s| s.parse().ok()).unwrap_or(0),
        denominator: packet.arg_str(4).and_then(|s| s.parse().ok()).unwrap_or(0),
        client_count: packet.arg_str(5).and_then(|s| s.parse().ok()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unit-tests the exact `retain` call `actor_loop`'s periodic sweep
    /// runs (see `JOB_EVENTS_PRUNE_INTERVAL`), in isolation from the timer
    /// that drives it — waiting out the real 30s interval in an integration
    /// test would be both slow and not actually exercise different logic
    /// than this. A caller dropping its `JobEventStream` early (e.g. via a
    /// timeout) before the job reaches a terminal state closes the
    /// channel's receiver; the sender must be pruned on the next sweep
    /// rather than living for the rest of the connection.
    #[test]
    fn closed_job_event_senders_get_pruned() {
        let mut job_events: HashMap<String, mpsc::UnboundedSender<JobEvent>> = HashMap::new();

        let (still_open_tx, _still_open_rx) = mpsc::unbounded_channel();
        job_events.insert("H:open:1".to_string(), still_open_tx);

        let (dropped_tx, dropped_rx) = mpsc::unbounded_channel();
        job_events.insert("H:dropped:1".to_string(), dropped_tx);
        drop(dropped_rx); // simulates an early-dropped JobEventStream

        job_events.retain(|_, sender| !sender.is_closed());

        assert!(job_events.contains_key("H:open:1"));
        assert!(!job_events.contains_key("H:dropped:1"));
    }
}
