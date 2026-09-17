mod job;
mod multi;

pub use job::{
    JobEvent, JobEventStream, JobHandle, JobStatus, Priority, SubmitOptions,
    SubmittedJob,
};

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};

use crate::error::{GearmanError, Result};
use crate::protocol::{Packet, PacketType};
use crate::Connection;

use multi::{ServerPool, ServerSlot};

pub struct ClientBuilder {
    servers: Vec<String>,
    with_exceptions: bool,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            servers: Vec::new(),
            with_exceptions: false,
        }
    }

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

#[derive(Clone)]
pub struct Client {
    pool: Arc<ServerPool>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub async fn submit(
        &self,
        function: &str,
        unique: Option<&str>,
        payload: impl Into<Bytes>,
        opts: SubmitOptions,
    ) -> Result<SubmittedJob> {
        let server_index = self
            .pool
            .next_healthy()
            .ok_or(GearmanError::NoServersAvailable)?;
        let slot = self.pool.get(server_index);

        let ptype = submit_packet_type(opts.priority, opts.background);
        let args = vec![
            Bytes::copy_from_slice(function.as_bytes()),
            Bytes::from(unique.unwrap_or("").to_string()),
            payload.into(),
        ];
        let packet = Packet::request(ptype, args);

        let (reply_tx, reply_rx) = oneshot::channel();
        let (events_tx, events_rx) = if opts.background {
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
    pub async fn submit_fg(
        &self,
        function: &str,
        payload: impl Into<Bytes>,
    ) -> Result<Bytes> {
        self.submit_fg_unique(function, None, payload).await
    }

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

    pub async fn submit_bg(
        &self,
        function: &str,
        payload: impl Into<Bytes>,
    ) -> Result<JobHandle> {
        let submitted = self
            .submit(function, None, payload, SubmitOptions::background())
            .await?;
        Ok(submitted.handle)
    }

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

    pub async fn get_status_unique(
        &self,
        handle: &JobHandle,
    ) -> Result<JobStatus> {
        let unique =
            handle
                .unique
                .clone()
                .ok_or_else(|| GearmanError::NoUniqueId {
                    handle: handle.handle.clone(),
                })?;
        let slot = self.pool.get(handle.server_index);
        let packet = Packet::request(
            PacketType::GetStatusUnique,
            vec![Bytes::from(unique)],
        );
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
) {
    let mut backoff = Duration::from_millis(100);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    loop {
        if let Ok(mut conn) = Connection::connect(&addr).await {
            let ready = if with_exceptions {
                let opt_pkt = Packet::request(
                    PacketType::OptionReq,
                    vec![Bytes::from_static(b"exceptions")],
                );
                conn.send(opt_pkt).await.is_ok()
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

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

async fn actor_loop(
    conn: &mut Connection,
    cmd_rx: &mut mpsc::UnboundedReceiver<ClientCommand>,
) -> ActorOutcome {
    let mut pending_replies: VecDeque<PendingReply> = VecDeque::new();
    let mut job_events: HashMap<String, mpsc::UnboundedSender<JobEvent>> =
        HashMap::new();

    let outcome = loop {
        tokio::select! {
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
            _ => tracing::warn!(
                "unexpected STATUS_RES_UNIQUE with no pending request"
            ),
        },
        PacketType::Error => {
            let code = packet.arg_str(0).unwrap_or_default().to_string();
            let text = packet.arg_str(1).unwrap_or_default().to_string();
            match pending_replies.pop_front() {
                Some(PendingReply::JobCreated { reply, .. }) => {
                    let _ = reply
                        .send(Err(GearmanError::ServerError { code, text }));
                }
                Some(PendingReply::Status(reply))
                | Some(PendingReply::StatusUnique(reply)) => {
                    let _ = reply
                        .send(Err(GearmanError::ServerError { code, text }));
                }
                None => tracing::warn!(
                    code,
                    text,
                    "unsolicited ERROR from job server"
                ),
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
        PacketType::WorkComplete
            | PacketType::WorkFail
            | PacketType::WorkException
    );

    let event = match packet.ptype {
        PacketType::WorkStatus => {
            let numerator =
                packet.arg_str(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            let denominator =
                packet.arg_str(2).and_then(|s| s.parse().ok()).unwrap_or(0);
            JobEvent::Status {
                numerator,
                denominator,
            }
        }
        PacketType::WorkComplete => {
            JobEvent::Complete(packet.args.get(1).cloned().unwrap_or_default())
        }
        PacketType::WorkFail => JobEvent::Fail,
        PacketType::WorkData => {
            JobEvent::Data(packet.args.get(1).cloned().unwrap_or_default())
        }
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
        denominator: packet
            .arg_str(4)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        client_count: None,
    }
}

fn parse_status_res_unique(packet: &Packet) -> JobStatus {
    JobStatus {
        known: packet.arg_str(1) == Some("1"),
        running: packet.arg_str(2) == Some("1"),
        numerator: packet.arg_str(3).and_then(|s| s.parse().ok()).unwrap_or(0),
        denominator: packet
            .arg_str(4)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        client_count: packet.arg_str(5).and_then(|s| s.parse().ok()),
    }
}
