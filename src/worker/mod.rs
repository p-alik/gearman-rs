mod job;
mod registry;

pub use job::{JobHandler, Reporter, WorkError, WorkerJob};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{broadcast, watch, RwLock};
use tokio::task::JoinSet;

use crate::error::Result;
use crate::net::{BoxedStream, Transport};
use crate::protocol::{Packet, PacketType};
use crate::Connection;

use job::SharedConnection;
use registry::{RegisteredHandler, Registry};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrabMode {
    /// `GRAB_JOB_UNIQ` (the default): the handler also receives the
    /// caller-supplied unique id.
    Uniq,
    /// `GRAB_JOB`: no unique id, for libgearman-parity testing.
    Plain,
    /// `GRAB_JOB_ALL`: like `Uniq`, but also receives the reducer name for
    /// jobs submitted via `submit_reduce_job`. The server replies
    /// `JOB_ASSIGN_ALL` for jobs that have a reducer and plain
    /// `JOB_ASSIGN_UNIQ` for ones that don't — both are handled the same
    /// way here, just with `WorkerJob::reducer` set or `None`.
    All,
}

pub struct WorkerBuilder {
    servers: Vec<String>,
    concurrency: usize,
    grab_mode: GrabMode,
    registry: Registry,
    transport: Transport,
}

impl WorkerBuilder {
    pub fn new() -> Self {
        Self {
            servers: Vec::new(),
            concurrency: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            grab_mode: GrabMode::Uniq,
            registry: HashMap::new(),
            transport: Transport::default(),
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

    /// Number of grab-loop tasks (and connections) run per server. Gearman
    /// allows only one job in flight per `GRAB_JOB` cycle on a connection,
    /// so this — not pipelining — is how concurrency is achieved. Defaults
    /// to the available parallelism.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    pub fn grab_mode(mut self, mode: GrabMode) -> Self {
        self.grab_mode = mode;
        self
    }

    /// Connect to every server over TLS instead of plain TCP. gearmand
    /// wraps the raw stream in TLS before any Gearman framing begins.
    #[cfg(feature = "tls")]
    pub fn tls(mut self, config: crate::tls::TlsConfig) -> Self {
        self.transport = Transport::Tls(config);
        self
    }

    pub fn register<H>(
        mut self,
        function: impl Into<String>,
        handler: H,
    ) -> Self
    where
        H: JobHandler,
    {
        self.registry.insert(
            function.into(),
            RegisteredHandler {
                handler: Arc::new(handler),
                timeout: None,
            },
        );
        self
    }

    pub fn register_with_timeout<H>(
        mut self,
        function: impl Into<String>,
        handler: H,
        timeout: Duration,
    ) -> Self
    where
        H: JobHandler,
    {
        self.registry.insert(
            function.into(),
            RegisteredHandler {
                handler: Arc::new(handler),
                timeout: Some(timeout),
            },
        );
        self
    }

    pub fn run(self) -> Worker {
        let registry = Arc::new(RwLock::new(self.registry));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (unregister_tx, _) = broadcast::channel(16);
        let mut tasks = JoinSet::new();

        for addr in &self.servers {
            for _ in 0..self.concurrency {
                tasks.spawn(run_worker_connection(
                    addr.clone(),
                    registry.clone(),
                    self.grab_mode,
                    shutdown_rx.clone(),
                    self.transport.clone(),
                    unregister_tx.subscribe(),
                ));
            }
        }

        Worker {
            shutdown_tx,
            tasks,
            registry,
            unregister_tx,
        }
    }
}

impl Default for WorkerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Worker {
    shutdown_tx: watch::Sender<bool>,
    tasks: JoinSet<()>,
    registry: Arc<RwLock<Registry>>,
    unregister_tx: broadcast::Sender<String>,
}

impl Worker {
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::new()
    }

    /// Stops advertising `function` to every connected server: removes it
    /// from the shared registry (so reconnects stop re-sending its
    /// `CAN_DO`, and any job for it grabbed after this call is failed back
    /// as unregistered) and notifies every live grab-loop connection to
    /// send `CANT_DO`. A no-op if `function` was never registered.
    pub async fn unregister(&self, function: &str) {
        self.registry.write().await.remove(function);
        let _ = self.unregister_tx.send(function.to_string());
    }

    /// Signals every grab-loop to stop once its current job (if any)
    /// finishes, without grabbing another, and waits for them all to exit.
    /// There is no mid-job cancellation: Gearman has no wire message for a
    /// worker to abort work it already accepted.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        while let Some(result) = self.tasks.join_next().await {
            if let Err(e) = result {
                tracing::error!(error = %e, "worker grab-loop task ended abnormally");
            }
        }
    }
}

enum LoopOutcome {
    ConnectionLost,
    ShuttingDown,
}

/// Owns one worker connection for its whole lifetime: (re)connects with
/// exponential backoff, re-registers every function on each new connection,
/// then runs the grab loop until the connection drops or shutdown fires.
async fn run_worker_connection(
    addr: String,
    registry: Arc<RwLock<Registry>>,
    grab_mode: GrabMode,
    mut shutdown_rx: watch::Receiver<bool>,
    transport: Transport,
    mut unregister_rx: broadcast::Receiver<String>,
) {
    let mut backoff = Duration::from_millis(100);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    loop {
        if *shutdown_rx.borrow() {
            return;
        }

        let mut conn = match crate::net::connect(&addr, &transport).await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!(server = %addr, error = %e, "failed to connect to job server");
                if interruptible_backoff(backoff, &mut shutdown_rx).await {
                    return;
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        if let Err(e) = register_functions(&mut conn, &registry).await {
            tracing::warn!(server = %addr, error = %e, "failed to register functions");
            if interruptible_backoff(backoff, &mut shutdown_rx).await {
                return;
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
            continue;
        }
        backoff = Duration::from_millis(100);

        // Discard any unregister notifications queued before this
        // connection registered its functions — it never advertised them,
        // so there's nothing to send CANT_DO for.
        while unregister_rx.try_recv().is_ok() {}

        let conn: SharedConnection = Arc::new(tokio::sync::Mutex::new(conn));
        match run_grab_loop(
            &conn,
            &registry,
            grab_mode,
            &mut shutdown_rx,
            &mut unregister_rx,
        )
        .await
        {
            LoopOutcome::ShuttingDown => return,
            LoopOutcome::ConnectionLost => {
                if interruptible_backoff(backoff, &mut shutdown_rx).await {
                    return;
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// Sleeps for `duration`, interruptible by shutdown. Returns `true` if
/// shutdown fired before the sleep elapsed, in which case the caller should
/// stop rather than continue reconnecting.
async fn interruptible_backoff(
    duration: Duration,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        _ = shutdown_rx.changed() => true,
    }
}

async fn register_functions(
    conn: &mut Connection<BoxedStream>,
    registry: &RwLock<Registry>,
) -> Result<()> {
    for (name, reg) in registry.read().await.iter() {
        let packet = match reg.timeout {
            Some(timeout) => Packet::request(
                PacketType::CanDoTimeout,
                vec![
                    Bytes::copy_from_slice(name.as_bytes()),
                    Bytes::from(timeout.as_secs().to_string()),
                ],
            ),
            None => Packet::request(
                PacketType::CanDo,
                vec![Bytes::copy_from_slice(name.as_bytes())],
            ),
        };
        conn.send(packet).await?;
    }
    Ok(())
}

async fn send_packet(conn: &SharedConnection, packet: Packet) -> Result<()> {
    conn.lock().await.send(packet).await
}

async fn recv_packet(conn: &SharedConnection) -> Result<Option<Packet>> {
    conn.lock().await.recv().await
}

async fn run_grab_loop(
    conn: &SharedConnection,
    registry: &Arc<RwLock<Registry>>,
    grab_mode: GrabMode,
    shutdown_rx: &mut watch::Receiver<bool>,
    unregister_rx: &mut broadcast::Receiver<String>,
) -> LoopOutcome {
    loop {
        if *shutdown_rx.borrow() {
            return LoopOutcome::ShuttingDown;
        }

        if !flush_unregistrations(conn, unregister_rx).await {
            return LoopOutcome::ConnectionLost;
        }

        let grab_type = match grab_mode {
            GrabMode::Uniq => PacketType::GrabJobUniq,
            GrabMode::Plain => PacketType::GrabJob,
            GrabMode::All => PacketType::GrabJobAll,
        };
        if send_packet(conn, Packet::request(grab_type, vec![]))
            .await
            .is_err()
        {
            return LoopOutcome::ConnectionLost;
        }

        let response = match recv_packet(conn).await {
            Ok(Some(pkt)) => pkt,
            Ok(None) | Err(_) => return LoopOutcome::ConnectionLost,
        };

        match response.ptype {
            PacketType::JobAssign
            | PacketType::JobAssignUniq
            | PacketType::JobAssignAll => {
                dispatch_job(conn, registry, response).await;
            }
            PacketType::NoJob => {
                if send_packet(
                    conn,
                    Packet::request(PacketType::PreSleep, vec![]),
                )
                .await
                .is_err()
                {
                    return LoopOutcome::ConnectionLost;
                }
                // The server wakes exactly one sleeping worker per NOOP; we
                // just need to be interruptible by shutdown while waiting.
                tokio::select! {
                    result = recv_packet(conn) => {
                        match result {
                            Ok(Some(pkt)) if pkt.ptype == PacketType::Noop => continue,
                            Ok(Some(pkt)) => {
                                tracing::warn!(ptype = ?pkt.ptype, "expected NOOP wake-up, got unexpected packet while sleeping");
                                continue;
                            }
                            Ok(None) | Err(_) => return LoopOutcome::ConnectionLost,
                        }
                    }
                    _ = shutdown_rx.changed() => return LoopOutcome::ShuttingDown,
                }
            }
            other => {
                tracing::debug!(ptype = ?other, "ignoring unexpected packet in worker grab loop");
            }
        }
    }
}

/// Non-blocking drain of any `Worker::unregister` notifications queued
/// since the last check, sending `CANT_DO` for each. Returns `false` if the
/// connection died while sending.
async fn flush_unregistrations(
    conn: &SharedConnection,
    unregister_rx: &mut broadcast::Receiver<String>,
) -> bool {
    loop {
        match unregister_rx.try_recv() {
            Ok(function) => {
                let packet = Packet::request(
                    PacketType::CantDo,
                    vec![Bytes::copy_from_slice(function.as_bytes())],
                );
                if send_packet(conn, packet).await.is_err() {
                    return false;
                }
            }
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(_) => return true,
        }
    }
}

async fn dispatch_job(
    conn: &SharedConnection,
    registry: &Arc<RwLock<Registry>>,
    pkt: Packet,
) {
    let handle = pkt.arg_str(0).unwrap_or_default().to_string();
    let function = pkt.arg_str(1).unwrap_or_default().to_string();
    let (unique, reducer, payload) = match pkt.ptype {
        PacketType::JobAssignAll => (
            pkt.arg_str(2).map(str::to_string),
            pkt.arg_str(3).map(str::to_string),
            pkt.args.get(4).cloned().unwrap_or_default(),
        ),
        PacketType::JobAssignUniq => (
            pkt.arg_str(2).map(str::to_string),
            None,
            pkt.args.get(3).cloned().unwrap_or_default(),
        ),
        _ => (None, None, pkt.args.get(2).cloned().unwrap_or_default()),
    };

    let handler = registry
        .read()
        .await
        .get(&function)
        .map(|reg| reg.handler.clone());
    let Some(handler) = handler else {
        tracing::warn!(function, "grabbed a job for an unregistered function");
        let _ = send_packet(
            conn,
            Packet::request(
                PacketType::WorkFail,
                vec![Bytes::copy_from_slice(handle.as_bytes())],
            ),
        )
        .await;
        return;
    };

    let reporter = Reporter {
        handle: handle.clone(),
        conn: conn.clone(),
    };
    let job = WorkerJob {
        handle: handle.clone(),
        function,
        unique,
        reducer,
        payload,
        reporter,
    };

    let response = match handler.run(job).await {
        Ok(payload) => Packet::request(
            PacketType::WorkComplete,
            vec![Bytes::copy_from_slice(handle.as_bytes()), payload],
        ),
        Err(WorkError::Fail) => Packet::request(
            PacketType::WorkFail,
            vec![Bytes::copy_from_slice(handle.as_bytes())],
        ),
        Err(WorkError::Exception(payload)) => Packet::request(
            PacketType::WorkException,
            vec![Bytes::copy_from_slice(handle.as_bytes()), payload],
        ),
    };
    let _ = send_packet(conn, response).await;
}
