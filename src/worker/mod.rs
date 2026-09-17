mod job;
mod registry;

pub use job::{JobHandler, Reporter, WorkError, WorkerJob};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::watch;
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
        let registry = Arc::new(self.registry);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();

        for addr in &self.servers {
            for _ in 0..self.concurrency {
                tasks.spawn(run_worker_connection(
                    addr.clone(),
                    registry.clone(),
                    self.grab_mode,
                    shutdown_rx.clone(),
                    self.transport.clone(),
                ));
            }
        }

        Worker { shutdown_tx, tasks }
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
}

impl Worker {
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::new()
    }

    /// Signals every grab-loop to stop once its current job (if any)
    /// finishes, without grabbing another, and waits for them all to exit.
    /// There is no mid-job cancellation: Gearman has no wire message for a
    /// worker to abort work it already accepted.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        while self.tasks.join_next().await.is_some() {}
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
    registry: Arc<Registry>,
    grab_mode: GrabMode,
    mut shutdown_rx: watch::Receiver<bool>,
    transport: Transport,
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
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        if let Err(e) = register_functions(&mut conn, &registry).await {
            tracing::warn!(server = %addr, error = %e, "failed to register functions");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
            continue;
        }
        backoff = Duration::from_millis(100);

        let conn: SharedConnection = Arc::new(tokio::sync::Mutex::new(conn));
        match run_grab_loop(&conn, &registry, grab_mode, &mut shutdown_rx).await
        {
            LoopOutcome::ShuttingDown => return,
            LoopOutcome::ConnectionLost => {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

async fn register_functions(
    conn: &mut Connection<BoxedStream>,
    registry: &Registry,
) -> Result<()> {
    for (name, reg) in registry.iter() {
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
    registry: &Arc<Registry>,
    grab_mode: GrabMode,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> LoopOutcome {
    loop {
        if *shutdown_rx.borrow() {
            return LoopOutcome::ShuttingDown;
        }

        let grab_type = match grab_mode {
            GrabMode::Uniq => PacketType::GrabJobUniq,
            GrabMode::Plain => PacketType::GrabJob,
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
            PacketType::JobAssign | PacketType::JobAssignUniq => {
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
                            Ok(Some(_)) => continue,
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

async fn dispatch_job(
    conn: &SharedConnection,
    registry: &Arc<Registry>,
    pkt: Packet,
) {
    let handle = pkt.arg_str(0).unwrap_or_default().to_string();
    let function = pkt.arg_str(1).unwrap_or_default().to_string();
    let (unique, payload) = match pkt.ptype {
        PacketType::JobAssignUniq => (
            pkt.arg_str(2).map(str::to_string),
            pkt.args.get(3).cloned().unwrap_or_default(),
        ),
        _ => (None, pkt.args.get(2).cloned().unwrap_or_default()),
    };

    let Some(reg) = registry.get(&function) else {
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
        payload,
        reporter,
    };

    let response = match reg.handler.run(job).await {
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
