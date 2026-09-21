use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::net::BoxedStream;
use crate::protocol::{Packet, PacketType};
use crate::Connection;

pub(crate) type SharedConnection = Arc<Mutex<Connection<BoxedStream>>>;

/// Why a handler failed. The wire protocol only lets `WORK_FAIL` carry a
/// handle, no message — a description of the failure can only reach the
/// client via `WORK_EXCEPTION`, and then only if the client opted in with
/// `ClientBuilder::with_exceptions`.
#[derive(Debug, Clone)]
pub enum WorkError {
    /// The job failed with no further detail (`WORK_FAIL`).
    Fail,
    /// The job failed with an exception payload (`WORK_EXCEPTION`), only
    /// delivered to clients that opted in with
    /// `ClientBuilder::with_exceptions`.
    Exception(Bytes),
}

/// A job assigned to a worker, passed to its [`JobHandler`].
pub struct WorkerJob {
    /// The job's handle.
    pub handle: String,
    /// The function name this job was submitted for.
    pub function: String,
    /// The caller-supplied unique id, if any (present when grabbed with
    /// `GrabMode::Uniq` or `GrabMode::All`).
    pub unique: Option<String>,
    /// Set only when grabbed via `GrabMode::All` for a job submitted
    /// through `Client::submit_reduce_job` with a reducer name. gearmand
    /// itself doesn't interpret this — it's an opaque passthrough field the
    /// handler can use however it wants (e.g. to select a reduction
    /// strategy).
    pub reducer: Option<String>,
    /// The job's workload payload.
    pub payload: Bytes,
    /// Lets the handler report progress while it runs.
    pub reporter: Reporter,
}

/// Lets a handler report progress while it runs. Safe to call concurrently
/// with nothing else touching the connection: each worker connection is
/// owned by exactly one grab-loop task, which is suspended awaiting the
/// handler for the whole time a `Reporter` might be used.
#[derive(Clone)]
pub struct Reporter {
    pub(crate) handle: String,
    pub(crate) conn: SharedConnection,
}

impl Reporter {
    /// Sends a `WORK_STATUS` progress update.
    pub async fn report_status(&self, numerator: u64, denominator: u64) -> Result<()> {
        let packet = Packet::request(
            PacketType::WorkStatus,
            vec![
                Bytes::copy_from_slice(self.handle.as_bytes()),
                Bytes::from(numerator.to_string()),
                Bytes::from(denominator.to_string()),
            ],
        );
        self.conn.lock().await.send(packet).await
    }

    /// Sends a `WORK_DATA` partial-result chunk.
    pub async fn send_data(&self, data: impl Into<Bytes>) -> Result<()> {
        let packet = Packet::request(
            PacketType::WorkData,
            vec![Bytes::copy_from_slice(self.handle.as_bytes()), data.into()],
        );
        self.conn.lock().await.send(packet).await
    }

    /// Sends a `WORK_WARNING` message.
    pub async fn warn(&self, data: impl Into<Bytes>) -> Result<()> {
        let packet = Packet::request(
            PacketType::WorkWarning,
            vec![Bytes::copy_from_slice(self.handle.as_bytes()), data.into()],
        );
        self.conn.lock().await.send(packet).await
    }
}

/// A handler for jobs submitted to a registered function. Implemented for
/// any `Fn(WorkerJob) -> impl Future<Output = Result<Bytes, WorkError>>`, so
/// an async closure or function usually suffices without implementing this
/// trait directly.
#[async_trait]
pub trait JobHandler: Send + Sync + 'static {
    /// Runs the job, returning its result payload or a [`WorkError`].
    async fn run(&self, job: WorkerJob) -> std::result::Result<Bytes, WorkError>;
}

#[async_trait]
impl<F, Fut> JobHandler for F
where
    F: Fn(WorkerJob) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<Bytes, WorkError>> + Send + 'static,
{
    async fn run(&self, job: WorkerJob) -> std::result::Result<Bytes, WorkError> {
        self(job).await
    }
}
