use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::protocol::{Packet, PacketType};
use crate::Connection;

pub(crate) type SharedConnection = Arc<Mutex<Connection<TcpStream>>>;

/// Why a handler failed. The wire protocol only lets `WORK_FAIL` carry a
/// handle, no message — a description of the failure can only reach the
/// client via `WORK_EXCEPTION`, and then only if the client opted in with
/// `ClientBuilder::with_exceptions`.
#[derive(Debug, Clone)]
pub enum WorkError {
    Fail,
    Exception(Bytes),
}

pub struct WorkerJob {
    pub handle: String,
    pub function: String,
    pub unique: Option<String>,
    pub payload: Bytes,
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
    pub async fn report_status(
        &self,
        numerator: u64,
        denominator: u64,
    ) -> Result<()> {
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

    pub async fn send_data(&self, data: impl Into<Bytes>) -> Result<()> {
        let packet = Packet::request(
            PacketType::WorkData,
            vec![Bytes::copy_from_slice(self.handle.as_bytes()), data.into()],
        );
        self.conn.lock().await.send(packet).await
    }

    pub async fn warn(&self, data: impl Into<Bytes>) -> Result<()> {
        let packet = Packet::request(
            PacketType::WorkWarning,
            vec![Bytes::copy_from_slice(self.handle.as_bytes()), data.into()],
        );
        self.conn.lock().await.send(packet).await
    }
}

#[async_trait]
pub trait JobHandler: Send + Sync + 'static {
    async fn run(
        &self,
        job: WorkerJob,
    ) -> std::result::Result<Bytes, WorkError>;
}

#[async_trait]
impl<F, Fut> JobHandler for F
where
    F: Fn(WorkerJob) -> Fut + Send + Sync + 'static,
    Fut:
        Future<Output = std::result::Result<Bytes, WorkError>> + Send + 'static,
{
    async fn run(
        &self,
        job: WorkerJob,
    ) -> std::result::Result<Bytes, WorkError> {
        self(job).await
    }
}
