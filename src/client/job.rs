use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    High,
    Normal,
    Low,
}

#[derive(Debug, Clone, Copy)]
pub struct SubmitOptions {
    pub priority: Priority,
    pub background: bool,
}

impl Default for SubmitOptions {
    fn default() -> Self {
        Self {
            priority: Priority::Normal,
            background: false,
        }
    }
}

impl SubmitOptions {
    pub fn foreground() -> Self {
        Self::default()
    }

    pub fn background() -> Self {
        Self {
            background: true,
            ..Self::default()
        }
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }
}

/// A job handle as returned by the job server, plus the routing information
/// (which server issued it) needed to send follow-up `GET_STATUS` requests.
/// Handles are server-local: `H:lap:1` from one job server means nothing to
/// another, so `get_status`/`get_status_unique` must go back to the same
/// server that created the job.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobHandle {
    pub(crate) server_index: usize,
    pub(crate) handle: String,
    pub(crate) unique: Option<String>,
}

impl JobHandle {
    pub fn as_str(&self) -> &str {
        &self.handle
    }
}

impl std::fmt::Display for JobHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.handle)
    }
}

/// One `WORK_*` event forwarded by the job server for a foreground job.
/// `Complete`/`Fail`/`Exception` are terminal; `Status`/`Data`/`Warning` may
/// arrive any number of times before a terminal event.
#[derive(Debug, Clone)]
pub enum JobEvent {
    Status { numerator: u64, denominator: u64 },
    Data(Bytes),
    Warning(Bytes),
    Complete(Bytes),
    Fail,
    Exception(Bytes),
}

pub struct JobEventStream {
    pub(crate) rx: mpsc::UnboundedReceiver<JobEvent>,
}

impl Stream for JobEventStream {
    type Item = JobEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<JobEvent>> {
        self.get_mut().rx.poll_recv(cx)
    }
}

pub struct SubmittedJob {
    pub handle: JobHandle,
    /// `Some` for foreground submissions (the caller can observe `WORK_*`
    /// events); `None` for background submissions, which never get any.
    pub events: Option<JobEventStream>,
}

#[derive(Debug, Clone, Default)]
pub struct JobStatus {
    pub known: bool,
    pub running: bool,
    pub numerator: u64,
    pub denominator: u64,
    /// Only populated by `get_status_unique` (`STATUS_RES_UNIQUE`'s trailing
    /// `CLIENT_COUNT` field); `None` for plain `get_status`.
    pub client_count: Option<u32>,
}
