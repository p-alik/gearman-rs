use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use tokio::sync::mpsc;

/// Job priority, mapped to the `SUBMIT_JOB_HIGH`/`SUBMIT_JOB`/`SUBMIT_JOB_LOW`
/// (and their background counterparts) packet types on submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// Processed ahead of normal- and low-priority jobs.
    High,
    /// The default priority.
    Normal,
    /// Processed after normal- and high-priority jobs.
    Low,
}

/// Options controlling how a job is submitted: priority and whether it runs
/// in the background (fire-and-forget, no `WORK_*` events).
#[derive(Debug, Clone, Copy)]
pub struct SubmitOptions {
    /// The job's priority.
    pub priority: Priority,
    /// Whether the job is submitted in the background.
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
    /// Normal-priority foreground submission (the default).
    pub fn foreground() -> Self {
        Self::default()
    }

    /// Normal-priority background submission.
    pub fn background() -> Self {
        Self {
            background: true,
            ..Self::default()
        }
    }

    /// Overrides the priority, keeping the current foreground/background
    /// setting.
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
    /// The raw handle string as issued by the job server (e.g. `H:lap:1`).
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
    /// A `WORK_STATUS` progress update.
    Status {
        /// The numerator of the progress fraction.
        numerator: u64,
        /// The denominator of the progress fraction.
        denominator: u64,
    },
    /// A `WORK_DATA` partial-result chunk.
    Data(Bytes),
    /// A `WORK_WARNING` message.
    Warning(Bytes),
    /// A `WORK_COMPLETE` terminal event carrying the job's result.
    Complete(Bytes),
    /// A `WORK_FAIL` terminal event.
    Fail,
    /// A `WORK_EXCEPTION` terminal event carrying the exception payload.
    Exception(Bytes),
}

/// A [`Stream`] of [`JobEvent`]s for a foreground job submission.
pub struct JobEventStream {
    pub(crate) rx: mpsc::UnboundedReceiver<JobEvent>,
}

impl Stream for JobEventStream {
    type Item = JobEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<JobEvent>> {
        self.get_mut().rx.poll_recv(cx)
    }
}

/// The result of submitting a job: its handle plus, for foreground
/// submissions, a stream of `WORK_*` events.
pub struct SubmittedJob {
    /// The job's handle, used for follow-up `GET_STATUS` requests.
    pub handle: JobHandle,
    /// `Some` for foreground submissions (the caller can observe `WORK_*`
    /// events); `None` for background submissions, which never get any.
    pub events: Option<JobEventStream>,
}

/// A `GET_STATUS`/`GET_STATUS_UNIQUE` response.
#[derive(Debug, Clone, Default)]
pub struct JobStatus {
    /// Whether the job server recognizes this handle.
    pub known: bool,
    /// Whether the job is currently running.
    pub running: bool,
    /// The numerator of the progress fraction.
    pub numerator: u64,
    /// The denominator of the progress fraction.
    pub denominator: u64,
    /// Only populated by `get_status_unique` (`STATUS_RES_UNIQUE`'s trailing
    /// `CLIENT_COUNT` field); `None` for plain `get_status`.
    pub client_count: Option<u32>,
}
