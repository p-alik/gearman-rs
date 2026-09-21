//! The crate's error and result types.

use bytes::Bytes;

use crate::protocol::PacketType;

/// The error type for all fallible operations in this crate.
#[derive(Debug, thiserror::Error)]
pub enum GearmanError {
    /// An underlying I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// A packet header's magic bytes were neither `\0REQ` nor `\0RES`.
    #[error("bad magic bytes: {0:?}")]
    BadMagic([u8; 4]),

    /// A packet header named a packet type number this crate doesn't
    /// recognize.
    #[error("unknown packet type: {0}")]
    UnknownPacketType(u32),

    /// A packet header declared a body larger than the codec's configured
    /// maximum.
    #[error("payload too large: {0} bytes")]
    PayloadTooLarge(u32),

    /// A packet's body didn't match the argument layout expected for its
    /// type.
    #[error("malformed packet body for {ptype:?}: {reason}")]
    MalformedBody {
        /// The packet type whose body was malformed.
        ptype: PacketType,
        /// A short, static description of what was wrong.
        reason: &'static str,
    },

    /// A packet had a different number of arguments than its type expects.
    #[error("wrong argument count for {ptype:?}: expected {expected}, got {actual}")]
    WrongArgCount {
        /// The packet type whose argument count was wrong.
        ptype: PacketType,
        /// The expected number of arguments.
        expected: usize,
        /// The actual number of arguments received.
        actual: usize,
    },

    /// The job server replied with an `ERROR` packet or admin-protocol
    /// `ERR` line.
    #[error("job server returned error {code}: {text}")]
    ServerError {
        /// The server's error code.
        code: String,
        /// The server's error message text.
        text: String,
    },

    /// A job terminated with `WORK_FAIL`.
    #[error("job {handle} failed")]
    JobFailed {
        /// The failed job's handle.
        handle: String,
    },

    /// A job terminated with `WORK_EXCEPTION`.
    #[error("job {handle} raised an exception")]
    JobException {
        /// The job's handle.
        handle: String,
        /// The exception payload sent by the worker.
        payload: Bytes,
    },

    /// No configured job server is currently reachable.
    #[error("no job servers available")]
    NoServersAvailable,

    /// `get_status_unique` was called on a job that was submitted without a
    /// unique id.
    #[error("job {handle} has no recorded unique id; submit with one to use get_status_unique")]
    NoUniqueId {
        /// The job's handle.
        handle: String,
    },

    /// The peer closed the connection.
    #[error("connection closed by peer")]
    ConnectionClosed,

    /// An admin-protocol response line couldn't be parsed.
    #[error("unparseable admin protocol response line: {line:?}")]
    AdminProtocolError {
        /// The offending line.
        line: String,
    },

    /// An admin-protocol response line exceeded the maximum length without
    /// a terminating newline.
    #[error("admin protocol line exceeded {0} bytes without a terminator")]
    AdminLineTooLong(usize),

    /// A TLS server name couldn't be parsed as a valid DNS name or IP
    /// address.
    #[error("invalid TLS server name: {host:?}")]
    InvalidServerName {
        /// The offending hostname.
        host: String,
    },

    /// An operation exceeded its configured timeout.
    #[error("operation timed out")]
    Timeout,
}

/// A specialized [`std::result::Result`] using [`GearmanError`].
pub type Result<T> = std::result::Result<T, GearmanError>;
