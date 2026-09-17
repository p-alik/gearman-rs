use bytes::Bytes;

use crate::protocol::PacketType;

#[derive(Debug, thiserror::Error)]
pub enum GearmanError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("bad magic bytes: {0:?}")]
    BadMagic([u8; 4]),

    #[error("unknown packet type: {0}")]
    UnknownPacketType(u32),

    #[error("payload too large: {0} bytes")]
    PayloadTooLarge(u32),

    #[error("malformed packet body for {ptype:?}: {reason}")]
    MalformedBody {
        ptype: PacketType,
        reason: &'static str,
    },

    #[error("job server returned error {code}: {text}")]
    ServerError { code: String, text: String },

    #[error("job {handle} failed")]
    JobFailed { handle: String },

    #[error("job {handle} raised an exception")]
    JobException { handle: String, payload: Bytes },

    #[error("no job servers available")]
    NoServersAvailable,

    #[error("job {handle} has no recorded unique id; submit with one to use get_status_unique")]
    NoUniqueId { handle: String },

    #[error("connection closed by peer")]
    ConnectionClosed,

    #[error("operation timed out")]
    Timeout,
}

pub type Result<T> = std::result::Result<T, GearmanError>;
