use crate::protocol::PacketType;

/// The 4-byte magic prefix for request packets: `\0REQ`.
pub const REQ_MAGIC: [u8; 4] = *b"\0REQ";
/// The 4-byte magic prefix for response packets: `\0RES`.
pub const RES_MAGIC: [u8; 4] = *b"\0RES";

/// Size in bytes of a packet header: 4 (magic) + 4 (type) + 4 (payload length).
pub const HEADER_LEN: usize = 12;

/// Which of the two magic prefixes a packet header carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketMagic {
    /// `\0REQ` — sent client->server or worker->server.
    Req,
    /// `\0RES` — sent server->client or server->worker.
    Res,
}

impl PacketMagic {
    /// The 4-byte wire representation of this magic value.
    pub fn bytes(self) -> [u8; 4] {
        match self {
            PacketMagic::Req => REQ_MAGIC,
            PacketMagic::Res => RES_MAGIC,
        }
    }
}

/// A decoded packet header, without its body.
#[derive(Debug, Clone, Copy)]
pub struct PacketHeader {
    /// Whether this is a request or response packet.
    pub magic: PacketMagic,
    /// The packet's type.
    pub ptype: PacketType,
    /// The declared length in bytes of the body that follows.
    pub len: u32,
}
