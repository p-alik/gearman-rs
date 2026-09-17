use crate::protocol::PacketType;

pub const REQ_MAGIC: [u8; 4] = *b"\0REQ";
pub const RES_MAGIC: [u8; 4] = *b"\0RES";

/// Size in bytes of a packet header: 4 (magic) + 4 (type) + 4 (payload length).
pub const HEADER_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketMagic {
    /// `\0REQ` — sent client->server or worker->server.
    Req,
    /// `\0RES` — sent server->client or server->worker.
    Res,
}

impl PacketMagic {
    pub fn bytes(self) -> [u8; 4] {
        match self {
            PacketMagic::Req => REQ_MAGIC,
            PacketMagic::Res => RES_MAGIC,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PacketHeader {
    pub magic: PacketMagic,
    pub ptype: PacketType,
    pub len: u32,
}
