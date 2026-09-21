//! The Gearman binary wire protocol: packet framing ([`GearmanCodec`]),
//! packet types ([`PacketType`]), and the decoded packet representation
//! ([`Packet`]).

mod codec;
mod command;
mod header;
mod packet;

pub use codec::{GearmanCodec, DEFAULT_MAX_PAYLOAD_SIZE};
pub use command::{PacketType, COMMAND_MAX};
pub use header::{PacketHeader, PacketMagic, HEADER_LEN, REQ_MAGIC, RES_MAGIC};
pub use packet::Packet;
