use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::GearmanError;
use crate::protocol::header::{PacketMagic, HEADER_LEN, REQ_MAGIC, RES_MAGIC};
use crate::protocol::packet::Packet;
use crate::protocol::PacketType;

/// Default cap on a single packet's declared payload length, guarding
/// against a corrupt or hostile peer's length field causing an unbounded
/// allocation before we've even validated the rest of the packet.
pub const DEFAULT_MAX_PAYLOAD_SIZE: u32 = 64 * 1024 * 1024;

pub struct GearmanCodec {
    max_payload_size: u32,
}

impl GearmanCodec {
    pub fn new() -> Self {
        Self {
            max_payload_size: DEFAULT_MAX_PAYLOAD_SIZE,
        }
    }

    pub fn with_max_payload_size(max_payload_size: u32) -> Self {
        Self { max_payload_size }
    }
}

impl Default for GearmanCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for GearmanCodec {
    type Item = Packet;
    type Error = GearmanError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Packet>, GearmanError> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }

        let magic_bytes: [u8; 4] = src[0..4].try_into().unwrap();
        let magic = if magic_bytes == REQ_MAGIC {
            PacketMagic::Req
        } else if magic_bytes == RES_MAGIC {
            PacketMagic::Res
        } else {
            return Err(GearmanError::BadMagic(magic_bytes));
        };

        let raw_type = u32::from_be_bytes(src[4..8].try_into().unwrap());
        let ptype = PacketType::try_from(raw_type)?;

        let len = u32::from_be_bytes(src[8..12].try_into().unwrap());
        if len > self.max_payload_size {
            return Err(GearmanError::PayloadTooLarge(len));
        }

        if src.len() < HEADER_LEN + len as usize {
            src.reserve(HEADER_LEN + len as usize - src.len());
            return Ok(None);
        }

        src.advance(HEADER_LEN);
        let payload = src.split_to(len as usize).freeze();
        let args = Packet::split_args(ptype, payload)?;
        Ok(Some(Packet { magic, ptype, args }))
    }
}

impl Encoder<Packet> for GearmanCodec {
    type Error = GearmanError;

    fn encode(&mut self, pkt: Packet, dst: &mut BytesMut) -> Result<(), GearmanError> {
        let expected = pkt.ptype.arg_count();
        if pkt.args.len() != expected {
            return Err(GearmanError::WrongArgCount {
                ptype: pkt.ptype,
                expected,
                actual: pkt.args.len(),
            });
        }

        let body_len = pkt.encoded_body_len();
        if body_len > self.max_payload_size as usize {
            return Err(GearmanError::PayloadTooLarge(self.max_payload_size));
        }

        dst.reserve(HEADER_LEN + body_len);
        dst.extend_from_slice(&pkt.magic.bytes());
        dst.extend_from_slice(&u32::from(pkt.ptype).to_be_bytes());
        dst.extend_from_slice(&(body_len as u32).to_be_bytes());
        for (i, arg) in pkt.args.iter().enumerate() {
            if i > 0 {
                dst.put_u8(0);
            }
            dst.extend_from_slice(arg);
        }
        Ok(())
    }
}
