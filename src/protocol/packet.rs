use bytes::{Buf, Bytes};

use crate::error::GearmanError;
use crate::protocol::header::PacketMagic;
use crate::protocol::PacketType;

/// A decoded (or to-be-encoded) Gearman binary protocol packet: a type plus
/// its NUL-separated arguments.
#[derive(Debug, Clone)]
pub struct Packet {
    /// Whether this is a request or response packet.
    pub magic: PacketMagic,
    /// The packet's type.
    pub ptype: PacketType,
    /// The packet's arguments, in wire order.
    pub args: Vec<Bytes>,
}

impl Packet {
    /// Builds a request packet (`magic` = [`PacketMagic::Req`]).
    ///
    /// Debug-asserts that `args.len()` matches `ptype.arg_count()`.
    pub fn request(ptype: PacketType, args: Vec<Bytes>) -> Self {
        debug_assert_eq!(
            args.len(),
            ptype.arg_count(),
            "wrong argument count for {ptype:?}"
        );
        Packet {
            magic: PacketMagic::Req,
            ptype,
            args,
        }
    }

    /// Builds a response packet (`magic` = [`PacketMagic::Res`]).
    ///
    /// Debug-asserts that `args.len()` matches `ptype.arg_count()`.
    pub fn response(ptype: PacketType, args: Vec<Bytes>) -> Self {
        debug_assert_eq!(
            args.len(),
            ptype.arg_count(),
            "wrong argument count for {ptype:?}"
        );
        Packet {
            magic: PacketMagic::Res,
            ptype,
            args,
        }
    }

    /// The argument at `index`, if present.
    pub fn arg(&self, index: usize) -> Option<&Bytes> {
        self.args.get(index)
    }

    /// The argument at `index` as a UTF-8 string, if present and valid
    /// UTF-8.
    pub fn arg_str(&self, index: usize) -> Option<&str> {
        self.arg(index).and_then(|b| std::str::from_utf8(b).ok())
    }

    /// The last argument: the wire format's one opaque, binary-safe field
    /// (job workload / result data), present on packet types that carry one.
    pub fn last_arg(&self) -> Option<&Bytes> {
        self.args.last()
    }

    pub(crate) fn encoded_body_len(&self) -> usize {
        let separators = self.args.len().saturating_sub(1);
        self.args.iter().map(|a| a.len()).sum::<usize>() + separators
    }

    /// Split a raw payload into `ptype.arg_count()` NUL-separated arguments.
    /// Every argument except the last is split at the first NUL byte; the
    /// last argument runs to the end of the payload and may itself contain
    /// NUL bytes, since it is the wire format's one opaque field.
    pub(crate) fn split_args(
        ptype: PacketType,
        mut payload: Bytes,
    ) -> Result<Vec<Bytes>, GearmanError> {
        let count = ptype.arg_count();
        if count == 0 {
            if !payload.is_empty() {
                return Err(GearmanError::MalformedBody {
                    ptype,
                    reason: "expected empty payload",
                });
            }
            return Ok(Vec::new());
        }

        let mut args = Vec::with_capacity(count);
        for _ in 0..count - 1 {
            let nul_pos =
                payload
                    .iter()
                    .position(|&b| b == 0)
                    .ok_or(GearmanError::MalformedBody {
                        ptype,
                        reason: "missing NUL argument separator",
                    })?;
            let arg = payload.split_to(nul_pos);
            payload.advance(1); // skip the NUL separator itself
            args.push(arg);
        }
        args.push(payload);
        Ok(args)
    }
}
