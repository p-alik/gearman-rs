use crate::error::GearmanError;

/// Gearman wire packet types, per `libgearman-1.0/protocol.h` in the
/// gearmand C sources. Numeric values are the wire codes; `GEARMAN_COMMAND_TEXT`
/// (0) is not a wire type (it only marks the admin text-protocol dispatch
/// path internally in gearmand) and `GEARMAN_COMMAND_UNUSED` (5) was never
/// assigned a meaning, so neither has a variant here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum PacketType {
    CanDo = 1,
    CantDo = 2,
    ResetAbilities = 3,
    PreSleep = 4,
    Noop = 6,
    SubmitJob = 7,
    JobCreated = 8,
    GrabJob = 9,
    NoJob = 10,
    JobAssign = 11,
    WorkStatus = 12,
    WorkComplete = 13,
    WorkFail = 14,
    GetStatus = 15,
    EchoReq = 16,
    EchoRes = 17,
    SubmitJobBg = 18,
    Error = 19,
    StatusRes = 20,
    SubmitJobHigh = 21,
    SetClientId = 22,
    CanDoTimeout = 23,
    AllYours = 24,
    WorkException = 25,
    OptionReq = 26,
    OptionRes = 27,
    WorkData = 28,
    WorkWarning = 29,
    GrabJobUniq = 30,
    JobAssignUniq = 31,
    SubmitJobHighBg = 32,
    SubmitJobLow = 33,
    SubmitJobLowBg = 34,
    /// Never emitted by this crate: unused by gearmand itself. Kept only so
    /// decoding a peer's packet stream stays exhaustive.
    SubmitJobSched = 35,
    SubmitJobEpoch = 36,
    SubmitReduceJob = 37,
    SubmitReduceJobBackground = 38,
    GrabJobAll = 39,
    JobAssignAll = 40,
    GetStatusUnique = 41,
    StatusResUnique = 42,
}

/// `GEARMAN_COMMAND_MAX` from `libgearman-1.0/protocol.h`.
pub const COMMAND_MAX: u32 = 43;

impl TryFrom<u32> for PacketType {
    type Error = GearmanError;

    fn try_from(value: u32) -> Result<Self, GearmanError> {
        use PacketType::*;
        Ok(match value {
            1 => CanDo,
            2 => CantDo,
            3 => ResetAbilities,
            4 => PreSleep,
            6 => Noop,
            7 => SubmitJob,
            8 => JobCreated,
            9 => GrabJob,
            10 => NoJob,
            11 => JobAssign,
            12 => WorkStatus,
            13 => WorkComplete,
            14 => WorkFail,
            15 => GetStatus,
            16 => EchoReq,
            17 => EchoRes,
            18 => SubmitJobBg,
            19 => Error,
            20 => StatusRes,
            21 => SubmitJobHigh,
            22 => SetClientId,
            23 => CanDoTimeout,
            24 => AllYours,
            25 => WorkException,
            26 => OptionReq,
            27 => OptionRes,
            28 => WorkData,
            29 => WorkWarning,
            30 => GrabJobUniq,
            31 => JobAssignUniq,
            32 => SubmitJobHighBg,
            33 => SubmitJobLow,
            34 => SubmitJobLowBg,
            35 => SubmitJobSched,
            36 => SubmitJobEpoch,
            37 => SubmitReduceJob,
            38 => SubmitReduceJobBackground,
            39 => GrabJobAll,
            40 => JobAssignAll,
            41 => GetStatusUnique,
            42 => StatusResUnique,
            other => return Err(GearmanError::UnknownPacketType(other)),
        })
    }
}

impl From<PacketType> for u32 {
    fn from(value: PacketType) -> Self {
        value as u32
    }
}

impl PacketType {
    /// Number of NUL-separated arguments this packet type's payload carries.
    /// The last argument is always opaque (may itself contain NUL bytes);
    /// earlier arguments are split on the first NUL. Field layouts are
    /// cross-checked against `libgearman/command.cc`'s
    /// `gearmand_command_info_list` (each entry's declared `argc` is the
    /// number of NUL-delimited fields; its `data` flag means one further
    /// opaque, unterminated field follows — so the total field count here
    /// is `argc + (data ? 1 : 0)`), not just the `libgearman-1.0/
    /// protocol.h` inline comments, which read as slightly ambiguous for
    /// `SUBMIT_REDUCE_JOB[_BACKGROUND]` (`argc=4, data=true`, i.e. 5 total:
    /// `FUNC\0UNIQ\0REDUCER\0UNUSED\0ARGS` — the `UNUSED` field is real on
    /// the wire, just never read by the server's reduce-job handler).
    pub fn arg_count(self) -> usize {
        use PacketType::*;
        match self {
            ResetAbilities | PreSleep | Noop | GrabJob | NoJob | AllYours | GrabJobUniq
            | GrabJobAll => 0,
            CanDo | CantDo | JobCreated | WorkFail | GetStatus | EchoReq | EchoRes
            | SetClientId | OptionReq | OptionRes | GetStatusUnique => 1,
            Error | CanDoTimeout | WorkComplete | WorkException | WorkData | WorkWarning => 2,
            SubmitJob | SubmitJobBg | SubmitJobHigh | SubmitJobHighBg | SubmitJobLow
            | SubmitJobLowBg | WorkStatus | JobAssign => 3,
            SubmitJobEpoch | JobAssignUniq => 4,
            SubmitReduceJob | SubmitReduceJobBackground | JobAssignAll | StatusRes => 5,
            StatusResUnique => 6,
            SubmitJobSched => 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every wire code `PacketType` claims to decode must round-trip back
    /// to the same numeric value, and vice versa — catches a transposed
    /// number in the `TryFrom` match arms that the compiler's exhaustiveness
    /// check wouldn't.
    #[test]
    fn try_from_u32_round_trips_for_every_variant() {
        for code in 1..COMMAND_MAX {
            if code == 5 {
                continue; // GEARMAN_COMMAND_UNUSED: intentionally unassigned
            }
            let packet_type = PacketType::try_from(code)
                .unwrap_or_else(|e| panic!("code {code} should decode: {e:?}"));
            assert_eq!(u32::from(packet_type), code);
        }
    }

    #[test]
    fn try_from_u32_rejects_text_and_unused_codes() {
        assert!(matches!(
            PacketType::try_from(0),
            Err(GearmanError::UnknownPacketType(0))
        ));
        assert!(matches!(
            PacketType::try_from(5),
            Err(GearmanError::UnknownPacketType(5))
        ));
    }

    #[test]
    fn try_from_u32_rejects_codes_at_and_past_command_max() {
        assert!(matches!(
            PacketType::try_from(COMMAND_MAX),
            Err(GearmanError::UnknownPacketType(v)) if v == COMMAND_MAX
        ));
        assert!(matches!(
            PacketType::try_from(COMMAND_MAX + 100),
            Err(GearmanError::UnknownPacketType(_))
        ));
    }

    #[test]
    fn arg_count_matches_protocol_field_layout() {
        assert_eq!(PacketType::Noop.arg_count(), 0);
        assert_eq!(PacketType::EchoReq.arg_count(), 1);
        assert_eq!(PacketType::WorkComplete.arg_count(), 2);
        assert_eq!(PacketType::SubmitJob.arg_count(), 3);
        assert_eq!(PacketType::SubmitJobEpoch.arg_count(), 4);
        assert_eq!(PacketType::SubmitReduceJob.arg_count(), 5);
        assert_eq!(PacketType::StatusResUnique.arg_count(), 6);
        assert_eq!(PacketType::SubmitJobSched.arg_count(), 8);
    }
}
