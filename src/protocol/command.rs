use crate::error::GearmanError;

/// Gearman wire packet types, per `libgearman-1.0/protocol.h` in the
/// gearmand C sources. Numeric values are the wire codes; `GEARMAN_COMMAND_TEXT`
/// (0) is not a wire type (it only marks the admin text-protocol dispatch
/// path internally in gearmand) and `GEARMAN_COMMAND_UNUSED` (5) was never
/// assigned a meaning, so neither has a variant here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum PacketType {
    /// `CAN_DO`: worker->server, registers a function the worker can
    /// perform.
    CanDo = 1,
    /// `CANT_DO`: worker->server, unregisters a previously registered
    /// function.
    CantDo = 2,
    /// `RESET_ABILITIES`: worker->server, unregisters every function.
    ResetAbilities = 3,
    /// `PRE_SLEEP`: worker->server, signals the worker is about to wait for
    /// a wake-up `NOOP` after a `NO_JOB` reply.
    PreSleep = 4,
    /// `NOOP`: server->worker, wakes a worker that sent `PRE_SLEEP`.
    Noop = 6,
    /// `SUBMIT_JOB`: client->server, submits a normal-priority foreground
    /// job.
    SubmitJob = 7,
    /// `JOB_CREATED`: server->client, acknowledges a submitted job with its
    /// handle.
    JobCreated = 8,
    /// `GRAB_JOB`: worker->server, requests a job with no unique id in the
    /// assignment.
    GrabJob = 9,
    /// `NO_JOB`: server->worker, no job is currently available for a
    /// `GRAB_JOB*` request.
    NoJob = 10,
    /// `JOB_ASSIGN`: server->worker, assigns a job (no unique id).
    JobAssign = 11,
    /// `WORK_STATUS`: worker->server->client, reports progress on a
    /// foreground job.
    WorkStatus = 12,
    /// `WORK_COMPLETE`: worker->server->client, reports a job's successful
    /// result.
    WorkComplete = 13,
    /// `WORK_FAIL`: worker->server->client, reports that a job failed.
    WorkFail = 14,
    /// `GET_STATUS`: client->server, requests a job's status by handle.
    GetStatus = 15,
    /// `ECHO_REQ`: round-trip test request; the server replies with
    /// `ECHO_RES` carrying the same payload.
    EchoReq = 16,
    /// `ECHO_RES`: reply to `ECHO_REQ`.
    EchoRes = 17,
    /// `SUBMIT_JOB_BG`: client->server, submits a normal-priority
    /// background job.
    SubmitJobBg = 18,
    /// `ERROR`: server->client/worker, reports an error condition.
    Error = 19,
    /// `STATUS_RES`: server->client, reply to `GET_STATUS`.
    StatusRes = 20,
    /// `SUBMIT_JOB_HIGH`: client->server, submits a high-priority
    /// foreground job.
    SubmitJobHigh = 21,
    /// `SET_CLIENT_ID`: worker->server, sets a human-readable client id
    /// (visible in the admin `workers` command's output).
    SetClientId = 22,
    /// `CAN_DO_TIMEOUT`: worker->server, like `CAN_DO` but with a timeout
    /// after which the server fails the job back if it isn't completed.
    CanDoTimeout = 23,
    /// `ALL_YOURS`: worker->server, part of an older multi-server worker
    /// coordination scheme; not sent or interpreted by this crate.
    AllYours = 24,
    /// `WORK_EXCEPTION`: worker->server->client, reports that a job raised
    /// an exception. Only forwarded to clients that opted in with
    /// `OPTION_REQ "exceptions"`.
    WorkException = 25,
    /// `OPTION_REQ`: client/worker->server, requests a named server option
    /// (this crate only ever requests `"exceptions"`).
    OptionReq = 26,
    /// `OPTION_RES`: server->client/worker, acknowledges an `OPTION_REQ`.
    OptionRes = 27,
    /// `WORK_DATA`: worker->server->client, a partial-result chunk for a
    /// foreground job.
    WorkData = 28,
    /// `WORK_WARNING`: worker->server->client, a warning message for a
    /// foreground job.
    WorkWarning = 29,
    /// `GRAB_JOB_UNIQ`: worker->server, requests a job whose assignment
    /// includes the caller-supplied unique id.
    GrabJobUniq = 30,
    /// `JOB_ASSIGN_UNIQ`: server->worker, assigns a job, including its
    /// unique id.
    JobAssignUniq = 31,
    /// `SUBMIT_JOB_HIGH_BG`: client->server, submits a high-priority
    /// background job.
    SubmitJobHighBg = 32,
    /// `SUBMIT_JOB_LOW`: client->server, submits a low-priority foreground
    /// job.
    SubmitJobLow = 33,
    /// `SUBMIT_JOB_LOW_BG`: client->server, submits a low-priority
    /// background job.
    SubmitJobLowBg = 34,
    /// `SUBMIT_JOB_SCHED`: a cron-like scheduled submission. Never emitted
    /// by this crate: unused by gearmand itself. Kept only so decoding a
    /// peer's packet stream stays exhaustive.
    SubmitJobSched = 35,
    /// `SUBMIT_JOB_EPOCH`: client->server, submits a job that becomes
    /// eligible to run at or after a given Unix-epoch time.
    SubmitJobEpoch = 36,
    /// `SUBMIT_REDUCE_JOB`: client->server, submits a foreground job
    /// tagged with a reducer name, forwarded to workers that grab with
    /// `GRAB_JOB_ALL`.
    SubmitReduceJob = 37,
    /// `SUBMIT_REDUCE_JOB_BACKGROUND`: background counterpart of
    /// `SUBMIT_REDUCE_JOB`.
    SubmitReduceJobBackground = 38,
    /// `GRAB_JOB_ALL`: worker->server, requests a job whose assignment
    /// includes both the unique id and, if present, the reducer name.
    GrabJobAll = 39,
    /// `JOB_ASSIGN_ALL`: server->worker, assigns a job, including its
    /// unique id and reducer name.
    JobAssignAll = 40,
    /// `GET_STATUS_UNIQUE`: client->server, requests a job's status by
    /// unique id, additionally returning a client count.
    GetStatusUnique = 41,
    /// `STATUS_RES_UNIQUE`: server->client, reply to `GET_STATUS_UNIQUE`.
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
