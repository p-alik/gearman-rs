//! Byte-exact regression tests against the worked example in the gearmand
//! C sources' `PROTOCOL` file (lines 613-716): a worker registers for
//! "reverse", a client submits job "test", and the worker replies "tset".

use bytes::{Bytes, BytesMut};
use gearman::error::GearmanError;
use gearman::protocol::{GearmanCodec, Packet, PacketMagic, PacketType};
use tokio_util::codec::{Decoder, Encoder};

const CAN_DO: &[u8] = &[
    0x00, 0x52, 0x45, 0x51, // \0REQ
    0x00, 0x00, 0x00, 0x01, // CAN_DO
    0x00, 0x00, 0x00, 0x07, // len 7
    b'r', b'e', b'v', b'e', b'r', b's', b'e',
];

const GRAB_JOB_REQ: &[u8] = &[
    0x00, 0x52, 0x45, 0x51, // \0REQ
    0x00, 0x00, 0x00, 0x09, // GRAB_JOB
    0x00, 0x00, 0x00, 0x00, // len 0
];

const NO_JOB: &[u8] = &[
    0x00, 0x52, 0x45, 0x53, // \0RES
    0x00, 0x00, 0x00, 0x0a, // NO_JOB
    0x00, 0x00, 0x00, 0x00,
];

const PRE_SLEEP: &[u8] = &[
    0x00, 0x52, 0x45, 0x51, // \0REQ
    0x00, 0x00, 0x00, 0x04, // PRE_SLEEP
    0x00, 0x00, 0x00, 0x00,
];

const SUBMIT_JOB: &[u8] = &[
    0x00, 0x52, 0x45, 0x51, // \0REQ
    0x00, 0x00, 0x00, 0x07, // SUBMIT_JOB
    0x00, 0x00, 0x00, 0x0d, // len 13
    b'r', b'e', b'v', b'e', b'r', b's', b'e', 0x00, // FUNC
    0x00, // UNIQ (empty)
    b't', b'e', b's', b't', // ARGS
];

const JOB_CREATED: &[u8] = &[
    0x00, 0x52, 0x45, 0x53, // \0RES
    0x00, 0x00, 0x00, 0x08, // JOB_CREATED
    0x00, 0x00, 0x00, 0x07, // len 7
    b'H', b':', b'l', b'a', b'p', b':', b'1',
];

const NOOP: &[u8] = &[
    0x00, 0x52, 0x45, 0x53, // \0RES
    0x00, 0x00, 0x00, 0x06, // NOOP
    0x00, 0x00, 0x00, 0x00,
];

const JOB_ASSIGN: &[u8] = &[
    0x00, 0x52, 0x45, 0x53, // \0RES
    0x00, 0x00, 0x00, 0x0b, // JOB_ASSIGN
    0x00, 0x00, 0x00, 0x14, // len 20
    b'H', b':', b'l', b'a', b'p', b':', b'1', 0x00, // HANDLE
    b'r', b'e', b'v', b'e', b'r', b's', b'e', 0x00, // FUNC
    b't', b'e', b's', b't', // ARG
];

const WORK_COMPLETE_FROM_WORKER: &[u8] = &[
    0x00, 0x52, 0x45, 0x51, // \0REQ
    0x00, 0x00, 0x00, 0x0d, // WORK_COMPLETE
    0x00, 0x00, 0x00, 0x0c, // len 12
    b'H', b':', b'l', b'a', b'p', b':', b'1', 0x00, // HANDLE
    b't', b's', b'e', b't', // RES
];

const WORK_COMPLETE_TO_CLIENT: &[u8] = &[
    0x00, 0x52, 0x45, 0x53, // \0RES
    0x00, 0x00, 0x00, 0x0d, // WORK_COMPLETE
    0x00, 0x00, 0x00, 0x0c, // len 12
    b'H', b':', b'l', b'a', b'p', b':', b'1', 0x00, // HANDLE
    b't', b's', b'e', b't', // RES
];

fn decode_one(bytes: &[u8]) -> Packet {
    let mut codec = GearmanCodec::new();
    let mut buf = BytesMut::from(bytes);
    let pkt = codec
        .decode(&mut buf)
        .expect("decode should not error")
        .expect("a full packet should be available");
    assert!(buf.is_empty(), "decoder must consume the whole buffer");
    pkt
}

fn encode_one(pkt: Packet) -> BytesMut {
    let mut codec = GearmanCodec::new();
    let mut buf = BytesMut::new();
    codec
        .encode(pkt, &mut buf)
        .expect("encode should not error");
    buf
}

fn assert_args(pkt: &Packet, expected: &[&[u8]]) {
    let actual: Vec<&[u8]> = pkt.args.iter().map(|b| b.as_ref()).collect();
    assert_eq!(actual, expected);
}

fn check_roundtrip(
    bytes: &[u8],
    expected_magic: PacketMagic,
    expected_type: PacketType,
    expected_args: &[&[u8]],
) {
    let pkt = decode_one(bytes);
    assert_eq!(pkt.magic, expected_magic);
    assert_eq!(pkt.ptype, expected_type);
    assert_args(&pkt, expected_args);

    let re_encoded = encode_one(pkt);
    assert_eq!(re_encoded.as_ref(), bytes);
}

#[test]
fn can_do_roundtrips() {
    check_roundtrip(CAN_DO, PacketMagic::Req, PacketType::CanDo, &[b"reverse"]);
}

#[test]
fn grab_job_roundtrips() {
    check_roundtrip(GRAB_JOB_REQ, PacketMagic::Req, PacketType::GrabJob, &[]);
}

#[test]
fn no_job_roundtrips() {
    check_roundtrip(NO_JOB, PacketMagic::Res, PacketType::NoJob, &[]);
}

#[test]
fn pre_sleep_roundtrips() {
    check_roundtrip(PRE_SLEEP, PacketMagic::Req, PacketType::PreSleep, &[]);
}

#[test]
fn submit_job_roundtrips() {
    check_roundtrip(
        SUBMIT_JOB,
        PacketMagic::Req,
        PacketType::SubmitJob,
        &[b"reverse", b"", b"test"],
    );
}

#[test]
fn job_created_roundtrips() {
    check_roundtrip(
        JOB_CREATED,
        PacketMagic::Res,
        PacketType::JobCreated,
        &[b"H:lap:1"],
    );
}

#[test]
fn noop_roundtrips() {
    check_roundtrip(NOOP, PacketMagic::Res, PacketType::Noop, &[]);
}

#[test]
fn job_assign_roundtrips() {
    check_roundtrip(
        JOB_ASSIGN,
        PacketMagic::Res,
        PacketType::JobAssign,
        &[b"H:lap:1", b"reverse", b"test"],
    );
}

#[test]
fn work_complete_from_worker_roundtrips() {
    check_roundtrip(
        WORK_COMPLETE_FROM_WORKER,
        PacketMagic::Req,
        PacketType::WorkComplete,
        &[b"H:lap:1", b"tset"],
    );
}

#[test]
fn work_complete_to_client_roundtrips() {
    check_roundtrip(
        WORK_COMPLETE_TO_CLIENT,
        PacketMagic::Res,
        PacketType::WorkComplete,
        &[b"H:lap:1", b"tset"],
    );
}

/// Feed the SUBMIT_JOB example one byte at a time to verify the decoder
/// correctly reports `Ok(None)` on a partial header/body and never drops or
/// duplicates bytes across polls.
#[test]
fn decoder_handles_partial_reads() {
    let mut codec = GearmanCodec::new();
    let mut buf = BytesMut::new();

    for (i, &byte) in SUBMIT_JOB.iter().enumerate() {
        buf.extend_from_slice(&[byte]);
        let result = codec.decode(&mut buf).expect("decode should not error");
        if i + 1 < SUBMIT_JOB.len() {
            assert!(
                result.is_none(),
                "should not yield a packet before all bytes arrive"
            );
        } else {
            let pkt = result.expect("full packet should now be available");
            assert_eq!(pkt.ptype, PacketType::SubmitJob);
            assert_args(&pkt, &[b"reverse", b"", b"test"]);
        }
    }
    assert!(buf.is_empty());
}

#[test]
fn decoder_rejects_bad_magic() {
    let mut codec = GearmanCodec::new();
    let mut buf =
        BytesMut::from(&b"\x00BAD\x00\x00\x00\x01\x00\x00\x00\x00"[..]);
    assert!(codec.decode(&mut buf).is_err());
}

#[test]
fn decoder_rejects_unknown_packet_type() {
    let mut codec = GearmanCodec::new();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"\x00REQ");
    bytes.extend_from_slice(&999u32.to_be_bytes());
    bytes.extend_from_slice(&0u32.to_be_bytes());
    let mut buf = BytesMut::from(&bytes[..]);
    assert!(codec.decode(&mut buf).is_err());
}

#[test]
fn decoder_rejects_oversized_payload() {
    let mut codec = GearmanCodec::with_max_payload_size(4);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"\x00REQ");
    bytes.extend_from_slice(&1u32.to_be_bytes()); // CAN_DO
    bytes.extend_from_slice(&100u32.to_be_bytes()); // declared len 100, over the 4-byte cap
    let mut buf = BytesMut::from(&bytes[..]);
    assert!(codec.decode(&mut buf).is_err());
}

/// `CAN_DO` takes exactly one argument; `Packet::request`'s `debug_assert_eq!`
/// only catches this in debug builds, so `encode` itself must reject it too
/// (constructing via the struct literal, not `Packet::request`, to bypass
/// that debug assert and exercise `encode`'s own check).
#[test]
fn encoder_rejects_wrong_arg_count() {
    let mut codec = GearmanCodec::new();
    let mut buf = BytesMut::new();
    let pkt = Packet {
        magic: PacketMagic::Req,
        ptype: PacketType::CanDo,
        args: vec![Bytes::from_static(b"foo"), Bytes::from_static(b"bar")],
    };
    let err = codec
        .encode(pkt, &mut buf)
        .expect_err("wrong argument count must be rejected");
    assert!(
        matches!(
            err,
            GearmanError::WrongArgCount {
                ptype: PacketType::CanDo,
                expected: 1,
                actual: 2,
            }
        ),
        "unexpected error: {err:?}"
    );
}

/// `encode` must enforce the same payload cap as `decode`, not just trust
/// the caller — otherwise a body that overflows `u32` on encode would wrap
/// the wire length header instead of erroring.
#[test]
fn encoder_rejects_oversized_payload() {
    let mut codec = GearmanCodec::with_max_payload_size(4);
    let mut buf = BytesMut::new();
    let pkt = Packet {
        magic: PacketMagic::Req,
        ptype: PacketType::CanDo,
        args: vec![Bytes::from_static(b"reverse")], // 7 bytes > 4-byte cap
    };
    let err = codec
        .encode(pkt, &mut buf)
        .expect_err("oversized payload must be rejected");
    assert!(
        matches!(err, GearmanError::PayloadTooLarge(4)),
        "unexpected error: {err:?}"
    );
}

/// The opaque last argument of WORK_COMPLETE must survive embedded NUL
/// bytes byte-for-byte, since it's binary job-result data.
#[test]
fn opaque_last_arg_preserves_embedded_nul_bytes() {
    let handle = b"H:lap:1";
    let payload: &[u8] = b"a\x00b\x00c";
    let body_len = handle.len() + 1 + payload.len();

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"\x00REQ");
    bytes.extend_from_slice(&13u32.to_be_bytes()); // WORK_COMPLETE
    bytes.extend_from_slice(&(body_len as u32).to_be_bytes());
    bytes.extend_from_slice(handle);
    bytes.push(0);
    bytes.extend_from_slice(payload);

    let pkt = decode_one(&bytes);
    assert_args(&pkt, &[handle, payload]);

    let re_encoded = encode_one(pkt);
    assert_eq!(re_encoded.as_ref(), bytes.as_slice());
}
