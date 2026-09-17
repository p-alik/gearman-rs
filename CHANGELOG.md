# Changelog

All notable changes to this project are documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Wire protocol codec (`GearmanCodec`): packet header framing, packet type
  table, NUL-delimited argument splitting with an opaque binary-safe last
  argument, byte-exact round-trip tests against the `PROTOCOL` worked
  example from the gearmand C sources.
- Low-level `Connection` wrapper (generic over the byte stream, ready for a
  TLS-wrapped stream later) with `send`/`recv` of raw packets.
- `examples/raw_echo.rs`: ECHO_REQ/ECHO_RES smoke test against a real
  `gearmand`.
- `Client`/`ClientBuilder`: multi-server round-robin submission with
  per-server reconnect (exponential backoff), `submit`/`submit_fg`/
  `submit_bg` covering all non-deferred `SUBMIT_JOB*` priority/background
  variants, `get_status`/`get_status_unique`, and an `OPTION_REQ
  "exceptions"` opt-in. A single per-connection actor task demuxes
  `WORK_*` events by job handle and FIFO-matches handle-less replies
  (`JOB_CREATED`/`STATUS_RES`/`ERROR`) to their requests.
- `tests/common`: a `GearmandProcess` fixture that spawns a real `gearmand`
  for integration tests (soft-skips if none is found locally).
- `examples/client_submit.rs`: background submit + status polling against a
  real `gearmand`.
