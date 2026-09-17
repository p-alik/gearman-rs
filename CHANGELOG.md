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
- `Worker`/`WorkerBuilder`: registers `JobHandler`s (or plain async
  closures) per function name via `CAN_DO`/`CAN_DO_TIMEOUT`, runs
  `concurrency` independent grab-loop tasks per server (each its own
  connection, since Gearman allows only one job in flight per `GRAB_JOB`
  cycle on a connection) defaulting to `GRAB_JOB_UNIQ` with a `GrabMode`
  opt-out to plain `GRAB_JOB`, and reports `WORK_COMPLETE`/`WORK_FAIL`/
  `WORK_EXCEPTION` from the handler's result. `Reporter` lets a handler
  send `WORK_STATUS`/`WORK_DATA`/`WORK_WARNING` while it runs. Graceful
  `Worker::shutdown` lets each grab-loop finish its current job (there is
  no mid-job cancellation on the wire) before exiting.
- `tests/client_worker_roundtrip.rs`: full client+worker round trip against
  a real `gearmand`, including the "reverse" scenario from the `PROTOCOL`
  worked example byte-for-byte, worker failure, background+poll,
  concurrent jobs, and graceful shutdown.
- `examples/worker_reverse.rs`: a running worker for the "reverse"
  function.
