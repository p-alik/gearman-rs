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
