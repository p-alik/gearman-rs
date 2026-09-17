# gearman

[![codecov](https://codecov.io/gh/p-alik/gearman-rs/graph/badge.svg)](https://codecov.io/gh/p-alik/gearman-rs)

Native async Rust implementation of the [Gearman](http://gearman.org/) binary
wire protocol — no dependency on the C `libgearman`. Built on
[tokio](https://tokio.rs/).

Status: early development. See [CHANGELOG.md](CHANGELOG.md) for what's
implemented so far.

## Protocol coverage

| Area | Status |
|---|---|
| Wire codec (framing, packet types, args) | done |
| Client: submit / status | done |
| Worker: register / grab / respond | done |
| Admin text protocol | done |
| TLS | done |
| `SUBMIT_JOB_EPOCH` | done |
| `SUBMIT_REDUCE_JOB*` / `GRAB_JOB_ALL` | done |
| `SUBMIT_JOB_SCHED` | not planned — unused by gearmand itself |

The protocol reference used to build this crate is the `PROTOCOL` file in
the [gearmand](https://github.com/gearman/gearmand) C sources.

## MSRV

`client`, `worker`, and `admin` support Rust 1.75, checked in CI.

`tls` does not: it pulls in rustls's `aws-lc-rs` crypto backend, which
depends on `zeroize`. `aws-lc-rs`/`rustls` themselves declare MSRV 1.71, but
`zeroize` 1.9.0 requires edition2024 (Rust 1.85+) — a minor-version release
raising its own MSRV, which is within the Rust ecosystem's usual semver
convention (documented by many crates as "MSRV bumps are non-breaking") but
still means `--all-features` needs Rust 1.85+ in practice. Since
`Cargo.lock` isn't committed, this floats to whatever's newest at build
time. We haven't reported this upstream — it isn't a bug in `aws-lc-rs` or
`zeroize` under that convention, it's a widely-hit, already-known pattern
(many other crates have independently worked around the same thing), and we
have full control over the outcome locally regardless. If it becomes a
problem, the options are: pin `zeroize` to `<1.9` ourselves, bump this
crate's own MSRV, or keep scoping the CI `tls` gap as done today.

## Development

```sh
cargo test --all-features
cargo clippy --all-features --all-targets -- -D warnings
cargo fmt --check
```

On NixOS, the rustup-managed toolchain on `PATH` may fail to link with a
`nix-support/ld-wrapper.sh: No such file or directory` error. If so, run the
above through a clean nixpkgs toolchain instead:
`nix-shell -p cargo rustc clippy rustfmt --run '<command>'`.

Integration tests spawn a real `gearmand` binary, found via the
`GEARMAND_BIN` environment variable or on `PATH`; if none is found they are
skipped with a notice rather than failing.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
