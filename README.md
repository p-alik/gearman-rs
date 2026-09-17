# gearman

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
| `SUBMIT_REDUCE_JOB*` / `GRAB_JOB_ALL` | planned (stretch) |
| `SUBMIT_JOB_SCHED` | not planned — unused by gearmand itself |

The protocol reference used to build this crate is the `PROTOCOL` file in
the [gearmand](https://github.com/gearman/gearmand) C sources.

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
