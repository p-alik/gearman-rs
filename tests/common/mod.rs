//! Spawns a real `gearmand` for integration tests. If no `gearmand` binary
//! can be found (via `GEARMAND_BIN` or `PATH`), tests using this fixture
//! soft-skip with a notice instead of failing, so `cargo test` still gives a
//! clean pass on machines that haven't built the C server.
//!
//! Each `tests/*.rs` file compiles its own copy of this module, and not
//! every file uses every helper here, so the compiler sees "dead code" in
//! any one binary that isn't real — hence the blanket allow.
#![allow(dead_code)]

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub struct GearmandProcess {
    child: Child,
    pub addr: String,
}

impl GearmandProcess {
    pub fn start() -> Option<Self> {
        Self::start_with_args(&[])
    }

    pub fn start_with_args(extra_args: &[&str]) -> Option<Self> {
        let bin = std::env::var("GEARMAND_BIN")
            .unwrap_or_else(|_| "gearmand".to_string());
        if which(&bin).is_none() {
            eprintln!(
                "skipping integration test: no `{bin}` binary found (set GEARMAND_BIN or add gearmand to PATH)"
            );
            return None;
        }

        let port = free_port();
        let child = Command::new(&bin)
            .args(["-p", &port.to_string()])
            .args(extra_args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn gearmand");

        let addr = format!("127.0.0.1:{port}");
        wait_for_ready(&addr);

        Some(Self { child, addr })
    }
}

impl Drop for GearmandProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a free port");
    listener.local_addr().unwrap().port()
}

fn wait_for_ready(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("gearmand did not become ready at {addr} in time");
}

fn which(bin: &str) -> Option<PathBuf> {
    if bin.contains('/') {
        let path = PathBuf::from(bin);
        return path.is_file().then_some(path);
    }
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths).find_map(|dir| {
            let candidate = dir.join(bin);
            candidate.is_file().then_some(candidate)
        })
    })
}

use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

/// Captures formatted `tracing` event messages emitted on the current
/// thread, for tests that need to assert a specific diagnostic actually
/// fired rather than just that behavior recovered (which can be identical
/// whether or not the diagnostic exists). Keep the returned
/// `DefaultGuard` alive for as long as events should be captured — it
/// resets the thread's subscriber back to default on drop.
#[derive(Clone, Default)]
pub struct LogCapture(Arc<Mutex<Vec<(tracing::Level, String)>>>);

impl LogCapture {
    pub fn install() -> (Self, tracing::subscriber::DefaultGuard) {
        let capture = Self::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);
        (capture, guard)
    }

    pub fn contains(&self, level: tracing::Level, needle: &str) -> bool {
        self.0
            .lock()
            .unwrap()
            .iter()
            .any(|(l, m)| *l == level && m.contains(needle))
    }

    pub fn messages(&self) -> Vec<(tracing::Level, String)> {
        self.0.lock().unwrap().clone()
    }
}

impl<S: tracing::Subscriber> Layer<S> for LogCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        struct MessageVisitor(String);
        impl tracing::field::Visit for MessageVisitor {
            fn record_debug(
                &mut self,
                field: &tracing::field::Field,
                value: &dyn std::fmt::Debug,
            ) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        self.0
            .lock()
            .unwrap()
            .push((*event.metadata().level(), visitor.0));
    }
}
