use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::client::ketama::Ring;
use crate::client::ClientCommand;

pub(crate) struct ServerSlot {
    pub(crate) cmd_tx: mpsc::UnboundedSender<ClientCommand>,
    pub(crate) connected: Arc<AtomicBool>,
    /// Set once the first connection attempt has finished, whether it
    /// succeeded or failed.
    pub(crate) attempted: Arc<AtomicBool>,
}

/// The set of job servers a `Client` routes submissions across: round-robin
/// by default, or by unique id when a ketama `Ring` is configured (see
/// [`crate::client::ServerSelection`]). Each server has its own persistent,
/// full-duplex connection (see `run_server_actor`), so this pool is just
/// routing, not pooling multiple connections per server.
pub(crate) struct ServerPool {
    servers: Vec<ServerSlot>,
    cursor: AtomicUsize,
    ring: Option<Ring>,
}

impl ServerPool {
    pub(crate) fn new(servers: Vec<ServerSlot>, ring: Option<Ring>) -> Self {
        Self {
            servers,
            cursor: AtomicUsize::new(0),
            ring,
        }
    }

    pub(crate) fn get(&self, index: usize) -> &ServerSlot {
        &self.servers[index]
    }

    pub(crate) fn any_healthy(&self) -> bool {
        (0..self.servers.len()).any(|idx| self.is_healthy(idx))
    }

    pub(crate) fn all_attempted(&self) -> bool {
        self.servers
            .iter()
            .all(|s| s.attempted.load(Ordering::Relaxed))
    }

    /// Picks the server for a job submission: by `unique` on the ketama ring
    /// when one is configured and `unique` is non-empty, otherwise
    /// round-robin.
    pub(crate) fn select_for_submit(&self, unique: Option<&str>) -> Option<usize> {
        match (&self.ring, unique) {
            (Some(ring), Some(unique)) if !unique.is_empty() => {
                ring.select(unique.as_bytes(), |idx| self.is_healthy(idx))
            }
            _ => self.next_healthy(),
        }
    }

    fn is_healthy(&self, index: usize) -> bool {
        self.servers[index].connected.load(Ordering::Relaxed)
    }

    /// Round-robin pick the next healthy server, skipping down ones.
    fn next_healthy(&self) -> Option<usize> {
        let len = self.servers.len();
        if len == 0 {
            return None;
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % len;
        (0..len)
            .map(|offset| (start + offset) % len)
            .find(|&idx| self.is_healthy(idx))
    }
}
