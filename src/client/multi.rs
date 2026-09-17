use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::client::ClientCommand;

pub(crate) struct ServerSlot {
    pub(crate) cmd_tx: mpsc::UnboundedSender<ClientCommand>,
    pub(crate) connected: Arc<AtomicBool>,
}

/// The set of job servers a `Client` round-robins submissions across. Each
/// server has its own persistent, full-duplex connection (see
/// `run_server_actor`), so this pool is just routing, not pooling multiple
/// connections per server.
pub(crate) struct ServerPool {
    servers: Vec<ServerSlot>,
    cursor: AtomicUsize,
}

impl ServerPool {
    pub(crate) fn new(servers: Vec<ServerSlot>) -> Self {
        Self {
            servers,
            cursor: AtomicUsize::new(0),
        }
    }

    pub(crate) fn get(&self, index: usize) -> &ServerSlot {
        &self.servers[index]
    }

    pub(crate) fn any_healthy(&self) -> bool {
        self.servers
            .iter()
            .any(|s| s.connected.load(Ordering::Relaxed))
    }

    /// Round-robin pick the next healthy server, skipping down ones.
    pub(crate) fn next_healthy(&self) -> Option<usize> {
        let len = self.servers.len();
        if len == 0 {
            return None;
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % len;
        (0..len)
            .map(|offset| (start + offset) % len)
            .find(|&idx| self.servers[idx].connected.load(Ordering::Relaxed))
    }
}
