//! Consistent-hash (ketama) server selection by job unique id, ported from
//! libgearman 2.2.0's `libgearman/server_selection.cc` so a Rust client and
//! a C client configured with the same server list route a given unique id
//! to the same server. That is what makes gearmand's unique-id coalescing
//! work across servers: it only merges jobs that reach the same server.

use md5::{Digest, Md5};

/// Replica points per server, as in libgearman: each replica's MD5 digest
/// is split into four 32-bit points.
const REPLICAS_PER_SERVER: u32 = 4;

/// gearmand's default port, which libgearman substitutes when a server is
/// given without one (or with port 0).
const DEFAULT_PORT: u16 = 4730;

pub(crate) struct Ring {
    /// `(point, server index)`, sorted by point.
    points: Vec<(u32, usize)>,
}

impl Ring {
    /// Builds the ring from the configured `host:port` strings. The points
    /// are derived from the host *as written* (not its resolved address)
    /// and the port in canonical decimal, matching libgearman's
    /// `"%s:%s-%u"` of `con->host()`, `con->service()`, and the replica.
    pub(crate) fn new(servers: &[String]) -> Self {
        let mut points = Vec::with_capacity(servers.len() * (REPLICAS_PER_SERVER as usize) * 4);
        for (index, addr) in servers.iter().enumerate() {
            let (host, port) = split_host_port(addr);
            for replica in 0..REPLICAS_PER_SERVER {
                let digest = Md5::digest(format!("{host}:{port}-{replica}").as_bytes());
                for chunk in digest.chunks_exact(4) {
                    points.push((u32::from_le_bytes(chunk.try_into().unwrap()), index));
                }
            }
        }
        points.sort_unstable_by_key(|&(point, _)| point);
        Self { points }
    }

    /// Picks the server for `unique`, or `None` if no server is healthy.
    ///
    /// libgearman first takes the server owning the first ring point at or
    /// after `md5(unique)`. When connecting to it fails, it asks again with
    /// that server as `after_con`, which restarts the walk just past the
    /// failed server's *first* ring point (not past the hash) and skips the
    /// failed server; it repeats that for each further failure. This
    /// replays the same chain using `is_healthy` in place of a connect
    /// attempt, so with the same set of servers down both clients agree.
    /// The C chain can cycle back to a server it already tried; in that
    /// case this falls back to the first healthy server in ring order from
    /// the hash, rather than giving up as long as any server is up.
    pub(crate) fn select(
        &self,
        unique: &[u8],
        is_healthy: impl Fn(usize) -> bool,
    ) -> Option<usize> {
        if self.points.is_empty() {
            return None;
        }
        let len = self.points.len();
        let hash = md5_u32(unique);
        let start = self.points.partition_point(|&(point, _)| point < hash) % len;

        let mut candidate = self.points[start].1;
        let mut tried = Vec::new();
        while !tried.contains(&candidate) {
            if is_healthy(candidate) {
                return Some(candidate);
            }
            tried.push(candidate);
            match self.next_after(candidate) {
                Some(next) => candidate = next,
                None => break,
            }
        }

        (0..len)
            .map(|offset| self.points[(start + offset) % len].1)
            .find(|&server| is_healthy(server))
    }

    /// libgearman's `after_con` step: the first server other than `failed`
    /// walking the ring from just past `failed`'s first point.
    fn next_after(&self, failed: usize) -> Option<usize> {
        let len = self.points.len();
        let after = self
            .points
            .iter()
            .position(|&(_, server)| server == failed)
            .unwrap_or(0);
        (1..=len)
            .map(|offset| self.points[(after + offset) % len].1)
            .find(|&server| server != failed)
    }
}

/// libhashkit's `hashkit_md5`: the digest's first four bytes, little-endian.
fn md5_u32(key: &[u8]) -> u32 {
    let digest = Md5::digest(key);
    u32::from_le_bytes(digest[..4].try_into().unwrap())
}

/// Splits `host:port` the way libgearman stores a server: the host without
/// IPv6 brackets, and the port as canonical decimal, defaulting to 4730
/// when absent or 0.
fn split_host_port(addr: &str) -> (&str, u16) {
    let (host, port) = if let Some(rest) = addr.strip_prefix('[') {
        match rest.split_once(']') {
            Some((host, tail)) => (host, tail.strip_prefix(':')),
            None => (addr, None),
        }
    } else {
        match addr.rsplit_once(':') {
            // More than one colon without brackets is a bare IPv6 address.
            Some((host, port)) if !host.contains(':') => (host, Some(port)),
            _ => (addr, None),
        }
    };
    let port = port
        .and_then(|p| p.parse::<u16>().ok())
        .filter(|&p| p != 0)
        .unwrap_or(DEFAULT_PORT);
    (host, port)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn servers(addrs: &[&str]) -> Vec<String> {
        addrs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn split_host_port_variants() {
        assert_eq!(split_host_port("localhost:4730"), ("localhost", 4730));
        assert_eq!(split_host_port("10.0.0.1:04731"), ("10.0.0.1", 4731));
        assert_eq!(split_host_port("gearman"), ("gearman", 4730));
        assert_eq!(split_host_port("gearman:0"), ("gearman", 4730));
        assert_eq!(split_host_port("[::1]:4731"), ("::1", 4731));
        assert_eq!(split_host_port("[::1]"), ("::1", 4730));
        assert_eq!(split_host_port("::1"), ("::1", 4730));
    }

    #[test]
    fn md5_u32_matches_hashkit_md5() {
        // md5("") = d41d8cd98f00b204e9800998ecf8427e; hashkit_md5 reads the
        // first four bytes little-endian.
        assert_eq!(md5_u32(b""), 0xd98c_1dd4);
    }

    /// Expected `(unique, primary, failover)` server indices, produced by
    /// compiling libgearman 2.2.0's `server_selection.cc` verbatim against
    /// stub connection types and calling `client_select_connection_by_key`
    /// with `after_con` = NULL, then = the primary.
    #[test]
    fn matches_libgearman_golden_values() {
        /// `(unique, primary, failover)`.
        type Expected = &'static [(&'static str, usize, usize)];
        let cases: &[(&[&str], Expected)] = &[
            (
                &["10.0.0.1:4730", "10.0.0.2:4730", "10.0.0.3:4730"],
                &[
                    ("a", 0, 1),
                    ("b", 0, 1),
                    ("order-42", 1, 2),
                    ("user:1001", 1, 2),
                    ("dedupe-me", 2, 0),
                    ("x", 1, 2),
                    ("unique-7", 2, 0),
                    ("job/2026-10-19", 2, 0),
                ],
            ),
            (
                &["localhost:4730", "localhost:4731"],
                &[
                    ("a", 1, 0),
                    ("b", 0, 1),
                    ("order-42", 1, 0),
                    ("user:1001", 0, 1),
                    ("dedupe-me", 0, 1),
                    ("x", 0, 1),
                    ("unique-7", 1, 0),
                    ("job/2026-10-19", 1, 0),
                ],
            ),
        ];
        for (addrs, expected) in cases {
            let ring = Ring::new(&servers(addrs));
            for &(unique, primary, failover) in *expected {
                assert_eq!(
                    ring.select(unique.as_bytes(), |_| true),
                    Some(primary),
                    "{unique}"
                );
                assert_eq!(
                    ring.select(unique.as_bytes(), |s| s != primary),
                    Some(failover),
                    "{unique} failover"
                );
            }
        }
    }

    #[test]
    fn ring_has_sixteen_points_per_server() {
        let ring = Ring::new(&servers(&["a:4730", "b:4730", "c:4730"]));
        assert_eq!(ring.points.len(), 48);
        assert!(ring.points.windows(2).all(|w| w[0].0 <= w[1].0));
    }

    #[test]
    fn selection_is_stable_and_independent_of_list_order() {
        let forward = servers(&["a:4730", "b:4730", "c:4730"]);
        let mut reversed = forward.clone();
        reversed.reverse();
        let ring_f = Ring::new(&forward);
        let ring_r = Ring::new(&reversed);
        for i in 0..200 {
            let unique = format!("unique-{i}");
            let f = ring_f.select(unique.as_bytes(), |_| true).unwrap();
            let r = ring_r.select(unique.as_bytes(), |_| true).unwrap();
            assert_eq!(forward[f], reversed[r], "{unique}");
            assert_eq!(ring_f.select(unique.as_bytes(), |_| true), Some(f));
        }
    }

    #[test]
    fn every_server_gets_some_uniques() {
        let ring = Ring::new(&servers(&["a:4730", "b:4730", "c:4730"]));
        let mut counts = [0usize; 3];
        for i in 0..3000 {
            counts[ring.select(format!("u{i}").as_bytes(), |_| true).unwrap()] += 1;
        }
        assert!(counts.iter().all(|&c| c > 300), "{counts:?}");
    }

    #[test]
    fn failover_follows_libgearman_after_con_walk() {
        let ring = Ring::new(&servers(&["a:4730", "b:4730", "c:4730"]));
        for i in 0..200 {
            let unique = format!("unique-{i}");
            let primary = ring.select(unique.as_bytes(), |_| true).unwrap();
            let expected = ring.next_after(primary).unwrap();
            assert_ne!(expected, primary);
            assert_eq!(
                ring.select(unique.as_bytes(), |s| s != primary),
                Some(expected),
                "{unique}"
            );
        }
    }

    #[test]
    fn falls_back_to_any_healthy_server_when_the_chain_cycles() {
        let ring = Ring::new(&servers(&["a:4730", "b:4730", "c:4730"]));
        for healthy in 0..3 {
            for i in 0..50 {
                let unique = format!("unique-{i}");
                assert_eq!(
                    ring.select(unique.as_bytes(), |s| s == healthy),
                    Some(healthy)
                );
            }
        }
        assert_eq!(ring.select(b"x", |_| false), None);
    }
}
