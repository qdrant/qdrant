use std::cmp;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::shards::shard::PeerId;

#[derive(Clone, Debug, Default)]
pub struct Registry {
    locally_disabled_peers: HashMap<PeerId, Backoff>,
}

impl Registry {
    pub fn is_disabled(&self, peer_id: PeerId) -> bool {
        self.locally_disabled_peers.contains_key(&peer_id)
    }

    pub fn is_all_disabled(&self, peer_ids: impl IntoIterator<Item = PeerId>) -> bool {
        peer_ids
            .into_iter()
            .all(|peer_id| self.is_disabled(peer_id))
    }

    pub fn disable_peer(&mut self, peer_id: PeerId) {
        self.locally_disabled_peers.entry(peer_id).or_default();
    }

    pub fn disable_peer_and_notify_if_elapsed(&mut self, peer_id: PeerId) -> bool {
        self.locally_disabled_peers
            .entry(peer_id)
            .or_default()
            .retry_if_elapsed()
    }

    pub fn enable_peer(&mut self, peer_id: PeerId) {
        let _ = self.locally_disabled_peers.remove(&peer_id);
    }

    pub fn clear(&mut self) {
        self.locally_disabled_peers.clear();
    }

    pub fn notify_elapsed(&mut self) -> impl Iterator<Item = PeerId> + '_ {
        self.locally_disabled_peers
            .iter_mut()
            .filter_map(|(&peer_id, backoff)| {
                if backoff.retry_if_elapsed() {
                    Some(peer_id)
                } else {
                    None
                }
            })
    }
}

#[derive(Copy, Clone, Debug)]
struct Backoff {
    last_attempt: Instant,
    delay: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            last_attempt: Instant::now(),
            delay: Duration::ZERO,
        }
    }
}

impl Backoff {
    const MAX_DELAY: Duration = Duration::from_secs(10);

    pub fn retry_if_elapsed(&mut self) -> bool {
        let is_elapsed = self.is_elapsed();

        if is_elapsed {
            self.retry();
        }

        is_elapsed
    }

    fn is_elapsed(&self) -> bool {
        self.last_attempt.elapsed() >= self.delay
    }

    fn retry(&mut self) {
        self.last_attempt = Instant::now();

        self.delay = if self.delay.is_zero() {
            Duration::from_secs(1)
        } else {
            cmp::min(self.delay * 2, Self::MAX_DELAY)
        }
    }
}
