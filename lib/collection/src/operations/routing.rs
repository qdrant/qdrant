use std::hash::{BuildHasher as _, Hasher as _};

use common::stable_hash::StableHash as _;

use crate::shards::shard::PeerId;

/// Fast, stable hash builder used for deterministic routing.
type StableHashBuilder = fnv::FnvBuildHasher;

/// A client-provided token used to deterministically route read requests to the
/// same shard replicas.
///
/// # The "blinking" problem
///
/// In a multi-replica cluster, deferred updates can make a point visible on some
/// replicas before others. Reads are normally routed to a (mostly) random replica,
/// so repeating the same read can "blink": a point shows up, then disappears, then
/// shows up again, as different replicas answer successive requests. This is
/// confusing for users and can break multi-request search pipelines.
///
/// # How routing works
///
/// A client sends a stable token with each read — typically derived from a user or
/// session id, so a given user is consistently pinned to the same replicas while
/// load is still spread across users.
///
/// When a token is present, the replica set's candidates (local replica + readable
/// remotes) are ordered by [`RoutingToken::peer_key`], a stable hash of
/// `(token, peer_id)`. The first candidate becomes the primary, the rest are the
/// ordered fallback. Because the hash is identical on every node, every node
/// computes the same order and routes the token to the same primary. Local-replica
/// preference is therefore intentionally disabled while a token is set — otherwise
/// each node would just answer from its own copy and the token would have no
/// effect.
///
/// Routing is applied per shard replica set, so a single node change can repoint a
/// token on some shards but not others (a partial break).
///
/// # Guarantees (best-effort)
///
/// Stickiness is best-effort, not absolute:
///
/// - Membership changes (node restart, shard transfer, resharding, a peer becoming
///   non-readable) change the ordering and may repoint a token to a behind replica,
///   causing a one-off blink. The system cannot detect this without a seen
///   watermark.
/// - Read fan-out, fan-out delay and failover/retry pull in additional replicas and
///   merge their results, which can re-introduce or drop a previously-seen point.
///   Token reads are weakened whenever these trigger.
/// - Hashing per user concentrates a hot tenant onto one replica (load skew). This
///   is an accepted trade-off for stickiness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoutingToken(u64);

impl RoutingToken {
    /// Build a routing token from a precomputed `u64` seed.
    pub fn from_seed(seed: u64) -> Self {
        Self(seed)
    }

    /// Build a routing token from arbitrary client-provided bytes, for example a
    /// user id, session id or hashed API key.
    ///
    /// The bytes are reduced to a seed with a stable hash, so the same input yields
    /// the same token on every node.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut hasher = StableHashBuilder::default().build_hasher();
        hasher.write(bytes);
        Self(hasher.finish())
    }

    /// Compute the stable sort key for `peer_id` under this token.
    ///
    /// Replicas are ordered by ascending key; the lowest key is the primary. The
    /// same `(token, peer_id)` pair always hashes to the same value — on every node
    /// and across restarts — which is what makes routing deterministic.
    pub fn peer_key(&self, peer_id: PeerId) -> u64 {
        let mut hasher = StableHashBuilder::default().build_hasher();
        (self.0, peer_id).stable_hash(&mut |bytes| hasher.write(bytes));
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// Orders peers the way the read path does: by `(peer_key, peer_id)`.
    fn order(token: RoutingToken, peers: &[PeerId]) -> Vec<PeerId> {
        let mut peers = peers.to_vec();
        peers.sort_by_cached_key(|&peer_id| (token.peer_key(peer_id), peer_id));
        peers
    }

    #[test]
    fn peer_key_is_deterministic() {
        let token = RoutingToken::from_seed(42);
        assert_eq!(token.peer_key(7), token.peer_key(7));
    }

    #[test]
    fn from_bytes_is_deterministic() {
        assert_eq!(
            RoutingToken::from_bytes(b"user-1"),
            RoutingToken::from_bytes(b"user-1"),
        );
        assert_ne!(
            RoutingToken::from_bytes(b"user-1"),
            RoutingToken::from_bytes(b"user-2"),
        );
    }

    /// The order a token imposes on a replica set must not depend on the order the
    /// peers happen to be discovered in — otherwise different nodes would route the
    /// same token differently.
    #[test]
    fn ordering_is_independent_of_input_order() {
        let token = RoutingToken::from_seed(123);
        assert_eq!(
            order(token, &[1, 2, 3, 4, 5]),
            order(token, &[5, 4, 3, 2, 1])
        );
        assert_eq!(
            order(token, &[1, 2, 3, 4, 5]),
            order(token, &[3, 1, 5, 2, 4])
        );
    }

    /// Different tokens should spread the chosen primary across the replica set,
    /// otherwise the feature would concentrate all load on a single replica.
    #[test]
    fn different_tokens_spread_across_replicas() {
        let peers = [1, 2, 3, 4, 5];
        let primaries: HashSet<_> = (0..100)
            .map(|seed| order(RoutingToken::from_seed(seed), &peers)[0])
            .collect();
        assert!(
            primaries.len() > 1,
            "expected tokens to route to more than one primary, got {primaries:?}",
        );
    }
}
