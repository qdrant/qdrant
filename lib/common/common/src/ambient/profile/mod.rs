//! Per-request query profiling (`EXPLAIN ANALYZE`), carried by the ambient context.
//!
//! A request opts in by creating its context with [`AmbientContext::request_profiled`].
//! Code on the request path then reports into the innermost *frame*:
//! - the request frame collects request-level phases and shard profiles,
//! - a shard frame ([`ShardScope`]) collects shard-level phases and segment profiles,
//! - a segment frame ([`SegmentScope`]) collects the strategy notes of one segment search.
//!
//! When profiling is off, every entry point costs one thread-local read and a branch on
//! `None`; no allocation, lock or clock read happens. The entry points are at segment, shard
//! and request granularity, never per point.

mod collector;
pub mod model;
#[cfg(test)]
mod tests;

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use collector::Frame;
pub(crate) use collector::ProfileHandle;
pub use collector::{SegmentInfo, SegmentScope, ShardScope};
pub use model::*;

use super::{AmbientContext, Handoff, slot};

static THIS_PEER_ID: OnceLock<u64> = OnceLock::new();

/// Set once at startup in distributed mode, so shard profiles name their peer.
pub fn set_this_peer_id(peer_id: u64) {
    let _ = THIS_PEER_ID.set(peer_id);
}

pub(crate) fn this_peer_id() -> Option<u64> {
    THIS_PEER_ID.get().copied()
}

/// Who receives the profile of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProfileAudience {
    /// The client: notes derived from the whole profile are added.
    Client,
    /// The coordinating peer (internal API): only recorded notes, since the coordinator
    /// derives notes once over the profile of all shards.
    Coordinator,
}

/// Profiling state of a whole request, owned by the API handler that created it.
#[derive(Clone)]
pub struct Profiler {
    handle: ProfileHandle,
    started: Instant,
    audience: ProfileAudience,
}

impl Profiler {
    pub fn new(mode: ProfileMode, audience: ProfileAudience) -> Self {
        Self {
            handle: ProfileHandle::new(mode),
            started: Instant::now(),
            audience,
        }
    }

    pub fn mode(&self) -> ProfileMode {
        self.handle.mode
    }

    pub(crate) fn handle(&self) -> ProfileHandle {
        self.handle.clone()
    }

    /// Snapshot of the profile so far, with notes derived from it.
    pub fn finish(&self) -> QueryProfile {
        let mut profile = self
            .handle
            .frame
            .snapshot_request(self.handle.mode, self.started.elapsed());
        match self.audience {
            ProfileAudience::Client => profile.notes = collector::derive_notes(&profile),
            ProfileAudience::Coordinator => {}
        }
        profile
    }
}

impl AmbientContext {
    /// A server request, like [`Self::request`], that is also profiled when `profiler` is set.
    pub fn request_profiled(
        name: &str,
        collection: std::sync::Arc<super::hw::HwSharedDrain>,
        profiler: Option<&Profiler>,
    ) -> Self {
        Self::root_profiled(
            name,
            Some(collection),
            super::trace::global().cloned(),
            profiler.map(Profiler::handle),
        )
    }
}

/// Run `f` on the profile of the current scope, if it is profiled.
#[inline]
fn with_handle<R>(f: impl FnOnce(&ProfileHandle) -> R) -> Option<R> {
    slot::with_context_unchecked(|ctx| ctx.and_then(AmbientContext::profile).map(f))
}

/// The profile mode of the current scope; `None` when not profiled.
#[inline]
pub fn mode() -> Option<ProfileMode> {
    with_handle(|handle| handle.mode)
}

/// Whether the current scope only plans: segment searches must not score vectors.
#[inline]
pub fn is_explain() -> bool {
    mode() == Some(ProfileMode::Explain)
}

/// The profile mode of `handoff`; `None` when not profiled.
#[inline]
pub fn handoff_mode(handoff: &Handoff) -> Option<ProfileMode> {
    handoff
        .context()
        .and_then(AmbientContext::profile)
        .map(|handle| handle.mode)
}

/// Add `elapsed` to the phase `name` of the innermost frame.
#[inline]
pub fn record_phase(name: &'static str, elapsed: Duration) {
    with_handle(|handle| handle.frame.record_phase(name, elapsed));
}

/// A started phase timer, `None` when not profiled. Record with [`PhaseTimer::stop`] or drop.
#[inline]
pub fn phase(name: &'static str) -> Option<PhaseTimer> {
    with_handle(|handle| PhaseTimer {
        frame: handle.frame.clone(),
        name,
        started: Instant::now(),
    })
}

/// Records a phase into the frame it was started in, when dropped.
pub struct PhaseTimer {
    frame: Frame,
    name: &'static str,
    started: Instant,
}

impl PhaseTimer {
    pub fn stop(self) {}
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        self.frame.record_phase(self.name, self.started.elapsed());
    }
}

/// Run `f`, a read of one segment (scroll, count, facet), as a profiled segment scope.
/// When not profiled, this is `f()` after one thread-local read; `info` is not built.
#[inline]
pub fn segment_read<R>(info: impl FnOnce() -> SegmentInfo, f: impl FnOnce() -> R) -> R {
    if mode().is_none() {
        return f();
    }
    let Some(scope) = SegmentScope::start(&super::current(), info) else {
        return f();
    };
    let output = {
        let _guard = scope.handoff().enter_guard();
        f()
    };
    scope.finish();
    output
}

/// Record a finding for the request, shown in its `notes`.
pub fn note(text: impl FnOnce() -> String) {
    with_handle(|handle| handle.frame.push_note(text()));
}

/// Record the strategy of the segment search in progress.
#[inline]
pub fn note_strategy(strategy: SearchStrategy, reason: StrategyReason) {
    with_handle(|handle| handle.frame.note_strategy(strategy, reason));
}

/// Record the estimated cardinality of the filter of the segment search in progress,
/// and the payload keys of its primary clauses (the payload indexes used), built lazily.
#[inline]
pub fn note_cardinality(
    min: usize,
    exp: usize,
    max: usize,
    primary_keys: impl FnOnce() -> Vec<String>,
) {
    with_handle(|handle| {
        handle.frame.note_cardinality(
            CardinalityProfile {
                min: min as u64,
                exp: exp as u64,
                max: max as u64,
            },
            primary_keys(),
        )
    });
}

/// Record the exact number of points matching the filter, when known for free.
#[inline]
pub fn note_actual_cardinality(count: usize) {
    with_handle(|handle| handle.frame.note_actual_cardinality(count as u64));
}

/// Record the `full_scan_threshold` (in points) the strategy was chosen with.
#[inline]
pub fn note_full_scan_threshold(threshold: usize) {
    with_handle(|handle| handle.frame.note_full_scan_threshold(threshold as u64));
}

/// Graft the profile returned by a remote peer for shard `shard_id`.
/// `round_trip` is the time of the remote call as seen from this node.
pub fn record_remote(
    shard_id: u32,
    peer_id: u64,
    operation: &'static str,
    round_trip: Duration,
    remote: QueryProfile,
) {
    with_handle(|handle| {
        handle
            .frame
            .push_remote(shard_id, peer_id, operation, round_trip, remote)
    });
}

/// What to do with the results of a remote shard call in a profiled request.
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteResults {
    Keep,
    /// Explain mode, but the peer did not return a profile: it is an older version that
    /// ignored the mode and executed the request. Its results must not be returned.
    Discard,
}

/// Record a remote shard call that returned no profile (e.g. an older peer).
pub fn record_remote_without_profile(
    shard_id: u32,
    peer_id: u64,
    operation: &'static str,
    round_trip: Duration,
) -> RemoteResults {
    with_handle(|handle| {
        handle
            .frame
            .push_shard(collector::remote_shard_without_profile(
                shard_id, peer_id, operation, round_trip,
            ));
        match handle.mode {
            ProfileMode::Analyze => {
                handle.frame.push_note(format!(
                    "Peer {peer_id} returned no profile for its shards (it may run an older \
                     version): only their round-trip time is shown."
                ));
                RemoteResults::Keep
            }
            ProfileMode::Explain => {
                handle.frame.push_note(format!(
                    "Peer {peer_id} does not support explain (it may run an older version) and \
                     executed the request: its results were discarded and its shards are not \
                     planned in this profile."
                ));
                RemoteResults::Discard
            }
        }
    })
    .unwrap_or(RemoteResults::Keep)
}
