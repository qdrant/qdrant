//! Frames that collect profile data while a request runs.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::model::*;
use super::os_io::ThreadIo;
use crate::ambient::hw::{HardwareData, HwMetric};
use crate::ambient::{AmbientContext, AmbientFutureExt, Handoff};

/// Bound on the segment entries kept per shard, so a profile of a shard with thousands of
/// segments or a huge batch stays readable and small. The rest are counted in a note.
const MAX_SEGMENTS_PER_SHARD: usize = 256;

#[derive(Clone)]
pub(crate) struct ProfileHandle {
    pub(crate) mode: ProfileMode,
    pub(crate) frame: Frame,
}

impl ProfileHandle {
    pub(crate) fn new(mode: ProfileMode) -> Self {
        Self {
            mode,
            frame: Frame::default(),
        }
    }

    fn child(&self) -> Self {
        Self {
            mode: self.mode,
            frame: Frame::default(),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct Frame(Arc<Mutex<FrameData>>);

#[derive(Default)]
struct FrameData {
    phases: Vec<PhaseProfile>,
    shards: Vec<ShardProfile>,
    segments: Vec<SegmentProfile>,
    dropped_segments: u64,
    /// Request-level findings recorded while running, e.g. a peer that cannot profile.
    notes: Vec<String>,
    // Notes of a segment frame
    strategy: Option<(SearchStrategy, StrategyReason)>,
    cardinality: Option<CardinalityProfile>,
    /// Payload keys of the primary clauses of the estimation: the indexes used.
    primary_keys: Option<Vec<String>>,
    actual_cardinality: Option<u64>,
    full_scan_threshold: Option<u64>,
}

impl Frame {
    pub(crate) fn record_phase(&self, name: &'static str, elapsed: Duration) {
        self.record_phase_profile(PhaseProfile {
            name: name.to_string(),
            time_ms: millis(elapsed),
            calls: 1,
            major_page_faults: None,
            disk_read_bytes: None,
        });
    }

    /// [`Self::record_phase`] of a phase that ran on one thread, with its OS I/O counters.
    pub(crate) fn record_phase_io(&self, name: &'static str, elapsed: Duration, io: ThreadIo) {
        let ThreadIo {
            major_page_faults,
            disk_read_bytes,
        } = io;
        self.record_phase_profile(PhaseProfile {
            name: name.to_string(),
            time_ms: millis(elapsed),
            calls: 1,
            major_page_faults,
            disk_read_bytes,
        });
    }

    pub(crate) fn note_strategy(&self, strategy: SearchStrategy, reason: StrategyReason) {
        self.0.lock().strategy = Some((strategy, reason));
    }

    pub(crate) fn note_cardinality(
        &self,
        cardinality: CardinalityProfile,
        primary_keys: Vec<String>,
    ) {
        let mut data = self.0.lock();
        data.cardinality = Some(cardinality);
        data.primary_keys = Some(primary_keys);
    }

    pub(crate) fn note_actual_cardinality(&self, count: u64) {
        let mut data = self.0.lock();
        *data.actual_cardinality.get_or_insert(0) += count;
    }

    pub(crate) fn note_full_scan_threshold(&self, threshold: u64) {
        self.0.lock().full_scan_threshold = Some(threshold);
    }

    pub(crate) fn push_note(&self, note: String) {
        let mut data = self.0.lock();
        if !data.notes.contains(&note) {
            data.notes.push(note);
        }
    }

    pub(crate) fn push_shard(&self, shard: ShardProfile) {
        self.0.lock().shards.push(shard);
    }

    fn push_segment(&self, segment: SegmentProfile) {
        let mut data = self.0.lock();
        if data.segments.len() < MAX_SEGMENTS_PER_SHARD {
            data.segments.push(segment);
        } else {
            data.dropped_segments += 1;
        }
    }

    pub(crate) fn push_remote(
        &self,
        shard_id: u32,
        peer_id: u64,
        operation: &'static str,
        round_trip: Duration,
        remote: QueryProfile,
    ) {
        let QueryProfile {
            version: _,
            mode: _,
            total_time_ms,
            phases,
            shards,
            notes, // recorded notes only: derived notes are derived again on this node
        } = remote;
        let network_time_ms = (millis(round_trip) - total_time_ms).max(0.0);
        for note in notes {
            self.push_note(note);
        }
        let mut data = self.0.lock();
        if shards.is_empty() {
            data.shards.push(ShardProfile {
                shard_id,
                peer_id: Some(peer_id),
                location: ShardLocation::Remote,
                operation: operation.to_string(),
                time_ms: total_time_ms,
                network_time_ms: Some(network_time_ms),
                phases,
                segments: Vec::new(),
            });
            return;
        }
        for mut shard in shards {
            shard.location = ShardLocation::Remote;
            shard.peer_id = shard.peer_id.or(Some(peer_id));
            shard.network_time_ms = Some(network_time_ms);
            data.shards.push(shard);
        }
    }

    fn take_shard_parts(&self) -> (Vec<PhaseProfile>, Vec<SegmentProfile>, u64, Vec<String>) {
        let mut data = self.0.lock();
        (
            std::mem::take(&mut data.phases),
            std::mem::take(&mut data.segments),
            std::mem::take(&mut data.dropped_segments),
            std::mem::take(&mut data.notes),
        )
    }

    fn take_segment_notes(&self) -> SegmentNotes {
        let mut data = self.0.lock();
        SegmentNotes {
            strategy: data.strategy.take(),
            cardinality: data.cardinality.take(),
            primary_keys: data.primary_keys.take(),
            actual_cardinality: data.actual_cardinality.take(),
            full_scan_threshold: data.full_scan_threshold.take(),
        }
    }

    pub(crate) fn snapshot_request(&self, mode: ProfileMode, elapsed: Duration) -> QueryProfile {
        let data = self.0.lock();
        let mut notes = data.notes.clone();
        if data.dropped_segments > 0 {
            notes.push(format!(
                "{} segment entries were omitted from the profile",
                data.dropped_segments
            ));
        }
        QueryProfile {
            version: PROFILE_SCHEMA_VERSION,
            mode,
            total_time_ms: millis(elapsed),
            phases: data.phases.clone(),
            shards: data.shards.clone(),
            notes,
        }
    }
}

/// A shard operation being profiled. Create with [`ShardScope::start`].
pub struct ShardScope {
    ctx: AmbientContext,
    parent: Frame,
    handle: ProfileHandle,
    shard_id: u32,
    operation: &'static str,
    started: Instant,
}

impl ShardScope {
    /// Start profiling a local shard operation; `None` when the current scope is not profiled.
    pub fn start(shard_id: u32, operation: &'static str) -> Option<Self> {
        super::with_handle(|handle| handle.clone()).and_then(|parent| {
            let ctx = crate::ambient::slot::with_context_unchecked(|ctx| ctx.cloned())?;
            let handle = parent.child();
            Some(Self {
                ctx: ctx.fork(handle.clone()),
                parent: parent.frame,
                handle,
                shard_id,
                operation,
                started: Instant::now(),
            })
        })
    }

    /// Run `future` in this shard scope, then record the shard profile.
    pub async fn run<F: Future>(self, future: F) -> F::Output {
        let output = future.measured(self.ctx.clone()).await;
        self.finish();
        output
    }

    fn finish(self) {
        let Self {
            ctx: _,
            parent,
            handle,
            shard_id,
            operation,
            started,
        } = self;
        let (phases, segments, dropped, notes) = handle.frame.take_shard_parts();
        for note in notes {
            parent.push_note(note);
        }
        if dropped > 0 {
            parent.push_note(format!(
                "{dropped} segment entries of shard {shard_id} were omitted from the profile"
            ));
        }
        parent.push_shard(ShardProfile {
            shard_id,
            peer_id: super::this_peer_id(),
            location: ShardLocation::Local,
            operation: operation.to_string(),
            time_ms: millis(started.elapsed()),
            network_time_ms: None,
            phases,
            segments,
        });
    }
}

struct SegmentNotes {
    strategy: Option<(SearchStrategy, StrategyReason)>,
    cardinality: Option<CardinalityProfile>,
    primary_keys: Option<Vec<String>>,
    actual_cardinality: Option<u64>,
    full_scan_threshold: Option<u64>,
}

/// Static facts about a segment search, known before it runs.
pub struct SegmentInfo {
    pub segment_id: String,
    pub appendable: bool,
    pub indexed: bool,
    pub points: usize,
    pub operation: &'static str,
    pub vector_name: Option<String>,
    pub batch_size: usize,
    pub limit: Option<usize>,
    /// `Some` when the search is filtered: payload keys of the filter without a payload
    /// index in this segment.
    pub unindexed_keys: Option<Vec<String>>,
}

/// One segment search being profiled.
pub struct SegmentScope {
    handoff: Handoff,
    parent: Frame,
    handle: ProfileHandle,
    info: SegmentInfo,
    started: Instant,
    /// OS I/O counters at the start, valid only on `thread`.
    started_io: ThreadIo,
    thread: std::thread::ThreadId,
}

impl SegmentScope {
    /// Start profiling a segment search that runs in `handoff`; `None` when not profiled.
    /// `info` is only built when profiled.
    pub fn start(handoff: &Handoff, info: impl FnOnce() -> SegmentInfo) -> Option<Self> {
        let ctx = handoff.context()?;
        let parent = ctx.profile()?.clone();
        let handle = parent.child();
        let forked = ctx.fork(handle.clone());
        let handoff = match handoff {
            Handoff::Measured(_) => Handoff::Measured(forked),
            Handoff::Unmeasured(_) => Handoff::Unmeasured(Some(forked)),
        };
        Some(Self {
            handoff,
            parent: parent.frame,
            handle,
            info: info(),
            started: Instant::now(),
            started_io: ThreadIo::now(),
            thread: std::thread::current().id(),
        })
    }

    /// The scope to run the segment search in.
    pub fn handoff(&self) -> &Handoff {
        &self.handoff
    }

    /// Record the segment profile into the shard frame.
    pub fn finish(self) {
        let Self {
            handoff,
            parent,
            handle,
            info,
            started,
            started_io,
            thread,
        } = self;
        let time_ms = millis(started.elapsed());
        // Per-thread counters: only meaningful if the scope ended where it started
        let io = (std::thread::current().id() == thread).then(|| ThreadIo::now().since(started_io));
        let hw = handoff
            .context()
            .map(AmbientContext::hw_data)
            .unwrap_or_default();
        let SegmentNotes {
            strategy,
            cardinality,
            primary_keys,
            actual_cardinality,
            full_scan_threshold,
        } = handle.frame.take_segment_notes();
        // Nested shard data cannot happen; nested phases are folded into the shard
        let (phases, _, _, notes) = handle.frame.take_shard_parts();
        for phase in phases {
            parent.record_phase_profile(phase);
        }
        for note in notes {
            parent.push_note(note);
        }

        let SegmentInfo {
            segment_id,
            appendable,
            indexed,
            points,
            operation,
            vector_name,
            batch_size,
            limit,
            unindexed_keys,
        } = info;

        let filter = unindexed_keys.map(|unindexed_keys| {
            // No primary clause: candidates are all points of the segment
            let full_payload_scan = primary_keys.as_ref().is_some_and(Vec::is_empty) && points > 0;
            FilterProfile {
                estimated_cardinality: cardinality,
                actual_cardinality,
                indexed_keys: primary_keys.unwrap_or_default(),
                unindexed_keys,
                full_payload_scan,
            }
        });

        parent.push_segment(SegmentProfile {
            segment_id,
            appendable,
            indexed,
            points: points as u64,
            operation: operation.to_string(),
            vector_name,
            batch_size: batch_size as u64,
            limit: limit.map(|limit| limit as u64),
            strategy: strategy.map(|(strategy, _)| strategy),
            strategy_reason: strategy.map(|(_, reason)| reason),
            full_scan_threshold,
            filter,
            time_ms,
            counters: counters_from_hw(hw, io),
        });
    }
}

impl Frame {
    fn record_phase_profile(&self, phase: PhaseProfile) {
        let mut data = self.0.lock();
        match data.phases.iter_mut().find(|p| p.name == phase.name) {
            Some(existing) => {
                let add = |a: Option<u64>, b: Option<u64>| match (a, b) {
                    (Some(a), Some(b)) => Some(a + b),
                    (a, b) => a.or(b),
                };
                existing.time_ms += phase.time_ms;
                existing.calls += phase.calls;
                existing.major_page_faults =
                    add(existing.major_page_faults, phase.major_page_faults);
                existing.disk_read_bytes = add(existing.disk_read_bytes, phase.disk_read_bytes);
            }
            None => data.phases.push(phase),
        }
    }
}

fn counters_from_hw(hw: HardwareData, io: Option<ThreadIo>) -> SegmentCounters {
    let m = |metric: HwMetric| hw[metric] as u64;
    let ThreadIo {
        major_page_faults,
        disk_read_bytes,
    } = io.unwrap_or(ThreadIo {
        major_page_faults: None,
        disk_read_bytes: None,
    });
    SegmentCounters {
        vector_scores: m(HwMetric::VectorScores),
        quantized_vector_scores: m(HwMetric::QuantizedVectorScores),
        vector_io_read_bytes: m(HwMetric::VectorIoRead),
        payload_io_read_bytes: m(HwMetric::PayloadIoRead),
        payload_index_io_read_bytes: m(HwMetric::PayloadIndexIoRead),
        remote_io_cached_read_bytes: m(HwMetric::RemoteIoCachedRead),
        remote_io_non_cached_read_bytes: m(HwMetric::RemoteIoNonCachedRead),
        major_page_faults,
        disk_read_bytes,
    }
}

pub(crate) fn remote_shard_without_profile(
    shard_id: u32,
    peer_id: u64,
    operation: &'static str,
    round_trip: Duration,
) -> ShardProfile {
    ShardProfile {
        shard_id,
        peer_id: Some(peer_id),
        location: ShardLocation::Remote,
        operation: operation.to_string(),
        time_ms: millis(round_trip),
        network_time_ms: None,
        phases: Vec::new(),
        segments: Vec::new(),
    }
}

fn millis(duration: Duration) -> f64 {
    // Microsecond resolution is plenty and keeps the output short
    (duration.as_micros() as f64) / 1000.0
}

/// Findings that answer "why is this slow" without reading every number.
pub(crate) fn derive_notes(profile: &QueryProfile) -> Vec<String> {
    let mut notes = profile.notes.clone();

    let segments = || profile.shards.iter().flat_map(|shard| &shard.segments);

    // Filters that miss a payload index
    let mut unindexed: BTreeSet<&str> = BTreeSet::new();
    let mut full_scans = 0usize;
    for segment in segments() {
        if let Some(filter) = &segment.filter {
            unindexed.extend(filter.unindexed_keys.iter().map(String::as_str));
            if filter.full_payload_scan {
                full_scans += 1;
            }
        }
    }
    if !unindexed.is_empty() {
        let keys = unindexed.into_iter().collect::<Vec<_>>().join("`, `");
        notes.push(format!(
            "Filter conditions on `{keys}` have no payload index: they are checked by reading \
             the payload of each candidate point. Consider creating a payload index."
        ));
    }
    if full_scans > 0 {
        notes.push(format!(
            "{full_scans} segment search(es) found no payload index to narrow down the filter \
             and considered every point of the segment."
        ));
    }

    // Unindexed segments, e.g. during indexing
    let unindexed_segments: Vec<&SegmentProfile> = segments()
        .filter(|segment| segment.operation == "search" && !segment.indexed && segment.points > 0)
        .collect();
    if !unindexed_segments.is_empty() {
        let points: u64 = unindexed_segments.iter().map(|s| s.points).sum();
        let time_ms: f64 = unindexed_segments.iter().map(|s| s.time_ms).sum();
        let total_ms: f64 = segments()
            .filter(|s| s.operation == "search")
            .map(|s| s.time_ms)
            .sum();
        let share = if total_ms > 0.0 {
            time_ms / total_ms * 100.0
        } else {
            0.0
        };
        let share = match profile.mode {
            // Nothing was scored: the time share says nothing about the real request
            ProfileMode::Explain => String::new(),
            ProfileMode::Analyze => format!(" ({share:.0}% of segment search time)"),
        };
        notes.push(format!(
            "{} segment(s) with {points} points have no vector index and are searched by brute \
             force{share}. This is expected for segments smaller than `indexing_threshold` and \
             while the optimizer is indexing; check `optimizer_status` and `indexing_threshold`.",
            unindexed_segments.len()
        ));
    }

    // Cold page cache: reads that waited for the disk
    let cold: Vec<&SegmentProfile> = segments()
        .filter(|segment| segment.counters.major_page_faults.unwrap_or(0) > 0)
        .collect();
    let phases = || {
        profile
            .phases
            .iter()
            .chain(profile.shards.iter().flat_map(|shard| &shard.phases))
    };
    let segment_faults: u64 = cold
        .iter()
        .filter_map(|s| s.counters.major_page_faults)
        .sum();
    let phase_faults: u64 = phases().filter_map(|p| p.major_page_faults).sum();
    if segment_faults + phase_faults > 0 {
        let bytes: u64 = cold
            .iter()
            .filter_map(|s| s.counters.disk_read_bytes)
            .chain(phases().filter_map(|p| p.disk_read_bytes))
            .sum();
        let mut places = Vec::new();
        if !cold.is_empty() {
            places.push(format!("{} segment(s)", cold.len()));
        }
        for phase in phases().filter(|p| p.major_page_faults.unwrap_or(0) > 0) {
            places.push(format!("`{}`", phase.name));
        }
        notes.push(format!(
            "Waited for the disk: {} major page fault(s) read {:.1} MiB, in {}. The data was not \
             in the page cache, e.g. after a restart or under memory pressure; repeated requests \
             are faster. Consider more RAM, or keeping the hot data in memory (`on_disk: false`, \
             quantization with `always_ram: true`).",
            segment_faults + phase_faults,
            bytes as f64 / (1024.0 * 1024.0),
            places.join(", "),
        ));
    }

    // Exact searches
    if segments().any(|segment| segment.strategy == Some(SearchStrategy::Exact)) {
        notes.push(
            "`exact: true` was requested: indexes and quantization are bypassed.".to_string(),
        );
    }

    // Network-dominated distributed requests
    for shard in &profile.shards {
        if let Some(network) = shard.network_time_ms
            && network > shard.time_ms
            && network > 5.0
        {
            notes.push(format!(
                "Shard {} on peer {}: {network:.1} ms of {:.1} ms were spent outside the remote \
                 shard (network, serialization, queueing).",
                shard.shard_id,
                shard.peer_id.unwrap_or_default(),
                network + shard.time_ms,
            ));
        }
    }

    if profile.mode == ProfileMode::Explain {
        notes.push(
            "Explain mode: strategies and cardinality estimates are real, but no vector was \
             scored. Stages that consume prefetch results (rescoring, fusion, grouping, \
             order_by) have no candidates and are not planned."
                .to_string(),
        );
    }

    notes
}
