use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::universal_io::{IsNotFound as _, UniversalReadFsAsync};
use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use segment::id_tracker::delete_versions::DeleteGating;
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;
use segment::types::SeqNumberType;
use uuid::Uuid;

use crate::read_only::load::{load_segments_parallel, reload_segments_parallel};
use crate::read_only::{ListedSegment, ReadOnlyEdgeShard};
use crate::{EdgeConfig, EdgeScope, Scoped};

/// How a single [`live_reload_attempt`](ReadOnlyEdgeShard::live_reload_attempt) ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveReloadOutcome {
    /// The attempt fully converged on its manifest snapshot.
    Complete,
    /// Segments vanished benignly mid-attempt (the leader removed them, confirmed
    /// against a re-read manifest); re-run against the fresh manifest to pick up
    /// their replacements.
    ManifestChanged,
}

impl<S: UniversalReadExt + 'static> Scoped<'_, ReadOnlyEdgeShard<S>> {
    /// [`ReadOnlyEdgeShard::live_reload`] in this scope.
    ///
    /// When cancelled, the shard keeps whatever state was reached: every swap is atomic and every
    /// segment reload is applied under its write lock, so reads stay consistent and the next
    /// live_reload continues from there.
    pub fn live_reload(&self) -> OperationResult<LiveReloadOutcome>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let _scope = self.scope.handoff().into_scope();
        self.shard.live_reload_impl(self.scope.is_stopped(), None)
    }

    /// [`ReadOnlyEdgeShard::live_reload_up_to`] in this scope.
    pub fn live_reload_up_to(&self, waterline: SeqNumberType) -> OperationResult<LiveReloadOutcome>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let _scope = self.scope.handoff().into_scope();
        self.shard
            .live_reload_impl(self.scope.is_stopped(), Some(waterline))
    }
}

impl<S: UniversalReadExt + 'static> ReadOnlyEdgeShard<S> {
    /// Live-reload the follower to the leader's current on-disk state.
    ///
    /// Caller-driven (never self-triggered), mirroring `ReadOnlySegment::live_reload`: the host
    /// owns the cadence (a timer, an explicit call after a known leader flush, or an FS watch).
    /// Eventually consistent — a point becomes visible once the leader has flushed it to the
    /// segment files and a subsequent `live_reload` has run.
    ///
    /// Leader-side segment churn is absorbed: a segment whose files vanish while its live-reload
    /// runs is checked against a re-read manifest, and if the leader indeed removed it, the segment
    /// is dropped and the live_reload re-runs (bounded) to pick up its replacements. A segment whose
    /// reload fails after its files were fetched is reopened in place. In-place CreateIndex /
    /// DropIndex on a surviving segment is applied via config reload before point-delta reload.
    /// `Err` therefore means the shard genuinely needs attention:
    /// - a segment's files could not be fetched: its reload was skipped, it keeps serving its
    ///   previous state, and the next live_reload retries it;
    /// - a segment failed to reload and could not be reopened either, so it serves a partially
    ///   applied reload;
    /// - a segment's essential files are missing while the manifest still lists it.
    /// - a newly listed segment failed to load for a reason other than missing files.
    pub fn live_reload(&self) -> OperationResult<LiveReloadOutcome>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        self.with(&EdgeScope::new()).live_reload()
    }

    #[deprecated(note = "use `shard.with(&scope).live_reload()` with a cancellable `EdgeScope`")]
    pub fn live_reload_with_cancellation(
        &self,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<LiveReloadOutcome>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        self.with(&EdgeScope::new().with_cancellation(is_stopped))
            .live_reload()
    }

    #[deprecated(note = "use `live_reload`")]
    pub fn live_reload_with(&self) -> OperationResult<LiveReloadOutcome>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        self.live_reload_impl(&AtomicBool::new(false), None)
    }

    /// [`live_reload`](Self::live_reload) of a follower opened with a waterline
    /// ([`open_with_waterline`](Self::open_with_waterline)), raising it to `waterline` first.
    ///
    /// `waterline` is the newest version whose writes are all durable and stored in segments the
    /// manifest lists: the updater's committed cursor, taken before this call. Every segment file
    /// the reload reads holds the writes up to it, so once the reload has held and reloaded every
    /// listed segment, it applies the deletes up to it, as its last step. Newer deletes stay held.
    /// A waterline lower than one passed before changes nothing, since waterlines can arrive out of
    /// order. On a follower opened without one, the waterline is ignored and deletes apply at once.
    ///
    /// A plain [`live_reload`](Self::live_reload) of such a follower reuses the highest waterline
    /// passed so far.
    pub fn live_reload_up_to(&self, waterline: SeqNumberType) -> OperationResult<LiveReloadOutcome>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        self.with(&EdgeScope::new()).live_reload_up_to(waterline)
    }

    #[deprecated(
        note = "use `shard.with(&scope).live_reload_up_to(waterline)` with a cancellable `EdgeScope`"
    )]
    pub fn live_reload_up_to_with_cancellation(
        &self,
        waterline: SeqNumberType,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<LiveReloadOutcome>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        self.with(&EdgeScope::new().with_cancellation(is_stopped))
            .live_reload_up_to(waterline)
    }

    pub(super) fn live_reload_impl(
        &self,
        is_stopped: &AtomicBool,
        waterline: Option<SeqNumberType>,
    ) -> OperationResult<LiveReloadOutcome>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        check_process_stopped(is_stopped)?;
        let _live_reload_guard = self.live_reload_lock.lock();
        check_process_stopped(is_stopped)?;

        // The waterline is taken before anything is read: every file read from here on holds the
        // writes up to it
        let waterline = self.waterline.as_ref().map(|state| {
            let mut state = state.lock();
            state.highest = state.highest.max(waterline.unwrap_or(0));
            state.highest
        });

        // A benign mid-attempt segment removal re-runs the attempt against the fresh manifest;
        // bound the re-runs so a leader churning segments faster than the follower converges
        // cannot spin this loop forever.
        const MAX_ATTEMPTS: usize = 3;

        let mut listed = None;
        let mut outcome = LiveReloadOutcome::ManifestChanged;
        for _ in 0..MAX_ATTEMPTS {
            let attempt = self.live_reload_attempt(is_stopped)?;
            listed = Some(attempt.listed);
            outcome = attempt.outcome;
            match outcome {
                LiveReloadOutcome::Complete => break,
                LiveReloadOutcome::ManifestChanged => {}
            }
        }

        match outcome {
            LiveReloadOutcome::Complete => {}
            LiveReloadOutcome::ManifestChanged => {
                // Attempts exhausted without converging. The shard is still consistent — every
                // completed swap was atomic — it just may not reflect the newest segments yet;
                // the next live_reload continues from here.
                log::warn!(
                    "shard live_reload did not converge after {MAX_ATTEMPTS} attempts \
                     (leader keeps replacing segments); serving the state reached so far",
                );
            }
        }

        if let Some(waterline) = waterline {
            let listed = listed.expect("at least one attempt ran");
            self.finish_gated_reload(&outcome, &listed, waterline)?;
        }
        Ok(outcome)
    }

    /// The last steps of a reload of a follower that gates deletes. A complete reload, one that
    /// converged and holds every listed segment, raises the gate to its `waterline` and drops the
    /// segments the manifest no longer lists. Either way, the held deletes the gate covers are
    /// released in every segment, now that all of them have loaded their new data.
    fn finish_gated_reload(
        &self,
        outcome: &LiveReloadOutcome,
        listed: &HashMap<Uuid, ListedSegment>,
        waterline: SeqNumberType,
    ) -> OperationResult<()>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let complete = match outcome {
            LiveReloadOutcome::Complete => {
                let holder = self.segments.read();
                listed.keys().all(|uuid| holder.contains(uuid))
            }
            LiveReloadOutcome::ManifestChanged => false,
        };

        let gate = {
            let mut state = self
                .waterline
                .as_ref()
                .expect("only a follower with a waterline gates deletes")
                .lock();
            if complete {
                state.gate = state.gate.max(waterline);
            }
            state.gate
        };

        let mut first_error = None;
        let segments = self.segments.read().read_handles();
        for segment in segments {
            if !segment.read().releases_held_deletes(gate) {
                continue;
            }
            if let Err(err) = segment.write().release_held_deletes(gate) {
                log::error!("releasing held deletes failed: {err}");
                first_error.get_or_insert(err);
            }
        }

        if complete {
            let dropped = {
                let mut holder = self.segments.write();
                let before = holder.len();
                holder.remove_missing(listed);
                holder.len() != before
            };
            if dropped {
                self.derive_config();
            }
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// How segments opened now treat versioned deletes.
    fn delete_gating(&self) -> DeleteGating {
        match &self.waterline {
            Some(state) => DeleteGating::Held {
                gate: state.lock().gate,
            },
            None => DeleteGating::Immediate,
        }
    }

    /// Re-derive the config from the current segments — a read-only follower has no
    /// edge_config.json, so the segments are the source of truth. Folded over all segments in UUID
    /// order, so the derivation is deterministic and a segment carrying no information about a
    /// parameter never masks one that does. No-op for an empty shard (the previous snapshot stays
    /// in place until segments appear).
    fn derive_config(&self) {
        let derived = {
            let holder = self.segments.read();
            let mut uuids = holder.uuids();
            uuids.sort_unstable();
            uuids
                .into_iter()
                .filter_map(|uuid| holder.segment_arc(&uuid))
                .fold(None, |acc, segment| {
                    Some(EdgeConfig::fold_from_segment_config(
                        acc,
                        &segment.read().segment_config,
                    ))
                })
        };
        if let Some(derived) = derived {
            *self.config.write() = Arc::new(derived);
        }
    }

    /// One live_reload pass over a single manifest snapshot.
    ///
    /// Completes as much as possible before reporting problems: newly-appeared segments are
    /// swapped in and every survivor is live-reloaded (they are independent) even when one of
    /// them fails. Not-found failures are then resolved against a re-read manifest — a segment
    /// the leader removed mid-attempt is dropped and reported as [`LiveReloadOutcome::ManifestChanged`]
    /// so the caller re-runs against the fresh manifest; one whose files are missing while the
    /// manifest still lists it escalates. Any other reload failure escalates after the loop.
    ///
    /// A follower that gates deletes keeps the segments the manifest no longer lists, and reloads
    /// them with the others, until a complete reload drops them, see
    /// [`finish_gated_reload`](Self::finish_gated_reload).
    fn live_reload_attempt(&self, is_stopped: &AtomicBool) -> OperationResult<Attempt>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        check_process_stopped(is_stopped)?;
        let gating = self.delete_gating();
        let keep_unlisted = match gating {
            DeleteGating::Held { gate: _ } => true,
            DeleteGating::Immediate => false,
        };

        // 1. Snapshot the current on-disk segment set (backend-specific; see `SegmentEnumerator`).
        let on_disk = self.enumerator.list_segments()?;
        check_process_stopped(is_stopped)?;

        // 2. Load newly-appeared segments in parallel (outside the holder lock), then under the lock
        //    add them and drop removed ones, and collect the survivors to live_reload *after*
        //    releasing the lock — so reads only block during the cheap add/drop, not during load
        //    or reload. The manifest is superset-biased, so segments whose files are not found yet are
        //    skipped by `load_segments_parallel` and retried on the next live_reload; any other
        //    load failure escalates.
        let new_segments: Vec<(Uuid, PathBuf)> = {
            let holder = self.segments.read();
            on_disk
                .iter()
                .filter(|(uuid, _)| !holder.contains(uuid))
                .map(|(uuid, listing)| (*uuid, listing.path.clone()))
                .collect()
        };
        let loaded = load_segments_parallel::<S>(
            &self.load_pool,
            &self.fs,
            new_segments,
            self.load_profile.as_ref(),
            gating,
            is_stopped,
        )?;

        // The swap below and the config re-derivation that follows it are one indivisible step:
        // a config lagging behind the segment set must never be observable.
        check_process_stopped(is_stopped)?;

        let survivors: Vec<(Uuid, Arc<RwLock<ReadOnlySegment<S>>>)> = {
            let mut holder = self.segments.write();

            // Segments present before this live_reload that still exist on disk, and the unlisted
            // ones a follower that gates deletes keeps.
            let survivor_uuids: Vec<Uuid> = holder
                .uuids()
                .into_iter()
                .filter(|uuid| keep_unlisted || on_disk.contains_key(uuid))
                .collect();

            // Add before drop: when an optimization migrates points from old to new segments, both
            // must be momentarily visible so migrated points never disappear.
            for (uuid, segment) in loaded {
                let appendable = segment.segment_config.is_appendable();
                holder.insert(uuid, appendable, Arc::new(RwLock::new(segment)));
            }

            if !keep_unlisted {
                holder.remove_missing(&on_disk);
            }

            survivor_uuids
                .into_iter()
                .filter_map(|uuid| Some((uuid, holder.segment_arc(&uuid)?)))
                .collect()
        };

        // 3. Re-derive the config from the current segments, see `derive_config`.
        self.derive_config();
        check_process_stopped(is_stopped)?;

        // 4. Live-reload survivors to assimilate new appends and deletes from data.
        let results = reload_segments_parallel(
            &self.load_pool,
            &self.fs,
            survivors,
            self.load_profile.as_ref(),
            gating,
            is_stopped,
        )?;

        let mut not_found: Vec<(Uuid, OperationError)> = Vec::new();
        let mut first_hard_error: Option<OperationError> = None;
        for (uuid, result) in results {
            match result {
                Ok(()) => {}
                // An essential file is gone; whether that is benign (the leader removed the
                // segment while we reloaded it) is decided against a re-read manifest below.
                Err(err) if err.is_not_found() => not_found.push((uuid, err)),
                Err(err) => {
                    log::error!("live_reload of segment {uuid} failed: {err}");
                    first_hard_error.get_or_insert(err);
                }
            }
        }

        // 5. Resolve not-found failures against a re-read manifest: gone from the manifest means
        //    the leader removed the segment mid-attempt — drop it and re-run to pick up its
        //    replacements; still listed means its essential files are genuinely missing — escalate.
        let outcome = if not_found.is_empty() {
            LiveReloadOutcome::Complete
        } else {
            check_process_stopped(is_stopped)?;
            let fresh = self.enumerator.list_segments()?;
            let mut gone: Vec<Uuid> = Vec::new();
            let mut still_listed_error: Option<OperationError> = None;
            for (uuid, err) in not_found {
                if fresh.contains_key(&uuid) {
                    log::error!(
                        "segment {uuid} is listed in the manifest but its files are missing: {err}",
                    );
                    still_listed_error.get_or_insert(err);
                } else {
                    log::debug!("segment {uuid} was removed by the leader during live_reload");
                    gone.push(uuid);
                }
            }

            // Drop the removed segments even when escalating below: they are confirmed gone, so
            // keeping them would only leave handles to vanished files.
            if !gone.is_empty() {
                let mut holder = self.segments.write();
                for uuid in &gone {
                    holder.remove(uuid);
                }
            }

            if let Some(err) = still_listed_error {
                return Err(err);
            }
            LiveReloadOutcome::ManifestChanged
        };

        if let Some(err) = first_hard_error {
            return Err(err);
        }
        check_process_stopped(is_stopped)?;
        Ok(Attempt {
            outcome,
            listed: on_disk,
        })
    }
}

/// What one [`live_reload_attempt`](ReadOnlyEdgeShard::live_reload_attempt) did.
struct Attempt {
    outcome: LiveReloadOutcome,
    /// The segments the manifest listed for it.
    listed: HashMap<Uuid, ListedSegment>,
}
