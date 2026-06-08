use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::btree_map::Entry;
use std::iter;

use byteorder::LittleEndian;
#[cfg(test)]
use common::bitpacking::make_bitmask;
use common::bitvec::{BitSlice, BitSliceExt as _, BitVec};
use common::types::{DeferredBehavior, PointOffsetType};
use itertools::{Either, Itertools};
#[cfg(test)]
use rand::RngExt;
use rand::distr::Distribution;
#[cfg(test)]
use rand::rngs::StdRng;
#[cfg(test)]
use rand::seq::SliceRandom as _;
use uuid::Uuid;

use crate::types::PointIdType;

/// Used endianness for storing PointMapping-files.
pub type FileEndianess = LittleEndian;

#[derive(Clone, PartialEq, Default, Debug)]
pub struct PointMappings {
    /// `deleted` specifies which points of internal_to_external was deleted.
    /// It is possible that `deleted` can be longer or shorter than `internal_to_external`.
    /// - if `deleted` is longer, then extra bits should be set to `false` and ignored.
    /// - if `deleted` is shorter, then extra indices are as if the bits were set to `true`.
    deleted: BitVec,
    /// Internal-id-indexed inverse mapping; no active-vs-deferred bias —
    /// each slot just holds whatever external id last linked to it. An
    /// active+deferred pair for the same ext (a shadowed active) occupies
    /// two slots with the same value. Always gate reads on `deleted`:
    /// `drop` resets entries to `NumId(u64::MAX)` but `set_link`'s
    /// same-track replacement leaves the prior ext in place.
    internal_to_external: Vec<PointIdType>,

    // Active head per external id (internal_id < deferred cutoff, or no cutoff).
    // Having two separate maps allows us iterating only over one type at a time without having to filter.
    external_to_internal_num: BTreeMap<u64, PointOffsetType>,
    external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType>,

    // Deferred head per external id (internal_id >= deferred cutoff). Same external
    // id can appear in both maps at once — see `shadowed` for that case. On a
    // segment without a deferred cutoff these stay empty.
    external_to_internal_num_deferred: BTreeMap<u64, PointOffsetType>,
    external_to_internal_uuid_deferred: BTreeMap<Uuid, PointOffsetType>,

    /// Bit set on active internal ids whose external id also has a deferred
    /// head. Read-side iteration in `WithDeferred` mode uses this to skip the
    /// stale active version when a deferred override exists, avoiding
    /// duplicate-by-external yields.
    ///
    /// PR A: declared and partition-checked at construction; no writer yet
    /// flips bits during mutations (PR B routes deferred writes through
    /// here).
    shadowed: BitVec,

    /// Points with internal id >= this value are hidden from reads.
    /// Only set for appendable segments with deferred points.
    deferred_internal_id: Option<PointOffsetType>,

    /// Number of deleted deferred points. Maintained incrementally so we can
    /// derive the visible deferred count without re-scanning the deleted bitslice.
    deferred_deleted_count: usize,
}

impl PointMappings {
    pub fn new(
        deleted: BitVec,
        internal_to_external: Vec<PointIdType>,
        mut external_to_internal_num: BTreeMap<u64, PointOffsetType>,
        mut external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType>,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> Self {
        // Partition the loaded single-map mappings into active (id < cutoff)
        // and deferred (id >= cutoff). Persisted format is unchanged — the
        // split is purely runtime. With no cutoff every entry stays active.
        let mut external_to_internal_num_deferred = BTreeMap::new();
        let mut external_to_internal_uuid_deferred = BTreeMap::new();
        if let Some(cutoff) = deferred_internal_id {
            external_to_internal_num.retain(|&k, &mut v| {
                if v >= cutoff {
                    external_to_internal_num_deferred.insert(k, v);
                    false
                } else {
                    true
                }
            });
            external_to_internal_uuid.retain(|&k, &mut v| {
                if v >= cutoff {
                    external_to_internal_uuid_deferred.insert(k, v);
                    false
                } else {
                    true
                }
            });
        }
        // Shadowed bits: any active id whose external also has a deferred
        // head. Impossible from a fresh load (each ext was in a single map),
        // but compute it so mutations land on a consistent starting state.
        // Grown lazily — out-of-bounds bits are treated as `false` by
        // readers, so the empty default is a valid no-shadow state.
        let mut shadowed = BitVec::new();
        let mut mark_shadow = |active_id: PointOffsetType| {
            let active_id = active_id as usize;
            if active_id >= shadowed.len() {
                shadowed.resize(active_id + 1, false);
            }
            shadowed.set(active_id, true);
        };
        for k in external_to_internal_num_deferred.keys() {
            if let Some(active_id) = external_to_internal_num.get(k) {
                mark_shadow(*active_id);
            }
        }
        for k in external_to_internal_uuid_deferred.keys() {
            if let Some(active_id) = external_to_internal_uuid.get(k) {
                mark_shadow(*active_id);
            }
        }

        let deferred_deleted_count = deferred_internal_id
            .map(|deferred_from| {
                let total = deleted.len();
                if total <= deferred_from as usize {
                    0
                } else {
                    deleted[deferred_from as usize..total].count_ones()
                }
            })
            .unwrap_or(0);
        Self {
            deleted,
            internal_to_external,
            external_to_internal_num,
            external_to_internal_uuid,
            external_to_internal_num_deferred,
            external_to_internal_uuid_deferred,
            shadowed,
            deferred_internal_id,
            deferred_deleted_count,
        }
    }

    /// ToDo: this function is temporary and should be removed before PR is merged
    pub fn deconstruct(
        self,
    ) -> (
        BitVec,
        Vec<PointIdType>,
        BTreeMap<u64, PointOffsetType>,
        BTreeMap<Uuid, PointOffsetType>,
    ) {
        (
            self.deleted,
            self.internal_to_external,
            self.external_to_internal_num,
            self.external_to_internal_uuid,
        )
    }

    /// Number of points, excluding deleted ones.
    ///
    /// Sum of entries across both tracks. A shadowed ext (active + deferred
    /// for the same external id) contributes two slots — matching the two
    /// non-tombstoned internal ids it actually occupies, and consistent with
    /// the cross-segment shadow case where the source segment keeps its copy
    /// while the deferred head lives in the appendable segment.
    pub(crate) fn available_point_count(&self) -> usize {
        self.external_to_internal_num.len()
            + self.external_to_internal_uuid.len()
            + self.external_to_internal_num_deferred.len()
            + self.external_to_internal_uuid_deferred.len()
    }

    pub(crate) fn deleted(&self) -> &BitSlice {
        &self.deleted
    }

    /// Internal id for `external_id` with explicit deferred semantics.
    ///
    /// - [`DeferredBehavior::VisibleOnly`] returns the active head only. An ext
    ///   that only has a deferred head returns `None` — query paths see no
    ///   deferred mutations at all.
    /// - [`DeferredBehavior::WithDeferred`] prefers the deferred head (the
    ///   latest mutation) and falls back to active for points that never
    ///   crossed the cutoff. Yields each ext at most once.
    pub(crate) fn internal_id_with_behavior(
        &self,
        external_id: &PointIdType,
        deferred_behavior: common::types::DeferredBehavior,
    ) -> Option<PointOffsetType> {
        if deferred_behavior.with_deferred_points() {
            self.internal_id_deferred(external_id)
                .or_else(|| self.internal_id_active(external_id))
        } else {
            self.internal_id_active(external_id)
        }
    }

    fn internal_id_active(&self, external_id: &PointIdType) -> Option<PointOffsetType> {
        match external_id {
            PointIdType::NumId(num) => self.external_to_internal_num.get(num).copied(),
            PointIdType::Uuid(uuid) => self.external_to_internal_uuid.get(uuid).copied(),
        }
    }

    fn internal_id_deferred(&self, external_id: &PointIdType) -> Option<PointOffsetType> {
        match external_id {
            PointIdType::NumId(num) => self.external_to_internal_num_deferred.get(num).copied(),
            PointIdType::Uuid(uuid) => self.external_to_internal_uuid_deferred.get(uuid).copied(),
        }
    }

    pub(crate) fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        if *self.deleted.get(internal_id as usize)? {
            return None;
        }

        self.internal_to_external
            .get(internal_id as usize)
            .map(Into::into)
    }

    pub(crate) fn drop(&mut self, external_id: PointIdType) -> Option<PointOffsetType> {
        // Drop from both tracks: an ext can be shadowed (active + deferred
        // head for the same external id), and `drop` must tombstone both.
        // We "temporarily" remove existing points from the BTreeMaps without writing them to disk
        // because we remove deleted points of a previous load directly when loading.
        let (active, deferred) = match external_id {
            PointIdType::NumId(num) => (
                self.external_to_internal_num.remove(&num),
                self.external_to_internal_num_deferred.remove(&num),
            ),
            PointIdType::Uuid(uuid) => (
                self.external_to_internal_uuid.remove(&uuid),
                self.external_to_internal_uuid_deferred.remove(&uuid),
            ),
        };

        for internal_id in [active, deferred].into_iter().flatten() {
            // Reset inverse mapping
            self.internal_to_external[internal_id as usize] = PointIdType::NumId(u64::MAX);
            self.tombstone_slot(internal_id);
        }

        // Preserve the prior single-return signature: prefer active (the
        // visible head) when both tracks held this ext.
        active.or(deferred)
    }

    /// Mark `internal_id` as soft-deleted. Idempotent on the deleted bit, and
    /// keeps `deferred_deleted_count` in sync — increments only on the
    /// live → tombstoned transition for slots at or above the cutoff. Also
    /// clears the shadowed bit so PR C's read-side filter doesn't double-skip
    /// a slot that's already filtered by `deleted`.
    fn tombstone_slot(&mut self, internal_id: PointOffsetType) {
        let internal_id_usize = internal_id as usize;
        let was_already_deleted = *self
            .deleted
            .get(internal_id_usize)
            .as_deref()
            .unwrap_or(&true);
        self.deleted.set(internal_id_usize, true);
        if internal_id_usize < self.shadowed.len() {
            self.shadowed.set(internal_id_usize, false);
        }
        if !was_already_deleted
            && self
                .deferred_internal_id
                .is_some_and(|deferred_from| internal_id >= deferred_from)
        {
            self.deferred_deleted_count += 1;
        }
    }

    /// Sample (external, internal) pairs in random order, with deferred
    /// filtering selected by `deferred_behavior`:
    /// - [`DeferredBehavior::VisibleOnly`] caps the sampling range at the
    ///   deferred threshold (so deferred slots are never sampled);
    /// - [`DeferredBehavior::WithDeferred`] samples the full slot range and
    ///   filters shadowed actives so each external id surfaces at most once.
    pub(crate) fn iter_random_with_behavior(
        &self,
        deferred_behavior: DeferredBehavior,
    ) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + '_ {
        let max_internal = match (deferred_behavior, self.deferred_internal_id) {
            (DeferredBehavior::VisibleOnly, Some(cutoff)) => cutoff as usize,
            (DeferredBehavior::VisibleOnly, None) | (DeferredBehavior::WithDeferred, _) => {
                self.internal_to_external.len()
            }
        };
        if max_internal == 0 {
            return Either::Left(iter::empty());
        }
        let rng = rand::rng();
        let uniform = rand::distr::Uniform::new(0, max_internal)
            .expect("above check guarantees max_internal > 0");
        let with_deferred = deferred_behavior.with_deferred_points();
        let iter = Distribution::sample_iter(uniform, rng)
            .unique()
            .take(max_internal)
            .filter_map(move |i| {
                if self.deleted[i] {
                    return None;
                }
                if with_deferred && self.shadowed.get_bit(i).unwrap_or(false) {
                    return None;
                }
                Some((self.internal_to_external[i], i as PointOffsetType))
            });
        Either::Right(iter)
    }

    pub(crate) fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + '_ {
        // Merge active + deferred BTreeMap views into one sorted-by-key
        // stream, deduping the case where the same ext exists in both
        // tracks. The dedup prefers the deferred entry — it's the latest
        // mutation (the deferred head shadows the stale active slot), which
        // keeps this merge consistent with the rest of the WithDeferred
        // contract: `internal_id_with_behavior` and `iter_random_with_behavior`
        // both surface the deferred head over the shadowed active. Consumers
        // that use the returned internal id (e.g. payload-filter checks, the
        // optimizer's version merge, HNSW old→new mapping) therefore see the
        // latest copy; consumers that keep only the external id are unaffected.
        let merged_num = |start: Option<u64>| {
            let active = match start {
                None => Either::Left(self.external_to_internal_num.iter()),
                Some(s) => Either::Right(self.external_to_internal_num.range(s..)),
            };
            let deferred = match start {
                None => Either::Left(self.external_to_internal_num_deferred.iter()),
                Some(s) => Either::Right(self.external_to_internal_num_deferred.range(s..)),
            };
            active
                .merge_join_by(deferred, |a, d| a.0.cmp(d.0))
                .map(|either| match either {
                    // Both tracks hold this ext: take the deferred head (the
                    // second operand) — it's the latest mutation.
                    itertools::EitherOrBoth::Both(_, (k, v))
                    | itertools::EitherOrBoth::Left((k, v))
                    | itertools::EitherOrBoth::Right((k, v)) => (PointIdType::NumId(*k), *v),
                })
        };
        let merged_uuid = |start: Option<Uuid>| {
            let active = match start {
                None => Either::Left(self.external_to_internal_uuid.iter()),
                Some(s) => Either::Right(self.external_to_internal_uuid.range(s..)),
            };
            let deferred = match start {
                None => Either::Left(self.external_to_internal_uuid_deferred.iter()),
                Some(s) => Either::Right(self.external_to_internal_uuid_deferred.range(s..)),
            };
            active
                .merge_join_by(deferred, |a, d| a.0.cmp(d.0))
                .map(|either| match either {
                    // Both tracks hold this ext: take the deferred head (the
                    // second operand) — it's the latest mutation.
                    itertools::EitherOrBoth::Both(_, (k, v))
                    | itertools::EitherOrBoth::Left((k, v))
                    | itertools::EitherOrBoth::Right((k, v)) => (PointIdType::Uuid(*k), *v),
                })
        };

        match external_id {
            // order is important here, we want to iterate over the u64 ids first
            None => Either::Left(merged_num(None).chain(merged_uuid(None))),
            // Because u64 keys are less than uuid keys, we can just use the full iterator for uuid
            Some(PointIdType::NumId(idx)) => {
                Either::Left(merged_num(Some(idx)).chain(merged_uuid(None)))
            }
            // if offset is a uuid, we can only iterate over uuids
            Some(PointIdType::Uuid(uuid)) => Either::Right(merged_uuid(Some(uuid))),
        }
    }

    /// Iterate (external, internal) pairs ordered by external id starting at
    /// `external_id`, with deferred filtering selected by `deferred_behavior`:
    /// - [`DeferredBehavior::VisibleOnly`] walks the active maps only (deferred
    ///   entries are hidden by definition);
    /// - [`DeferredBehavior::WithDeferred`] merges active and deferred entries
    ///   per id.
    pub(crate) fn iter_from_with_behavior(
        &self,
        external_id: Option<PointIdType>,
        deferred_behavior: DeferredBehavior,
    ) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + '_ {
        if deferred_behavior.with_deferred_points() {
            return Either::Left(self.iter_from(external_id));
        }
        // VisibleOnly: walk the active maps only — no merge, no deferred entries.
        let active_num = |start: Option<u64>| {
            let iter = match start {
                None => Either::Left(self.external_to_internal_num.iter()),
                Some(s) => Either::Right(self.external_to_internal_num.range(s..)),
            };
            iter.map(|(k, v)| (PointIdType::NumId(*k), *v))
        };
        let active_uuid = |start: Option<Uuid>| {
            let iter = match start {
                None => Either::Left(self.external_to_internal_uuid.iter()),
                Some(s) => Either::Right(self.external_to_internal_uuid.range(s..)),
            };
            iter.map(|(k, v)| (PointIdType::Uuid(*k), *v))
        };
        Either::Right(match external_id {
            None => Either::Left(active_num(None).chain(active_uuid(None))),
            Some(PointIdType::NumId(idx)) => {
                Either::Left(active_num(Some(idx)).chain(active_uuid(None)))
            }
            Some(PointIdType::Uuid(uuid)) => Either::Right(active_uuid(Some(uuid))),
        })
    }

    pub(crate) fn iter_external(&self) -> impl Iterator<Item = PointIdType> + '_ {
        // Merge sorted active + deferred key streams, deduping identical
        // keys (PR B can introduce active+deferred pairs for one ext).
        let iter_num = self
            .external_to_internal_num
            .keys()
            .merge(self.external_to_internal_num_deferred.keys())
            .dedup()
            .map(|i| PointIdType::NumId(*i));
        let iter_uuid = self
            .external_to_internal_uuid
            .keys()
            .merge(self.external_to_internal_uuid_deferred.keys())
            .dedup()
            .map(|i| PointIdType::Uuid(*i));
        // order is important here, we want to iterate over the u64 ids first
        iter_num.chain(iter_uuid)
    }

    pub(crate) fn iter_internal(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        (0..self.internal_to_external.len() as PointOffsetType)
            .filter(move |i| !self.deleted[*i as usize])
    }

    /// Iterate over non-deleted internal IDs, with deferred filtering
    /// selected by `deferred_behavior`:
    /// - [`DeferredBehavior::VisibleOnly`] takes ids strictly below the
    ///   mapping's deferred threshold (yields everything when there's no
    ///   threshold);
    /// - [`DeferredBehavior::WithDeferred`] yields every non-deleted id,
    ///   filtering out shadowed actives so each external id surfaces
    ///   at most once.
    pub(crate) fn iter_internal_with_behavior(
        &self,
        deferred_behavior: DeferredBehavior,
    ) -> impl Iterator<Item = PointOffsetType> + '_ {
        if deferred_behavior.with_deferred_points() {
            let shadowed = &self.shadowed;
            Either::Left(
                self.iter_internal()
                    .filter(move |&id| !shadowed.get_bit(id as usize).unwrap_or(false)),
            )
        } else {
            match self.deferred_internal_id {
                None => Either::Right(Either::Left(self.iter_internal())),
                Some(cutoff) => Either::Right(Either::Right(
                    self.iter_internal().take_while(move |&id| id < cutoff),
                )),
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn iter_internal_raw(
        &self,
    ) -> impl Iterator<Item = (PointOffsetType, PointIdType)> + '_ {
        self.internal_to_external
            .iter()
            .enumerate()
            .map(|(offset, point_id)| (offset as _, *point_id))
    }

    pub(crate) fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        let key = key as usize;
        if key >= self.deleted.len() {
            return true;
        }
        self.deleted[key]
    }

    /// Sets the link between an external and internal id.
    /// Returns the previous head for the same track if it existed.
    ///
    /// Routing by `internal_id` vs `deferred_internal_id` (the cutoff):
    ///
    /// - **Active write** (`internal_id < cutoff`, or no cutoff): insert
    ///   into the active map, tombstone the prior active head if any. If
    ///   a deferred head also exists for this ext, that deferred head is
    ///   superseded — drop it from the deferred map and tombstone its
    ///   slot. The new active becomes the single visible head.
    /// - **Deferred write** (`internal_id >= cutoff`): insert into the
    ///   deferred map, tombstone the prior deferred head if any. If an
    ///   active head exists for this ext, **shadow** it (set the
    ///   shadowed bit) but do NOT tombstone or drop it — read paths in
    ///   `VisibleOnly` mode keep returning the active version until the
    ///   optimiser rolls a fresh segment. `WithDeferred` consumers skip
    ///   shadowed actives via PR C's filter so the deferred head wins
    ///   without yielding the same external id twice.
    ///
    /// If the target slot is currently live under a *different* external id
    /// (only reachable when recovering from a corrupted/truncated mappings
    /// log — a well-formed caller drops a point before its slot is reused),
    /// the stale external's forward head for this slot is detached so the
    /// reverse mapping and the forward maps stay consistent.
    ///
    /// Return value: the prior same-track head (now tombstoned). Shadowed
    /// actives aren't reported since they remain live.
    pub(crate) fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> Option<PointOffsetType> {
        let is_deferred = self
            .deferred_internal_id
            .is_some_and(|cutoff| internal_id >= cutoff);

        let internal_id_usize = internal_id as usize;
        if internal_id_usize >= self.internal_to_external.len() {
            self.internal_to_external
                .resize(internal_id_usize + 1, PointIdType::NumId(u64::MAX));
        }
        if internal_id_usize >= self.deleted.len() {
            self.deleted.resize(internal_id_usize + 1, true);
        }

        // Same-track insert; capture the prior head for tombstoning.
        let same_track_prior = match (external_id, is_deferred) {
            (PointIdType::NumId(idx), false) => {
                self.external_to_internal_num.insert(idx, internal_id)
            }
            (PointIdType::NumId(idx), true) => self
                .external_to_internal_num_deferred
                .insert(idx, internal_id),
            (PointIdType::Uuid(uuid), false) => {
                self.external_to_internal_uuid.insert(uuid, internal_id)
            }
            (PointIdType::Uuid(uuid), true) => self
                .external_to_internal_uuid_deferred
                .insert(uuid, internal_id),
        };

        // Cross-track handling.
        if is_deferred {
            // Deferred write: shadow any visible active head for this ext.
            let active_id = match external_id {
                PointIdType::NumId(idx) => self.external_to_internal_num.get(&idx).copied(),
                PointIdType::Uuid(uuid) => self.external_to_internal_uuid.get(&uuid).copied(),
            };
            if let Some(active_id) = active_id {
                let active_id_usize = active_id as usize;
                if active_id_usize >= self.shadowed.len() {
                    self.shadowed.resize(active_id_usize + 1, false);
                }
                self.shadowed.set(active_id_usize, true);
            }
        } else {
            // Active write: any deferred head is now superseded — drop +
            // tombstone. Treat it like any other replaced head.
            let prior_deferred = match external_id {
                PointIdType::NumId(idx) => self.external_to_internal_num_deferred.remove(&idx),
                PointIdType::Uuid(uuid) => self.external_to_internal_uuid_deferred.remove(&uuid),
            };
            if let Some(old) = prior_deferred
                && old as usize != internal_id_usize
            {
                self.tombstone_slot(old);
            }
        }

        // Tombstone the same-track prior head.
        if let Some(old) = same_track_prior
            && old as usize != internal_id_usize
        {
            self.tombstone_slot(old);
        }

        // Detach a stale live occupant of this slot before overwriting the
        // reverse mapping. A well-formed caller only reuses an internal id
        // after dropping its prior point (so the slot is tombstoned here), but
        // a corrupted/truncated mappings log can replay a reuse with no
        // intervening delete. Without this, the prior external id would keep a
        // forward head pointing at this slot while the reverse mapping now
        // resolves to `external_id` — a dangling entry that resolves the wrong
        // external id for a live slot. Remove it from the track that owns this
        // slot's region; the prior external's other-track head (if it is
        // shadowed) points at a different slot and is left intact.
        let replaced_external_id = self.internal_to_external[internal_id_usize];
        if !self.deleted[internal_id_usize] && replaced_external_id != external_id {
            match (replaced_external_id, is_deferred) {
                (PointIdType::NumId(idx), false) => {
                    if self.external_to_internal_num.get(&idx) == Some(&internal_id) {
                        self.external_to_internal_num.remove(&idx);
                    }
                }
                (PointIdType::NumId(idx), true) => {
                    if self.external_to_internal_num_deferred.get(&idx) == Some(&internal_id) {
                        self.external_to_internal_num_deferred.remove(&idx);
                    }
                }
                (PointIdType::Uuid(uuid), false) => {
                    if self.external_to_internal_uuid.get(&uuid) == Some(&internal_id) {
                        self.external_to_internal_uuid.remove(&uuid);
                    }
                }
                (PointIdType::Uuid(uuid), true) => {
                    if self.external_to_internal_uuid_deferred.get(&uuid) == Some(&internal_id) {
                        self.external_to_internal_uuid_deferred.remove(&uuid);
                    }
                }
            }
        }

        self.internal_to_external[internal_id_usize] = external_id;
        self.deleted.set(internal_id_usize, false);

        same_track_prior
    }

    /// Whether `internal_id` is an active head shadowed by a deferred
    /// override. Read-side iteration in `WithDeferred` mode uses this to
    /// dedup by external id without scanning both maps.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "consumed by PR C's deferred-aware lookups")
    )]
    pub(crate) fn is_shadowed(&self, internal_id: PointOffsetType) -> bool {
        self.shadowed
            .get(internal_id as usize)
            .as_deref()
            .copied()
            .unwrap_or(false)
    }

    /// Read-only view of the shadowed bitslice — used by
    /// `PointMappingsRefEnum::filter_deferred_and_deleted` in
    /// [`DeferredBehavior::WithDeferred`] mode to skip shadowed actives.
    pub(crate) fn shadowed_bitslice(&self) -> &BitSlice {
        &self.shadowed
    }

    pub(crate) fn total_point_count(&self) -> usize {
        self.internal_to_external.len()
    }

    pub(crate) fn deferred_internal_id(&self) -> Option<PointOffsetType> {
        self.deferred_internal_id
    }

    pub(crate) fn deferred_deleted_count(&self) -> usize {
        self.deferred_deleted_count
    }

    /// Generate a random [`PointMappings`].
    #[cfg(test)]
    pub fn random(rand: &mut StdRng, total_size: u32) -> Self {
        Self::random_with_params(rand, total_size, 128)
    }

    /// Generate a random [`PointMappings`] using the following parameters:
    ///
    /// - `total_size`: total number of points, including deleted ones.
    /// - `preserved_size`: number of points that are not deleted.
    /// - `bits_in_id`: number of bits in generated ids.
    ///   Decrease this value to restrict the amount of unique ids across all
    ///   multiple invocations of this function.
    ///   E.g. if `bits_in_id` is 8, then only 512 unique ids will be generated.
    ///   (256 uuids + 256 u64s)
    #[cfg(test)]
    pub fn random_with_params(rand: &mut StdRng, total_size: u32, bits_in_id: u8) -> Self {
        let mask: u128 = make_bitmask(bits_in_id);
        let mask_u64: u64 = mask as u64;

        const UUID_LIKELYNESS: f64 = 0.5;

        let mut external_to_internal_num = BTreeMap::new();
        let mut external_to_internal_uuid = BTreeMap::new();

        let mut internal_ids = (0..total_size).collect_vec();
        internal_ids.shuffle(rand);

        let mut deleted = BitVec::repeat(true, total_size as usize);
        for id in &internal_ids {
            deleted.set(*id as usize, false);
        }

        let internal_to_external = (0..total_size)
            .map(|pos| {
                loop {
                    if rand.random_bool(UUID_LIKELYNESS) {
                        let uuid = Uuid::from_u128(rand.random_range(0..=mask));
                        if let Entry::Vacant(e) = external_to_internal_uuid.entry(uuid) {
                            e.insert(pos);
                            return PointIdType::Uuid(uuid);
                        }
                    } else {
                        let num = rand.random_range(0..=mask_u64);
                        if let Entry::Vacant(e) = external_to_internal_num.entry(num) {
                            e.insert(pos);
                            return PointIdType::NumId(num);
                        }
                    }
                }
            })
            .collect();

        Self {
            deleted,
            internal_to_external,
            external_to_internal_num,
            external_to_internal_uuid,
            external_to_internal_num_deferred: BTreeMap::new(),
            external_to_internal_uuid_deferred: BTreeMap::new(),
            shadowed: BitVec::new(),
            deferred_internal_id: None,
            deferred_deleted_count: 0,
        }
    }

    #[cfg(debug_assertions)]
    pub fn assert_mappings(&self) {
        for (external_id, internal_id) in self.external_to_internal_num.iter() {
            debug_assert!(
                self.internal_to_external[*internal_id as usize]
                    == PointIdType::NumId(*external_id),
                "Internal id {internal_id} is mapped to external id {}, but should be {}",
                self.internal_to_external[*internal_id as usize],
                PointIdType::NumId(*external_id),
            );
        }
    }

    /// Approximate RAM usage in bytes for the in-memory data structures.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            deleted,
            internal_to_external,
            external_to_internal_num,
            external_to_internal_uuid,
            external_to_internal_num_deferred,
            external_to_internal_uuid_deferred,
            shadowed,
            deferred_internal_id: _,
            deferred_deleted_count: _,
        } = self;

        let deleted_bytes = deleted.capacity().div_ceil(u8::BITS as usize);
        let shadowed_bytes = shadowed.capacity().div_ceil(u8::BITS as usize);
        let internal_to_external_bytes =
            internal_to_external.capacity() * std::mem::size_of::<PointIdType>();
        // BTreeMap node overhead: key + value + 2 child pointers + parent pointer + metadata.
        // Approximation based on std BTreeMap B=6 node layout.
        let btree_node_overhead = std::mem::size_of::<usize>() * 3;
        let num_entry_size = std::mem::size_of::<u64>()
            + std::mem::size_of::<PointOffsetType>()
            + btree_node_overhead;
        let uuid_entry_size = std::mem::size_of::<Uuid>()
            + std::mem::size_of::<PointOffsetType>()
            + btree_node_overhead;
        let num_map_bytes = (external_to_internal_num.len()
            + external_to_internal_num_deferred.len())
            * num_entry_size;
        let uuid_map_bytes = (external_to_internal_uuid.len()
            + external_to_internal_uuid_deferred.len())
            * uuid_entry_size;
        deleted_bytes + shadowed_bytes + internal_to_external_bytes + num_map_bytes + uuid_map_bytes
    }
}

#[cfg(test)]
mod set_link_shadow_tests {
    use super::*;

    fn fresh_mapping(cutoff: Option<PointOffsetType>) -> PointMappings {
        PointMappings::new(
            BitVec::new(),
            Vec::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            cutoff,
        )
    }

    fn ext(n: u64) -> PointIdType {
        PointIdType::NumId(n)
    }

    #[test]
    fn no_cutoff_active_only() {
        let mut m = fresh_mapping(None);
        m.set_link(ext(42), 0);
        m.set_link(ext(42), 1);

        assert_eq!(
            m.internal_id_with_behavior(&ext(42), common::types::DeferredBehavior::VisibleOnly),
            Some(1)
        );
        assert!(m.is_deleted_point(0), "prior active head tombstoned");
        assert!(!m.is_shadowed(0));
        assert!(m.external_to_internal_num_deferred.is_empty());
    }

    #[test]
    fn active_write_below_cutoff_no_shadow() {
        // Cutoff at 5. Both writes are below — pure active replacement.
        let mut m = fresh_mapping(Some(5));
        m.set_link(ext(7), 0);
        m.set_link(ext(7), 1);

        assert_eq!(
            m.internal_id_with_behavior(&ext(7), common::types::DeferredBehavior::VisibleOnly),
            Some(1)
        );
        assert!(m.is_deleted_point(0));
        assert!(!m.is_shadowed(0));
        assert!(!m.is_shadowed(1));
    }

    #[test]
    fn deferred_write_shadows_active() {
        // Cutoff at 5. Insert active at 2, then deferred at 7.
        // Active must stay visible (not tombstoned) and gain the shadow bit.
        let mut m = fresh_mapping(Some(5));
        m.set_link(ext(7), 2);
        m.set_link(ext(7), 7);

        // Active head still present in active map.
        assert_eq!(m.external_to_internal_num.get(&7).copied(), Some(2));
        // Deferred head present in deferred map.
        assert_eq!(
            m.external_to_internal_num_deferred.get(&7).copied(),
            Some(7)
        );
        // Active not tombstoned, but shadowed.
        assert!(!m.is_deleted_point(2));
        assert!(m.is_shadowed(2));
        // Deferred slot itself isn't shadowed.
        assert!(!m.is_shadowed(7));
        // VisibleOnly-style lookup (active-first fall-through) returns active.
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), common::types::DeferredBehavior::VisibleOnly),
            Some(2)
        );
    }

    #[test]
    fn second_deferred_write_supersedes_prior_deferred_keeps_shadow() {
        // Cutoff at 5. Active at 2; first deferred at 7; second deferred at 9.
        // The shadow on 2 must persist; the first deferred (7) is tombstoned.
        let mut m = fresh_mapping(Some(5));
        m.set_link(ext(7), 2);
        m.set_link(ext(7), 7);
        m.set_link(ext(7), 9);

        assert_eq!(m.external_to_internal_num.get(&7).copied(), Some(2));
        assert_eq!(
            m.external_to_internal_num_deferred.get(&7).copied(),
            Some(9)
        );
        assert!(m.is_shadowed(2));
        assert!(m.is_deleted_point(7), "prior deferred head tombstoned");
        assert!(!m.is_deleted_point(9));
    }

    #[test]
    fn fresh_deferred_insert_no_active_no_shadow() {
        // No prior active — a fresh insert above cutoff.
        let mut m = fresh_mapping(Some(5));
        m.set_link(ext(7), 7);

        assert!(!m.external_to_internal_num.contains_key(&7));
        assert_eq!(
            m.external_to_internal_num_deferred.get(&7).copied(),
            Some(7)
        );
        assert!(!m.is_shadowed(7));
        // Active-first lookup falls through to deferred.
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), common::types::DeferredBehavior::WithDeferred),
            Some(7)
        );
    }

    #[test]
    fn internal_id_with_behavior_prefers_deferred_on_include_all() {
        use common::types::DeferredBehavior;

        let mut m = fresh_mapping(Some(5));
        m.set_link(ext(7), 2);
        m.set_link(ext(7), 7);

        // VisibleOnly: visible-only — active head, even though it's shadowed.
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), DeferredBehavior::VisibleOnly),
            Some(2)
        );
        // WithDeferred: the latest — the deferred head wins.
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), DeferredBehavior::WithDeferred),
            Some(7)
        );
    }

    #[test]
    fn internal_id_with_behavior_excludes_deferred_only_in_exclude() {
        use common::types::DeferredBehavior;

        // Fresh insert above the cutoff — no active head.
        let mut m = fresh_mapping(Some(5));
        m.set_link(ext(7), 7);

        // VisibleOnly readers never see deferred-only ext ids.
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), DeferredBehavior::VisibleOnly),
            None
        );
        // WithDeferred consumers do.
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), DeferredBehavior::WithDeferred),
            Some(7)
        );
    }

    #[test]
    fn filter_deferred_and_deleted_skips_shadowed_on_include_all() {
        use common::types::DeferredBehavior;

        use crate::id_tracker::PointMappingsRefEnum;

        // Cutoff = 5. Three points:
        // - ext 7: active at 2, deferred at 7 (active shadowed)
        // - ext 8: active at 3 only (never crossed cutoff)
        // - ext 9: deferred at 8 only (fresh insert above cutoff)
        let mut m = fresh_mapping(Some(5));
        m.set_link(ext(7), 2);
        m.set_link(ext(7), 7);
        m.set_link(ext(8), 3);
        m.set_link(ext(9), 8);

        let r = PointMappingsRefEnum::Plain(&m);
        let candidates: Vec<PointOffsetType> = vec![2, 3, 7, 8];

        // VisibleOnly: visible-only path — drops everything above cutoff.
        let exclude: Vec<_> = r
            .filter_deferred_and_deleted(candidates.iter().copied(), DeferredBehavior::VisibleOnly)
            .collect();
        assert_eq!(exclude, vec![2, 3]);

        // WithDeferred: every ext yields exactly one slot — its latest.
        // Shadowed 2 (ext 7's stale active) is filtered out; 7 (its deferred
        // head) is kept. Plain active 3 (ext 8) is kept. Deferred-only 8
        // (ext 9) is kept.
        let include_all: Vec<_> = r
            .filter_deferred_and_deleted(candidates.iter().copied(), DeferredBehavior::WithDeferred)
            .collect();
        assert_eq!(include_all, vec![3, 7, 8]);
    }

    #[test]
    fn drop_clears_both_tracks_and_shadow() {
        let mut m = fresh_mapping(Some(5));
        m.set_link(ext(7), 2);
        m.set_link(ext(7), 7);
        assert!(m.is_shadowed(2));

        let returned = m.drop(ext(7));

        // Prefer-active return shape from PR A is preserved.
        assert_eq!(returned, Some(2));
        // Both slots tombstoned, shadow cleared.
        assert!(m.is_deleted_point(2));
        assert!(m.is_deleted_point(7));
        assert!(!m.is_shadowed(2));
        assert!(!m.external_to_internal_num.contains_key(&7));
        assert!(!m.external_to_internal_num_deferred.contains_key(&7));
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), common::types::DeferredBehavior::VisibleOnly),
            None
        );
    }

    /// Review finding #1: `iter_from_with_behavior(WithDeferred)` must surface
    /// the deferred (latest) internal id for a shadowed ext, the same way
    /// `internal_id_with_behavior` and `iter_internal_with_behavior` already do.
    /// It currently delegates to `iter_from`, whose merge resolves the
    /// active/deferred collision to the *active* (stale) head — so consumers
    /// that use the yielded internal id (optimizer merge via
    /// `for_each_unique_point`, `filtered_read_by_id_stream`) observe the
    /// pre-mutation version.
    #[test]
    fn iter_from_with_behavior_with_deferred_yields_latest_head() {
        use common::types::DeferredBehavior;

        // Cutoff = 5. ext 7: active@2, deferred@9 (active shadowed).
        let mut m = fresh_mapping(Some(5));
        m.set_link(ext(7), 2);
        m.set_link(ext(7), 9);

        // The point-lookup sibling agrees the latest head is the deferred slot.
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), DeferredBehavior::WithDeferred),
            Some(9),
        );
        // The internal-id iteration sibling also yields only the deferred slot
        // (the shadowed active is skipped).
        let via_internal: Vec<_> = m
            .iter_internal_with_behavior(DeferredBehavior::WithDeferred)
            .collect();
        assert_eq!(
            via_internal,
            vec![9],
            "iter_internal_with_behavior(WithDeferred) yields the deferred head",
        );

        // iter_from_with_behavior(WithDeferred) must agree: one yield per ext,
        // carrying the latest (deferred) internal id.
        let via_from: Vec<_> = m
            .iter_from_with_behavior(None, DeferredBehavior::WithDeferred)
            .collect();
        assert_eq!(
            via_from,
            vec![(ext(7), 9)],
            "iter_from_with_behavior(WithDeferred) must surface the deferred \
             (latest) internal id, consistent with internal_id_with_behavior and \
             iter_internal_with_behavior; it instead yields the stale active slot 2",
        );
    }
}
