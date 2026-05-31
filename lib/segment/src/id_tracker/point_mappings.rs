use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::btree_map::Entry;
use std::iter;

use byteorder::LittleEndian;
#[cfg(test)]
use common::bitpacking::make_bitmask;
use common::bitvec::{BitSlice, BitVec};
use common::types::PointOffsetType;
use itertools::Itertools;
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
    /// head. Read-side iteration in `IncludeAll` mode uses this to skip the
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
    /// Counts each distinct external id once. An ext that lives only in the
    /// deferred map (no active head) still counts — preserves the pre-split
    /// observable count from when both tracks shared one map.
    pub(crate) fn available_point_count(&self) -> usize {
        let active = self.external_to_internal_num.len() + self.external_to_internal_uuid.len();
        let deferred_only_num = self
            .external_to_internal_num_deferred
            .keys()
            .filter(|k| !self.external_to_internal_num.contains_key(k))
            .count();
        let deferred_only_uuid = self
            .external_to_internal_uuid_deferred
            .keys()
            .filter(|k| !self.external_to_internal_uuid.contains_key(k))
            .count();
        active + deferred_only_num + deferred_only_uuid
    }

    pub(crate) fn deleted(&self) -> &BitSlice {
        &self.deleted
    }

    /// Internal id for `external_id`. Checks the active map first and falls
    /// back to deferred — preserves "any matching id" semantics from before
    /// the split, where every ext lived in a single map.
    pub(crate) fn internal_id(&self, external_id: &PointIdType) -> Option<PointOffsetType> {
        let active = self.internal_id_active(external_id);
        active.or_else(|| self.internal_id_deferred(external_id))
    }

    /// Internal id for `external_id` with explicit deferred semantics.
    ///
    /// - [`DeferredBehavior::Exclude`] returns the active head only. An ext
    ///   that only has a deferred head returns `None` — query paths see no
    ///   deferred mutations at all.
    /// - [`DeferredBehavior::IncludeAll`] prefers the deferred head (the
    ///   latest mutation) and falls back to active for points that never
    ///   crossed the cutoff. Yields each ext at most once.
    pub(crate) fn internal_id_with_behavior(
        &self,
        external_id: &PointIdType,
        deferred_behavior: common::types::DeferredBehavior,
    ) -> Option<PointOffsetType> {
        if deferred_behavior.include_all_points() {
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
        // Drop from both tracks. A point can live in only one map at a time
        // today (PR A), but PR B will introduce shadowed-active + deferred
        // pairs for the same ext, and `drop` is the API both states wire
        // through. Tombstoning both is the safe behaviour to land first.
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

            let was_already_deleted = *self
                .deleted
                .get(internal_id as usize)
                .as_deref()
                .unwrap_or(&true);
            self.deleted.set(internal_id as usize, true);
            // Clearing the shadowed bit on tombstone keeps PR C's read-side
            // filter from skipping a slot that's already filtered by
            // `deleted` — no observable change today, but cheap insurance.
            if (internal_id as usize) < self.shadowed.len() {
                self.shadowed.set(internal_id as usize, false);
            }

            // Count newly-deleted deferred points so we can report visible deferred totals
            // without rescanning the deleted bitslice.
            if !was_already_deleted
                && self
                    .deferred_internal_id
                    .is_some_and(|deferred_from| internal_id >= deferred_from)
            {
                self.deferred_deleted_count += 1;
            }
        }

        // Preserve the prior single-return signature: prefer active (the
        // visible head) when both tracks held this ext.
        active.or(deferred)
    }

    pub(crate) fn iter_random(
        &self,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        let rng = rand::rng();
        let max_internal = self.internal_to_external.len();
        if max_internal == 0 {
            return Box::new(iter::empty());
        }
        let uniform = rand::distr::Uniform::new(0, max_internal)
            .expect("above check guarantees max_internal > 0");
        let iter = Distribution::sample_iter(uniform, rng)
            // TODO: this is not efficient if `max_internal` is large and we iterate over most of them,
            // but it's good enough for low limits.
            //
            // We could improve it by using a variable-period PRNG to adjust depending on the number of available points.
            .unique()
            .take(max_internal)
            .filter_map(move |i| {
                if self.deleted[i] {
                    None
                } else {
                    Some((self.internal_to_external[i], i as PointOffsetType))
                }
            });

        Box::new(iter)
    }

    pub(crate) fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        // Merge active + deferred BTreeMap views into one sorted-by-key
        // stream, deduping the rare case where the same ext exists in
        // both tracks (PR B will introduce that; PR A loads at most one).
        // The dedup prefers the active entry because that's the visible
        // head — keeping callers (which today don't expect to see
        // deferred-only writes) on the same offset as before.
        let merged_num = |start: Option<u64>| {
            let active: Box<dyn Iterator<Item = (&u64, &PointOffsetType)>> = match start {
                None => Box::new(self.external_to_internal_num.iter()),
                Some(s) => Box::new(self.external_to_internal_num.range(s..)),
            };
            let deferred: Box<dyn Iterator<Item = (&u64, &PointOffsetType)>> = match start {
                None => Box::new(self.external_to_internal_num_deferred.iter()),
                Some(s) => Box::new(self.external_to_internal_num_deferred.range(s..)),
            };
            active
                .merge_join_by(deferred, |a, d| a.0.cmp(d.0))
                .map(|either| match either {
                    itertools::EitherOrBoth::Both((k, v), _)
                    | itertools::EitherOrBoth::Left((k, v))
                    | itertools::EitherOrBoth::Right((k, v)) => (PointIdType::NumId(*k), *v),
                })
        };
        let merged_uuid = |start: Option<Uuid>| {
            let active: Box<dyn Iterator<Item = (&Uuid, &PointOffsetType)>> = match start {
                None => Box::new(self.external_to_internal_uuid.iter()),
                Some(s) => Box::new(self.external_to_internal_uuid.range(s..)),
            };
            let deferred: Box<dyn Iterator<Item = (&Uuid, &PointOffsetType)>> = match start {
                None => Box::new(self.external_to_internal_uuid_deferred.iter()),
                Some(s) => Box::new(self.external_to_internal_uuid_deferred.range(s..)),
            };
            active
                .merge_join_by(deferred, |a, d| a.0.cmp(d.0))
                .map(|either| match either {
                    itertools::EitherOrBoth::Both((k, v), _)
                    | itertools::EitherOrBoth::Left((k, v))
                    | itertools::EitherOrBoth::Right((k, v)) => (PointIdType::Uuid(*k), *v),
                })
        };

        match external_id {
            None => {
                // order is important here, we want to iterate over the u64 ids first
                Box::new(merged_num(None).chain(merged_uuid(None)))
            }
            Some(offset) => match offset {
                PointIdType::NumId(idx) => {
                    // Because u64 keys are less that uuid key, we can just use the full iterator for uuid
                    Box::new(merged_num(Some(idx)).chain(merged_uuid(None)))
                }
                PointIdType::Uuid(uuid) => {
                    // if offset is a uuid, we can only iterate over uuids
                    Box::new(merged_uuid(Some(uuid)))
                }
            },
        }
    }

    pub(crate) fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
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
        Box::new(iter_num.chain(iter_uuid))
    }

    pub(crate) fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(
            (0..self.internal_to_external.len() as PointOffsetType)
                .filter(move |i| !self.deleted[*i as usize]),
        )
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
    ///   `Exclude` mode keep returning the active version until the
    ///   optimiser rolls a fresh segment. `IncludeAll` consumers skip
    ///   shadowed actives via PR C's filter so the deferred head wins
    ///   without yielding the same external id twice.
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
            if let Some(old) = prior_deferred {
                let old = old as usize;
                if old != internal_id_usize {
                    self.deleted.set(old, true);
                    if old < self.shadowed.len() {
                        self.shadowed.set(old, false);
                    }
                }
            }
        }

        // Tombstone the same-track prior head.
        if let Some(old) = same_track_prior {
            let old = old as usize;
            if old != internal_id_usize {
                self.deleted.set(old, true);
                if old < self.shadowed.len() {
                    self.shadowed.set(old, false);
                }
            }
        }

        self.internal_to_external[internal_id_usize] = external_id;
        self.deleted.set(internal_id_usize, false);

        same_track_prior
    }

    /// Whether `internal_id` is an active head shadowed by a deferred
    /// override. Read-side iteration in `IncludeAll` mode uses this to
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
    /// [`DeferredBehavior::IncludeAll`] mode to skip shadowed actives.
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

        assert_eq!(m.internal_id(&ext(42)), Some(1));
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

        assert_eq!(m.internal_id(&ext(7)), Some(1));
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
        // Exclude-style lookup (active-first fall-through) returns active.
        assert_eq!(m.internal_id(&ext(7)), Some(2));
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
        assert_eq!(m.internal_id(&ext(7)), Some(7));
    }

    #[test]
    fn internal_id_with_behavior_prefers_deferred_on_include_all() {
        use common::types::DeferredBehavior;

        let mut m = fresh_mapping(Some(5));
        m.set_link(ext(7), 2);
        m.set_link(ext(7), 7);

        // Exclude: visible-only — active head, even though it's shadowed.
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), DeferredBehavior::Exclude),
            Some(2)
        );
        // IncludeAll: the latest — the deferred head wins.
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), DeferredBehavior::IncludeAll),
            Some(7)
        );
    }

    #[test]
    fn internal_id_with_behavior_excludes_deferred_only_in_exclude() {
        use common::types::DeferredBehavior;

        // Fresh insert above the cutoff — no active head.
        let mut m = fresh_mapping(Some(5));
        m.set_link(ext(7), 7);

        // Exclude readers never see deferred-only ext ids.
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), DeferredBehavior::Exclude),
            None
        );
        // IncludeAll consumers do.
        assert_eq!(
            m.internal_id_with_behavior(&ext(7), DeferredBehavior::IncludeAll),
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

        // Exclude: visible-only path — drops everything above cutoff.
        let exclude: Vec<_> = r
            .filter_deferred_and_deleted(candidates.iter().copied(), DeferredBehavior::Exclude)
            .collect();
        assert_eq!(exclude, vec![2, 3]);

        // IncludeAll: every ext yields exactly one slot — its latest.
        // Shadowed 2 (ext 7's stale active) is filtered out; 7 (its deferred
        // head) is kept. Plain active 3 (ext 8) is kept. Deferred-only 8
        // (ext 9) is kept.
        let include_all: Vec<_> = r
            .filter_deferred_and_deleted(candidates.iter().copied(), DeferredBehavior::IncludeAll)
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
        assert_eq!(m.internal_id(&ext(7)), None);
    }
}
