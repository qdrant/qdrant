use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::btree_map::Entry;
use std::iter;

use bitvec::prelude::{BitSlice, BitVec};
use byteorder::LittleEndian;
#[cfg(test)]
use common::bitpacking::make_bitmask;
use common::types::PointOffsetType;
use itertools::Itertools;
#[cfg(test)]
use rand::Rng as _;
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

    // Having two separate maps allows us iterating only over one type at a time without having to filter.
    external_to_internal_num: BTreeMap<u64, PointOffsetType>,
    external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType>,
}

impl PointMappings {
    pub fn new(
        deleted: BitVec,
        internal_to_external: Vec<PointIdType>,
        external_to_internal_num: BTreeMap<u64, PointOffsetType>,
        external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType>,
    ) -> Self {
        Self {
            deleted,
            internal_to_external,
            external_to_internal_num,
            external_to_internal_uuid,
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
    pub(crate) fn available_point_count(&self) -> usize {
        self.external_to_internal_num.len() + self.external_to_internal_uuid.len()
    }

    pub(crate) fn deleted(&self) -> &BitSlice {
        &self.deleted
    }

    pub(crate) fn internal_id(&self, external_id: &PointIdType) -> Option<PointOffsetType> {
        match external_id {
            PointIdType::NumId(num) => self.external_to_internal_num.get(num).copied(),
            PointIdType::Uuid(uuid) => self.external_to_internal_uuid.get(uuid).copied(),
        }
    }

    pub(crate) fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        if *self.deleted.get(internal_id as usize)? {
            return None;
        }

        self.internal_to_external
            .get(internal_id as usize)
            .map(|i| i.into())
    }

    pub(crate) fn drop(&mut self, external_id: PointIdType) -> Option<PointOffsetType> {
        let internal_id = match external_id {
            // We "temporarily" remove existing points from the BTreeMaps without writing them to disk
            // because we remove deleted points of a previous load directly when loading.
            PointIdType::NumId(num) => self.external_to_internal_num.remove(&num),
            PointIdType::Uuid(uuid) => self.external_to_internal_uuid.remove(&uuid),
        };

        // Also reset inverse mapping
        if let Some(internal_id) = internal_id {
            self.internal_to_external[internal_id as usize] = PointIdType::NumId(u64::MAX);
        }

        if let Some(internal_id) = &internal_id {
            self.deleted.set(*internal_id as usize, true);
        }

        internal_id
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
        let full_num_iter = || {
            self.external_to_internal_num
                .iter()
                .map(|(k, v)| (PointIdType::NumId(*k), *v))
        };
        let offset_num_iter = |offset: u64| {
            self.external_to_internal_num
                .range(offset..)
                .map(|(k, v)| (PointIdType::NumId(*k), *v))
        };
        let full_uuid_iter = || {
            self.external_to_internal_uuid
                .iter()
                .map(|(k, v)| (PointIdType::Uuid(*k), *v))
        };
        let offset_uuid_iter = |offset: Uuid| {
            self.external_to_internal_uuid
                .range(offset..)
                .map(|(k, v)| (PointIdType::Uuid(*k), *v))
        };

        match external_id {
            None => {
                let iter_num = full_num_iter();
                let iter_uuid = full_uuid_iter();
                // order is important here, we want to iterate over the u64 ids first
                Box::new(iter_num.chain(iter_uuid))
            }
            Some(offset) => match offset {
                PointIdType::NumId(idx) => {
                    // Because u64 keys are less that uuid key, we can just use the full iterator for uuid
                    let iter_num = offset_num_iter(idx);
                    let iter_uuid = full_uuid_iter();
                    // order is important here, we want to iterate over the u64 ids first
                    Box::new(iter_num.chain(iter_uuid))
                }
                PointIdType::Uuid(uuid) => {
                    // if offset is a uuid, we can only iterate over uuids
                    Box::new(offset_uuid_iter(uuid))
                }
            },
        }
    }

    pub(crate) fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        let iter_num = self
            .external_to_internal_num
            .keys()
            .map(|i| PointIdType::NumId(*i));

        let iter_uuid = self
            .external_to_internal_uuid
            .keys()
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
    /// Returns the previous internal id if it existed.
    pub(crate) fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> Option<PointOffsetType> {
        let old_internal_id = match external_id {
            PointIdType::NumId(idx) => self.external_to_internal_num.insert(idx, internal_id),
            PointIdType::Uuid(uuid) => self.external_to_internal_uuid.insert(uuid, internal_id),
        };

        let internal_id = internal_id as usize;
        if internal_id >= self.internal_to_external.len() {
            self.internal_to_external
                .resize(internal_id + 1, PointIdType::NumId(u64::MAX));
        }
        if internal_id >= self.deleted.len() {
            self.deleted.resize(internal_id + 1, true);
        }

        if let Some(old_internal_id) = &old_internal_id {
            let old_internal_id = *old_internal_id as usize;
            if old_internal_id != internal_id {
                self.deleted.set(old_internal_id, true);
            }
        }

        self.internal_to_external[internal_id] = external_id;
        self.deleted.set(internal_id, false);

        old_internal_id
    }

    pub(crate) fn total_point_count(&self) -> usize {
        self.internal_to_external.len()
    }

    /// Generate a random [`PointMappings`].
    #[cfg(test)]
    pub fn random(rand: &mut StdRng, total_size: u32) -> Self {
        Self::random_with_params(rand, total_size, total_size, 128)
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
    pub fn random_with_params(
        rand: &mut StdRng,
        total_size: u32,
        preserved_size: u32,
        bits_in_id: u8,
    ) -> Self {
        let mask: u128 = make_bitmask(bits_in_id);
        let mask_u64: u64 = mask as u64;

        const UUID_LIKELYNESS: f64 = 0.5;

        let mut external_to_internal_num = BTreeMap::new();
        let mut external_to_internal_uuid = BTreeMap::new();

        let mut internal_ids = (0..total_size).collect_vec();
        internal_ids.shuffle(rand);
        internal_ids.truncate(preserved_size as usize);

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
}
