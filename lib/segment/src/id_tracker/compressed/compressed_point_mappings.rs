#[cfg(test)]
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
#[cfg(test)]
use uuid::Uuid;

use crate::id_tracker::compressed::external_to_internal::CompressedExternalToInternal;
use crate::id_tracker::compressed::internal_to_external::CompressedInternalToExternal;
use crate::id_tracker::point_mappings::PointMappings;
use crate::types::PointIdType;

/// Used endianness for storing PointMapping-files.
pub type FileEndianess = LittleEndian;

#[derive(Clone, PartialEq, Default, Debug)]
pub struct CompressedPointMappings {
    /// `deleted` specifies which points of internal_to_external was deleted.
    /// Its size is exactly the same as `internal_to_external`.
    deleted: BitVec,
    internal_to_external: CompressedInternalToExternal,

    // Having two separate maps allows us iterating only over one type at a time without having to filter.
    external_to_internal: CompressedExternalToInternal,
}

impl CompressedPointMappings {
    pub fn new(
        mut deleted: BitVec,
        internal_to_external: CompressedInternalToExternal,
        external_to_internal: CompressedExternalToInternal,
    ) -> Self {
        // Resize deleted to have the same number of elements as internal_to_external
        // Not all structures we may source this from enforce the same size
        deleted.resize(internal_to_external.len(), false);

        Self {
            deleted,
            internal_to_external,
            external_to_internal,
        }
    }

    pub fn from_mappings(mapping: PointMappings) -> Self {
        let (deleted, internal_to_external, external_to_internal_num, external_to_internal_uuid) =
            mapping.deconstruct();

        let compressed_internal_to_external =
            CompressedInternalToExternal::from_slice(&internal_to_external);

        let compressed_external_to_internal = CompressedExternalToInternal::from_maps(
            external_to_internal_num,
            external_to_internal_uuid,
        );
        Self {
            deleted,
            internal_to_external: compressed_internal_to_external,
            external_to_internal: compressed_external_to_internal,
        }
    }

    /// Number of points, excluding deleted ones.
    pub(crate) fn available_point_count(&self) -> usize {
        self.external_to_internal.len()
    }

    pub(crate) fn deleted(&self) -> &BitSlice {
        &self.deleted
    }

    pub(crate) fn internal_id(&self, external_id: &PointIdType) -> Option<PointOffsetType> {
        self.external_to_internal.get(external_id)
    }

    pub(crate) fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        if *self.deleted.get(internal_id as usize)? {
            return None;
        }

        self.internal_to_external.get(internal_id)
    }

    pub(crate) fn drop(&mut self, external_id: PointIdType) -> Option<PointOffsetType> {
        let internal_id = self.external_to_internal.remove(&external_id);

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
                    let point_offset = i as PointOffsetType;
                    Some((
                        self.internal_to_external.get(point_offset).unwrap(),
                        point_offset,
                    ))
                }
            });

        Box::new(iter)
    }

    pub(crate) fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        match external_id {
            None => Box::new(self.external_to_internal.iter()),
            Some(point_id) => Box::new(self.external_to_internal.iter_from(point_id)),
        }
    }

    pub(crate) fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        Box::new(
            self.external_to_internal
                .iter()
                .map(|(point_id, _)| point_id),
        )
    }

    pub(crate) fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(
            (0..self.internal_to_external.len() as PointOffsetType)
                .filter(move |i| !self.deleted[*i as usize]),
        )
    }

    pub(crate) fn iter_internal_raw(
        &self,
    ) -> impl Iterator<Item = (PointOffsetType, PointIdType)> + '_ {
        self.internal_to_external
            .iter()
            .enumerate()
            .map(|(offset, point_id)| (offset as _, point_id))
    }

    pub(crate) fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        let key = key as usize;
        if key >= self.deleted.len() {
            return true;
        }
        self.deleted[key]
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

        let internal_to_external: Vec<_> = (0..total_size)
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

        let compressed_internal_to_external =
            CompressedInternalToExternal::from_slice(&internal_to_external);

        let external_to_internal = CompressedExternalToInternal::from_maps(
            external_to_internal_num,
            external_to_internal_uuid,
        );

        Self {
            deleted,
            internal_to_external: compressed_internal_to_external,
            external_to_internal,
        }
    }
}
