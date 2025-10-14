use std::path::PathBuf;

use bitvec::prelude::BitSlice;
use common::types::PointOffsetType;
#[cfg(test)]
use rand::Rng as _;
#[cfg(test)]
use rand::rngs::StdRng;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::point_mappings::PointMappings;
use crate::id_tracker::{DELETED_POINT_VERSION, IdTracker};
use crate::types::{PointIdType, SeqNumberType};

/// A non-persistent ID tracker for faster and more efficient building of `ImmutableIdTracker`.
#[derive(Debug, Default)]
pub struct InMemoryIdTracker {
    internal_to_version: Vec<SeqNumberType>,
    mappings: PointMappings,
}

impl InMemoryIdTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_internal(self) -> (Vec<SeqNumberType>, PointMappings) {
        (self.internal_to_version, self.mappings)
    }

    /// Generate a random [`InMemoryIdTracker`].
    #[cfg(test)]
    pub fn random(rand: &mut StdRng, size: u32, preserved_size: u32, bits_in_id: u8) -> Self {
        Self {
            internal_to_version: vec![rand.random(); size as usize],
            mappings: PointMappings::random_with_params(rand, size, preserved_size, bits_in_id),
        }
    }
}

impl IdTracker for InMemoryIdTracker {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        self.internal_to_version.get(internal_id as usize).copied()
    }

    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        if self.external_id(internal_id).is_some() {
            if let Some(old_version) = self.internal_to_version.get_mut(internal_id as usize) {
                *old_version = version;
            } else {
                self.internal_to_version.resize(internal_id as usize + 1, 0);
                self.internal_to_version[internal_id as usize] = version;
            }
        }

        Ok(())
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        self.mappings.internal_id(&external_id)
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        self.mappings.external_id(internal_id)
    }

    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        let _replaced_internal_id = self.mappings.set_link(external_id, internal_id);
        Ok(())
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        // Unset version first because it still requires the mapping to exist
        if let Some(internal_id) = self.internal_id(external_id) {
            self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;
        }
        self.mappings.drop(external_id);
        Ok(())
    }

    fn drop_internal(&mut self, internal_id: PointOffsetType) -> OperationResult<()> {
        // Unset version first because it still requires the mapping to exist
        self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;
        if let Some(external_id) = self.mappings.external_id(internal_id) {
            self.mappings.drop(external_id);
        }
        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        self.mappings.iter_external()
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.mappings.iter_internal()
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        self.mappings.iter_from(external_id)
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.iter_internal()
    }

    fn iter_random(&self) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        self.mappings.iter_random()
    }

    /// Creates a flusher function, that writes the deleted points bitvec to disk.
    fn mapping_flusher(&self) -> Flusher {
        debug_assert!(false, "InMemoryIdTracker should not be flushed");
        Box::new(|| Ok(()))
    }

    /// Creates a flusher function, that writes the points versions to disk.
    fn versions_flusher(&self) -> Flusher {
        debug_assert!(false, "InMemoryIdTracker should not be flushed");
        Box::new(|| Ok(()))
    }

    fn total_point_count(&self) -> usize {
        self.mappings.total_point_count()
    }

    fn available_point_count(&self) -> usize {
        self.mappings.available_point_count()
    }

    fn deleted_point_count(&self) -> usize {
        self.total_point_count() - self.available_point_count()
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        self.mappings.deleted()
    }

    fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        self.mappings.is_deleted_point(key)
    }

    fn name(&self) -> &'static str {
        "in memory id tracker"
    }

    fn iter_internal_versions(
        &self,
    ) -> Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_> {
        Box::new(
            self.internal_to_version
                .iter()
                .enumerate()
                .map(|(i, version)| (i as PointOffsetType, *version)),
        )
    }

    fn files(&self) -> Vec<PathBuf> {
        debug_assert!(false, "InMemoryIdTracker should not be persisted");
        vec![]
    }
}
