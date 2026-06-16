use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::super::mutable_null_index::MutableNullIndex;
use super::super::read_ops::NullIndexRead;
use super::ImmutableNullIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex};

impl ImmutableNullIndex {
    pub fn builder(
        path: &Path,
        total_point_count: usize,
    ) -> OperationResult<ImmutableNullIndexBuilder> {
        Ok(ImmutableNullIndexBuilder(
            MutableNullIndex::open(path, total_point_count, true)?.ok_or_else(|| {
                OperationError::service_error("Failed to create and open MutableNullIndex")
            })?,
        ))
    }

    pub fn from_mutable(mutable_index: MutableNullIndex) -> OperationResult<Self> {
        mutable_index.flusher()()?;
        Ok(Self(mutable_index))
    }

    pub fn open(
        path: &Path,
        total_point_count: usize,
        deleted: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        Ok(MutableNullIndex::open_immutable(path, total_point_count, deleted)?.map(Self))
    }

    #[inline]
    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.0.remove_point_immutable(id);
        Ok(())
    }
}

impl PayloadFieldIndex for ImmutableNullIndex {
    #[inline]
    fn wipe(self) -> OperationResult<()> {
        self.0.wipe()
    }

    #[inline]
    fn files(&self) -> Vec<PathBuf> {
        NullIndexRead::files(self)
    }

    #[inline]
    fn immutable_files(&self) -> Vec<PathBuf> {
        NullIndexRead::files(self) // All the files are immutable in this index.
    }

    #[inline]
    fn flusher(&self) -> crate::common::Flusher {
        Box::new(|| Ok(())) // No op for an immutable index.
    }
}

pub struct ImmutableNullIndexBuilder(pub(super) MutableNullIndex);

impl FieldIndexBuilderTrait for ImmutableNullIndexBuilder {
    type FieldIndexType = ImmutableNullIndex;

    fn init(&mut self) -> OperationResult<()> {
        // After Self is created, it is already initialized
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&serde_json::Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.0.add_point(id, payload, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        self.0.flusher()()?; // Immutable index has noop flusher, so we have to ensure the data is flushed now.
        Ok(ImmutableNullIndex(self.0))
    }
}
