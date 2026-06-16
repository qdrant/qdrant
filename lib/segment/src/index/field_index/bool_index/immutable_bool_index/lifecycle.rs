use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::super::mutable_bool_index::MutableBoolIndex;
use super::super::read_ops::BoolIndexRead;
use super::ImmutableBoolIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex, ValueIndexer};

impl ImmutableBoolIndex {
    pub fn builder(path: &Path) -> OperationResult<ImmutableBoolIndexBuilder> {
        Ok(ImmutableBoolIndexBuilder(
            MutableBoolIndex::open(path, true)?.ok_or_else(|| {
                OperationError::service_error("Failed to create and open MutableBoolIndex")
            })?,
        ))
    }

    pub fn from_mutable(mutable_index: MutableBoolIndex) -> OperationResult<Self> {
        mutable_index.flusher()()?;
        Ok(Self(mutable_index))
    }

    pub fn open(path: &Path, deleted: &BitSlice) -> OperationResult<Option<Self>> {
        Ok(MutableBoolIndex::open_immutable(path, deleted)?.map(Self))
    }

    #[inline]
    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.0.set_or_insert_immutable(id, false, false);
        Ok(())
    }
}

impl PayloadFieldIndex for ImmutableBoolIndex {
    #[inline]
    fn wipe(self) -> OperationResult<()> {
        self.0.wipe()
    }

    #[inline]
    fn files(&self) -> Vec<PathBuf> {
        BoolIndexRead::files(self)
    }

    #[inline]
    fn immutable_files(&self) -> Vec<PathBuf> {
        BoolIndexRead::files(self) // All the files are immutable in this index.
    }

    #[inline]
    fn flusher(&self) -> crate::common::Flusher {
        Box::new(|| Ok(())) // No op for an immutable index.
    }
}

pub struct ImmutableBoolIndexBuilder(MutableBoolIndex);

impl FieldIndexBuilderTrait for ImmutableBoolIndexBuilder {
    type FieldIndexType = ImmutableBoolIndex;

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
        Ok(ImmutableBoolIndex(self.0))
    }
}
