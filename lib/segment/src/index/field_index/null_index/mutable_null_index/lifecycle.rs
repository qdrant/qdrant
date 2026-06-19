use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};
use fs_err as fs;
use serde_json::Value;

use super::super::read_ops::NullIndexRead;
use super::{HAS_VALUES_DIRNAME, IS_NULL_DIRNAME, MutableNullIndex, Storage};
use crate::common::Flusher;
use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;
use crate::common::flags::roaring_flags::RoaringFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex};
use crate::index::payload_config::IndexMutability;

impl MutableNullIndex {
    pub fn builder(
        path: &Path,
        total_point_count: usize,
    ) -> OperationResult<MutableNullIndexBuilder> {
        Ok(MutableNullIndexBuilder(
            Self::open(path, total_point_count, true)?.ok_or_else(|| {
                OperationError::service_error(format!(
                    "Failed to create and open mutable null index at path: {}",
                    path.display(),
                ))
            })?,
        ))
    }

    /// Open and load or create a mutable null index at the given path.
    ///
    /// # Arguments
    /// - `path` - The directory where the index files should live, must be exclusive to this index.
    /// - `total_point_count` - Total number of points in the segment.
    /// - `create_if_missing` - If true, creates the index if it doesn't exist.
    pub fn open(
        path: &Path,
        total_point_count: usize,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        let has_values_dir = path.join(HAS_VALUES_DIRNAME);

        // If has values directory doesn't exist, assume the index doesn't exist on disk
        if !has_values_dir.is_dir() && !create_if_missing {
            return Ok(None);
        }

        Ok(Some(Self::open_or_create(path, total_point_count)?))
    }

    pub(in super::super) fn open_immutable(
        path: &Path,
        total_point_count: usize,
        deleted: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let mutable_null_index = Self::open(path, total_point_count, false)?;
        match mutable_null_index {
            Some(mut mutable_null_index) => {
                for pos in deleted.iter_ones() {
                    mutable_null_index.remove_point_immutable(pos as PointOffsetType);
                }
                Ok(Some(mutable_null_index))
            }
            None => Ok(None),
        }
    }

    fn open_or_create(path: &Path, total_point_count: usize) -> OperationResult<Self> {
        fs::create_dir_all(path).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to create mutable-null-index directory: {err}, path: {path:?}"
            ))
        })?;

        let has_values_path = path.join(HAS_VALUES_DIRNAME);
        let has_values_mmap = DynamicStoredFlags::open(&MmapFs, &has_values_path, Populate::No)?;
        let has_values_flags = RoaringFlags::new(MmapFs, has_values_mmap)?;

        let is_null_path = path.join(IS_NULL_DIRNAME);
        let is_null_mmap = DynamicStoredFlags::open(&MmapFs, &is_null_path, Populate::No)?;
        let is_null_flags = RoaringFlags::new(MmapFs, is_null_mmap)?;

        let storage = Storage {
            has_values_flags,
            is_null_flags,
        };

        Ok(Self {
            base_dir: path.to_path_buf(),
            storage,
            total_point_count,
        })
    }

    pub fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut is_null = false;
        let mut has_values = false;

        for value in payload {
            match value {
                Value::Null => {
                    is_null = true;
                }
                Value::Bool(_) => {
                    has_values = true;
                }
                Value::Number(_) => {
                    has_values = true;
                }
                Value::String(_) => {
                    has_values = true;
                }
                Value::Array(array) => {
                    if array.iter().any(|v| v.is_null()) {
                        is_null = true;
                    }
                    if !array.is_empty() {
                        has_values = true;
                    }
                }
                Value::Object(_) => {
                    has_values = true;
                }
            }
            if is_null && has_values {
                break;
            }
        }

        self.storage.has_values_flags.set(id, has_values);
        self.storage.is_null_flags.set(id, is_null);

        // Bump total points
        self.total_point_count = std::cmp::max(self.total_point_count, id as usize + 1);

        // Account for I/O cost as if we were writing to disk now
        hw_counter.payload_index_io_write_counter().incr_delta(2);

        Ok(())
    }

    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        // Update bitmaps immediately
        self.storage.has_values_flags.set(id, false);
        self.storage.is_null_flags.set(id, false);

        // Bump total points
        // We MUST bump the total point count when removing a point too
        // On upsert without this respective field, remove point is called rather than add point
        // Bumping the total point count ensures we correctly estimate the number of points
        // Bug: <https://github.com/qdrant/qdrant/pull/6882>
        self.total_point_count = std::cmp::max(self.total_point_count, id as usize + 1);

        // Account for I/O cost as if we were writing to disk now
        let hw_counter = HardwareCounterCell::disposable();
        hw_counter.payload_index_io_write_counter().incr_delta(2);

        Ok(())
    }

    pub(in super::super) fn remove_point_immutable(&mut self, id: PointOffsetType) {
        // Update bitmaps immediately
        self.storage.has_values_flags.set_immutable(id, false);
        self.storage.is_null_flags.set_immutable(id, false);

        // N.B. We do not update total_point_count because it comes from the id tracker and is not not changed
        // in non-appendable segments.

        // N.B. No I/O, do not update hw_counter.
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        IndexMutability::Mutable
    }
}

impl PayloadFieldIndex for MutableNullIndex {
    fn wipe(self) -> OperationResult<()> {
        let base_dir = self.base_dir.clone();
        // drop mmap handles before deleting files
        drop(self);
        if base_dir.is_dir() {
            fs::remove_dir_all(&base_dir)?;
        }
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let flush_has_values = self.storage.has_values_flags.flusher();
        let flush_is_null = self.storage.is_null_flags.flusher();

        Box::new(move || {
            flush_has_values()?;
            flush_is_null()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        NullIndexRead::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new() // everything is mutable
    }
}

pub struct MutableNullIndexBuilder(pub(super) MutableNullIndex);

impl FieldIndexBuilderTrait for MutableNullIndexBuilder {
    type FieldIndexType = MutableNullIndex;

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
        // Flush any remaining buffered updates
        self.0.flusher()()?;
        Ok(self.0)
    }
}
