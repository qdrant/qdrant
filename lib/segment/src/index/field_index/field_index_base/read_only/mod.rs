mod lifecycle;
mod read_ops;

use std::fmt::{Debug, Formatter};
use std::path::PathBuf;

use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalReadFs};
use futures::future::BoxFuture;

pub(crate) use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::index::UniversalReadExt;
use crate::index::field_index::bool_index::{BoolIndexRead, ReadOnlyBoolIndex};
use crate::index::field_index::full_text_index::full_text_index_read::FullTextIndexRead;
use crate::index::field_index::full_text_index::read_only::ReadOnlyFullTextIndex;
use crate::index::field_index::geo_index::{GeoIndexRead, ReadOnlyGeoIndex};
use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;
use crate::index::field_index::map_index::read_ops::MapIndexRead;
use crate::index::field_index::null_index::{NullIndexRead, ReadOnlyNullIndex};
use crate::index::field_index::numeric_index::{NumericIndexRead, ReadOnlyNumericIndex};
use crate::index::payload_config::{
    FullPayloadIndexType, IndexMutability, PayloadIndexType, StorageType,
};
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, UuidIntType, UuidPayloadType,
};

pub enum ReadOnlyFieldIndex<S: UniversalReadExt> {
    IntIndex(ReadOnlyNumericIndex<IntPayloadType, IntPayloadType, S>),
    DatetimeIndex(ReadOnlyNumericIndex<IntPayloadType, DateTimePayloadType, S>),
    IntMapIndex(ReadOnlyMapIndex<IntPayloadType, S>),
    KeywordIndex(ReadOnlyMapIndex<str, S>),
    FloatIndex(ReadOnlyNumericIndex<FloatPayloadType, FloatPayloadType, S>),
    GeoIndex(ReadOnlyGeoIndex<S>),
    FullTextIndex(ReadOnlyFullTextIndex<S>),
    BoolIndex(ReadOnlyBoolIndex<S>),
    UuidIndex(ReadOnlyNumericIndex<UuidIntType, UuidPayloadType, S>),
    UuidMapIndex(ReadOnlyMapIndex<UuidIntType, S>),
    NullIndex(ReadOnlyNullIndex<S>),
}

/// Mirrors [`impl Debug for FieldIndex`][1] one-for-one: each arm prints
/// the variant's discriminant. No payload is rendered (matches the
/// writable side, where the underlying typed index is also not formatted).
///
/// [1]: crate::index::field_index::FieldIndex
impl<S: UniversalReadExt> Debug for ReadOnlyFieldIndex<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadOnlyFieldIndex::IntIndex(_) => write!(f, "IntIndex"),
            ReadOnlyFieldIndex::DatetimeIndex(_) => write!(f, "DatetimeIndex"),
            ReadOnlyFieldIndex::IntMapIndex(_) => write!(f, "IntMapIndex"),
            ReadOnlyFieldIndex::KeywordIndex(_) => write!(f, "KeywordIndex"),
            ReadOnlyFieldIndex::FloatIndex(_) => write!(f, "FloatIndex"),
            ReadOnlyFieldIndex::GeoIndex(_) => write!(f, "GeoIndex"),
            ReadOnlyFieldIndex::BoolIndex(_) => write!(f, "BoolIndex"),
            ReadOnlyFieldIndex::FullTextIndex(_) => write!(f, "FullTextIndex"),
            ReadOnlyFieldIndex::UuidIndex(_) => write!(f, "UuidIndex"),
            ReadOnlyFieldIndex::UuidMapIndex(_) => write!(f, "UuidMapIndex"),
            ReadOnlyFieldIndex::NullIndex(_) => write!(f, "NullIndex"),
        }
    }
}

/// Read-only mirror of the lifecycle / introspection surface on the
/// writable [`FieldIndex`][1].
///
/// Skipped from the writable surface:
/// - `wipe(self)`, `flusher`, `add_point`, `remove_point` — write-only;
///   they never make sense on the read-only wrapper.
///
/// Mirrored here:
/// - [`Self::files`] / [`Self::immutable_files`] — file enumeration for
///   cache management.
/// - [`Self::is_cold`] / [`Self::ram_usage_bytes`] — telemetry /
///   placement queries.
/// - [`Self::populate`] / [`Self::clear_cache`] — OS page-cache control.
/// - [`Self::get_full_index_type`] / [`Self::get_mutability_type`] /
///   [`Self::get_storage_type`] — payload-config round-tripping.
///
/// [1]: crate::index::field_index::FieldIndex
impl<S: UniversalReadExt> ReadOnlyFieldIndex<S> {
    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(index) => index.files(),
            ReadOnlyFieldIndex::KeywordIndex(index) => index.files(),
            ReadOnlyFieldIndex::UuidMapIndex(index) => index.files(),
            ReadOnlyFieldIndex::IntIndex(index) => index.files(),
            ReadOnlyFieldIndex::DatetimeIndex(index) => index.files(),
            ReadOnlyFieldIndex::FloatIndex(index) => index.files(),
            ReadOnlyFieldIndex::UuidIndex(index) => index.files(),
            ReadOnlyFieldIndex::FullTextIndex(index) => index.files(),
            ReadOnlyFieldIndex::GeoIndex(index) => GeoIndexRead::files(index),
            ReadOnlyFieldIndex::BoolIndex(index) => BoolIndexRead::files(index),
            ReadOnlyFieldIndex::NullIndex(index) => NullIndexRead::files(index),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(index) => index.immutable_files(),
            ReadOnlyFieldIndex::KeywordIndex(index) => index.immutable_files(),
            ReadOnlyFieldIndex::UuidMapIndex(index) => index.immutable_files(),
            ReadOnlyFieldIndex::IntIndex(index) => index.immutable_files(),
            ReadOnlyFieldIndex::DatetimeIndex(index) => index.immutable_files(),
            ReadOnlyFieldIndex::FloatIndex(index) => index.immutable_files(),
            ReadOnlyFieldIndex::UuidIndex(index) => index.immutable_files(),
            ReadOnlyFieldIndex::FullTextIndex(index) => index.immutable_files(),
            ReadOnlyFieldIndex::BoolIndex(index) => BoolIndexRead::immutable_files(index),
            ReadOnlyFieldIndex::NullIndex(index) => NullIndexRead::immutable_files(index),
            ReadOnlyFieldIndex::GeoIndex(index) => GeoIndexRead::immutable_files(index),
        }
    }

    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            ReadOnlyFieldIndex::IntIndex(index) => NumericIndexRead::ram_usage_bytes(index),
            ReadOnlyFieldIndex::DatetimeIndex(index) => NumericIndexRead::ram_usage_bytes(index),
            ReadOnlyFieldIndex::IntMapIndex(index) => MapIndexRead::ram_usage_bytes(index),
            ReadOnlyFieldIndex::KeywordIndex(index) => MapIndexRead::ram_usage_bytes(index),
            ReadOnlyFieldIndex::FloatIndex(index) => NumericIndexRead::ram_usage_bytes(index),
            ReadOnlyFieldIndex::GeoIndex(index) => GeoIndexRead::ram_usage_bytes(index),
            ReadOnlyFieldIndex::FullTextIndex(index) => FullTextIndexRead::ram_usage_bytes(index),
            ReadOnlyFieldIndex::BoolIndex(index) => BoolIndexRead::ram_usage_bytes(index),
            ReadOnlyFieldIndex::UuidIndex(index) => NumericIndexRead::ram_usage_bytes(index),
            ReadOnlyFieldIndex::UuidMapIndex(index) => MapIndexRead::ram_usage_bytes(index),
            ReadOnlyFieldIndex::NullIndex(index) => NullIndexRead::ram_usage_bytes(index),
        }
    }

    pub fn is_cold(&self) -> bool {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(index) => index.is_cold(),
            ReadOnlyFieldIndex::KeywordIndex(index) => index.is_cold(),
            ReadOnlyFieldIndex::UuidMapIndex(index) => index.is_cold(),
            ReadOnlyFieldIndex::IntIndex(index) => index.is_cold(),
            ReadOnlyFieldIndex::DatetimeIndex(index) => index.is_cold(),
            ReadOnlyFieldIndex::FloatIndex(index) => index.is_cold(),
            ReadOnlyFieldIndex::UuidIndex(index) => index.is_cold(),
            ReadOnlyFieldIndex::GeoIndex(index) => GeoIndexRead::is_cold(index),
            ReadOnlyFieldIndex::FullTextIndex(index) => FullTextIndexRead::is_cold(index),
            ReadOnlyFieldIndex::BoolIndex(index) => BoolIndexRead::is_cold(index),
            ReadOnlyFieldIndex::NullIndex(index) => NullIndexRead::is_cold(index),
        }
    }

    /// Populate all pages in the mmap. Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(index) => index.populate(),
            ReadOnlyFieldIndex::KeywordIndex(index) => index.populate(),
            ReadOnlyFieldIndex::UuidMapIndex(index) => index.populate(),
            ReadOnlyFieldIndex::IntIndex(index) => index.populate(),
            ReadOnlyFieldIndex::DatetimeIndex(index) => index.populate(),
            ReadOnlyFieldIndex::FloatIndex(index) => index.populate(),
            ReadOnlyFieldIndex::UuidIndex(index) => index.populate(),
            ReadOnlyFieldIndex::FullTextIndex(index) => index.populate(),
            ReadOnlyFieldIndex::GeoIndex(index) => GeoIndexRead::populate(index),
            ReadOnlyFieldIndex::BoolIndex(index) => BoolIndexRead::populate(index),
            ReadOnlyFieldIndex::NullIndex(index) => NullIndexRead::populate(index),
        }
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(index) => index.clear_cache(),
            ReadOnlyFieldIndex::KeywordIndex(index) => index.clear_cache(),
            ReadOnlyFieldIndex::UuidMapIndex(index) => index.clear_cache(),
            ReadOnlyFieldIndex::IntIndex(index) => index.clear_cache(),
            ReadOnlyFieldIndex::DatetimeIndex(index) => index.clear_cache(),
            ReadOnlyFieldIndex::FloatIndex(index) => index.clear_cache(),
            ReadOnlyFieldIndex::UuidIndex(index) => index.clear_cache(),
            ReadOnlyFieldIndex::FullTextIndex(index) => index.clear_cache(),
            ReadOnlyFieldIndex::GeoIndex(index) => GeoIndexRead::clear_cache(index),
            ReadOnlyFieldIndex::BoolIndex(index) => BoolIndexRead::clear_cache(index),
            ReadOnlyFieldIndex::NullIndex(index) => NullIndexRead::clear_cache(index),
        }
    }

    /// Composes [`FullPayloadIndexType`] from the discriminant + the per-arm
    /// `get_mutability_type` / `get_storage_type` — same shape as the
    /// writable side.
    pub fn get_full_index_type(&self) -> FullPayloadIndexType {
        let index_type = match self {
            ReadOnlyFieldIndex::IntIndex(_) => PayloadIndexType::IntIndex,
            ReadOnlyFieldIndex::DatetimeIndex(_) => PayloadIndexType::DatetimeIndex,
            ReadOnlyFieldIndex::IntMapIndex(_) => PayloadIndexType::IntMapIndex,
            ReadOnlyFieldIndex::KeywordIndex(_) => PayloadIndexType::KeywordIndex,
            ReadOnlyFieldIndex::FloatIndex(_) => PayloadIndexType::FloatIndex,
            ReadOnlyFieldIndex::GeoIndex(_) => PayloadIndexType::GeoIndex,
            ReadOnlyFieldIndex::FullTextIndex(_) => PayloadIndexType::FullTextIndex,
            ReadOnlyFieldIndex::BoolIndex(_) => PayloadIndexType::BoolIndex,
            ReadOnlyFieldIndex::UuidIndex(_) => PayloadIndexType::UuidIndex,
            ReadOnlyFieldIndex::UuidMapIndex(_) => PayloadIndexType::UuidMapIndex,
            ReadOnlyFieldIndex::NullIndex(_) => PayloadIndexType::NullIndex,
        };
        FullPayloadIndexType {
            index_type,
            mutability: self.get_mutability_type(),
            storage_type: self.get_storage_type(),
        }
    }

    fn get_mutability_type(&self) -> IndexMutability {
        match self {
            ReadOnlyFieldIndex::IntIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::DatetimeIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::IntMapIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::KeywordIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::FloatIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::GeoIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::FullTextIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::BoolIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::UuidIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::UuidMapIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::NullIndex(index) => index.get_mutability_type(),
        }
    }

    fn get_storage_type(&self) -> StorageType {
        match self {
            ReadOnlyFieldIndex::IntIndex(index) => NumericIndexRead::storage_type(index),
            ReadOnlyFieldIndex::DatetimeIndex(index) => NumericIndexRead::storage_type(index),
            ReadOnlyFieldIndex::IntMapIndex(index) => MapIndexRead::storage_type(index),
            ReadOnlyFieldIndex::KeywordIndex(index) => MapIndexRead::storage_type(index),
            ReadOnlyFieldIndex::FloatIndex(index) => NumericIndexRead::storage_type(index),
            ReadOnlyFieldIndex::GeoIndex(index) => GeoIndexRead::get_storage_type(index),
            ReadOnlyFieldIndex::FullTextIndex(index) => FullTextIndexRead::get_storage_type(index),
            ReadOnlyFieldIndex::BoolIndex(index) => BoolIndexRead::get_storage_type(index),
            ReadOnlyFieldIndex::UuidIndex(index) => NumericIndexRead::storage_type(index),
            ReadOnlyFieldIndex::UuidMapIndex(index) => MapIndexRead::storage_type(index),
            ReadOnlyFieldIndex::NullIndex(index) => NullIndexRead::get_storage_type(index),
        }
    }
}

impl<S: UniversalReadExt> LiveReload for ReadOnlyFieldIndex<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        match self {
            ReadOnlyFieldIndex::IntIndex(index) => index.live_preload(fs),
            ReadOnlyFieldIndex::DatetimeIndex(index) => index.live_preload(fs),
            ReadOnlyFieldIndex::IntMapIndex(index) => index.live_preload(fs),
            ReadOnlyFieldIndex::KeywordIndex(index) => index.live_preload(fs),
            ReadOnlyFieldIndex::FloatIndex(index) => index.live_preload(fs),
            ReadOnlyFieldIndex::GeoIndex(index) => index.live_preload(fs),
            ReadOnlyFieldIndex::FullTextIndex(index) => index.live_preload(fs),
            ReadOnlyFieldIndex::BoolIndex(index) => index.live_preload(fs),
            ReadOnlyFieldIndex::UuidIndex(index) => index.live_preload(fs),
            ReadOnlyFieldIndex::UuidMapIndex(index) => index.live_preload(fs),
            ReadOnlyFieldIndex::NullIndex(index) => index.live_preload(fs),
        }
    }

    fn apply_deletions(
        &mut self,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyFieldIndex::IntIndex(index) => index.apply_deletions(deleted_points),
            ReadOnlyFieldIndex::DatetimeIndex(index) => index.apply_deletions(deleted_points),
            ReadOnlyFieldIndex::IntMapIndex(index) => index.apply_deletions(deleted_points),
            ReadOnlyFieldIndex::KeywordIndex(index) => index.apply_deletions(deleted_points),
            ReadOnlyFieldIndex::FloatIndex(index) => index.apply_deletions(deleted_points),
            ReadOnlyFieldIndex::GeoIndex(index) => index.apply_deletions(deleted_points),
            ReadOnlyFieldIndex::FullTextIndex(index) => index.apply_deletions(deleted_points),
            ReadOnlyFieldIndex::BoolIndex(index) => index.apply_deletions(deleted_points),
            ReadOnlyFieldIndex::UuidIndex(index) => index.apply_deletions(deleted_points),
            ReadOnlyFieldIndex::UuidMapIndex(index) => index.apply_deletions(deleted_points),
            ReadOnlyFieldIndex::NullIndex(index) => index.apply_deletions(deleted_points),
        }
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyFieldIndex::IntIndex(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyFieldIndex::DatetimeIndex(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyFieldIndex::IntMapIndex(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyFieldIndex::KeywordIndex(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyFieldIndex::FloatIndex(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyFieldIndex::GeoIndex(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyFieldIndex::FullTextIndex(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyFieldIndex::BoolIndex(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyFieldIndex::UuidIndex(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyFieldIndex::UuidMapIndex(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyFieldIndex::NullIndex(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
        }
    }
}
