mod lifecycle;
mod read_ops;

use std::fmt::{Debug, Formatter};
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;

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

// `lifecycle::open_gridstore` / `open_mmap` construct every variant, but they
// have no in-lib caller yet, so the variants would trip `dead_code`. Allow at
// the enum level until a read-only segment wires the opens in.
#[allow(dead_code, clippy::enum_variant_names)]
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
/// - [`Self::is_on_disk`] / [`Self::ram_usage_bytes`] — telemetry /
///   placement queries.
/// - [`Self::populate`] / [`Self::clear_cache`] — OS page-cache control.
/// - [`Self::get_full_index_type`] / [`Self::get_mutability_type`] /
///   [`Self::get_storage_type`] — payload-config round-tripping.
///
/// The `todo!` arms are intentional placeholders, not soft failures — they
/// panic if hit at runtime so the missing wiring surfaces immediately rather
/// than degrading silently.
///
/// [1]: crate::index::field_index::FieldIndex
#[allow(dead_code)] // skeleton: no caller in the lib yet; surface is here for follow-ups
impl<S: UniversalReadExt> ReadOnlyFieldIndex<S> {
    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: forward `files` through `ReadOnlyMapIndex`")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("follow-up: forward `files` through `ReadOnlyNumericIndex`")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("follow-up: forward `files` through `ReadOnlyFullTextIndex`")
            }
            ReadOnlyFieldIndex::GeoIndex(index) => GeoIndexRead::files(index),
            ReadOnlyFieldIndex::BoolIndex(index) => BoolIndexRead::files(index),
            ReadOnlyFieldIndex::NullIndex(index) => NullIndexRead::files(index),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: forward `immutable_files` through `ReadOnlyMapIndex`")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("follow-up: forward `immutable_files` through `ReadOnlyNumericIndex`")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("follow-up: forward `immutable_files` through `ReadOnlyFullTextIndex`")
            }
            ReadOnlyFieldIndex::BoolIndex(_) => {
                todo!("follow-up: add `immutable_files` to `BoolIndexRead`")
            }
            ReadOnlyFieldIndex::NullIndex(_) => {
                todo!("follow-up: add `immutable_files` to `NullIndexRead`")
            }
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

    pub fn is_on_disk(&self) -> bool {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: forward `is_on_disk` through `ReadOnlyMapIndex`")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("follow-up: forward `is_on_disk` through `ReadOnlyNumericIndex`")
            }
            ReadOnlyFieldIndex::GeoIndex(index) => GeoIndexRead::is_on_disk(index),
            ReadOnlyFieldIndex::FullTextIndex(index) => FullTextIndexRead::is_on_disk(index),
            ReadOnlyFieldIndex::BoolIndex(index) => BoolIndexRead::is_on_disk(index),
            ReadOnlyFieldIndex::NullIndex(index) => NullIndexRead::is_on_disk(index),
        }
    }

    /// Populate all pages in the mmap. Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: forward `populate` through `ReadOnlyMapIndex`")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("follow-up: forward `populate` through `ReadOnlyNumericIndex`")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("follow-up: forward `populate` through `ReadOnlyFullTextIndex`")
            }
            ReadOnlyFieldIndex::GeoIndex(index) => GeoIndexRead::populate(index),
            ReadOnlyFieldIndex::BoolIndex(index) => BoolIndexRead::populate(index),
            ReadOnlyFieldIndex::NullIndex(index) => NullIndexRead::populate(index),
        }
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: forward `clear_cache` through `ReadOnlyMapIndex`")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("follow-up: forward `clear_cache` through `ReadOnlyNumericIndex`")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("follow-up: forward `clear_cache` through `ReadOnlyFullTextIndex`")
            }
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
    type Fs = S::Fs;

    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyFieldIndex::IntIndex(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFieldIndex::DatetimeIndex(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFieldIndex::IntMapIndex(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFieldIndex::KeywordIndex(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFieldIndex::FloatIndex(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFieldIndex::GeoIndex(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFieldIndex::FullTextIndex(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFieldIndex::BoolIndex(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFieldIndex::UuidIndex(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFieldIndex::UuidMapIndex(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyFieldIndex::NullIndex(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
        }
    }
}
