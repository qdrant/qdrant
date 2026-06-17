mod lifecycle;
mod read_ops;

use std::fmt::{Debug, Formatter};
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

pub(crate) use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::bool_index::ReadOnlyBoolIndex;
use crate::index::field_index::full_text_index::read_only::ReadOnlyFullTextIndex;
use crate::index::field_index::geo_index::ReadOnlyGeoIndex;
use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;
use crate::index::field_index::null_index::ReadOnlyNullIndex;
use crate::index::field_index::numeric_index::ReadOnlyNumericIndex;
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
pub enum ReadOnlyFieldIndex<S: UniversalRead> {
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
impl<S: UniversalRead> Debug for ReadOnlyFieldIndex<S> {
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
/// **Skeleton state.** Most arms are still `todo!` placeholders because the
/// per-index parent enums don't yet expose the matching inherent methods
/// in the branches that supply their `open_*`:
/// - [`ReadOnlyMapIndex`] only has `get_mutability_type` (added with the
///   map open PR), so the map-backed variants of `get_mutability_type` /
///   `get_full_index_type` are wired here.
/// - Every other lifecycle method (`populate`, `files`, ...) needs a small
///   follow-up that adds the corresponding method on each
///   `ReadOnly*Index` parent, dispatching to the leaf variant. That
///   follow-up lifts every arm in this block out of `todo!`.
///
/// The `todo!` arms are intentional skeleton placeholders, not soft
/// failures — they panic if hit at runtime so the missing wiring surfaces
/// immediately rather than degrading silently.
///
/// [1]: crate::index::field_index::FieldIndex
#[allow(dead_code)] // skeleton: no caller in the lib yet; surface is here for follow-ups
impl<S: UniversalRead> ReadOnlyFieldIndex<S> {
    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: add `ReadOnlyMapIndex::files` and dispatch")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("blocked on #9213 (numeric parent) + `files` on it")
            }
            ReadOnlyFieldIndex::GeoIndex(_) => {
                todo!("blocked on #9211 (geo parent) + `files` on it")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("blocked on #9222 (full-text parent) + `files` on it")
            }
            ReadOnlyFieldIndex::BoolIndex(_) => {
                todo!("blocked on #9200 (bool parent) + `files` on it")
            }
            ReadOnlyFieldIndex::NullIndex(_) => {
                todo!("blocked on #9197 (null parent) + `files` on it")
            }
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: add `ReadOnlyMapIndex::immutable_files` and dispatch")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("blocked on #9213 (numeric parent) + `immutable_files` on it")
            }
            ReadOnlyFieldIndex::GeoIndex(_) => {
                todo!("blocked on #9211 (geo parent) + `immutable_files` on it")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("blocked on #9222 (full-text parent) + `immutable_files` on it")
            }
            ReadOnlyFieldIndex::BoolIndex(_) => {
                todo!("blocked on #9200 (bool parent) + `immutable_files` on it")
            }
            ReadOnlyFieldIndex::NullIndex(_) => {
                todo!("blocked on #9197 (null parent) + `immutable_files` on it")
            }
        }
    }

    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: add `ReadOnlyMapIndex::ram_usage_bytes` and dispatch")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("blocked on #9213 (numeric parent) + `ram_usage_bytes` on it")
            }
            ReadOnlyFieldIndex::GeoIndex(_) => {
                todo!("blocked on #9211 (geo parent) + `ram_usage_bytes` on it")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("blocked on #9222 (full-text parent) + `ram_usage_bytes` on it")
            }
            ReadOnlyFieldIndex::BoolIndex(_) => {
                todo!("blocked on #9200 (bool parent) + `ram_usage_bytes` on it")
            }
            ReadOnlyFieldIndex::NullIndex(_) => {
                todo!("blocked on #9197 (null parent) + `ram_usage_bytes` on it")
            }
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: add `ReadOnlyMapIndex::is_on_disk` and dispatch")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("blocked on #9213 (numeric parent) + `is_on_disk` on it")
            }
            ReadOnlyFieldIndex::GeoIndex(_) => {
                todo!("blocked on #9211 (geo parent) + `is_on_disk` on it")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("blocked on #9222 (full-text parent) + `is_on_disk` on it")
            }
            ReadOnlyFieldIndex::BoolIndex(_) => {
                todo!("blocked on #9200 (bool parent) + `is_on_disk` on it")
            }
            ReadOnlyFieldIndex::NullIndex(_) => {
                todo!("blocked on #9197 (null parent) + `is_on_disk` on it")
            }
        }
    }

    /// Populate all pages in the mmap. Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: add `ReadOnlyMapIndex::populate` and dispatch")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("blocked on #9213 (numeric parent) + `populate` on it")
            }
            ReadOnlyFieldIndex::GeoIndex(_) => {
                todo!("blocked on #9211 (geo parent) + `populate` on it")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("blocked on #9222 (full-text parent) + `populate` on it")
            }
            ReadOnlyFieldIndex::BoolIndex(_) => {
                todo!("blocked on #9200 (bool parent) + `populate` on it")
            }
            ReadOnlyFieldIndex::NullIndex(_) => {
                todo!("blocked on #9197 (null parent) + `populate` on it")
            }
        }
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: add `ReadOnlyMapIndex::clear_cache` and dispatch")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("blocked on #9213 (numeric parent) + `clear_cache` on it")
            }
            ReadOnlyFieldIndex::GeoIndex(_) => {
                todo!("blocked on #9211 (geo parent) + `clear_cache` on it")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("blocked on #9222 (full-text parent) + `clear_cache` on it")
            }
            ReadOnlyFieldIndex::BoolIndex(_) => {
                todo!("blocked on #9200 (bool parent) + `clear_cache` on it")
            }
            ReadOnlyFieldIndex::NullIndex(_) => {
                todo!("blocked on #9197 (null parent) + `clear_cache` on it")
            }
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

    /// Map-backed variants dispatch to the wired
    /// [`ReadOnlyMapIndex::get_mutability_type`][1]; the rest are blocked
    /// on the per-index parent `get_mutability_type`, added with each
    /// per-index PR listed in [`Self::populate`].
    ///
    /// [1]: crate::index::field_index::map_index::read_only::ReadOnlyMapIndex::get_mutability_type
    fn get_mutability_type(&self) -> IndexMutability {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::KeywordIndex(index) => index.get_mutability_type(),
            ReadOnlyFieldIndex::UuidMapIndex(index) => index.get_mutability_type(),

            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("blocked on #9213 (numeric `get_mutability_type`)")
            }
            ReadOnlyFieldIndex::GeoIndex(_) => {
                todo!("blocked on #9211 (geo `get_mutability_type`)")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("blocked on #9222 (full-text `get_mutability_type`)")
            }
            ReadOnlyFieldIndex::BoolIndex(_) => {
                todo!("blocked on #9200 (bool `get_mutability_type`)")
            }
            ReadOnlyFieldIndex::NullIndex(_) => {
                todo!("blocked on #9197 (null `get_mutability_type`)")
            }
        }
    }

    fn get_storage_type(&self) -> StorageType {
        match self {
            ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_) => {
                todo!("follow-up: add `ReadOnlyMapIndex::get_storage_type` and dispatch")
            }
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_) => {
                todo!("blocked on #9213 (numeric `get_storage_type`)")
            }
            ReadOnlyFieldIndex::GeoIndex(_) => {
                todo!("blocked on #9211 (geo `get_storage_type`)")
            }
            ReadOnlyFieldIndex::FullTextIndex(_) => {
                todo!("blocked on #9222 (full-text `get_storage_type`)")
            }
            ReadOnlyFieldIndex::BoolIndex(_) => {
                todo!("blocked on #9200 (bool `get_storage_type`)")
            }
            ReadOnlyFieldIndex::NullIndex(_) => {
                todo!("blocked on #9197 (null `get_storage_type`)")
            }
        }
    }
}

impl<S: UniversalRead> LiveReload for ReadOnlyFieldIndex<S> {
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
