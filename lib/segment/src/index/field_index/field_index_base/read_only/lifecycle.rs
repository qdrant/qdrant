use std::path::Path;

use common::bitvec::BitSlice;
use common::universal_io::UniversalRead;

use super::ReadOnlyFieldIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::bool_index::ReadOnlyBoolIndex;
use crate::index::field_index::full_text_index::read_only::ReadOnlyFullTextIndex;
use crate::index::field_index::geo_index::ReadOnlyGeoMapIndex;
use crate::index::field_index::index_selector::{
    bool_dir, map_dir, null_dir, numeric_dir, text_dir,
};
use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;
use crate::index::field_index::null_index::ReadOnlyNullIndex;
use crate::index::field_index::numeric_index::ReadOnlyNumericIndex;
use crate::index::payload_config::{FullPayloadIndexType, PayloadIndexType};
use crate::json_path::JsonPath;
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, PayloadFieldSchema, UuidIntType,
};

impl<S: UniversalRead> ReadOnlyFieldIndex<S> {
    /// Read-only mirror of [`IndexSelector::new_index_with_type`][1] for the
    /// Gridstore (appendable) storage path: dispatches on
    /// [`FullPayloadIndexType::index_type`] and forwards to each per-index
    /// parent's appendable open, wrapping the leaf in the matching variant.
    ///
    /// `payload_schema` is consulted only by the full-text arm (it carries the
    /// [`TextIndexParams`] the leaf open needs) and `total_point_count` only by
    /// the null arm (it is segment-wide, not recoverable from the index files);
    /// the other arms ignore both, mirroring the writable selector.
    ///
    /// [1]: crate::index::field_index::index_selector::IndexSelector::new_index_with_type
    #[allow(dead_code)] // no caller in the lib yet
    pub fn open_gridstore(
        fs: &S::Fs,
        dir: &Path,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
        index_type: &FullPayloadIndexType,
        total_point_count: usize,
    ) -> OperationResult<Option<Self>> {
        let index = match index_type.index_type {
            PayloadIndexType::KeywordIndex => {
                ReadOnlyMapIndex::<str, S>::open_appendable(fs, map_dir(dir, field))?
                    .map(Self::KeywordIndex)
            }
            PayloadIndexType::IntMapIndex => {
                ReadOnlyMapIndex::<IntPayloadType, S>::open_appendable(fs, map_dir(dir, field))?
                    .map(Self::IntMapIndex)
            }
            // Matches the writable selector's `(PayloadIndexType::UuidIndex,
            // PayloadSchemaParams::Uuid(_))` arm, which constructs a
            // `MapIndex<UuidIntType>` and wraps it in `FieldIndex::UuidMapIndex`
            // — the `UuidIndex` discriminant is historically map-backed.
            PayloadIndexType::UuidIndex | PayloadIndexType::UuidMapIndex => {
                ReadOnlyMapIndex::<UuidIntType, S>::open_appendable(fs, map_dir(dir, field))?
                    .map(Self::UuidMapIndex)
            }
            PayloadIndexType::IntIndex => {
                ReadOnlyNumericIndex::<IntPayloadType, IntPayloadType, S>::open_appendable(
                    fs,
                    numeric_dir(dir, field),
                )?
                .map(Self::IntIndex)
            }
            PayloadIndexType::DatetimeIndex => {
                ReadOnlyNumericIndex::<IntPayloadType, DateTimePayloadType, S>::open_appendable(
                    fs,
                    numeric_dir(dir, field),
                )?
                .map(Self::DatetimeIndex)
            }
            PayloadIndexType::FloatIndex => {
                ReadOnlyNumericIndex::<FloatPayloadType, FloatPayloadType, S>::open_appendable(
                    fs,
                    numeric_dir(dir, field),
                )?
                .map(Self::FloatIndex)
            }
            // Geo reuses the writable selector's `map_dir` (`-map` suffix).
            PayloadIndexType::GeoIndex => {
                ReadOnlyGeoMapIndex::open_gridstore(fs, map_dir(dir, field))?.map(Self::GeoIndex)
            }
            PayloadIndexType::FullTextIndex => {
                let config = TextIndexParams::try_from(payload_schema)?;
                ReadOnlyFullTextIndex::open_appendable(fs, text_dir(dir, field), config)?
                    .map(Self::FullTextIndex)
            }
            PayloadIndexType::BoolIndex => {
                ReadOnlyBoolIndex::open::<S>(fs, &bool_dir(dir, field))?.map(Self::BoolIndex)
            }
            PayloadIndexType::NullIndex => {
                ReadOnlyNullIndex::open::<S>(fs, &null_dir(dir, field), total_point_count)?
                    .map(Self::NullIndex)
            }
        };
        Ok(index)
    }

    /// Read-only mirror of [`IndexSelector::new_index_with_type`][1] for the
    /// mmap (immutable) storage path: dispatches on
    /// [`FullPayloadIndexType::index_type`] and forwards to each per-index
    /// parent's immutable open.
    ///
    /// Generic over `S` like [`Self::open_gridstore`]: every immutable
    /// per-index open threads the filesystem handle `fs` (the map, numeric, geo
    /// and full-text leaves are all fs-generic), so the dispatcher needn't fix
    /// `S = MmapFile`. `is_on_disk` / `deleted_points` reach the mmap-only
    /// leaves; the roaring-flag bool and null leaves ignore them (a single
    /// `open` serves both storage paths).
    ///
    /// [1]: crate::index::field_index::index_selector::IndexSelector::new_index_with_type
    #[allow(dead_code, clippy::too_many_arguments)] // no caller in the lib yet
    pub fn open_mmap(
        fs: &S::Fs,
        dir: &Path,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
        index_type: &FullPayloadIndexType,
        total_point_count: usize,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let index = match index_type.index_type {
            PayloadIndexType::KeywordIndex => ReadOnlyMapIndex::<str, S>::open_immutable(
                fs,
                &map_dir(dir, field),
                is_on_disk,
                deleted_points,
            )?
            .map(Self::KeywordIndex),
            PayloadIndexType::IntMapIndex => ReadOnlyMapIndex::<IntPayloadType, S>::open_immutable(
                fs,
                &map_dir(dir, field),
                is_on_disk,
                deleted_points,
            )?
            .map(Self::IntMapIndex),
            PayloadIndexType::UuidIndex | PayloadIndexType::UuidMapIndex => {
                ReadOnlyMapIndex::<UuidIntType, S>::open_immutable(
                    fs,
                    &map_dir(dir, field),
                    is_on_disk,
                    deleted_points,
                )?
                .map(Self::UuidMapIndex)
            }
            PayloadIndexType::IntIndex => {
                ReadOnlyNumericIndex::<IntPayloadType, IntPayloadType, S>::open_immutable(
                    fs,
                    &numeric_dir(dir, field),
                    is_on_disk,
                    deleted_points,
                )?
                .map(Self::IntIndex)
            }
            PayloadIndexType::DatetimeIndex => {
                ReadOnlyNumericIndex::<IntPayloadType, DateTimePayloadType, S>::open_immutable(
                    fs,
                    &numeric_dir(dir, field),
                    is_on_disk,
                    deleted_points,
                )?
                .map(Self::DatetimeIndex)
            }
            PayloadIndexType::FloatIndex => {
                ReadOnlyNumericIndex::<FloatPayloadType, FloatPayloadType, S>::open_immutable(
                    fs,
                    &numeric_dir(dir, field),
                    is_on_disk,
                    deleted_points,
                )?
                .map(Self::FloatIndex)
            }
            // Geo reuses the writable selector's `map_dir` (`-map` suffix).
            PayloadIndexType::GeoIndex => ReadOnlyGeoMapIndex::open_mmap(
                fs,
                &map_dir(dir, field),
                is_on_disk,
                deleted_points,
            )?
            .map(Self::GeoIndex),
            PayloadIndexType::FullTextIndex => {
                let config = TextIndexParams::try_from(payload_schema)?;
                ReadOnlyFullTextIndex::open_immutable(
                    fs,
                    text_dir(dir, field),
                    config,
                    is_on_disk,
                    deleted_points,
                )?
                .map(Self::FullTextIndex)
            }
            // Bool and null are roaring-flag backed: a single read-only `open`
            // serves both storage paths, so the mmap arm matches the gridstore
            // arm (neither consumes `is_on_disk` / `deleted_points`).
            PayloadIndexType::BoolIndex => {
                ReadOnlyBoolIndex::open::<S>(fs, &bool_dir(dir, field))?.map(Self::BoolIndex)
            }
            PayloadIndexType::NullIndex => {
                ReadOnlyNullIndex::open::<S>(fs, &null_dir(dir, field), total_point_count)?
                    .map(Self::NullIndex)
            }
        };
        Ok(index)
    }
}
