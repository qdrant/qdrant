use std::path::Path;

use common::bitvec::BitSlice;
use common::universal_io::{CachedReadFs, UniversalReadFs};

use super::ReadOnlyFieldIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::index::TextIndexParams;
use crate::index::UniversalReadExt;
use crate::index::field_index::bool_index::ReadOnlyBoolIndex;
use crate::index::field_index::full_text_index::read_only::ReadOnlyFullTextIndex;
use crate::index::field_index::geo_index::ReadOnlyGeoIndex;
use crate::index::field_index::index_selector::{
    bool_dir, map_dir, null_dir, numeric_dir, text_dir,
};
use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;
use crate::index::field_index::null_index::ReadOnlyNullIndex;
use crate::index::field_index::numeric_index::ReadOnlyNumericIndex;
use crate::index::payload_config::{FullPayloadIndexType, PayloadIndexType, StorageType};
use crate::json_path::JsonPath;
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, PayloadFieldSchema, UuidIntType,
};

/// Which read-only open path a leaf index should take, derived from the stored
/// [`StorageType`]. Phrased as a mutability distinction (matching the
/// `open_appendable` / `open_immutable` leaf methods) rather than a concrete
/// backend: the read-only stack is generic over [`UniversalRead`](common::universal_io::UniversalRead), so the
/// on-disk-vs-in-memory choice is just the `is_on_disk` flag on the immutable
/// variant, picked from the index type rather than passed in by the caller.
#[derive(Clone, Copy)]
enum ReadMode {
    /// Appendable on-disk format — opened via `open_appendable`.
    Appendable,
    /// Immutable on-disk format — opened via `open_immutable`. `is_on_disk`
    /// selects keeping the data on disk versus loading it into memory.
    Immutable { is_on_disk: bool },
}

impl<S: UniversalReadExt> ReadOnlyFieldIndex<S> {
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        dir: &Path,
        field: &JsonPath,
        index_type: &FullPayloadIndexType,
    ) -> OperationResult<bool> {
        let mode = match index_type.storage_type {
            StorageType::Gridstore => ReadMode::Appendable,
            StorageType::Mmap { is_on_disk } => ReadMode::Immutable { is_on_disk },
        };

        let preopened = match index_type.index_type {
            PayloadIndexType::KeywordIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyMapIndex::<str, S>::preopen_appendable(fs, map_dir(dir, field))?
                }
                ReadMode::Immutable { is_on_disk } => {
                    ReadOnlyMapIndex::<str, S>::preopen_immutable(
                        fs,
                        &map_dir(dir, field),
                        is_on_disk,
                    )?
                }
            },
            PayloadIndexType::IntMapIndex => match mode {
                ReadMode::Appendable => ReadOnlyMapIndex::<IntPayloadType, S>::preopen_appendable(
                    fs,
                    map_dir(dir, field),
                )?,
                ReadMode::Immutable { is_on_disk } => {
                    ReadOnlyMapIndex::<IntPayloadType, S>::preopen_immutable(
                        fs,
                        &map_dir(dir, field),
                        is_on_disk,
                    )?
                }
            },
            PayloadIndexType::UuidIndex | PayloadIndexType::UuidMapIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyMapIndex::<UuidIntType, S>::preopen_appendable(fs, map_dir(dir, field))?
                }
                ReadMode::Immutable { is_on_disk } => {
                    ReadOnlyMapIndex::<UuidIntType, S>::preopen_immutable(
                        fs,
                        &map_dir(dir, field),
                        is_on_disk,
                    )?
                }
            },
            PayloadIndexType::IntIndex => match mode {
                ReadMode::Appendable => false,
                ReadMode::Immutable { is_on_disk: _ } => false,
            },
            PayloadIndexType::DatetimeIndex => match mode {
                ReadMode::Appendable => false,
                ReadMode::Immutable { is_on_disk: _ } => false,
            },
            PayloadIndexType::FloatIndex => match mode {
                ReadMode::Appendable => false,
                ReadMode::Immutable { is_on_disk: _ } => false,
            },
            // Geo reuses the writable selector's `map_dir` (`-map` suffix).
            PayloadIndexType::GeoIndex => match mode {
                ReadMode::Appendable => false,
                ReadMode::Immutable { is_on_disk: _ } => false,
            },
            PayloadIndexType::FullTextIndex => match mode {
                ReadMode::Appendable => false,
                ReadMode::Immutable { is_on_disk: _ } => false,
            },
            // Bool and null are roaring-flag backed: a single read-only `open`
            // serves both modes (neither consumes the immutable-only
            // `is_on_disk` / `deleted_points`).
            PayloadIndexType::BoolIndex => false,
            PayloadIndexType::NullIndex => false,
        };

        Ok(preopened)
    }

    /// Read-only mirror of [`IndexSelector::new_index_with_type`][1]: dispatches
    /// on [`FullPayloadIndexType::index_type`] and forwards to each per-index
    /// parent's open, wrapping the leaf in the matching variant.
    ///
    /// The open path (appendable vs immutable) is picked from the stored
    /// [`FullPayloadIndexType::storage_type`]; `is_on_disk` rides on the
    /// immutable mode. Generic over `S`: every per-index open threads the
    /// [`UniversalRead`](common::universal_io::UniversalRead) handle `fs` (the map, numeric, geo and full-text leaves
    /// are all fs-generic), so the dispatcher needn't fix a concrete backend.
    ///
    /// `payload_schema` is consulted only by the full-text arm (it carries the
    /// [`TextIndexParams`] the leaf open needs) and `total_point_count` only by
    /// the null arm (it is segment-wide, not recoverable from the index files);
    /// the other arms ignore both, mirroring the writable selector.
    /// `deleted_points` reaches the immutable-only leaves; the roaring-flag bool
    /// and null leaves ignore it (a single `open` serves both modes).
    ///
    /// [1]: crate::index::field_index::index_selector::IndexSelector::new_index_with_type
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        dir: &Path,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
        index_type: &FullPayloadIndexType,
        total_point_count: usize,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let mode = match index_type.storage_type {
            StorageType::Gridstore => ReadMode::Appendable,
            StorageType::Mmap { is_on_disk } => ReadMode::Immutable { is_on_disk },
        };

        let index = match index_type.index_type {
            PayloadIndexType::KeywordIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyMapIndex::<str, S>::open_appendable(fs, map_dir(dir, field))?
                }
                ReadMode::Immutable { is_on_disk } => ReadOnlyMapIndex::<str, S>::open_immutable(
                    fs,
                    &map_dir(dir, field),
                    is_on_disk,
                    deleted_points,
                )?,
            }
            .map(Self::KeywordIndex),
            PayloadIndexType::IntMapIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyMapIndex::<IntPayloadType, S>::open_appendable(fs, map_dir(dir, field))?
                }
                ReadMode::Immutable { is_on_disk } => {
                    ReadOnlyMapIndex::<IntPayloadType, S>::open_immutable(
                        fs,
                        &map_dir(dir, field),
                        is_on_disk,
                        deleted_points,
                    )?
                }
            }
            .map(Self::IntMapIndex),
            // Matches the writable selector's `(PayloadIndexType::UuidIndex,
            // PayloadSchemaParams::Uuid(_))` arm, which constructs a
            // `MapIndex<UuidIntType>` and wraps it in `FieldIndex::UuidMapIndex`
            // — the `UuidIndex` discriminant is historically map-backed.
            PayloadIndexType::UuidIndex | PayloadIndexType::UuidMapIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyMapIndex::<UuidIntType, S>::open_appendable(fs, map_dir(dir, field))?
                }
                ReadMode::Immutable { is_on_disk } => {
                    ReadOnlyMapIndex::<UuidIntType, S>::open_immutable(
                        fs,
                        &map_dir(dir, field),
                        is_on_disk,
                        deleted_points,
                    )?
                }
            }
            .map(Self::UuidMapIndex),
            PayloadIndexType::IntIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyNumericIndex::<IntPayloadType, IntPayloadType, S>::open_appendable(
                        fs,
                        numeric_dir(dir, field),
                    )?
                }
                ReadMode::Immutable { is_on_disk } => {
                    ReadOnlyNumericIndex::<IntPayloadType, IntPayloadType, S>::open_immutable(
                        fs,
                        &numeric_dir(dir, field),
                        is_on_disk,
                        deleted_points,
                    )?
                }
            }
            .map(Self::IntIndex),
            PayloadIndexType::DatetimeIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyNumericIndex::<IntPayloadType, DateTimePayloadType, S>::open_appendable(
                        fs,
                        numeric_dir(dir, field),
                    )?
                }
                ReadMode::Immutable { is_on_disk } => {
                    ReadOnlyNumericIndex::<IntPayloadType, DateTimePayloadType, S>::open_immutable(
                        fs,
                        &numeric_dir(dir, field),
                        is_on_disk,
                        deleted_points,
                    )?
                }
            }
            .map(Self::DatetimeIndex),
            PayloadIndexType::FloatIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyNumericIndex::<FloatPayloadType, FloatPayloadType, S>::open_appendable(
                        fs,
                        numeric_dir(dir, field),
                    )?
                }
                ReadMode::Immutable { is_on_disk } => {
                    ReadOnlyNumericIndex::<FloatPayloadType, FloatPayloadType, S>::open_immutable(
                        fs,
                        &numeric_dir(dir, field),
                        is_on_disk,
                        deleted_points,
                    )?
                }
            }
            .map(Self::FloatIndex),
            // Geo reuses the writable selector's `map_dir` (`-map` suffix).
            PayloadIndexType::GeoIndex => match mode {
                ReadMode::Appendable => ReadOnlyGeoIndex::open_appendable(fs, map_dir(dir, field))?,
                ReadMode::Immutable { is_on_disk } => ReadOnlyGeoIndex::open_immutable(
                    fs,
                    &map_dir(dir, field),
                    is_on_disk,
                    deleted_points,
                )?,
            }
            .map(Self::GeoIndex),
            PayloadIndexType::FullTextIndex => {
                let config = TextIndexParams::try_from(payload_schema)?;
                match mode {
                    ReadMode::Appendable => {
                        ReadOnlyFullTextIndex::open_appendable(fs, text_dir(dir, field), config)?
                    }
                    ReadMode::Immutable { is_on_disk } => ReadOnlyFullTextIndex::open_immutable(
                        fs,
                        text_dir(dir, field),
                        config,
                        is_on_disk,
                        deleted_points,
                    )?,
                }
                .map(Self::FullTextIndex)
            }
            // Bool and null are roaring-flag backed: a single read-only `open`
            // serves both modes (neither consumes the immutable-only
            // `is_on_disk` / `deleted_points`).
            PayloadIndexType::BoolIndex => {
                ReadOnlyBoolIndex::<S>::open(fs, &bool_dir(dir, field))?.map(Self::BoolIndex)
            }
            PayloadIndexType::NullIndex => {
                ReadOnlyNullIndex::<S>::open(fs, &null_dir(dir, field), total_point_count)?
                    .map(Self::NullIndex)
            }
        };
        Ok(index)
    }
}
