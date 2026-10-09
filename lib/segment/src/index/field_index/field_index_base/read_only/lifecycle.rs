use std::path::Path;

use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, Populate, UniversalReadFs};

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
    DateTimePayloadType, FloatPayloadType, IntPayloadType, Memory, PayloadFieldSchema, UuidIntType,
};

/// Which read-only open path a leaf index should take, derived from the stored
/// [`StorageType`]. Phrased as a mutability distinction (matching the
/// `open_appendable` / `open_immutable` leaf methods) rather than a concrete
/// backend: the read-only stack is generic over [`UniversalRead`](common::universal_io::UniversalRead), so the
/// on-disk-vs-in-memory choice is the placement on the immutable variant.
#[derive(Clone, Copy)]
enum ReadMode {
    /// Appendable on-disk format — opened via `open_appendable`.
    Appendable,
    /// Immutable on-disk format — opened via `open_immutable` with `memory`.
    Immutable { memory: Memory },
}

impl ReadMode {
    /// Read mode for `storage_type`, placed as the writable open would place it.
    ///
    /// A `populate_override` (from a request-specific
    /// [`LoadProfile`](crate::data_types::load_profile::LoadProfile)) re-decides
    /// the immutable placement over the same files, see
    /// [`Memory::with_populate_override`]. The appendable (Gridstore) mode ignores
    /// it: its open reconstructs in-memory state and cannot be demoted.
    fn new(
        storage_type: StorageType,
        payload_schema: &PayloadFieldSchema,
        populate_override: Option<Populate>,
    ) -> Self {
        match storage_type.immutable_memory(payload_schema.memory_placement()) {
            None => ReadMode::Appendable,
            Some(memory) => ReadMode::Immutable {
                memory: memory.with_populate_override(populate_override),
            },
        }
    }
}

impl<S: UniversalReadExt> ReadOnlyFieldIndex<S> {
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        dir: &Path,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
        index_type: &FullPayloadIndexType,
        populate_override: Option<Populate>,
    ) -> OperationResult<bool> {
        let mode = ReadMode::new(index_type.storage_type, payload_schema, populate_override);

        // Derive whether how to populate from the payload schema; a
        // `populate_override` replaces the schema-derived decision.
        let schema_populate = || {
            populate_override.unwrap_or({
                if payload_schema.memory_placement().populate_on_open() {
                    Populate::PreferBackground
                } else {
                    Populate::No
                }
            })
        };

        let preopened = match index_type.index_type {
            PayloadIndexType::KeywordIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyMapIndex::<str, S>::preopen_appendable(fs, map_dir(dir, field))?
                }
                ReadMode::Immutable { memory } => {
                    ReadOnlyMapIndex::<str, S>::preopen_immutable(fs, &map_dir(dir, field), memory)?
                }
            },
            PayloadIndexType::IntMapIndex => match mode {
                ReadMode::Appendable => ReadOnlyMapIndex::<IntPayloadType, S>::preopen_appendable(
                    fs,
                    map_dir(dir, field),
                )?,
                ReadMode::Immutable { memory } => {
                    ReadOnlyMapIndex::<IntPayloadType, S>::preopen_immutable(
                        fs,
                        &map_dir(dir, field),
                        memory,
                    )?
                }
            },
            PayloadIndexType::UuidIndex | PayloadIndexType::UuidMapIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyMapIndex::<UuidIntType, S>::preopen_appendable(fs, map_dir(dir, field))?
                }
                ReadMode::Immutable { memory } => {
                    ReadOnlyMapIndex::<UuidIntType, S>::preopen_immutable(
                        fs,
                        &map_dir(dir, field),
                        memory,
                    )?
                }
            },
            PayloadIndexType::IntIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyNumericIndex::<IntPayloadType, IntPayloadType, S>::preopen_appendable(
                        fs,
                        numeric_dir(dir, field),
                    )?
                }
                ReadMode::Immutable { memory } => {
                    ReadOnlyNumericIndex::<IntPayloadType, IntPayloadType, S>::preopen_immutable(
                        fs,
                        &numeric_dir(dir, field),
                        memory,
                    )?
                }
            },
            PayloadIndexType::DatetimeIndex => match mode {
                ReadMode::Appendable => ReadOnlyNumericIndex::<
                    IntPayloadType,
                    DateTimePayloadType,
                    S,
                >::preopen_appendable(
                    fs, numeric_dir(dir, field)
                )?,
                ReadMode::Immutable { memory } => ReadOnlyNumericIndex::<
                    IntPayloadType,
                    DateTimePayloadType,
                    S,
                >::preopen_immutable(
                    fs, &numeric_dir(dir, field), memory
                )?,
            },
            PayloadIndexType::FloatIndex => match mode {
                ReadMode::Appendable => ReadOnlyNumericIndex::<
                    FloatPayloadType,
                    FloatPayloadType,
                    S,
                >::preopen_appendable(
                    fs, numeric_dir(dir, field)
                )?,
                ReadMode::Immutable { memory } => ReadOnlyNumericIndex::<
                    FloatPayloadType,
                    FloatPayloadType,
                    S,
                >::preopen_immutable(
                    fs, &numeric_dir(dir, field), memory
                )?,
            },
            // Geo reuses the writable selector's `map_dir` (`-map` suffix).
            PayloadIndexType::GeoIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyGeoIndex::<S>::preopen_appendable(fs, map_dir(dir, field))?
                }
                ReadMode::Immutable { memory } => {
                    ReadOnlyGeoIndex::<S>::preopen_immutable(fs, &map_dir(dir, field), memory)?
                }
            },
            PayloadIndexType::FullTextIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyFullTextIndex::<S>::preopen_appendable(fs, text_dir(dir, field))?
                }
                ReadMode::Immutable { memory } => ReadOnlyFullTextIndex::<S>::preopen_immutable(
                    fs,
                    &text_dir(dir, field),
                    memory,
                )?,
            },
            // Bool and null keep a single roaring-flag format, but we can choose populate
            // param based on schema placement.
            PayloadIndexType::BoolIndex => {
                let populate = schema_populate();
                ReadOnlyBoolIndex::<S>::preopen(fs, &bool_dir(dir, field), populate)?
            }
            PayloadIndexType::NullIndex => {
                let populate = schema_populate();
                ReadOnlyNullIndex::<S>::preopen(fs, &null_dir(dir, field), populate)?
            }
        };

        Ok(preopened)
    }

    /// Read-only mirror of [`IndexSelector::new_index_with_type`][1]: dispatches
    /// on [`FullPayloadIndexType::index_type`] and forwards to each per-index
    /// parent's open, wrapping the leaf in the matching variant.
    ///
    /// The open path (appendable vs immutable) is picked from the stored
    /// [`FullPayloadIndexType::storage_type`]; the placement rides on the
    /// immutable mode. Generic over `S`: every per-index open threads the
    /// [`UniversalRead`](common::universal_io::UniversalRead) handle `fs` (the map, numeric, geo and full-text leaves
    /// are all fs-generic), so the dispatcher needn't fix a concrete backend.
    ///
    /// `payload_schema` refines the placement of the mmap variant and carries
    /// the [`TextIndexParams`] the full-text leaf open needs. `total_point_count` sizes the
    /// null arm and caps what the appendable arms load: their storage may hold
    /// values for offsets the id tracker does not cover yet.
    /// `deleted_points` reaches the immutable-only leaves; the roaring-flag bool
    /// and null leaves ignore it (a single `open` serves both modes).
    ///
    /// [1]: crate::index::field_index::index_selector::IndexSelector::new_index_with_type
    /// `populate_override` mirrors [`preopen`](Self::preopen): it re-decides
    /// the immutable mode's placement.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        dir: &Path,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
        index_type: &FullPayloadIndexType,
        total_point_count: usize,
        deleted_points: &BitSlice,
        populate_override: Option<Populate>,
    ) -> OperationResult<Option<Self>> {
        let mode = ReadMode::new(index_type.storage_type, payload_schema, populate_override);
        let max_point_offset = total_point_count as PointOffsetType;

        let index = match index_type.index_type {
            PayloadIndexType::KeywordIndex => match mode {
                ReadMode::Appendable => ReadOnlyMapIndex::<str, S>::open_appendable(
                    fs,
                    map_dir(dir, field),
                    max_point_offset,
                )?,
                ReadMode::Immutable { memory } => ReadOnlyMapIndex::<str, S>::open_immutable(
                    fs,
                    &map_dir(dir, field),
                    memory,
                    deleted_points,
                )?,
            }
            .map(Self::KeywordIndex),
            PayloadIndexType::IntMapIndex => match mode {
                ReadMode::Appendable => ReadOnlyMapIndex::<IntPayloadType, S>::open_appendable(
                    fs,
                    map_dir(dir, field),
                    max_point_offset,
                )?,
                ReadMode::Immutable { memory } => {
                    ReadOnlyMapIndex::<IntPayloadType, S>::open_immutable(
                        fs,
                        &map_dir(dir, field),
                        memory,
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
                ReadMode::Appendable => ReadOnlyMapIndex::<UuidIntType, S>::open_appendable(
                    fs,
                    map_dir(dir, field),
                    max_point_offset,
                )?,
                ReadMode::Immutable { memory } => {
                    ReadOnlyMapIndex::<UuidIntType, S>::open_immutable(
                        fs,
                        &map_dir(dir, field),
                        memory,
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
                        max_point_offset,
                    )?
                }
                ReadMode::Immutable { memory } => {
                    ReadOnlyNumericIndex::<IntPayloadType, IntPayloadType, S>::open_immutable(
                        fs,
                        &numeric_dir(dir, field),
                        memory,
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
                        max_point_offset,
                    )?
                }
                ReadMode::Immutable { memory } => {
                    ReadOnlyNumericIndex::<IntPayloadType, DateTimePayloadType, S>::open_immutable(
                        fs,
                        &numeric_dir(dir, field),
                        memory,
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
                        max_point_offset,
                    )?
                }
                ReadMode::Immutable { memory } => {
                    ReadOnlyNumericIndex::<FloatPayloadType, FloatPayloadType, S>::open_immutable(
                        fs,
                        &numeric_dir(dir, field),
                        memory,
                        deleted_points,
                    )?
                }
            }
            .map(Self::FloatIndex),
            // Geo reuses the writable selector's `map_dir` (`-map` suffix).
            PayloadIndexType::GeoIndex => match mode {
                ReadMode::Appendable => {
                    ReadOnlyGeoIndex::open_appendable(fs, map_dir(dir, field), max_point_offset)?
                }
                ReadMode::Immutable { memory } => ReadOnlyGeoIndex::open_immutable(
                    fs,
                    &map_dir(dir, field),
                    memory,
                    deleted_points,
                )?,
            }
            .map(Self::GeoIndex),
            PayloadIndexType::FullTextIndex => {
                let config = TextIndexParams::try_from(payload_schema)?;
                match mode {
                    ReadMode::Appendable => ReadOnlyFullTextIndex::open_appendable(
                        fs,
                        text_dir(dir, field),
                        max_point_offset,
                        config,
                    )?,
                    ReadMode::Immutable { memory } => ReadOnlyFullTextIndex::open_immutable(
                        fs,
                        text_dir(dir, field),
                        config,
                        memory,
                        deleted_points,
                    )?,
                }
                .map(Self::FullTextIndex)
            }
            // Bool and null are roaring-flag backed: a single read-only `open`
            // serves both modes (neither consumes the immutable-only
            // `memory` / `deleted_points`).
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

#[cfg(test)]
mod tests {
    use common::ambient;
    use common::bitvec::BitVec;
    use common::universal_io::MmapFs;
    use serde_json::json;

    use super::*;
    use crate::data_types::index::KeywordIndexParams;
    use crate::index::field_index::FieldIndexBuilderTrait;
    use crate::index::field_index::index_selector::IndexSelector;
    use crate::types::PayloadSchemaParams;

    /// An mmap-variant index opens read-only where the schema places it:
    /// populated when cached, left on disk when cold.
    #[test]
    fn read_only_open_follows_schema_placement() {
        let dir = tempfile::tempdir().unwrap();
        let field = JsonPath::new("field");
        let schema = |memory| {
            PayloadFieldSchema::FieldParams(PayloadSchemaParams::Keyword(KeywordIndexParams {
                memory: Some(memory),
                ..Default::default()
            }))
        };
        let deleted = BitVec::repeat(false, 1);

        let mut builders = IndexSelector::NonAppendable {
            dir: dir.path(),
            memory: Memory::Cold,
        }
        .index_builder(&field, &schema(Memory::Cold), &deleted)
        .unwrap();
        let mut builder = builders.pop().unwrap();
        builder.init().unwrap();
        let _scope = ambient::test_guard();
        builder.add_point(0, &[&json!("a")]).unwrap();
        let index_type = builder.finalize().unwrap().get_full_index_type();

        for (memory, cold) in [(Memory::Cold, true), (Memory::Cached, false)] {
            let index = ReadOnlyFieldIndex::open(
                &MmapFs,
                dir.path(),
                &field,
                &schema(memory),
                &index_type,
                1,
                &deleted,
                None,
            )
            .unwrap()
            .unwrap();
            assert_eq!(index.is_cold(), cold, "{memory:?}");
        }
    }
}
