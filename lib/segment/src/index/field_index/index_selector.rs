use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use gridstore::Blob;

use super::bool_index::BoolIndex;
use super::bool_index::immutable_bool_index::ImmutableBoolIndex;
use super::bool_index::mutable_bool_index::MutableBoolIndex;
use super::geo_index::{GeoIndexGridstoreBuilder, GeoIndexMmapBuilder};
use super::map_index::{MapIndex, MapIndexGridstoreBuilder, MapIndexKey, MapIndexMmapBuilder};
use super::null_index::{ImmutableNullIndex, NullIndex};
use super::numeric_index::{
    NumericIndexGridstoreBuilder, NumericIndexIntoInnerValue, NumericIndexMmapBuilder,
};
use super::{FieldIndexBuilder, ValueIndexer};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::field_index::FieldIndex;
use crate::index::field_index::full_text_index::FullTextIndex;
use crate::index::field_index::geo_index::GeoIndex;
use crate::index::field_index::null_index::MutableNullIndex;
use crate::index::field_index::numeric_index::{NumericIndex, NumericIndexValue};
use crate::index::payload_config::{FullPayloadIndexType, IndexMutability, PayloadIndexType};
use crate::json_path::JsonPath;
use crate::types::{Memory, PayloadFieldSchema, PayloadSchemaParams};

/// Selects index and index builder types based on field type.
#[derive(Copy, Clone)]
pub enum IndexSelector<'a> {
    /// On disk or in-memory index on mmaps, non-appendable
    NonAppendable { dir: &'a Path, memory: Memory },
    /// In-memory index on gridstore, appendable
    Appendable { dir: &'a Path },
}

impl IndexSelector<'_> {
    /// Loads the correct index based on `index_type`.
    pub fn new_index_with_type(
        &self,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
        index_type: &FullPayloadIndexType,
        create_if_missing: bool,
        id_tracker: &IdTrackerEnum,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<FieldIndex>> {
        let index = match (&index_type.index_type, payload_schema.expand().as_ref()) {
            (PayloadIndexType::IntIndex, PayloadSchemaParams::Integer(params)) => {
                // IntIndex only gets created if `range` is true. This will only throw an error if storage is corrupt.
                //
                // Note that `params.range == None` means the index was created without directly specifying these parameters.
                // In those cases it defaults to `true` so we don't need to cover this case.
                if params.range == Some(false) {
                    log::warn!(
                        "Inconsistent payload schema: Int index configured but schema.range is false"
                    );
                }

                self.numeric_new(field, create_if_missing, deleted_points)?
                    .map(FieldIndex::IntIndex)
            }
            (PayloadIndexType::IntMapIndex, PayloadSchemaParams::Integer(params)) => {
                // IntMapIndex only gets created if `lookup` is true. This will only throw an error if storage is corrupt.
                //
                // Note that `params.lookup == None` means the index was created without directly specifying these parameters.
                // In those cases it defaults to `true` so we don't need to cover this case.
                if params.lookup == Some(false) {
                    log::warn!(
                        "Inconsistent payload schema: IntMap index configured but schema.lookup is false",
                    );
                }

                self.map_new(field, create_if_missing, deleted_points, false)?
                    .map(FieldIndex::IntMapIndex)
            }
            (PayloadIndexType::DatetimeIndex, PayloadSchemaParams::Datetime(_)) => self
                .numeric_new(field, create_if_missing, deleted_points)?
                .map(FieldIndex::DatetimeIndex),

            (PayloadIndexType::KeywordIndex, PayloadSchemaParams::Keyword(params)) => self
                .map_new(
                    field,
                    create_if_missing,
                    deleted_points,
                    params.prefix.unwrap_or_default(),
                )?
                .map(FieldIndex::KeywordIndex),

            (PayloadIndexType::FloatIndex, PayloadSchemaParams::Float(_)) => self
                .numeric_new(field, create_if_missing, deleted_points)?
                .map(FieldIndex::FloatIndex),

            (PayloadIndexType::GeoIndex, PayloadSchemaParams::Geo(_)) => self
                .geo_new(field, create_if_missing, deleted_points)?
                .map(FieldIndex::GeoIndex),

            (PayloadIndexType::FullTextIndex, PayloadSchemaParams::Text(params)) => self
                .text_new(field, params.clone(), create_if_missing, deleted_points)?
                .map(FieldIndex::FullTextIndex),

            (PayloadIndexType::BoolIndex, PayloadSchemaParams::Bool(_)) => self
                .bool_new(
                    field,
                    create_if_missing,
                    deleted_points,
                    index_type.mutability,
                )?
                .map(FieldIndex::BoolIndex),

            (PayloadIndexType::UuidIndex, PayloadSchemaParams::Uuid(_)) => self
                .map_new(field, create_if_missing, deleted_points, false)?
                .map(FieldIndex::UuidMapIndex),

            (PayloadIndexType::UuidMapIndex, PayloadSchemaParams::Uuid(_)) => self
                .map_new(field, create_if_missing, deleted_points, false)?
                .map(FieldIndex::UuidMapIndex),

            (PayloadIndexType::NullIndex, _) => {
                self.new_null_index(field, create_if_missing, id_tracker, index_type.mutability)?
            }

            // Storage inconsistency. Should never happen.
            (index_type, schema) => {
                return Err(OperationError::service_error(format!(
                    "Payload index storage inconsistent. Schema defines {schema:?} but storage is {index_type:?}"
                )));
            }
        };

        Ok(index)
    }

    /// Selects index type based on field type.
    pub fn new_index(
        &self,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
        create_if_missing: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Vec<FieldIndex>>> {
        let indexes = match payload_schema.expand().as_ref() {
            PayloadSchemaParams::Keyword(params) => self
                .map_new(
                    field,
                    create_if_missing,
                    deleted_points,
                    params.prefix.unwrap_or_default(),
                )?
                .map(|index| vec![FieldIndex::KeywordIndex(index)]),
            PayloadSchemaParams::Integer(integer_params) => {
                let use_lookup = integer_params.lookup.unwrap_or(true);
                let use_range = integer_params.range.unwrap_or(true);

                let lookup = if use_lookup {
                    match self.map_new(field, create_if_missing, deleted_points, false)? {
                        Some(index) => Some(FieldIndex::IntMapIndex(index)),
                        None => return Ok(None),
                    }
                } else {
                    None
                };
                let range = if use_range {
                    match self.numeric_new(field, create_if_missing, deleted_points)? {
                        Some(index) => Some(FieldIndex::IntIndex(index)),
                        None => return Ok(None),
                    }
                } else {
                    None
                };

                Some(lookup.into_iter().chain(range).collect())
            }
            PayloadSchemaParams::Float(_) => self
                .numeric_new(field, create_if_missing, deleted_points)?
                .map(|index| vec![FieldIndex::FloatIndex(index)]),
            PayloadSchemaParams::Geo(_) => self
                .geo_new(field, create_if_missing, deleted_points)?
                .map(|index| vec![FieldIndex::GeoIndex(index)]),
            PayloadSchemaParams::Text(text_index_params) => self
                .text_new(
                    field,
                    text_index_params.clone(),
                    create_if_missing,
                    deleted_points,
                )?
                .map(|index| vec![FieldIndex::FullTextIndex(index)]),
            PayloadSchemaParams::Bool(_) => self
                .bool_new(
                    field,
                    create_if_missing,
                    deleted_points,
                    self.default_mutability(),
                )?
                .map(|index| vec![FieldIndex::BoolIndex(index)]),
            PayloadSchemaParams::Datetime(_) => self
                .numeric_new(field, create_if_missing, deleted_points)?
                .map(|index| vec![FieldIndex::DatetimeIndex(index)]),
            PayloadSchemaParams::Uuid(_) => self
                .map_new(field, create_if_missing, deleted_points, false)?
                .map(|index| vec![FieldIndex::UuidMapIndex(index)]),
        };

        Ok(indexes)
    }

    /// Selects index builder based on field type.
    pub fn index_builder(
        &self,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
        deleted_points: &BitSlice,
    ) -> OperationResult<Vec<FieldIndexBuilder>> {
        let builders = match payload_schema.expand().as_ref() {
            PayloadSchemaParams::Keyword(params) => {
                vec![self.map_builder(
                    field,
                    FieldIndexBuilder::KeywordMmapIndex,
                    FieldIndexBuilder::KeywordGridstoreIndex,
                    deleted_points,
                    params.prefix.unwrap_or_default(),
                )]
            }
            PayloadSchemaParams::Integer(integer_params) => {
                let use_lookup = integer_params.lookup.unwrap_or(true);
                let use_range = integer_params.range.unwrap_or(true);

                let lookup = if use_lookup {
                    Some(self.map_builder(
                        field,
                        FieldIndexBuilder::IntMapMmapIndex,
                        FieldIndexBuilder::IntMapGridstoreIndex,
                        deleted_points,
                        false,
                    ))
                } else {
                    None
                };

                let range = if use_range {
                    Some(self.numeric_builder(
                        field,
                        FieldIndexBuilder::IntMmapIndex,
                        FieldIndexBuilder::IntGridstoreIndex,
                        deleted_points,
                    ))
                } else {
                    None
                };

                lookup.into_iter().chain(range).collect()
            }
            PayloadSchemaParams::Float(_) => {
                vec![self.numeric_builder(
                    field,
                    FieldIndexBuilder::FloatMmapIndex,
                    FieldIndexBuilder::FloatGridstoreIndex,
                    deleted_points,
                )]
            }
            PayloadSchemaParams::Geo(_) => {
                vec![self.geo_builder(
                    field,
                    FieldIndexBuilder::GeoMmapIndex,
                    FieldIndexBuilder::GeoGridstoreIndex,
                    deleted_points,
                )]
            }
            PayloadSchemaParams::Text(text_index_params) => {
                vec![self.text_builder(field, text_index_params.clone(), deleted_points)]
            }
            PayloadSchemaParams::Bool(_) => {
                vec![self.bool_builder(field)?]
            }
            PayloadSchemaParams::Datetime(_) => {
                vec![self.numeric_builder(
                    field,
                    FieldIndexBuilder::DatetimeMmapIndex,
                    FieldIndexBuilder::DatetimeGridstoreIndex,
                    deleted_points,
                )]
            }
            PayloadSchemaParams::Uuid(_) => {
                vec![self.map_builder(
                    field,
                    FieldIndexBuilder::UuidMmapIndex,
                    FieldIndexBuilder::UuidGridstoreIndex,
                    deleted_points,
                    false,
                )]
            }
        };

        Ok(builders)
    }

    /// `prefix_index` enables the sorted key dictionary for prefix matching;
    /// only meaningful for the keyword (string-keyed) index, other callers
    /// pass `false`.
    fn map_new<N: MapIndexKey + ?Sized>(
        &self,
        field: &JsonPath,
        create_if_missing: bool,
        deleted_points: &BitSlice,
        prefix_index: bool,
    ) -> OperationResult<Option<MapIndex<N>>>
    where
        Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
    {
        Ok(match self {
            // The immutable variants detect prefix support from the presence
            // of the prefix index file, written at build time.
            IndexSelector::NonAppendable { dir, memory } => {
                MapIndex::new_immutable(&map_dir(dir, field), *memory, deleted_points)?
            }
            IndexSelector::Appendable { dir } => {
                MapIndex::new_mutable(map_dir(dir, field), create_if_missing, prefix_index)?
            }
        })
    }

    fn map_builder<N: MapIndexKey + ?Sized>(
        &self,
        field: &JsonPath,
        make_mmap: fn(MapIndexMmapBuilder<N>) -> FieldIndexBuilder,
        make_gridstore: fn(MapIndexGridstoreBuilder<N>) -> FieldIndexBuilder,
        deleted_points: &BitSlice,
        prefix_index: bool,
    ) -> FieldIndexBuilder
    where
        Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
    {
        match self {
            IndexSelector::NonAppendable { dir, memory } => make_mmap(MapIndex::builder_immutable(
                &map_dir(dir, field),
                !memory.is_heap(),
                deleted_points,
                prefix_index,
            )),
            IndexSelector::Appendable { dir } => {
                make_gridstore(MapIndex::builder_mutable(map_dir(dir, field), prefix_index))
            }
        }
    }

    fn numeric_new<T: NumericIndexValue, P>(
        &self,
        field: &JsonPath,
        create_if_missing: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<NumericIndex<T, P>>>
    where
        Vec<T>: Blob,
    {
        Ok(match self {
            IndexSelector::NonAppendable { dir, memory } => {
                NumericIndex::new_immutable(&numeric_dir(dir, field), *memory, deleted_points)?
            }
            IndexSelector::Appendable { dir } => {
                NumericIndex::new_mutable(numeric_dir(dir, field), create_if_missing)?
            }
        })
    }

    fn numeric_builder<T: NumericIndexValue, P>(
        &self,
        field: &JsonPath,
        make_mmap: fn(NumericIndexMmapBuilder<T, P>) -> FieldIndexBuilder,
        make_gridstore: fn(NumericIndexGridstoreBuilder<T, P>) -> FieldIndexBuilder,
        deleted_points: &BitSlice,
    ) -> FieldIndexBuilder
    where
        NumericIndex<T, P>: ValueIndexer<ValueType = P> + NumericIndexIntoInnerValue<T, P>,
        Vec<T>: Blob,
    {
        match self {
            IndexSelector::NonAppendable { dir, memory } => make_mmap(NumericIndex::builder_mmap(
                &numeric_dir(dir, field),
                !memory.is_heap(),
                deleted_points,
            )),
            IndexSelector::Appendable { dir } => {
                make_gridstore(NumericIndex::builder_gridstore(numeric_dir(dir, field)))
            }
        }
    }

    fn geo_new(
        &self,
        field: &JsonPath,
        create_if_missing: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<GeoIndex>> {
        Ok(match self {
            IndexSelector::NonAppendable { dir, memory } => {
                GeoIndex::new_immutable(&map_dir(dir, field), *memory, deleted_points)?
            }
            IndexSelector::Appendable { dir } => {
                GeoIndex::new_mutable(map_dir(dir, field), create_if_missing)?
            }
        })
    }

    fn geo_builder(
        &self,
        field: &JsonPath,
        make_mmap: fn(GeoIndexMmapBuilder) -> FieldIndexBuilder,
        make_gridstore: fn(GeoIndexGridstoreBuilder) -> FieldIndexBuilder,
        deleted_points: &BitSlice,
    ) -> FieldIndexBuilder {
        match self {
            IndexSelector::NonAppendable { dir, memory } => make_mmap(GeoIndex::builder_mmap(
                &map_dir(dir, field),
                !memory.is_heap(),
                deleted_points,
            )),
            IndexSelector::Appendable { dir } => {
                make_gridstore(GeoIndex::builder_gridstore(map_dir(dir, field)))
            }
        }
    }

    /// Default mutability for indexes that share an on-disk format across
    /// mutability variants (e.g. bool, null): Mmap segments are immutable,
    /// Gridstore segments are appendable/mutable.
    pub fn default_mutability(&self) -> IndexMutability {
        match self {
            IndexSelector::NonAppendable { dir: _, memory: _ } => IndexMutability::Immutable,
            IndexSelector::Appendable { dir: _ } => IndexMutability::Mutable,
        }
    }

    pub fn null_builder(
        &self,
        field: &JsonPath,
        total_point_count: usize,
    ) -> OperationResult<FieldIndexBuilder> {
        let builder = match self {
            IndexSelector::NonAppendable { dir, memory: _ } => {
                FieldIndexBuilder::ImmutableNullIndex(ImmutableNullIndex::builder(
                    &null_dir(dir, field),
                    total_point_count,
                )?)
            }
            IndexSelector::Appendable { dir } => FieldIndexBuilder::MutableNullIndex(
                MutableNullIndex::builder(&null_dir(dir, field), total_point_count)?,
            ),
        };
        Ok(builder)
    }

    pub fn new_null_index(
        &self,
        field: &JsonPath,
        create_if_missing: bool,
        id_tracker: &IdTrackerEnum,
        mutability: IndexMutability,
    ) -> OperationResult<Option<FieldIndex>> {
        let total_point_count = id_tracker.total_point_count();
        // `MutableNullIndex` and `ImmutableNullIndex` share the same on-disk
        // format; stored mutability picks which in-memory wrapper to build.
        // Gridstore segments are always appendable, so the null index is
        // always mutable regardless of the stored mutability marker.
        match (self, mutability) {
            (IndexSelector::NonAppendable { dir, memory: _ }, IndexMutability::Immutable) => {
                Ok(ImmutableNullIndex::open(
                    &null_dir(dir, field),
                    total_point_count,
                    id_tracker.deleted_point_bitslice(),
                )?
                .map(NullIndex::Immutable)
                .map(FieldIndex::NullIndex))
            }
            (IndexSelector::NonAppendable { dir, memory: _ }, IndexMutability::Mutable)
            | (IndexSelector::Appendable { dir }, IndexMutability::Mutable)
            | (IndexSelector::Appendable { dir }, IndexMutability::Immutable) => {
                Ok(MutableNullIndex::open(
                    &null_dir(dir, field),
                    total_point_count,
                    create_if_missing,
                )?
                .map(NullIndex::Mutable)
                .map(FieldIndex::NullIndex))
            }
        }
    }

    fn text_new(
        &self,
        field: &JsonPath,
        config: TextIndexParams,
        create_if_missing: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<FullTextIndex>> {
        Ok(match self {
            IndexSelector::NonAppendable { dir, memory } => {
                FullTextIndex::new_mmap(text_dir(dir, field), config, *memory, deleted_points)?
            }
            IndexSelector::Appendable { dir } => {
                FullTextIndex::new_gridstore(text_dir(dir, field), config, create_if_missing)?
            }
        })
    }

    fn text_builder(
        &self,
        field: &JsonPath,
        config: TextIndexParams,
        deleted_points: &BitSlice,
    ) -> FieldIndexBuilder {
        match self {
            IndexSelector::NonAppendable { dir, memory } => {
                FieldIndexBuilder::FullTextMmapIndex(FullTextIndex::builder_mmap(
                    text_dir(dir, field),
                    config,
                    !memory.is_heap(),
                    deleted_points,
                ))
            }
            IndexSelector::Appendable { dir } => FieldIndexBuilder::FullTextGridstoreIndex(
                FullTextIndex::builder_gridstore(text_dir(dir, field), config),
            ),
        }
    }

    fn bool_builder(&self, field: &JsonPath) -> OperationResult<FieldIndexBuilder> {
        match self {
            IndexSelector::NonAppendable { dir, memory: _ } => {
                let dir = bool_dir(dir, field);
                Ok(FieldIndexBuilder::BoolMmapIndex(
                    ImmutableBoolIndex::builder(&dir)?,
                ))
            }
            // Skip Gridstore for boolean index, mmap index is simpler and is also mutable
            IndexSelector::Appendable { dir } => {
                let dir = bool_dir(dir, field);
                Ok(FieldIndexBuilder::BoolGridstoreIndex(
                    MutableBoolIndex::builder(&dir)?,
                ))
            }
        }
    }

    fn bool_new(
        &self,
        field: &JsonPath,
        create_if_missing: bool,
        deleted_points: &BitSlice,
        mutability: IndexMutability,
    ) -> OperationResult<Option<BoolIndex>> {
        // `MutableBoolIndex` and `ImmutableBoolIndex` share the same on-disk
        // format; stored mutability picks which in-memory wrapper to build.
        Ok(match (self, mutability) {
            (IndexSelector::NonAppendable { dir, memory: _ }, IndexMutability::Immutable) => {
                let dir = bool_dir(dir, field);
                ImmutableBoolIndex::open(&dir, deleted_points)?.map(BoolIndex::Immutable)
            }
            (IndexSelector::NonAppendable { dir, memory: _ }, IndexMutability::Mutable)
            | (IndexSelector::Appendable { dir }, _) => {
                let dir = bool_dir(dir, field);
                MutableBoolIndex::open(&dir, create_if_missing)?.map(BoolIndex::Mutable)
            }
        })
    }
}

/// Remove all on-disk leftovers of `field`'s indexes under the payload index root `dir`.
///
/// A full rebuild must start from a clean slate: files can be left behind by a build
/// whose config entry never became durable (config persistence is deferred to the
/// flush pipeline) or by an index that failed to load. Appendable builders open
/// existing storages, so leftovers would leak stale postings into the fresh build.
pub(crate) fn wipe_field_dirs(dir: &Path, field: &JsonPath) -> std::io::Result<()> {
    for candidate in [
        map_dir(dir, field),
        numeric_dir(dir, field),
        text_dir(dir, field),
        bool_dir(dir, field),
        null_dir(dir, field),
    ] {
        if candidate.exists() {
            fs_err::remove_dir_all(&candidate)?;
        }
    }
    Ok(())
}

pub(crate) fn map_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-map", field.filename()))
}

pub(crate) fn numeric_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-numeric", field.filename()))
}

pub(crate) fn text_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-text", field.filename()))
}

pub(crate) fn bool_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-bool", field.filename()))
}

pub(crate) fn null_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-null", field.filename()))
}
