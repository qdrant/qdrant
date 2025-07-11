use std::path::{Path, PathBuf};
#[cfg(feature = "rocksdb")]
use std::sync::Arc;

use gridstore::Blob;

use super::bool_index::BoolIndex;
use super::bool_index::mmap_bool_index::MmapBoolIndex;
#[cfg(feature = "rocksdb")]
use super::bool_index::simple_bool_index::SimpleBoolIndex;
use super::geo_index::{GeoMapIndexGridstoreBuilder, GeoMapIndexMmapBuilder};
use super::histogram::Numericable;
use super::map_index::{MapIndex, MapIndexGridstoreBuilder, MapIndexKey, MapIndexMmapBuilder};
use super::mmap_point_to_values::MmapValue;
use super::numeric_index::{
    Encodable, NumericIndexGridstoreBuilder, NumericIndexIntoInnerValue, NumericIndexMmapBuilder,
};
use super::{FieldIndexBuilder, ValueIndexer};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::FieldIndex;
use crate::index::field_index::full_text_index::text_index::FullTextIndex;
use crate::index::field_index::geo_index::GeoMapIndex;
use crate::index::field_index::null_index::mmap_null_index::MmapNullIndex;
use crate::index::field_index::numeric_index::NumericIndex;
use crate::index::payload_config::{FullPayloadIndexType, PayloadIndexType};
use crate::json_path::JsonPath;
use crate::types::{PayloadFieldSchema, PayloadSchemaParams};

/// Selects index and index builder types based on field type.
#[derive(Copy, Clone)]
pub enum IndexSelector<'a> {
    /// In-memory index on RocksDB, appendable or non-appendable
    #[cfg(feature = "rocksdb")]
    RocksDb(IndexSelectorRocksDb<'a>),
    /// On disk or in-memory index on mmaps, non-appendable
    Mmap(IndexSelectorMmap<'a>),
    /// In-memory index on gridstore, appendable
    Gridstore(IndexSelectorGridstore<'a>),
}

#[cfg(feature = "rocksdb")]
#[derive(Copy, Clone)]
pub struct IndexSelectorRocksDb<'a> {
    pub db: &'a Arc<parking_lot::RwLock<rocksdb::DB>>,
    pub is_appendable: bool,
}

#[derive(Copy, Clone)]
pub struct IndexSelectorMmap<'a> {
    pub dir: &'a Path,
    pub is_on_disk: bool,
}

#[derive(Copy, Clone)]
pub struct IndexSelectorGridstore<'a> {
    pub dir: &'a Path,
}

impl IndexSelector<'_> {
    /// Loads the correct index based on `index_type`.
    pub fn new_index_with_type(
        &self,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
        index_type: &FullPayloadIndexType,
        path: &Path,
        total_point_count: usize,
        create_if_missing: bool,
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

                FieldIndex::IntIndex(self.numeric_new(field, create_if_missing)?)
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

                FieldIndex::IntMapIndex(self.map_new(field, create_if_missing)?)
            }
            (PayloadIndexType::DatetimeIndex, PayloadSchemaParams::Datetime(_)) => {
                FieldIndex::DatetimeIndex(self.numeric_new(field, create_if_missing)?)
            }

            (PayloadIndexType::KeywordIndex, PayloadSchemaParams::Keyword(_)) => {
                FieldIndex::KeywordIndex(self.map_new(field, create_if_missing)?)
            }

            (PayloadIndexType::FloatIndex, PayloadSchemaParams::Float(_)) => {
                FieldIndex::FloatIndex(self.numeric_new(field, create_if_missing)?)
            }

            (PayloadIndexType::GeoIndex, PayloadSchemaParams::Geo(_)) => {
                FieldIndex::GeoIndex(self.geo_new(field, create_if_missing)?)
            }

            (PayloadIndexType::FullTextIndex, PayloadSchemaParams::Text(params)) => {
                FieldIndex::FullTextIndex(self.text_new(
                    field,
                    params.clone(),
                    create_if_missing,
                )?)
            }

            (PayloadIndexType::BoolIndex, PayloadSchemaParams::Bool(_)) => self.bool_new(field)?,

            (PayloadIndexType::UuidIndex, PayloadSchemaParams::Uuid(_)) => {
                FieldIndex::UuidMapIndex(self.map_new(field, create_if_missing)?)
            }

            (PayloadIndexType::UuidMapIndex, PayloadSchemaParams::Uuid(_)) => {
                FieldIndex::UuidMapIndex(self.map_new(field, create_if_missing)?)
            }

            (PayloadIndexType::NullIndex, _) => {
                let Some(null_index) = MmapNullIndex::open_if_exists(path, total_point_count)?
                else {
                    return Ok(None);
                };
                FieldIndex::NullIndex(null_index)
            }

            // Storage inconsistency. Should never happen.
            (index_type, schema) => {
                return Err(OperationError::service_error(format!(
                    "Payload index storage inconsistent. Schema defines {schema:?} but storage is {index_type:?}"
                )));
            }
        };

        Ok(Some(index))
    }

    /// Selects index type based on field type.
    pub fn new_index(
        &self,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
        create_if_missing: bool,
    ) -> OperationResult<Vec<FieldIndex>> {
        let indexes = match payload_schema.expand().as_ref() {
            PayloadSchemaParams::Keyword(_) => {
                vec![FieldIndex::KeywordIndex(
                    self.map_new(field, create_if_missing)?,
                )]
            }
            PayloadSchemaParams::Integer(integer_params) => itertools::chain(
                integer_params
                    .lookup
                    .unwrap_or(true)
                    .then(|| {
                        OperationResult::Ok(FieldIndex::IntMapIndex(
                            self.map_new(field, create_if_missing)?,
                        ))
                    })
                    .transpose()?,
                integer_params
                    .range
                    .unwrap_or(true)
                    .then(|| {
                        OperationResult::Ok(FieldIndex::IntIndex(
                            self.numeric_new(field, create_if_missing)?,
                        ))
                    })
                    .transpose()?,
            )
            .collect(),
            PayloadSchemaParams::Float(_) => {
                vec![FieldIndex::FloatIndex(
                    self.numeric_new(field, create_if_missing)?,
                )]
            }
            PayloadSchemaParams::Geo(_) => vec![FieldIndex::GeoIndex(
                self.geo_new(field, create_if_missing)?,
            )],
            PayloadSchemaParams::Text(text_index_params) => {
                vec![FieldIndex::FullTextIndex(self.text_new(
                    field,
                    text_index_params.clone(),
                    create_if_missing,
                )?)]
            }
            PayloadSchemaParams::Bool(_) => {
                vec![self.bool_new(field)?]
            }
            PayloadSchemaParams::Datetime(_) => {
                vec![FieldIndex::DatetimeIndex(
                    self.numeric_new(field, create_if_missing)?,
                )]
            }
            PayloadSchemaParams::Uuid(_) => {
                vec![FieldIndex::UuidMapIndex(
                    self.map_new(field, create_if_missing)?,
                )]
            }
        };

        Ok(indexes)
    }

    /// Selects index builder based on field type.
    pub fn index_builder(
        &self,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<Vec<FieldIndexBuilder>> {
        let builders = match payload_schema.expand().as_ref() {
            PayloadSchemaParams::Keyword(_) => {
                vec![self.map_builder(
                    field,
                    #[cfg(feature = "rocksdb")]
                    FieldIndexBuilder::KeywordIndex,
                    FieldIndexBuilder::KeywordMmapIndex,
                    FieldIndexBuilder::KeywordGridstoreIndex,
                )]
            }
            PayloadSchemaParams::Integer(integer_params) => itertools::chain(
                integer_params.lookup.unwrap_or(true).then(|| {
                    self.map_builder(
                        field,
                        #[cfg(feature = "rocksdb")]
                        FieldIndexBuilder::IntMapIndex,
                        FieldIndexBuilder::IntMapMmapIndex,
                        FieldIndexBuilder::IntMapGridstoreIndex,
                    )
                }),
                integer_params.range.unwrap_or(true).then(|| {
                    self.numeric_builder(
                        field,
                        #[cfg(feature = "rocksdb")]
                        FieldIndexBuilder::IntIndex,
                        FieldIndexBuilder::IntMmapIndex,
                        FieldIndexBuilder::IntGridstoreIndex,
                    )
                }),
            )
            .collect(),
            PayloadSchemaParams::Float(_) => {
                vec![self.numeric_builder(
                    field,
                    #[cfg(feature = "rocksdb")]
                    FieldIndexBuilder::FloatIndex,
                    FieldIndexBuilder::FloatMmapIndex,
                    FieldIndexBuilder::FloatGridstoreIndex,
                )]
            }
            PayloadSchemaParams::Geo(_) => {
                vec![self.geo_builder(
                    field,
                    #[cfg(feature = "rocksdb")]
                    FieldIndexBuilder::GeoIndex,
                    FieldIndexBuilder::GeoMmapIndex,
                    FieldIndexBuilder::GeoGridstoreIndex,
                )]
            }
            PayloadSchemaParams::Text(text_index_params) => {
                vec![self.text_builder(field, text_index_params.clone())]
            }
            PayloadSchemaParams::Bool(_) => {
                vec![self.bool_builder(field)?]
            }
            PayloadSchemaParams::Datetime(_) => {
                vec![self.numeric_builder(
                    field,
                    #[cfg(feature = "rocksdb")]
                    FieldIndexBuilder::DatetimeIndex,
                    FieldIndexBuilder::DatetimeMmapIndex,
                    FieldIndexBuilder::DatetimeGridstoreIndex,
                )]
            }
            PayloadSchemaParams::Uuid(_) => {
                vec![self.map_builder(
                    field,
                    #[cfg(feature = "rocksdb")]
                    FieldIndexBuilder::UuidIndex,
                    FieldIndexBuilder::UuidMmapIndex,
                    FieldIndexBuilder::UuidGridstoreIndex,
                )]
            }
        };

        Ok(builders)
    }

    fn map_new<N: MapIndexKey + ?Sized>(
        &self,
        field: &JsonPath,
        create_if_missing: bool,
    ) -> OperationResult<MapIndex<N>>
    where
        Vec<N::Owned>: Blob + Send + Sync,
    {
        Ok(match self {
            #[cfg(feature = "rocksdb")]
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, is_appendable }) => {
                MapIndex::new_rocksdb(Arc::clone(db), &field.to_string(), *is_appendable)
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                MapIndex::new_mmap(&map_dir(dir, field), *is_on_disk)?
            }
            IndexSelector::Gridstore(IndexSelectorGridstore { dir }) => {
                MapIndex::new_gridstore(map_dir(dir, field), create_if_missing)?
            }
        })
    }

    fn map_builder<N: MapIndexKey + ?Sized>(
        &self,
        field: &JsonPath,
        #[cfg(feature = "rocksdb")] make_rocksdb: fn(
            super::map_index::MapIndexBuilder<N>,
        ) -> FieldIndexBuilder,
        make_mmap: fn(MapIndexMmapBuilder<N>) -> FieldIndexBuilder,
        make_gridstore: fn(MapIndexGridstoreBuilder<N>) -> FieldIndexBuilder,
    ) -> FieldIndexBuilder
    where
        Vec<N::Owned>: Blob + Send + Sync,
    {
        match self {
            #[cfg(feature = "rocksdb")]
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, .. }) => make_rocksdb(
                MapIndex::builder_rocksdb(Arc::clone(db), &field.to_string()),
            ),
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                make_mmap(MapIndex::builder_mmap(&map_dir(dir, field), *is_on_disk))
            }
            IndexSelector::Gridstore(IndexSelectorGridstore { dir }) => {
                make_gridstore(MapIndex::builder_gridstore(map_dir(dir, field)))
            }
        }
    }

    fn numeric_new<T: Encodable + Numericable + MmapValue + Send + Sync + Default, P>(
        &self,
        field: &JsonPath,
        create_if_missing: bool,
    ) -> OperationResult<NumericIndex<T, P>>
    where
        Vec<T>: Blob,
    {
        Ok(match self {
            #[cfg(feature = "rocksdb")]
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, is_appendable }) => {
                NumericIndex::new_rocksdb(Arc::clone(db), &field.to_string(), *is_appendable)
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                NumericIndex::new_mmap(&numeric_dir(dir, field), *is_on_disk)?
            }
            IndexSelector::Gridstore(IndexSelectorGridstore { dir }) => {
                NumericIndex::new_gridstore(numeric_dir(dir, field), create_if_missing)?
            }
        })
    }

    fn numeric_builder<T: Encodable + Numericable + MmapValue + Send + Sync + Default, P>(
        &self,
        field: &JsonPath,
        #[cfg(feature = "rocksdb")] make_rocksdb: fn(
            super::numeric_index::NumericIndexBuilder<T, P>,
        ) -> FieldIndexBuilder,
        make_mmap: fn(NumericIndexMmapBuilder<T, P>) -> FieldIndexBuilder,
        make_gridstore: fn(NumericIndexGridstoreBuilder<T, P>) -> FieldIndexBuilder,
    ) -> FieldIndexBuilder
    where
        NumericIndex<T, P>: ValueIndexer<ValueType = P> + NumericIndexIntoInnerValue<T, P>,
        Vec<T>: Blob,
    {
        match self {
            #[cfg(feature = "rocksdb")]
            IndexSelector::RocksDb(IndexSelectorRocksDb {
                db,
                is_appendable: _,
            }) => make_rocksdb(NumericIndex::builder_rocksdb(
                Arc::clone(db),
                &field.to_string(),
            )),
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => make_mmap(
                NumericIndex::builder_mmap(&numeric_dir(dir, field), *is_on_disk),
            ),
            IndexSelector::Gridstore(IndexSelectorGridstore { dir }) => {
                make_gridstore(NumericIndex::builder_gridstore(numeric_dir(dir, field)))
            }
        }
    }

    fn geo_new(&self, field: &JsonPath, create_if_missing: bool) -> OperationResult<GeoMapIndex> {
        Ok(match self {
            #[cfg(feature = "rocksdb")]
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, is_appendable }) => {
                GeoMapIndex::new_memory(Arc::clone(db), &field.to_string(), *is_appendable)
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                GeoMapIndex::new_mmap(&map_dir(dir, field), *is_on_disk)?
            }
            IndexSelector::Gridstore(IndexSelectorGridstore { dir }) => {
                GeoMapIndex::new_gridstore(map_dir(dir, field), create_if_missing)?
            }
        })
    }

    fn geo_builder(
        &self,
        field: &JsonPath,
        #[cfg(feature = "rocksdb")] make_rocksdb: fn(
            super::geo_index::GeoMapIndexBuilder,
        ) -> FieldIndexBuilder,
        make_mmap: fn(GeoMapIndexMmapBuilder) -> FieldIndexBuilder,
        make_gridstore: fn(GeoMapIndexGridstoreBuilder) -> FieldIndexBuilder,
    ) -> FieldIndexBuilder {
        match self {
            #[cfg(feature = "rocksdb")]
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, .. }) => {
                make_rocksdb(GeoMapIndex::builder(Arc::clone(db), &field.to_string()))
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                make_mmap(GeoMapIndex::builder_mmap(&map_dir(dir, field), *is_on_disk))
            }
            IndexSelector::Gridstore(IndexSelectorGridstore { dir }) => {
                make_gridstore(GeoMapIndex::builder_gridstore(map_dir(dir, field)))
            }
        }
    }

    pub fn null_builder(dir: &Path, field: &JsonPath) -> OperationResult<FieldIndexBuilder> {
        // null index is always on disk and appendable
        Ok(FieldIndexBuilder::NullIndex(MmapNullIndex::builder(
            &null_dir(dir, field),
        )?))
    }

    pub fn new_null_index(
        dir: &Path,
        field: &JsonPath,
        total_point_count: usize,
    ) -> OperationResult<Option<FieldIndex>> {
        // null index is always on disk and is appendable
        Ok(
            MmapNullIndex::open_if_exists(&null_dir(dir, field), total_point_count)?
                .map(FieldIndex::NullIndex),
        )
    }

    fn text_new(
        &self,
        field: &JsonPath,
        config: TextIndexParams,
        create_if_missing: bool,
    ) -> OperationResult<FullTextIndex> {
        Ok(match self {
            #[cfg(feature = "rocksdb")]
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, is_appendable }) => {
                FullTextIndex::new_rocksdb(
                    Arc::clone(db),
                    config,
                    &field.to_string(),
                    *is_appendable,
                )
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                FullTextIndex::new_mmap(text_dir(dir, field), config, *is_on_disk)?
            }
            IndexSelector::Gridstore(IndexSelectorGridstore { dir }) => {
                FullTextIndex::new_gridstore(text_dir(dir, field), config, create_if_missing)?
            }
        })
    }

    fn text_builder(&self, field: &JsonPath, config: TextIndexParams) -> FieldIndexBuilder {
        match self {
            #[cfg(feature = "rocksdb")]
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, is_appendable }) => {
                FieldIndexBuilder::FullTextIndex(FullTextIndex::builder_rocksdb(
                    Arc::clone(db),
                    config,
                    &field.to_string(),
                    *is_appendable,
                ))
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                FieldIndexBuilder::FullTextMmapIndex(FullTextIndex::builder_mmap(
                    text_dir(dir, field),
                    config,
                    *is_on_disk,
                ))
            }
            IndexSelector::Gridstore(IndexSelectorGridstore { dir }) => {
                FieldIndexBuilder::FullTextGridstoreIndex(FullTextIndex::builder_gridstore(
                    text_dir(dir, field),
                    config,
                ))
            }
        }
    }

    fn bool_builder(&self, field: &JsonPath) -> OperationResult<FieldIndexBuilder> {
        match self {
            #[cfg(feature = "rocksdb")]
            IndexSelector::RocksDb(IndexSelectorRocksDb {
                db,
                is_appendable: _,
            }) => Ok(FieldIndexBuilder::BoolIndex(SimpleBoolIndex::builder(
                Arc::clone(db),
                &field.to_string(),
            ))),
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                let dir = bool_dir(dir, field);
                Ok(FieldIndexBuilder::BoolMmapIndex(MmapBoolIndex::builder(
                    &dir,
                    *is_on_disk,
                )?))
            }
            // Skip Gridstore for boolean index, mmap index is simpler and is also mutable
            IndexSelector::Gridstore(IndexSelectorGridstore { dir }) => {
                let dir = bool_dir(dir, field);
                Ok(FieldIndexBuilder::BoolMmapIndex(MmapBoolIndex::builder(
                    &dir, false,
                )?))
            }
        }
    }

    fn bool_new(&self, field: &JsonPath) -> OperationResult<FieldIndex> {
        Ok(match self {
            #[cfg(feature = "rocksdb")]
            IndexSelector::RocksDb(IndexSelectorRocksDb {
                db,
                is_appendable: _,
            }) => FieldIndex::BoolIndex(BoolIndex::Simple(SimpleBoolIndex::new(
                Arc::clone(db),
                &field.to_string(),
            ))),
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                let dir = bool_dir(dir, field);
                FieldIndex::BoolIndex(BoolIndex::Mmap(MmapBoolIndex::open_or_create(
                    &dir,
                    *is_on_disk,
                )?))
            }
            // Skip Gridstore for boolean index, mmap index is simpler and is also mutable
            IndexSelector::Gridstore(IndexSelectorGridstore { dir }) => {
                let dir = bool_dir(dir, field);
                FieldIndex::BoolIndex(BoolIndex::Mmap(MmapBoolIndex::open_or_create(&dir, false)?))
            }
        })
    }
}

fn map_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-map", &field.filename()))
}

fn numeric_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-numeric", &field.filename()))
}

fn text_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-text", &field.filename()))
}

fn bool_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-bool", &field.filename()))
}

fn null_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-null", &field.filename()))
}
