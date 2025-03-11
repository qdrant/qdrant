use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;

use super::bool_index::BoolIndex;
use super::bool_index::mmap_bool_index::MmapBoolIndex;
use super::bool_index::simple_bool_index::SimpleBoolIndex;
use super::geo_index::{GeoMapIndexBuilder, GeoMapIndexMmapBuilder};
use super::histogram::Numericable;
use super::map_index::{MapIndex, MapIndexBuilder, MapIndexKey, MapIndexMmapBuilder};
use super::mmap_point_to_values::MmapValue;
use super::numeric_index::{
    Encodable, NumericIndexBuilder, NumericIndexIntoInnerValue, NumericIndexMmapBuilder,
};
use super::{FieldIndexBuilder, ValueIndexer};
use crate::common::operation_error::OperationResult;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::FieldIndex;
use crate::index::field_index::full_text_index::text_index::FullTextIndex;
use crate::index::field_index::geo_index::GeoMapIndex;
use crate::index::field_index::null_index::mmap_null_index::MmapNullIndex;
use crate::index::field_index::numeric_index::NumericIndex;
use crate::json_path::JsonPath;
use crate::types::{PayloadFieldSchema, PayloadSchemaParams};

/// Selects index and index builder types based on field type.
#[derive(Copy, Clone)]
pub enum IndexSelector<'a> {
    RocksDb(IndexSelectorRocksDb<'a>),
    Mmap(IndexSelectorMmap<'a>),
}

#[derive(Copy, Clone)]
pub struct IndexSelectorRocksDb<'a> {
    pub db: &'a Arc<RwLock<DB>>,
    pub is_appendable: bool,
}

#[derive(Copy, Clone)]
pub struct IndexSelectorMmap<'a> {
    pub dir: &'a Path,
    pub is_on_disk: bool,
}

impl IndexSelector<'_> {
    /// Selects index type based on field type.
    pub fn new_index(
        &self,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<Vec<FieldIndex>> {
        let mut indexes = match payload_schema.expand().as_ref() {
            PayloadSchemaParams::Keyword(_) => vec![FieldIndex::KeywordIndex(self.map_new(field)?)],
            PayloadSchemaParams::Integer(integer_params) => itertools::chain(
                integer_params
                    .lookup
                    .unwrap_or(true)
                    .then(|| OperationResult::Ok(FieldIndex::IntMapIndex(self.map_new(field)?)))
                    .transpose()?,
                integer_params
                    .range
                    .unwrap_or(true)
                    .then(|| OperationResult::Ok(FieldIndex::IntIndex(self.numeric_new(field)?)))
                    .transpose()?,
            )
            .collect(),
            PayloadSchemaParams::Float(_) => vec![FieldIndex::FloatIndex(self.numeric_new(field)?)],
            PayloadSchemaParams::Geo(_) => vec![FieldIndex::GeoIndex(self.geo_new(field)?)],
            PayloadSchemaParams::Text(text_index_params) => {
                vec![FieldIndex::FullTextIndex(
                    self.text_new(field, text_index_params.clone())?,
                )]
            }
            PayloadSchemaParams::Bool(_) => {
                vec![self.bool_new(field)?]
            }
            PayloadSchemaParams::Datetime(_) => {
                vec![FieldIndex::DatetimeIndex(self.numeric_new(field)?)]
            }
            PayloadSchemaParams::Uuid(_) => {
                vec![FieldIndex::UuidMapIndex(self.map_new(field)?)]
            }
        };

        if let Some(null_index) = self.new_null_index(field)? {
            indexes.push(null_index);
        }

        Ok(indexes)
    }

    /// Selects index builder based on field type.
    pub fn index_builder(
        &self,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<Vec<FieldIndexBuilder>> {
        let mut builders = match payload_schema.expand().as_ref() {
            PayloadSchemaParams::Keyword(_) => {
                vec![self.map_builder(
                    field,
                    FieldIndexBuilder::KeywordIndex,
                    FieldIndexBuilder::KeywordMmapIndex,
                )]
            }
            PayloadSchemaParams::Integer(integer_params) => itertools::chain(
                integer_params.lookup.unwrap_or(true).then(|| {
                    self.map_builder(
                        field,
                        FieldIndexBuilder::IntMapIndex,
                        FieldIndexBuilder::IntMapMmapIndex,
                    )
                }),
                integer_params.range.unwrap_or(true).then(|| {
                    self.numeric_builder(
                        field,
                        FieldIndexBuilder::IntIndex,
                        FieldIndexBuilder::IntMmapIndex,
                    )
                }),
            )
            .collect(),
            PayloadSchemaParams::Float(_) => {
                vec![self.numeric_builder(
                    field,
                    FieldIndexBuilder::FloatIndex,
                    FieldIndexBuilder::FloatMmapIndex,
                )]
            }
            PayloadSchemaParams::Geo(_) => {
                vec![self.geo_builder(
                    field,
                    FieldIndexBuilder::GeoIndex,
                    FieldIndexBuilder::GeoMmapIndex,
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
                    FieldIndexBuilder::DatetimeIndex,
                    FieldIndexBuilder::DatetimeMmapIndex,
                )]
            }
            PayloadSchemaParams::Uuid(_) => {
                vec![self.map_builder(
                    field,
                    FieldIndexBuilder::UuidIndex,
                    FieldIndexBuilder::UuidMmapIndex,
                )]
            }
        };

        if let Some(null_builder) = self.null_builder(field)? {
            builders.push(null_builder);
        }

        Ok(builders)
    }

    fn map_new<N: MapIndexKey + ?Sized>(&self, field: &JsonPath) -> OperationResult<MapIndex<N>> {
        Ok(match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, is_appendable }) => {
                MapIndex::new_memory(Arc::clone(db), &field.to_string(), *is_appendable)
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                MapIndex::new_mmap(&map_dir(dir, field), *is_on_disk)?
            }
        })
    }

    fn map_builder<N: MapIndexKey + ?Sized>(
        &self,
        field: &JsonPath,
        make_rocksdb: fn(MapIndexBuilder<N>) -> FieldIndexBuilder,
        make_mmap: fn(MapIndexMmapBuilder<N>) -> FieldIndexBuilder,
    ) -> FieldIndexBuilder {
        match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, .. }) => {
                make_rocksdb(MapIndex::builder(Arc::clone(db), &field.to_string()))
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                make_mmap(MapIndex::mmap_builder(&map_dir(dir, field), *is_on_disk))
            }
        }
    }

    fn numeric_new<T: Encodable + Numericable + MmapValue + Default, P>(
        &self,
        field: &JsonPath,
    ) -> OperationResult<NumericIndex<T, P>> {
        Ok(match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, is_appendable }) => {
                NumericIndex::new(Arc::clone(db), &field.to_string(), *is_appendable)
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                NumericIndex::new_mmap(&numeric_dir(dir, field), *is_on_disk)?
            }
        })
    }

    fn numeric_builder<T: Encodable + Numericable + MmapValue + Default, P>(
        &self,
        field: &JsonPath,
        make_rocksdb: fn(NumericIndexBuilder<T, P>) -> FieldIndexBuilder,
        make_mmap: fn(NumericIndexMmapBuilder<T, P>) -> FieldIndexBuilder,
    ) -> FieldIndexBuilder
    where
        NumericIndex<T, P>: ValueIndexer<ValueType = P> + NumericIndexIntoInnerValue<T, P>,
    {
        match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb {
                db,
                is_appendable: _,
            }) => make_rocksdb(NumericIndex::builder(Arc::clone(db), &field.to_string())),
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => make_mmap(
                NumericIndex::builder_mmap(&numeric_dir(dir, field), *is_on_disk),
            ),
        }
    }

    fn geo_new(&self, field: &JsonPath) -> OperationResult<GeoMapIndex> {
        Ok(match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, is_appendable }) => {
                GeoMapIndex::new_memory(Arc::clone(db), &field.to_string(), *is_appendable)
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                GeoMapIndex::new_mmap(&map_dir(dir, field), *is_on_disk)?
            }
        })
    }

    fn null_builder(&self, field: &JsonPath) -> OperationResult<Option<FieldIndexBuilder>> {
        Ok(match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb { .. }) => None, // ToDo: appendable index should also be created
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk: _ }) => Some(
                // null index is always on disk
                FieldIndexBuilder::NullIndex(MmapNullIndex::builder(&null_dir(dir, field))?),
            ),
        })
    }

    fn new_null_index(&self, field: &JsonPath) -> OperationResult<Option<FieldIndex>> {
        Ok(match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb { .. }) => None, // ToDo: appendable index should also be created
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk: _ }) => {
                // null index is always on disk
                MmapNullIndex::open_if_exists(&null_dir(dir, field))?.map(FieldIndex::NullIndex)
            }
        })
    }

    fn geo_builder(
        &self,
        field: &JsonPath,
        make_rocksdb: fn(GeoMapIndexBuilder) -> FieldIndexBuilder,
        make_mmap: fn(GeoMapIndexMmapBuilder) -> FieldIndexBuilder,
    ) -> FieldIndexBuilder {
        match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, .. }) => {
                make_rocksdb(GeoMapIndex::builder(Arc::clone(db), &field.to_string()))
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                make_mmap(GeoMapIndex::mmap_builder(&map_dir(dir, field), *is_on_disk))
            }
        }
    }

    fn text_new(
        &self,
        field: &JsonPath,
        config: TextIndexParams,
    ) -> OperationResult<FullTextIndex> {
        Ok(match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, is_appendable }) => {
                FullTextIndex::new_memory(
                    Arc::clone(db),
                    config,
                    &field.to_string(),
                    *is_appendable,
                )
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                FullTextIndex::new_mmap(text_dir(dir, field), config, *is_on_disk)?
            }
        })
    }

    fn text_builder(&self, field: &JsonPath, config: TextIndexParams) -> FieldIndexBuilder {
        match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb {
                db,
                is_appendable: _,
            }) => FieldIndexBuilder::FullTextIndex(FullTextIndex::builder(
                Arc::clone(db),
                config,
                &field.to_string(),
            )),
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                FieldIndexBuilder::FullTextMmapIndex(FullTextIndex::builder_mmap(
                    text_dir(dir, field),
                    config,
                    *is_on_disk,
                ))
            }
        }
    }

    fn bool_builder(&self, field: &JsonPath) -> OperationResult<FieldIndexBuilder> {
        match self {
            IndexSelector::RocksDb(index_selector_rocks_db) => Ok(FieldIndexBuilder::BoolIndex(
                SimpleBoolIndex::builder(index_selector_rocks_db.db.clone(), &field.to_string()),
            )),
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                let dir = bool_dir(dir, field);
                Ok(FieldIndexBuilder::BoolMmapIndex(MmapBoolIndex::builder(
                    &dir,
                    *is_on_disk,
                )?))
            }
        }
    }

    fn bool_new(&self, field: &JsonPath) -> OperationResult<FieldIndex> {
        Ok(match self {
            IndexSelector::RocksDb(index_selector_rocks_db) => {
                FieldIndex::BoolIndex(BoolIndex::Simple(SimpleBoolIndex::new(
                    index_selector_rocks_db.db.clone(),
                    &field.to_string(),
                )))
            }
            IndexSelector::Mmap(IndexSelectorMmap { dir, is_on_disk }) => {
                let dir = bool_dir(dir, field);
                FieldIndex::BoolIndex(BoolIndex::Mmap(MmapBoolIndex::open_or_create(
                    &dir,
                    *is_on_disk,
                )?))
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
