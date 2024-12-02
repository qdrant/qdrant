use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;

use super::bool_index::simple_bool_index::BoolIndex;
use super::geo_index::{GeoMapIndexBuilder, GeoMapIndexMmapBuilder};
use super::histogram::Numericable;
use super::map_index::{MapIndex, MapIndexBuilder, MapIndexKey, MapIndexMmapBuilder};
use super::mmap_point_to_values::MmapValue;
use super::numeric_index::{
    Encodable, NumericIndexBuilder, NumericIndexIntoInnerValue, NumericIndexMmapBuilder,
};
use super::{FieldIndexBuilder, ValueIndexer};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::full_text_index::text_index::FullTextIndex;
use crate::index::field_index::geo_index::GeoMapIndex;
use crate::index::field_index::numeric_index::NumericIndex;
use crate::index::field_index::FieldIndex;
use crate::json_path::JsonPath;
use crate::types::{PayloadFieldSchema, PayloadSchemaParams};

/// Selects index and index builder types based on field type.
#[derive(Copy, Clone)]
pub enum IndexSelector<'a> {
    RocksDb(IndexSelectorRocksDb<'a>),
    OnDisk(IndexSelectorOnDisk<'a>),
}

#[derive(Copy, Clone)]
pub struct IndexSelectorRocksDb<'a> {
    pub db: &'a Arc<RwLock<DB>>,
    pub is_appendable: bool,
}

#[derive(Copy, Clone)]
pub struct IndexSelectorOnDisk<'a> {
    pub dir: &'a Path,
}

impl IndexSelector<'_> {
    /// Selects index type based on field type.
    pub fn new_index(
        &self,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<Vec<FieldIndex>> {
        Ok(match payload_schema.expand().as_ref() {
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
                vec![FieldIndex::BoolIndex(BoolIndex::new(
                    self.as_rocksdb()?.db.clone(),
                    &field.to_string(),
                ))]
            }
            PayloadSchemaParams::Datetime(_) => {
                vec![FieldIndex::DatetimeIndex(self.numeric_new(field)?)]
            }
            PayloadSchemaParams::Uuid(_) => {
                vec![FieldIndex::UuidMapIndex(self.map_new(field)?)]
            }
        })
    }

    /// Selects index builder based on field type.
    pub fn index_builder(
        &self,
        field: &JsonPath,
        payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<Vec<FieldIndexBuilder>> {
        Ok(match payload_schema.expand().as_ref() {
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
                vec![FieldIndexBuilder::BoolIndex(BoolIndex::builder(
                    self.as_rocksdb()?.db.clone(),
                    &field.to_string(),
                ))]
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
        })
    }

    fn map_new<N: MapIndexKey + ?Sized>(&self, field: &JsonPath) -> OperationResult<MapIndex<N>> {
        Ok(match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, is_appendable }) => {
                MapIndex::new_memory(Arc::clone(db), &field.to_string(), *is_appendable)
            }
            IndexSelector::OnDisk(IndexSelectorOnDisk { dir }) => {
                MapIndex::new_mmap(&map_dir(dir, field))?
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
            IndexSelector::OnDisk(IndexSelectorOnDisk { dir }) => {
                make_mmap(MapIndex::mmap_builder(&map_dir(dir, field)))
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
            IndexSelector::OnDisk(IndexSelectorOnDisk { dir }) => {
                NumericIndex::new_mmap(&numeric_dir(dir, field))?
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
            IndexSelector::OnDisk(IndexSelectorOnDisk { dir }) => {
                make_mmap(NumericIndex::builder_mmap(&numeric_dir(dir, field)))
            }
        }
    }

    fn geo_new(&self, field: &JsonPath) -> OperationResult<GeoMapIndex> {
        Ok(match self {
            IndexSelector::RocksDb(IndexSelectorRocksDb { db, is_appendable }) => {
                GeoMapIndex::new_memory(Arc::clone(db), &field.to_string(), *is_appendable)
            }
            IndexSelector::OnDisk(IndexSelectorOnDisk { dir }) => {
                GeoMapIndex::new_mmap(&map_dir(dir, field))?
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
            IndexSelector::OnDisk(IndexSelectorOnDisk { dir }) => {
                make_mmap(GeoMapIndex::mmap_builder(&map_dir(dir, field)))
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
            IndexSelector::OnDisk(IndexSelectorOnDisk { dir }) => {
                FullTextIndex::new_mmap(text_dir(dir, field), config)?
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
            IndexSelector::OnDisk(IndexSelectorOnDisk { dir }) => {
                FieldIndexBuilder::FullTextMmapIndex(FullTextIndex::builder_mmap(
                    text_dir(dir, field),
                    config,
                ))
            }
        }
    }

    fn as_rocksdb(&self) -> OperationResult<&IndexSelectorRocksDb> {
        match self {
            IndexSelector::RocksDb(mode) => Ok(mode),
            IndexSelector::OnDisk(_) => Err(OperationError::service_error("Expected RocksDB mode")), // Should never happen
        }
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
