use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;

use super::binary_index::BinaryIndex;
use super::histogram::Numericable;
use super::map_index::{MapIndex, MapIndexBuilder, MapIndexKey, MapIndexMmapBuilder};
use super::mmap_point_to_values::MmapValue;
use super::numeric_index::{
    Encodable, NumericIndexBuilder, NumericIndexIntoInnerValue, NumericIndexMmapBuilder,
};
use super::{FieldIndexBuilder, ValueIndexer};
use crate::common::operation_error::{OperationError, OperationResult};
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

impl<'a> IndexSelector<'a> {
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
                    .then(|| OperationResult::Ok(FieldIndex::IntMapIndex(self.map_new(field)?)))
                    .transpose()?,
                integer_params
                    .range
                    .then(|| OperationResult::Ok(FieldIndex::IntIndex(self.numeric_new(field)?)))
                    .transpose()?,
            )
            .collect(),
            PayloadSchemaParams::Float(_) => vec![FieldIndex::FloatIndex(self.numeric_new(field)?)],
            PayloadSchemaParams::Geo(_) => vec![FieldIndex::GeoIndex(GeoMapIndex::new(
                self.as_rocksdb()?.db.clone(),
                &field.to_string(),
                self.as_rocksdb()?.is_appendable,
            ))],
            PayloadSchemaParams::Text(text_index_params) => {
                vec![FieldIndex::FullTextIndex(FullTextIndex::new(
                    self.as_rocksdb()?.db.clone(),
                    text_index_params.clone(),
                    &field.to_string(),
                    self.as_rocksdb()?.is_appendable,
                ))]
            }
            PayloadSchemaParams::Bool(_) => {
                vec![FieldIndex::BinaryIndex(BinaryIndex::new(
                    self.as_rocksdb()?.db.clone(),
                    &field.to_string(),
                ))]
            }
            PayloadSchemaParams::Datetime(_) => {
                vec![FieldIndex::DatetimeIndex(self.numeric_new(field)?)]
            }
            PayloadSchemaParams::Uuid(_) => vec![FieldIndex::UuidIndex(self.numeric_new(field)?)],
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
                integer_params.lookup.then(|| {
                    self.map_builder(
                        field,
                        FieldIndexBuilder::IntMapIndex,
                        FieldIndexBuilder::IntMapMmapIndex,
                    )
                }),
                integer_params.range.then(|| {
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
                vec![FieldIndexBuilder::GeoIndex(GeoMapIndex::builder(
                    self.as_rocksdb()?.db.clone(),
                    &field.to_string(),
                ))]
            }
            PayloadSchemaParams::Text(text_index_params) => {
                vec![FieldIndexBuilder::FullTextIndex(FullTextIndex::builder(
                    self.as_rocksdb()?.db.clone(),
                    text_index_params.clone(),
                    &field.to_string(),
                ))]
            }
            PayloadSchemaParams::Bool(_) => {
                vec![FieldIndexBuilder::BinaryIndex(BinaryIndex::builder(
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
                vec![self.numeric_builder(
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
                MapIndex::new(Arc::clone(db), &field.to_string(), *is_appendable)
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

    fn as_rocksdb(&self) -> OperationResult<&IndexSelectorRocksDb> {
        match self {
            IndexSelector::RocksDb(mode) => Ok(mode),
            _ => Err(OperationError::service_error("Expected RocksDB mode")), // Should never happen
        }
    }
}

fn map_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-map", &field.filename()))
}

fn numeric_dir(dir: &Path, field: &JsonPath) -> PathBuf {
    dir.join(format!("{}-numeric", &field.filename()))
}
