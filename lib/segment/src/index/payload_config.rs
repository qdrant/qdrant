use std::collections::HashMap;
use std::path::{Path, PathBuf};

use io::file_operations::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;
use crate::types::{PayloadFieldSchema, PayloadKeyType};

pub const PAYLOAD_INDEX_CONFIG_FILE: &str = "config.json";

/// Keeps information of which field should be index
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct PayloadConfig {
    /// Mapping of payload index schemas and types
    #[serde(flatten)]
    pub indices: PayloadIndices,

    /// If true, don't create/initialize RocksDB for payload index
    /// This is required for migrating away from RocksDB in favor of the
    /// custom storage engine
    #[cfg(feature = "rocksdb")]
    pub skip_rocksdb: Option<bool>,
}

impl PayloadConfig {
    pub fn get_config_path(path: &Path) -> PathBuf {
        path.join(PAYLOAD_INDEX_CONFIG_FILE)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        Ok(read_json(path)?)
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        Ok(atomic_save_json(path, self)?)
    }
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct PayloadIndices {
    /// Map of indexed fields and their schema
    #[serde(rename = "indexed_fields")]
    pub schemas: HashMap<PayloadKeyType, PayloadFieldSchema>,

    /// Map of indexed fields and their explicit index types
    ///
    /// If empty, no explicit payload index type mappings have been stored yet.
    /// Then use `schemas` to determine the index types.
    ///
    /// Added since Qdrant 1.15
    #[serde(
        default,
        rename = "indexed_types",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub types: HashMap<PayloadKeyType, Vec<FullPayloadIndexType>>,
}

impl PayloadIndices {
    /// Get index schema and type configuration for the given field
    pub fn get(&self, field: &PayloadKeyType) -> Option<PayloadFieldSchemaWithIndexType> {
        self.schemas.get(field).map(|schema| {
            let index_types = self.types.get(field).cloned().unwrap_or_default();
            PayloadFieldSchemaWithIndexType::new(schema.clone(), index_types)
        })
    }

    /// Add a payload field with its schema and index types.
    pub fn insert(
        &mut self,
        field: PayloadKeyType,
        schema: PayloadFieldSchemaWithIndexType,
    ) -> Option<PayloadFieldSchemaWithIndexType> {
        let old_schema = self.schemas.insert(field.clone(), schema.schema);
        let old_types = if !schema.index_types.is_empty() {
            self.types.insert(field, schema.index_types)
        } else {
            self.types.remove(&field)
        };

        old_schema.map(|schema| PayloadFieldSchemaWithIndexType {
            schema,
            index_types: old_types.unwrap_or_default(),
        })
    }

    /// Remove the given field
    pub fn remove(&mut self, field: &PayloadKeyType) -> Option<PayloadFieldSchema> {
        let removed = self.schemas.remove(field);
        self.types.remove(field);
        removed
    }

    /// Get number of fields a payload index is configured on
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// Check if no payload fields have been configured
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }

    /// Check if any payload field has no explicit types configured
    ///
    /// Returns true if empty.
    pub fn any_has_no_type(&self) -> bool {
        // No fields configured at all
        if self.is_empty() {
            return true;
        }

        // Check if any field has no types at all, or an empty list of types
        self.schemas
            .keys()
            .any(|key| self.types.get(key).is_none_or(|types| types.is_empty()))
    }

    pub fn iter(&self) -> impl Iterator<Item = (PayloadKeyType, PayloadFieldSchemaWithIndexType)> {
        self.schemas.iter().map(move |(field_name, schema)| {
            let index_types = self.types.get(field_name).cloned().unwrap_or_default();
            (
                field_name.clone(),
                PayloadFieldSchemaWithIndexType::new(schema.clone(), index_types),
            )
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn into_iter(
        mut self,
    ) -> impl Iterator<Item = (PayloadKeyType, PayloadFieldSchemaWithIndexType)> {
        self.schemas.into_iter().map(move |(field_name, schema)| {
            let index_types = self.types.remove(&field_name).unwrap_or_default();
            (
                field_name,
                PayloadFieldSchemaWithIndexType::new(schema, index_types),
            )
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PayloadFieldSchemaWithIndexType {
    pub schema: PayloadFieldSchema,
    pub index_types: Vec<FullPayloadIndexType>,
}

impl PayloadFieldSchemaWithIndexType {
    pub fn new(schema: PayloadFieldSchema, index_types: Vec<FullPayloadIndexType>) -> Self {
        Self {
            schema,
            index_types,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum FullPayloadIndexType {
    IntIndex(IndexMutability),
    DatetimeIndex(IndexMutability),
    IntMapIndex(IndexMutability),
    KeywordIndex(IndexMutability),
    FloatIndex(IndexMutability),
    GeoIndex(IndexMutability),
    FullTextIndex(IndexMutability),
    BoolIndex(IndexMutability),
    UuidIndex(IndexMutability),
    UuidMapIndex(IndexMutability),
    NullIndex(IndexMutability),
}

impl FullPayloadIndexType {
    pub fn mutability(&self) -> &IndexMutability {
        match self {
            FullPayloadIndexType::IntIndex(index_mutability)
            | FullPayloadIndexType::DatetimeIndex(index_mutability)
            | FullPayloadIndexType::IntMapIndex(index_mutability)
            | FullPayloadIndexType::KeywordIndex(index_mutability)
            | FullPayloadIndexType::FloatIndex(index_mutability)
            | FullPayloadIndexType::GeoIndex(index_mutability)
            | FullPayloadIndexType::FullTextIndex(index_mutability)
            | FullPayloadIndexType::BoolIndex(index_mutability)
            | FullPayloadIndexType::UuidIndex(index_mutability)
            | FullPayloadIndexType::UuidMapIndex(index_mutability)
            | FullPayloadIndexType::NullIndex(index_mutability) => index_mutability,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
pub enum IndexMutability {
    Mutable(StorageType),
    Immutable(StorageType),
    Mmap { is_on_disk: bool },
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
pub enum StorageType {
    Gridstore,
    RocksDB,
    Mmap { is_on_disk: bool },
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use serde_json::Value;

    use super::*;
    use crate::json_path::JsonPath;

    #[test]
    fn test_storage_compatibility() {
        // Check that old format can be parsed with new format.

        let old = r#"{"indexed_fields":{"c":{"type":"integer","lookup":true,"range":false,"is_principal":false,"on_disk":false}}}"#;
        let payload_config: PayloadConfig = serde_json::from_str(old).unwrap();

        let old_value: Value = serde_json::from_str(old).unwrap();
        let old_schema = old_value
            .as_object()
            .unwrap()
            .get("indexed_fields")
            .unwrap()
            .get("c")
            .unwrap()
            .clone();

        let old_config: PayloadFieldSchema = serde_json::from_value(old_schema).unwrap();
        assert_eq!(
            payload_config
                .indices
                .get(&JsonPath::from_str("c").unwrap())
                .unwrap()
                .schema,
            old_config
        );
    }
}
