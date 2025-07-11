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

/// Map of indexed fields with their schema and type
///
/// Virtual structure, serialized and deserialized through `PayloadIndicesStorage`.
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
#[serde(from = "PayloadIndicesStorage", into = "PayloadIndicesStorage")]
pub struct PayloadIndices {
    fields: HashMap<PayloadKeyType, PayloadFieldSchemaWithIndexType>,
}

impl PayloadIndices {
    /// Get index schema and type configuration for the given field
    pub fn get(&self, field: &PayloadKeyType) -> Option<&PayloadFieldSchemaWithIndexType> {
        self.fields.get(field)
    }

    /// Get index schema and type configuration for the given field
    pub fn get_mut(
        &mut self,
        field: &PayloadKeyType,
    ) -> Option<&mut PayloadFieldSchemaWithIndexType> {
        self.fields.get_mut(field)
    }

    /// Add a payload field with its schema and index types
    ///
    /// Returns the previous value if it existed.
    pub fn insert(
        &mut self,
        field: PayloadKeyType,
        schema: PayloadFieldSchemaWithIndexType,
    ) -> Option<PayloadFieldSchemaWithIndexType> {
        self.fields.insert(field, schema)
    }

    pub fn remove(&mut self, field: &PayloadKeyType) -> Option<PayloadFieldSchemaWithIndexType> {
        self.fields.remove(field)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Check if any payload field has no explicit types configured
    ///
    /// Returns false if empty.
    pub fn any_has_no_type(&self) -> bool {
        self.fields
            .values()
            .any(|index| index.index_types.is_empty())
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&PayloadKeyType, &PayloadFieldSchemaWithIndexType)> {
        self.fields.iter()
    }

    pub fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&PayloadKeyType, &mut PayloadFieldSchemaWithIndexType)> {
        self.fields.iter_mut()
    }

    pub fn to_schemas(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        self.fields
            .iter()
            .map(|(field, index)| (field.clone(), index.schema.clone()))
            .collect()
    }
}

/// Storage helper for `PayloadIndices`
///
/// This type is used for serialization and deserialization of the payload indices. It is
/// compatible with the old format of payload indices, which only stored the indexed fields.
#[derive(Deserialize, Serialize)]
pub struct PayloadIndicesStorage {
    /// Map of indexed fields and their schema
    pub indexed_fields: HashMap<PayloadKeyType, PayloadFieldSchema>,

    /// Map of indexed fields and their explicit index types
    ///
    /// If empty, no explicit payload index type mappings have been stored yet.
    /// Then use `schemas` to determine the index types.
    ///
    /// Added since Qdrant 1.15
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub indexed_types: HashMap<PayloadKeyType, Vec<FullPayloadIndexType>>,
}

impl From<PayloadIndicesStorage> for PayloadIndices {
    fn from(mut storage: PayloadIndicesStorage) -> Self {
        let fields = storage
            .indexed_fields
            .into_iter()
            .map(|(field, schema)| {
                let index_types = storage.indexed_types.remove(&field).unwrap_or_default();
                (
                    field,
                    PayloadFieldSchemaWithIndexType::new(schema, index_types),
                )
            })
            .collect::<HashMap<_, _>>();
        Self { fields }
    }
}

impl From<PayloadIndices> for PayloadIndicesStorage {
    fn from(storage: PayloadIndices) -> Self {
        let (indexed_fields, indexed_types) = storage.fields.into_iter().fold(
            (HashMap::new(), HashMap::new()),
            |(mut fields, mut types), (field, schema)| {
                fields.insert(field.clone(), schema.schema);
                if !schema.index_types.is_empty() {
                    types.insert(field, schema.index_types);
                }
                (fields, types)
            },
        );
        Self {
            indexed_fields,
            indexed_types,
        }
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
