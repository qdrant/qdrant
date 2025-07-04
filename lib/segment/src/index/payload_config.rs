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
    pub indexed_fields: HashMap<PayloadKeyType, PayloadFieldSchemaWithIndexType>,

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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PayloadFieldSchemaWithIndexType {
    #[serde(flatten)]
    pub schema: PayloadFieldSchema,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum IndexMutability {
    Mutable(StorageType),
    Immutable(StorageType),
    Mmap { is_on_disk: bool },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
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
                .indexed_fields
                .get(&JsonPath::from_str("c").unwrap())
                .unwrap()
                .schema,
            old_config
        );
    }
}
