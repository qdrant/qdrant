use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use common::fs::{atomic_save_json, read_json};
use common::universal_io::{OkNotFound, UniversalReadFs, read_json_via};
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;
use crate::types::{Memory, PayloadFieldSchema, PayloadKeyType};

pub const PAYLOAD_INDEX_CONFIG_FILE: &str = "config.json";

/// Keeps information of which field should be index
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct PayloadConfig {
    /// Mapping of payload index schemas and types
    #[serde(flatten)]
    pub indices: PayloadIndices,
}

impl PayloadConfig {
    pub fn get_config_path(path: &Path) -> PathBuf {
        path.join(PAYLOAD_INDEX_CONFIG_FILE)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        Ok(read_json(path)?)
    }

    /// Read-only mirror of [`load`](Self::load) over the universal filesystem
    /// `fs`. Returns `Ok(None)` when the config file is absent.
    pub fn load_universal<Fs: UniversalReadFs>(
        fs: &Fs,
        path: &Path,
    ) -> OperationResult<Option<Self>> {
        Ok(read_json_via(fs, path).ok_not_found()?)
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
    pub fn to_schemas(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        self.fields
            .iter()
            .map(|(field, index)| (field.clone(), index.schema.clone()))
            .collect()
    }
}

impl Deref for PayloadIndices {
    type Target = HashMap<PayloadKeyType, PayloadFieldSchemaWithIndexType>;
    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl DerefMut for PayloadIndices {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fields
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
    /// If empty for a field, no explicit payload index type mappings have been stored yet.
    /// Then use `schemas` to determine the index types with a best effort approach.
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
                if !schema.types.is_empty() {
                    types.insert(field, schema.types);
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
    pub types: Vec<FullPayloadIndexType>,
}

impl PayloadFieldSchemaWithIndexType {
    pub fn new(schema: PayloadFieldSchema, types: Vec<FullPayloadIndexType>) -> Self {
        Self { schema, types }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, strum::EnumIter)]
#[serde(rename_all = "snake_case")]
pub enum PayloadIndexType {
    IntIndex,
    DatetimeIndex,
    IntMapIndex,
    KeywordIndex,
    FloatIndex,
    GeoIndex,
    FullTextIndex,
    BoolIndex,
    UuidIndex,
    UuidMapIndex,
    NullIndex,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct FullPayloadIndexType {
    pub index_type: PayloadIndexType,
    pub mutability: IndexMutability,
    pub storage_type: StorageType,
}

impl FullPayloadIndexType {
    pub fn mutability(&self) -> &IndexMutability {
        &self.mutability
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IndexMutability {
    /// Supports insertions, updates, and deletions
    Mutable,
    /// Only supports deletions
    Immutable,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StorageType {
    Gridstore,
    Mmap {
        #[serde(rename = "is_on_disk")]
        layout: ImmutableLayout,
    },
}

/// In-memory layout an immutable index was built for.
///
/// Persisted as the legacy `is_on_disk` bool: `true` is [`Self::Mmap`].
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(from = "bool", into = "bool")]
pub enum ImmutableLayout {
    /// Reads go straight to the mmap files (`OnDisk*` index types).
    Mmap,
    /// Files are loaded into heap at open (`Immutable*` index types).
    Heap,
}

impl From<bool> for ImmutableLayout {
    fn from(is_on_disk: bool) -> Self {
        if is_on_disk { Self::Mmap } else { Self::Heap }
    }
}

impl From<ImmutableLayout> for bool {
    fn from(layout: ImmutableLayout) -> Self {
        match layout {
            ImmutableLayout::Mmap => true,
            ImmutableLayout::Heap => false,
        }
    }
}

impl StorageType {
    /// Placement to open an immutable index with, `None` for the appendable
    /// format: the built variant decides heap vs mmap, and the schema's
    /// requested placement refines cold vs cached for the mmap variant.
    pub fn immutable_memory(self, schema_memory: Memory) -> Option<Memory> {
        match self {
            StorageType::Gridstore => None,
            StorageType::Mmap {
                layout: ImmutableLayout::Mmap,
            } => Some(match schema_memory {
                Memory::Cached => Memory::Cached,
                Memory::Cold | Memory::Pinned => Memory::Cold,
            }),
            StorageType::Mmap {
                layout: ImmutableLayout::Heap,
            } => Some(Memory::Pinned),
        }
    }
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

    #[test]
    fn storage_type_layout_wire_format() {
        for (layout, json) in [
            (ImmutableLayout::Mmap, r#"{"mmap":{"is_on_disk":true}}"#),
            (ImmutableLayout::Heap, r#"{"mmap":{"is_on_disk":false}}"#),
        ] {
            let storage_type = StorageType::Mmap { layout };
            assert_eq!(serde_json::to_string(&storage_type).unwrap(), json);
            assert_eq!(
                serde_json::from_str::<StorageType>(json).unwrap(),
                storage_type,
            );
        }
    }
}
