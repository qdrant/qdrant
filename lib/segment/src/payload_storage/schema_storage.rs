use parking_lot::RwLock;

use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::{PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType, TheMap};

/// a shared storage for schema data
pub struct SchemaStorage {
    schema: RwLock<TheMap<PayloadKeyType, PayloadSchemaType>>,
}

impl SchemaStorage {
    pub fn new() -> Self {
        let schema: TheMap<PayloadKeyType, PayloadSchemaType> = TheMap::new();
        Self {
            schema: RwLock::new(schema),
        }
    }

    pub fn update_schema_value(
        &self,
        key: PayloadKeyTypeRef,
        schema_type: PayloadSchemaType,
    ) -> OperationResult<()> {
        if schema_type == PayloadSchemaType::Unknown {
            return Err(OperationError::ServiceError {
                description: "unknown type".to_string(),
            });
        }

        let schema_read = self.schema.read();

        return match schema_read.get(key) {
            None => {
                drop(schema_read);
                let mut schema_write = self.schema.write();
                match schema_write.get(key) {
                    None => {
                        schema_write.insert(key.to_owned(), schema_type);
                        Ok(())
                    }
                    Some(current_schema_type) => {
                        SchemaStorage::check_schema_type(key, &schema_type, current_schema_type)
                    }
                }
            }
            Some(current_schema_type) => {
                SchemaStorage::check_schema_type(key, &schema_type, current_schema_type)
            }
        };
    }

    pub fn insert(&self, key: PayloadKeyType, value: PayloadSchemaType) {
        let mut map = self.schema.write();
        map.insert(key, value);
    }

    pub fn as_map(&self) -> TheMap<PayloadKeyType, PayloadSchemaType> {
        self.schema.read().clone()
    }

    pub fn remove(&self, key: PayloadKeyTypeRef) {
        self.schema.write().remove(key);
    }

    fn check_schema_type(
        key: PayloadKeyTypeRef,
        value_schema_type: &PayloadSchemaType,
        current_schema_type: &PayloadSchemaType,
    ) -> OperationResult<()> {
        if current_schema_type == value_schema_type {
            println!("**** OK   ");
            Ok(())
        } else {
            Err(OperationError::TypeError {
                field_name: key.to_owned(),
                expected_type: format!("{:?}", current_schema_type),
            })
        }
    }
}

impl Default for SchemaStorage {
    fn default() -> Self {
        Self::new()
    }
}
