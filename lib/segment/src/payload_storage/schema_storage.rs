use parking_lot::RwLock;

use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::{PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType, PayloadType, TheMap};

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
        value: &PayloadType,
    ) -> OperationResult<()> {
        let mut schema = self.schema.write();
        return match schema.get(key) {
            None => {
                schema.insert(key.to_owned(), value.into());
                Ok(())
            }
            Some(schema_type) => {
                if schema_type == &value.into() {
                    Ok(())
                } else {
                    Err(OperationError::TypeError {
                        field_name: key.to_owned(),
                        expected_type: format!("{:?}", schema_type),
                    })
                }
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
}

impl Default for SchemaStorage {
    fn default() -> Self {
        Self::new()
    }
}
