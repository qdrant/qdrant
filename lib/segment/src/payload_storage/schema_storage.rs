use parking_lot::RwLock;
use std::borrow::Borrow;
use std::path::{Path, PathBuf};

use crate::common::file_operations::{atomic_save_json, read_json};
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::{PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType, TheMap};

pub const SCHEMA_CONFIG_FILE: &str = "schema.json";

/// a shared storage for schema data
pub struct SchemaStorage {
    schema: RwLock<TheMap<PayloadKeyType, Option<PayloadSchemaType>>>,
    path: PathBuf,
}

impl SchemaStorage {
    pub fn load(path: &Path) -> OperationResult<Self> {
        let schema: TheMap<PayloadKeyType, Option<PayloadSchemaType>> = read_json(path)?;
        Ok(Self {
            schema: RwLock::new(schema),
            path: path.join(SCHEMA_CONFIG_FILE),
        })
    }

    pub fn save(
        path: &Path,
        map: &TheMap<PayloadKeyType, PayloadSchemaType>,
    ) -> OperationResult<()> {
        atomic_save_json(path, map)
    }

    pub fn update_schema_value(
        &self,
        key: PayloadKeyTypeRef,
        schema_type: &Option<PayloadSchemaType>,
    ) -> OperationResult<()> {
        let schema_read = self.schema.read();

        return match schema_read.get(key) {
            None => {
                drop(schema_read);
                self.set(key, schema_type)
            }
            Some(Some(current_schema_type)) => {
                SchemaStorage::check_schema_type(key, schema_type, current_schema_type)
            }
            Some(None) => {
                drop(schema_read);
                self.set(key, schema_type)
            }
        };
    }

    fn set(
        &mut self,
        key: PayloadKeyTypeRef,
        schema_type: &Option<PayloadSchemaType>,
    ) -> OperationResult<()> {
        let mut schema_write = self.schema.write();

        match schema_write.get(key) {
            None => {
                schema_write.insert(key.to_owned(), schema_type.to_owned());
                SchemaStorage::save(self.path, schema_write)?;
                Ok(())
            }
            Some(Some(current_schema_type)) => {
                SchemaStorage::check_schema_type(key, schema_type, current_schema_type)
            }
            Some(None) => {
                schema_write.insert(key.to_owned(), schema_type.to_owned());
                SchemaStorage::save(self.path, schema_write)?;
                Ok(())
            }
        };

        Ok(())
    }

    pub fn as_map(&self) -> TheMap<PayloadKeyType, Option<PayloadSchemaType>> {
        self.schema.read().clone()
    }

    fn check_schema_type(
        key: PayloadKeyTypeRef,
        new_schema_type: &Option<PayloadSchemaType>,
        current_schema_type: &PayloadSchemaType,
    ) -> OperationResult<()> {
        match new_schema_type {
            None => Err(OperationError::TypeError {
                field_name: key.to_owned(),
                expected_type: format!("{:?}", current_schema_type),
            }),
            Some(schema_type) => {
                if current_schema_type == schema_type {
                    Ok(())
                } else {
                    Err(OperationError::TypeError {
                        field_name: key.to_owned(),
                        expected_type: format!("{:?}", current_schema_type),
                    })
                }
            }
        }
    }
}
