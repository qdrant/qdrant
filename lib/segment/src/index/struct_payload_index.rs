use std::collections::HashMap;
use std::fs::{create_dir_all, File, remove_file};
use std::io::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use itertools::Itertools;
use log::debug;
use uuid::Builder;

use crate::entry::entry_point::{OperationError, OperationResult};
use crate::index::field_index::CardinalityEstimation;
use crate::index::field_index::field_index::{FieldIndex, PayloadFieldIndexBuilder};
use crate::index::field_index::index_selector::index_selector;
use crate::index::field_index::numeric_index::PersistedNumericIndex;
use crate::index::index::PayloadIndex;
use crate::index::payload_config::PayloadConfig;
use crate::payload_storage::payload_storage::{ConditionChecker, PayloadStorage};
use crate::types::{Filter, PayloadKeyType, PayloadSchemaType, PayloadType};

pub const PAYLOAD_FIELD_INDEX_PATH: &str = "fields";

type IndexesMap = HashMap<PayloadKeyType, Vec<FieldIndex>>;

pub struct StructPayloadIndex {
    condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
    payload: Arc<AtomicRefCell<dyn PayloadStorage>>,
    field_indexes: IndexesMap,
    config: PayloadConfig,
    path: PathBuf,
    total_points: usize,
}

impl StructPayloadIndex {
    fn config_path(&self) -> PathBuf {
        PayloadConfig::get_config_path(&self.path)
    }

    fn save_config(&self) -> OperationResult<()> {
        let config_path = self.config_path();
        self.config.save(&config_path)
    }

    fn get_field_index_dir(path: &Path) -> PathBuf {
        path.join(PAYLOAD_FIELD_INDEX_PATH)
    }

    fn get_field_index_path(path: &Path, field: &PayloadKeyType) -> PathBuf {
        Self::get_field_index_dir(path).join(format!("{}.idx", field))
    }

    fn save_field_index(&self, field: &PayloadKeyType) -> OperationResult<()> {
        let field_index_dir = Self::get_field_index_dir(&self.path);
        let field_index_path = Self::get_field_index_path(&self.path, field);
        create_dir_all(field_index_dir)?;

        match self.field_indexes.get(field) {
            None => {}
            Some(indexes) => {
                let file = File::create(field_index_path.as_path())?;
                serde_cbor::to_writer(file, indexes)
                    .map_err(|err| OperationError::ServiceError { description: format!("Unable to save index: {:?}", err) })?;
            }
        }
        Ok(())
    }

    fn load_field_index(&self, field: &PayloadKeyType) -> OperationResult<Vec<FieldIndex>> {
        let field_index_path = Self::get_field_index_path(&self.path, field);
        debug!("Loading field `{}` index from {}", field, field_index_path.to_str().unwrap());
        let file = File::open(field_index_path)?;
        let field_indexes: Vec<FieldIndex> = serde_cbor::from_reader(file)
            .map_err(|err| OperationError::ServiceError { description: format!("Unable to load index: {:?}", err) })?;

        Ok(field_indexes)
    }

    fn load_all_fields(&mut self) -> OperationResult<()> {
        let mut field_indexes: IndexesMap = Default::default();
        for field in self.config.indexed_fields.iter() {
            let field_index = self.load_field_index(field)?;
            field_indexes.insert(field.clone(), field_index);
        }
        self.field_indexes = field_indexes;
        Ok(())
    }


    pub fn open(condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
                payload: Arc<AtomicRefCell<dyn PayloadStorage>>,
                path: &Path,
                total_points: usize,
    ) -> OperationResult<Self> {
        let config_path = PayloadConfig::get_config_path(path);
        let config = PayloadConfig::load(&config_path)?;

        let mut index = StructPayloadIndex {
            condition_checker,
            payload,
            field_indexes: Default::default(),
            config,
            path: path.to_owned(),
            total_points,
        };

        index.load_all_fields()?;

        Ok(index)
    }

    pub fn build_field_index(&self, field: &PayloadKeyType) -> OperationResult<Vec<FieldIndex>> {
        let payload_ref = self.payload.borrow();
        let schema = payload_ref.schema();

        let field_type_opt = schema.get(field);

        if field_type_opt.is_none() {
            // There is not data to index
            return Ok(vec![]);
        }

        let field_type = field_type_opt.unwrap();

        let mut builders = index_selector(field_type);

        for point_id in payload_ref.iter_ids() {
            let point_payload = payload_ref.payload(point_id);
            let field_value_opt = point_payload.get(field);
            match field_value_opt {
                None => {}
                Some(field_value) => {
                    for builder in builders.iter_mut() {
                        builder.add(point_id, field_value)
                    }
                }
            }
        }

        let field_indexes = builders.iter_mut().map(|builder| builder.build()).collect_vec();

        Ok(field_indexes)
    }

    fn build_all_fields(&mut self) -> OperationResult<()> {
        let mut field_indexes: IndexesMap = Default::default();
        for field in self.config.indexed_fields.iter() {
            let field_index = self.build_field_index(field)?;
            field_indexes.insert(field.clone(), field_index);
        }
        self.field_indexes = field_indexes;
        for field in self.config.indexed_fields.iter() {
            self.save_field_index(field)?;
        }
        Ok(())
    }

    fn build_and_save(&mut self, field: &PayloadKeyType) -> OperationResult<()> {
        if !self.config.indexed_fields.contains(field) {
            self.config.indexed_fields.push(field.clone());
            self.save_config()?;
        }

        let field_indexes = self.build_field_index(field)?;
        self.field_indexes.insert(
            field.clone(),
            field_indexes,
        );

        self.save_field_index(field)?;

        Ok(())
    }

    pub fn new(
        condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
        payload: Arc<AtomicRefCell<dyn PayloadStorage>>,
        path: &Path,
        config: Option<PayloadConfig>,
        total_points: usize,
    ) -> OperationResult<Self> {
        create_dir_all(path)?;
        let payload_config = config.unwrap_or_default();
        let mut payload_index = Self {
            condition_checker,
            payload,
            field_indexes: Default::default(),
            config: payload_config,
            path: path.to_owned(),
            total_points,
        };

        payload_index.build_all_fields()?;

        Ok(payload_index)
    }

    fn save(&self) -> OperationResult<()> {
        let file = File::create(self.path.as_path())?;
        serde_cbor::to_writer(file, &self.field_indexes)
            .map_err(|err| OperationError::ServiceError { description: format!("Unable to save index: {:?}", err) })?;
        Ok(())
    }

    pub fn total_points(&self) -> usize {
        self.total_points
    }
}


impl PayloadIndex for StructPayloadIndex {
    fn indexed_fields(&self) -> Vec<PayloadKeyType> {
        self.config.indexed_fields.clone()
    }

    fn mark_indexed(&mut self, field: &PayloadKeyType) -> OperationResult<()> {
        if !self.config.indexed_fields.contains(field) {
            self.config.indexed_fields.push(field.clone());
            self.save_config()?;
            self.build_and_save(field)?;
        }
        Ok(())
    }

    fn drop_index(&mut self, field: &PayloadKeyType) -> OperationResult<()> {
        self.config.indexed_fields = self.config.indexed_fields.iter().cloned().filter(|x| x != field).collect();
        self.save_config()?;
        self.field_indexes.remove(field);

        let field_index_path = Self::get_field_index_path(&self.path, field);

        if field_index_path.exists() {
            remove_file(&field_index_path)?;
        }

        Ok(())
    }

    fn query_points(&self, query: &Filter) -> Vec<usize> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;

    use super::*;

    #[test]
    fn test_index_save_and_load() {
        let dir = TempDir::new("storage_dir").unwrap();
        let mut storage = SimplePayloadStorage::open(dir.path()).unwrap();
    }
}