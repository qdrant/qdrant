use crate::index::index::{PayloadIndex};
use crate::types::{Filter, PayloadKeyType, PayloadSchemaType, PayloadType};
use std::sync::Arc;
use atomic_refcell::AtomicRefCell;
use crate::payload_storage::payload_storage::{ConditionChecker, PayloadStorage};
use crate::index::field_index::{CardinalityEstimation, FieldIndex};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::fs::{File, create_dir_all};
use std::io::Error;
use crate::entry::entry_point::{OperationResult, OperationError};
use crate::index::field_index::index_builder::{IndexBuilderTypes, IndexBuilder};
use crate::index::field_index::numeric_index::PersistedNumericIndex;
use uuid::Builder;
use crate::index::field_index::field_index::PayloadFieldIndexBuilder;
use crate::index::field_index::index_selector::index_selector;
use crate::index::payload_config::PayloadConfig;

pub const PAYLOAD_FIELD_INDEX_PATH: &str = "fields";

type IndexesMap = HashMap<PayloadKeyType, Vec<FieldIndex>>;

struct StructPayloadIndex {
    condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
    payload: Arc<AtomicRefCell<dyn PayloadStorage>>,
    field_indexes: IndexesMap,
    config: PayloadConfig,
    path: PathBuf,
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

    fn load_field_index(&mut self, field: &PayloadKeyType) -> OperationResult<()> {
        let field_index_path = Self::get_field_index_path(&self.path, field);
        let file = File::open(field_index_path)?;
        let field_indexes: Vec<FieldIndex> = serde_cbor::from_reader(file)
            .map_err(|err| OperationError::ServiceError { description: format!("Unable to load index: {:?}", err) })?;
        self.field_indexes.insert(field.clone(), field_indexes);

        Ok(())
    }

    fn load_all_fields(&mut self) -> OperationResult<()> {
        let field_iterator = self.config.indexed_fields.iter();
        for field in field_iterator {
            self.load_field_index(field)?;
        }
        Ok(())
    }


    pub fn open(condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
                payload: Arc<AtomicRefCell<dyn PayloadStorage>>,
                path: &Path,
    ) -> OperationResult<Self> {
        let config_path = PayloadConfig::get_config_path(path);
        let config = PayloadConfig::load(&config_path)?;

        let file = File::open(path);
        let field_indexes: IndexesMap = match file {
            Ok(file_reader) => serde_cbor::from_reader(file_reader).unwrap(),
            Err(_) => Default::default()
        };

        let index = StructPayloadIndex {
            condition_checker,
            payload,
            field_indexes,
            config,
            path: path.to_owned(),
        };

        Ok(index)
    }

    pub fn build_field_index(&mut self, field: &PayloadKeyType) -> OperationResult<()> {
        let payload_ref = self.payload.borrow();
        let schema = payload_ref.schema();

        let field_type_opt = schema.get(field);

        if field_type_opt.is_none() {
            // There is not data to index
            return Ok(());
        }

        let field_type = field_type_opt.unwrap();

        let mut builders = index_selector(field_type);

        let mut field_indexes: IndexesMap = Default::default();

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

        self.field_indexes.insert(
            field.clone(),
            builders.iter_mut().map(|builder| builder.build()).collect(),
        );

        self.save_field_index(field)
    }

    fn build_all_fields(&mut self) -> OperationResult<()> {
        for field in self.config.indexed_fields.iter() {
            self.build_field_index(field)?;
        }
        Ok(())
    }

    pub fn new(
        condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
        payload: Arc<AtomicRefCell<dyn PayloadStorage>>,
        path: &Path,
        config: Option<PayloadConfig>,
    ) -> OperationResult<Self> {
        create_dir_all(path)?;
        let payload_config = config.unwrap_or_default();
        let mut payload_index = Self {
            condition_checker,
            payload,
            field_indexes: Default::default(),
            config: payload_config,
            path: path.to_owned(),
        };

        Ok(payload_index)
    }

    fn save(&self) -> OperationResult<()> {
        let file = File::create(self.path.as_path())?;
        serde_cbor::to_writer(file, &self.field_indexes)
            .map_err(|err| OperationError::ServiceError { description: format!("Unable to save index: {:?}", err) })?;
        Ok(())
    }

    fn total_points(&self) -> usize {
        unimplemented!()
    }
}


impl PayloadIndex for StructPayloadIndex {
    fn indexed_fields(&self) -> Vec<PayloadKeyType> {
        unimplemented!()
    }

    fn mark_indexed(&mut self, field: &PayloadKeyType) -> OperationResult<()> {
        unimplemented!()
    }

    fn drop_index(&mut self, field: &PayloadKeyType) -> OperationResult<()> {
        unimplemented!()
    }

    fn estimate_cardinality(&self, query: &Filter) -> CardinalityEstimation {
        unimplemented!()
    }

    fn query_points(&self, query: &Filter) -> Vec<usize> {
        unimplemented!()
    }
}

