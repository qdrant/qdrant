use std::collections::{HashMap, HashSet};
use std::fs::{create_dir_all, File, remove_file};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use itertools::Itertools;
use log::debug;

use crate::entry::entry_point::{OperationError, OperationResult};
use crate::index::field_index::field_index::{FieldIndex, PayloadFieldIndex};
use crate::index::field_index::index_selector::index_selector;
use crate::index::index::PayloadIndex;
use crate::index::payload_config::PayloadConfig;
use crate::payload_storage::payload_storage::{ConditionChecker, PayloadStorage};
use crate::types::{Filter, PayloadKeyType, FieldCondition, Condition, PointOffsetType};
use crate::index::field_index::{CardinalityEstimation, PrimaryCondition};
use crate::index::query_estimator::estimate_filter;
use crate::vector_storage::vector_storage::VectorStorage;
use std::iter::FromIterator;
use crate::id_mapper::id_mapper::IdMapper;

pub const PAYLOAD_FIELD_INDEX_PATH: &str = "fields";

type IndexesMap = HashMap<PayloadKeyType, Vec<FieldIndex>>;

pub struct StructPayloadIndex {
    condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
    vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
    payload: Arc<AtomicRefCell<dyn PayloadStorage>>,
    id_mapper: Arc<AtomicRefCell<dyn IdMapper>>,
    field_indexes: IndexesMap,
    config: PayloadConfig,
    path: PathBuf,
}

impl StructPayloadIndex {
    pub fn estimate_field_condition(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        self.field_indexes.get(&condition.key).and_then(|indexes| {
            let mut result_estimation: Option<CardinalityEstimation> = None;
            for index in indexes {
                result_estimation = index.estimate_cardinality(condition);
                if result_estimation.is_some() {
                    break;
                }
            }
            result_estimation
        })
    }

    fn query_field(&self, field_condition: &FieldCondition) -> Option<Box<dyn Iterator<Item=PointOffsetType> + '_>> {
        let indexes = self.field_indexes
            .get(&field_condition.key)
            .and_then(|indexes|
                indexes
                    .iter()
                    .map(|field_index| field_index.filter(field_condition))
                    .skip_while(|filter_iter| filter_iter.is_none())
                    .next()
                    .map(|filter_iter| filter_iter.unwrap())
            );
        indexes
    }

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
                vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
                payload: Arc<AtomicRefCell<dyn PayloadStorage>>,
                id_mapper: Arc<AtomicRefCell<dyn IdMapper>>,
                path: &Path,
    ) -> OperationResult<Self> {
        let config_path = PayloadConfig::get_config_path(path);
        let config = if config_path.exists() {
            PayloadConfig::load(&config_path)?
        } else {
            PayloadConfig::default()
        };

        let mut index = StructPayloadIndex {
            condition_checker,
            vector_storage,
            payload,
            id_mapper,
            field_indexes: Default::default(),
            config,
            path: path.to_owned()
        };

        if !index.config_path().exists() {
            // Save default config
            index.save_config()?
        }

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

    fn save(&self) -> OperationResult<()> {
        let file = File::create(self.path.as_path())?;
        serde_cbor::to_writer(file, &self.field_indexes)
            .map_err(|err| OperationError::ServiceError { description: format!("Unable to save index: {:?}", err) })?;
        Ok(())
    }

    pub fn total_points(&self) -> usize {
        self.vector_storage.borrow().vector_count()
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

    fn estimate_cardinality(&self, query: &Filter) -> CardinalityEstimation {
        let total = self.total_points();

        let estimator = |condition: &Condition| {
            match condition {
                Condition::Filter(_) => panic!("Unexpected branching"),
                Condition::HasId(ids) => {
                    let id_mapper_ref = self.id_mapper.borrow();
                    let mapped_ids: HashSet<PointOffsetType> = ids.iter()
                        .filter_map(|external_id| id_mapper_ref.internal_id(*external_id))
                        .collect();
                    let num_ids = mapped_ids.len();
                    CardinalityEstimation {
                        primary_clauses: vec![PrimaryCondition::Ids(mapped_ids)],
                        min: 0,
                        exp: num_ids,
                        max: num_ids,
                    }
                }
                Condition::Field(field_condition) => self
                    .estimate_field_condition(field_condition)
                    .unwrap_or(CardinalityEstimation {
                        primary_clauses: vec![],
                        min: 0,
                        exp: self.total_points() / 2,
                        max: self.total_points(),
                    }),
            }
        };

        estimate_filter(&estimator, query, total)
    }

    fn query_points(&self, query: &Filter) -> Box<dyn Iterator<Item=PointOffsetType> + '_> {
        // Assume query is already estimated to be small enough so we can iterate over all matched ids
        let query_cardinality = self.estimate_cardinality(query);
        let condition_checker = self.condition_checker.borrow();
        let vector_storage_ref = self.vector_storage.borrow();
        let full_scan_iterator = vector_storage_ref.iter_ids(); // Should not be used if filter restricted by indexed fields
        return if query_cardinality.primary_clauses.is_empty() {
            // Worst case: query expected to return few matches, but index can't be used
            let matched_points = full_scan_iterator
                .filter(|i| condition_checker.check(*i, query))
                .collect_vec();

            Box::new(matched_points.into_iter())
        } else {
            // CPU-optimized strategy here: points are made unique before applying other filters.
            let preselected: HashSet<PointOffsetType> = query_cardinality.primary_clauses.iter()
                .map(|clause| {
                    match clause {
                        PrimaryCondition::Condition(field_condition) => self.query_field(field_condition)
                            .unwrap_or(vector_storage_ref.iter_ids() /* index is not built */),
                        PrimaryCondition::Ids(ids) => Box::new(ids.iter().cloned())
                    }
                })
                .flat_map(|x| x)
                .collect();
            let matched_points = preselected.into_iter()
                .filter(|i| condition_checker.check(*i, query))
                .collect_vec();
            Box::new(matched_points.into_iter())
        };
    }
}
