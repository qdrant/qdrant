use crate::index::index::{PayloadIndex};
use crate::types::{Filter, PayloadKeyType, PayloadSchema};
use std::sync::Arc;
use atomic_refcell::AtomicRefCell;
use crate::payload_storage::payload_storage::{ConditionChecker, PayloadStorage};
use crate::index::field_index::{Estimation, FieldIndex};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::fs::File;
use std::io::Error;
use crate::entry::entry_point::{OperationResult, OperationError};
use crate::index::field_index::index_builder::{IndexBuilderTypes, IndexBuilder};
use crate::index::field_index::numeric_index::PersistedNumericIndex;

type IndexesMap = HashMap<PayloadKeyType, Vec<FieldIndex>>;

struct StructPayloadIndex {
    condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
    field_indexes: IndexesMap,
    path: PathBuf,
}

impl StructPayloadIndex {
    pub fn open(condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
                path: &Path,
    ) -> Self {
        let file = File::open(path);
        let field_indexes: IndexesMap = match file {
            Ok(file_reader) => serde_cbor::from_reader(file_reader).unwrap(),
            Err(_) => Default::default()
        };

        StructPayloadIndex {
            condition_checker,
            field_indexes,
            path: path.to_owned(),
        }
    }

    pub fn build(
        condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
        payload: Arc<AtomicRefCell<dyn PayloadStorage>>,
        path: &Path,
    ) -> OperationResult<Self> {
        let mut field_indexes: IndexesMap = Default::default();

        let payload_ref = payload.borrow();
        let schema = payload_ref.schema();

        let mut builders: HashMap<_, _> = schema
            .into_iter()
            .map(|(field, schema_type)| {
                let builder = match schema_type {
                    PayloadSchema::Keyword => IndexBuilderTypes::Keyword(IndexBuilder::new()),
                    PayloadSchema::Integer => IndexBuilderTypes::Integer(IndexBuilder::new()),
                    PayloadSchema::Float => IndexBuilderTypes::Float(IndexBuilder::new()),
                    PayloadSchema::Geo => IndexBuilderTypes::Geo(IndexBuilder::new())
                };
                return (field, builder);
            }).collect();

        for point_id in payload_ref.iter_ids() {
            let point_payload = payload_ref.payload(point_id);
            for (key, value) in point_payload.iter() {
                builders.get_mut(key).unwrap().add(point_id, value)
            }
        }

        for (key, builder) in builders.iter() {
            let mut indexes: Vec<FieldIndex> = vec![];
            match builder {
                IndexBuilderTypes::Float(builder) => {
                    indexes.push(FieldIndex::FloatIndex(builder.into()))
                }
                IndexBuilderTypes::Integer(builder) => {
                    indexes.push(FieldIndex::IntIndex(builder.into()));
                    indexes.push(FieldIndex::IntMapIndex(builder.into()));
                }
                IndexBuilderTypes::Keyword(builder) => {
                    indexes.push(FieldIndex::KeywordIndex(builder.into()));
                }
                IndexBuilderTypes::Geo(builder) => {}
            }
            field_indexes.insert(key.to_owned(), indexes);
        }

        Ok(StructPayloadIndex {
            condition_checker,
            field_indexes,
            path: path.to_owned(),
        })
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
    fn estimate_cardinality(&self, query: &Filter) -> Estimation {
        unimplemented!()
    }

    fn query_points(&self, query: &Filter) -> Vec<usize> {
        unimplemented!()
    }
}

