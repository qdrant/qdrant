use crate::index::index::{PayloadIndex};
use crate::types::{Filter, PayloadKeyType};
use std::sync::Arc;
use atomic_refcell::AtomicRefCell;
use crate::payload_storage::payload_storage::{ConditionChecker, PayloadStorage};
use crate::index::field_index::{Estimation, FieldIndex};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::fs::File;
use std::io::Error;
use crate::entry::entry_point::{OperationResult, OperationError};

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

        // ToDo: implement build indexes

        Ok(StructPayloadIndex {
            condition_checker,
            field_indexes,
            path: path.to_owned(),
        })
    }

    fn save(&self) -> OperationResult<()> {
        let file = File::create(self.path.as_path())?;
        serde_cbor::to_writer(file, &self.field_indexes)
            .map_err(| err| OperationError::ServiceError { description: format!("Unable to save index: {:?}", err) })?;
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

