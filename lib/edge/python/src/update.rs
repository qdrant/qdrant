pub mod upsert;

use derive_more::Into;
use pyo3::prelude::*;
use shard::operations::CollectionUpdateOperations;

#[pyclass(name = "UpdateOperation")]
#[derive(Clone, Debug, Into)]
pub struct PyUpdateOperation(CollectionUpdateOperations);
