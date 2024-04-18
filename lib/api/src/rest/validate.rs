use validator::Validate;

use super::schema::{BatchVectorStruct, Vector, VectorStruct};
use crate::rest::{MultiDenseVector, NamedVectorStruct};

impl Validate for VectorStruct {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            VectorStruct::Single(_) => Ok(()),
            VectorStruct::Multi(v) => common::validation::validate_iter(v.values()),
        }
    }
}

impl Validate for BatchVectorStruct {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            BatchVectorStruct::Single(_) => Ok(()),
            BatchVectorStruct::Multi(v) => {
                common::validation::validate_iter(v.values().flat_map(|batch| batch.iter()))
            }
        }
    }
}

impl Validate for Vector {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Vector::Dense(_) => Ok(()),
            Vector::Sparse(v) => v.validate(),
            Vector::MultiDense(v) => v.validate(),
        }
    }
}

impl Validate for NamedVectorStruct {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            NamedVectorStruct::Default(_) => Ok(()),
            NamedVectorStruct::Dense(_) => Ok(()),
            NamedVectorStruct::Sparse(v) => v.validate(),
        }
    }
}

impl Validate for MultiDenseVector {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        common::validation::validate_multi_vector_len(self.dim as u32, &self.inner_vector)
    }
}
