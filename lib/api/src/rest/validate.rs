use std::borrow::Cow;

use common::validation::validate_multi_vector;
use validator::{Validate, ValidationError, ValidationErrors};

use super::schema::BatchVectorStruct;
use super::{
    Batch, ContextInput, Fusion, OrderByInterface, PointVectors, Query, QueryInterface,
    RecommendInput, Sample, VectorInput,
};
use crate::rest::NamedVectorStruct;

impl Validate for NamedVectorStruct {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            NamedVectorStruct::Default(_) => Ok(()),
            NamedVectorStruct::Dense(_) => Ok(()),
            NamedVectorStruct::Sparse(v) => v.validate(),
        }
    }
}

impl Validate for QueryInterface {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            QueryInterface::Nearest(vector) => vector.validate(),
            QueryInterface::Query(query) => query.validate(),
        }
    }
}

impl Validate for Query {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Query::Nearest(vector) => vector.nearest.validate(),
            Query::Recommend(recommend) => recommend.recommend.validate(),
            Query::Discover(discover) => discover.discover.validate(),
            Query::Context(context) => context.context.validate(),
            Query::Fusion(fusion) => fusion.fusion.validate(),
            Query::OrderBy(order_by) => order_by.order_by.validate(),
            Query::Sample(sample) => sample.sample.validate(),
        }
    }
}

impl Validate for VectorInput {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            VectorInput::Id(_id) => Ok(()),
            VectorInput::DenseVector(_dense) => Ok(()),
            VectorInput::SparseVector(sparse) => sparse.validate(),
            VectorInput::MultiDenseVector(multi) => validate_multi_vector(multi),
            VectorInput::Document(doc) => doc.validate(),
            VectorInput::Image(image) => image.validate(),
            VectorInput::Object(obj) => obj.validate(),
        }
    }
}

impl Validate for RecommendInput {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        let no_positives = self.positive.as_ref().map(|p| p.is_empty()).unwrap_or(true);
        let no_negatives = self.negative.as_ref().map(|n| n.is_empty()).unwrap_or(true);

        if no_positives && no_negatives {
            let mut errors = validator::ValidationErrors::new();
            errors.add(
                "positives, negatives",
                ValidationError::new(
                    "At least one positive or negative vector/id must be provided",
                ),
            );
            return Err(errors);
        }

        for item in self.iter() {
            item.validate()?;
        }

        Ok(())
    }
}

impl Validate for ContextInput {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        for item in self.0.iter().flatten().flat_map(|item| item.iter()) {
            item.validate()?;
        }

        Ok(())
    }
}

impl Validate for Fusion {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Fusion::Rrf | Fusion::Dbsf => Ok(()),
        }
    }
}

impl Validate for OrderByInterface {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            OrderByInterface::Key(_key) => Ok(()), // validated during parsing
            OrderByInterface::Struct(order_by) => order_by.validate(),
        }
    }
}

impl Validate for Sample {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            Sample::Random => Ok(()),
        }
    }
}

impl Validate for BatchVectorStruct {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            BatchVectorStruct::Single(_) => Ok(()),
            BatchVectorStruct::MultiDense(vectors) => {
                for vector in vectors {
                    validate_multi_vector(vector)?;
                }
                Ok(())
            }
            BatchVectorStruct::Named(v) => {
                common::validation::validate_iter(v.values().flat_map(|batch| batch.iter()))
            }
            BatchVectorStruct::Document(_) => Ok(()),
            BatchVectorStruct::Image(_) => Ok(()),
            BatchVectorStruct::Object(_) => Ok(()),
        }
    }
}

impl Validate for Batch {
    fn validate(&self) -> Result<(), ValidationErrors> {
        let batch = self;

        let bad_input_description = |ids: usize, vecs: usize| -> String {
            format!("number of ids and vectors must be equal ({ids} != {vecs})")
        };
        let create_error = |message: String| -> ValidationErrors {
            let mut errors = ValidationErrors::new();
            errors.add("batch", {
                let mut error = ValidationError::new("point_insert_operation");
                error.message.replace(Cow::from(message));
                error
            });
            errors
        };

        self.vectors.validate()?;
        match &batch.vectors {
            BatchVectorStruct::Single(vectors) => {
                if batch.ids.len() != vectors.len() {
                    return Err(create_error(bad_input_description(
                        batch.ids.len(),
                        vectors.len(),
                    )));
                }
            }
            BatchVectorStruct::MultiDense(vectors) => {
                if batch.ids.len() != vectors.len() {
                    return Err(create_error(bad_input_description(
                        batch.ids.len(),
                        vectors.len(),
                    )));
                }
            }
            BatchVectorStruct::Named(named_vectors) => {
                for vectors in named_vectors.values() {
                    if batch.ids.len() != vectors.len() {
                        return Err(create_error(bad_input_description(
                            batch.ids.len(),
                            vectors.len(),
                        )));
                    }
                }
            }
            BatchVectorStruct::Document(_) => {}
            BatchVectorStruct::Image(_) => {}
            BatchVectorStruct::Object(_) => {}
        }
        if let Some(payload_vector) = &batch.payloads {
            if payload_vector.len() != batch.ids.len() {
                return Err(create_error(format!(
                    "number of ids and payloads must be equal ({} != {})",
                    batch.ids.len(),
                    payload_vector.len(),
                )));
            }
        }
        Ok(())
    }
}

impl Validate for PointVectors {
    fn validate(&self) -> Result<(), ValidationErrors> {
        if self.vector.is_empty() {
            let mut err = ValidationError::new("length");
            err.message = Some(Cow::from("must specify vectors to update for point"));
            err.add_param(Cow::from("min"), &1);
            let mut errors = ValidationErrors::new();
            errors.add("vector", err);
            Err(errors)
        } else {
            self.vector.validate()
        }
    }
}
