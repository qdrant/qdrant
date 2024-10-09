use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
use uuid::Uuid;

use super::schema::{ScoredPoint, Vector};
use super::{
    FacetRequestInternal, FacetResponse, FacetValue, FacetValueHit, NearestQuery, OrderByInterface,
    Query, QueryInterface, VectorOutput, VectorStructOutput,
};
use crate::rest::{DenseVector, NamedVectorStruct};

impl From<VectorInternal> for VectorOutput {
    fn from(value: VectorInternal) -> Self {
        match value {
            VectorInternal::Dense(vector) => VectorOutput::Dense(vector),
            VectorInternal::Sparse(vector) => VectorOutput::Sparse(vector),
            VectorInternal::MultiDense(vector) => {
                VectorOutput::MultiDense(vector.into_multi_vectors())
            }
        }
    }
}

impl From<VectorStructInternal> for VectorStructOutput {
    fn from(value: VectorStructInternal) -> Self {
        // ToDo: this conversion should be removed
        match value {
            VectorStructInternal::Single(vector) => VectorStructOutput::Single(vector),
            VectorStructInternal::MultiDense(vector) => {
                VectorStructOutput::MultiDense(vector.into_multi_vectors())
            }
            VectorStructInternal::Named(vectors) => VectorStructOutput::Named(
                vectors
                    .into_iter()
                    .map(|(k, v)| (k, VectorOutput::from(v)))
                    .collect(),
            ),
        }
    }
}

impl From<Vector> for VectorInternal {
    fn from(value: Vector) -> Self {
        match value {
            Vector::Dense(vector) => VectorInternal::Dense(vector),
            Vector::Sparse(vector) => VectorInternal::Sparse(vector),
            Vector::MultiDense(vectors) => VectorInternal::MultiDense(
                segment::data_types::vectors::MultiDenseVectorInternal::new_unchecked(vectors),
            ),
            Vector::Document(_) | Vector::Image(_) | Vector::Object(_) => {
                // If this is reached, it means validation failed
                unimplemented!("Inference is not implemented, please use vectors instead")
            }
        }
    }
}

impl From<segment::types::ScoredPoint> for ScoredPoint {
    fn from(value: segment::types::ScoredPoint) -> Self {
        ScoredPoint {
            id: value.id,
            version: value.version,
            score: value.score,
            payload: value.payload,
            vector: value.vector.map(VectorStructOutput::from),
            shard_key: value.shard_key,
            order_value: value.order_value.map(From::from),
        }
    }
}

impl From<NamedVectorStruct> for segment::data_types::vectors::NamedVectorStruct {
    fn from(value: NamedVectorStruct) -> Self {
        match value {
            NamedVectorStruct::Default(vector) => {
                segment::data_types::vectors::NamedVectorStruct::Default(vector)
            }
            NamedVectorStruct::Dense(vector) => {
                segment::data_types::vectors::NamedVectorStruct::Dense(vector)
            }
            NamedVectorStruct::Sparse(vector) => {
                segment::data_types::vectors::NamedVectorStruct::Sparse(vector)
            }
        }
    }
}

impl From<DenseVector> for NamedVectorStruct {
    fn from(v: DenseVector) -> Self {
        NamedVectorStruct::Default(v)
    }
}

impl From<segment::data_types::vectors::NamedVector> for NamedVectorStruct {
    fn from(v: segment::data_types::vectors::NamedVector) -> Self {
        NamedVectorStruct::Dense(v)
    }
}

impl From<OrderByInterface> for OrderBy {
    fn from(order_by: OrderByInterface) -> Self {
        match order_by {
            OrderByInterface::Key(key) => OrderBy {
                key,
                direction: None,
                start_from: None,
            },
            OrderByInterface::Struct(order_by) => order_by,
        }
    }
}

impl From<QueryInterface> for Query {
    fn from(value: QueryInterface) -> Self {
        match value {
            QueryInterface::Nearest(vector) => Query::Nearest(NearestQuery { nearest: vector }),
            QueryInterface::Query(query) => query,
        }
    }
}

impl From<segment::data_types::facets::FacetValue> for FacetValue {
    fn from(value: segment::data_types::facets::FacetValue) -> Self {
        match value {
            segment::data_types::facets::FacetValue::Keyword(keyword) => Self::String(keyword),
            segment::data_types::facets::FacetValue::Int(integer) => Self::Integer(integer),
            segment::data_types::facets::FacetValue::Uuid(uuid_int) => {
                Self::String(Uuid::from_u128(uuid_int).to_string())
            }
            segment::data_types::facets::FacetValue::Bool(b) => Self::Bool(b),
        }
    }
}

impl From<segment::data_types::facets::FacetValueHit> for FacetValueHit {
    fn from(value: segment::data_types::facets::FacetValueHit) -> Self {
        Self {
            value: From::from(value.value),
            count: value.count,
        }
    }
}

impl From<segment::data_types::facets::FacetResponse> for FacetResponse {
    fn from(value: segment::data_types::facets::FacetResponse) -> Self {
        Self {
            hits: value.hits.into_iter().map(From::from).collect(),
        }
    }
}

impl From<FacetRequestInternal> for segment::data_types::facets::FacetParams {
    fn from(value: FacetRequestInternal) -> Self {
        Self {
            key: value.key,
            limit: value.limit.unwrap_or(Self::DEFAULT_LIMIT),
            filter: value.filter,
            exact: value.exact.unwrap_or(Self::DEFAULT_EXACT),
        }
    }
}
