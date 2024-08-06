use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;

use super::schema::{BatchVectorStruct, ScoredPoint, Vector, VectorStruct};
use super::{
    FacetResponse, FacetValue, FacetValueHit, NearestQuery, OrderByInterface, Query, QueryInterface,
};
use crate::rest::{DenseVector, NamedVectorStruct};

impl From<segment::data_types::vectors::Vector> for Vector {
    fn from(value: segment::data_types::vectors::Vector) -> Self {
        match value {
            segment::data_types::vectors::Vector::Dense(vector) => Vector::Dense(vector),
            segment::data_types::vectors::Vector::Sparse(vector) => Vector::Sparse(vector),
            segment::data_types::vectors::Vector::MultiDense(vector) => {
                Vector::MultiDense(vector.into_multi_vectors())
            }
        }
    }
}

impl From<Vector> for segment::data_types::vectors::Vector {
    fn from(value: Vector) -> Self {
        match value {
            Vector::Dense(vector) => segment::data_types::vectors::Vector::Dense(vector),
            Vector::Sparse(vector) => segment::data_types::vectors::Vector::Sparse(vector),
            Vector::MultiDense(vector) => {
                // the REST vectors have been validated already
                // we can use an internal constructor
                segment::data_types::vectors::Vector::MultiDense(
                    segment::data_types::vectors::MultiDenseVectorInternal::new_unchecked(vector),
                )
            }
        }
    }
}

impl From<segment::data_types::vectors::VectorStructInternal> for VectorStruct {
    fn from(value: segment::data_types::vectors::VectorStructInternal) -> Self {
        match value {
            segment::data_types::vectors::VectorStructInternal::Single(vector) => {
                VectorStruct::Single(vector)
            }
            segment::data_types::vectors::VectorStructInternal::MultiDense(vector) => {
                VectorStruct::MultiDense(vector.into_multi_vectors())
            }
            segment::data_types::vectors::VectorStructInternal::Named(vectors) => {
                VectorStruct::Named(vectors.into_iter().map(|(k, v)| (k, v.into())).collect())
            }
        }
    }
}

impl From<VectorStruct> for segment::data_types::vectors::VectorStructInternal {
    fn from(value: VectorStruct) -> Self {
        match value {
            VectorStruct::Single(vector) => {
                segment::data_types::vectors::VectorStructInternal::Single(vector)
            }
            VectorStruct::MultiDense(vector) => {
                segment::data_types::vectors::VectorStructInternal::MultiDense(
                    segment::data_types::vectors::MultiDenseVectorInternal::new_unchecked(vector),
                )
            }
            VectorStruct::Named(vectors) => {
                segment::data_types::vectors::VectorStructInternal::Named(
                    vectors.into_iter().map(|(k, v)| (k, v.into())).collect(),
                )
            }
        }
    }
}

impl<'a> From<VectorStruct> for segment::data_types::named_vectors::NamedVectors<'a> {
    fn from(value: VectorStruct) -> Self {
        match value {
            VectorStruct::Single(vector) => {
                segment::data_types::named_vectors::NamedVectors::from_pairs([(
                    DEFAULT_VECTOR_NAME.to_string(),
                    vector,
                )])
            }
            VectorStruct::MultiDense(vector) => {
                let mut named_vector = segment::data_types::named_vectors::NamedVectors::default();
                let multivec =
                    segment::data_types::vectors::MultiDenseVectorInternal::new_unchecked(vector);

                named_vector.insert(
                    DEFAULT_VECTOR_NAME.to_string(),
                    segment::data_types::vectors::Vector::from(multivec),
                );
                named_vector
            }
            VectorStruct::Named(vectors) => {
                let mut named_vector = segment::data_types::named_vectors::NamedVectors::default();
                for (name, vector) in vectors {
                    named_vector.insert(name, segment::data_types::vectors::Vector::from(vector));
                }
                named_vector
            }
        }
    }
}

impl From<segment::data_types::vectors::BatchVectorStructInternal> for BatchVectorStruct {
    fn from(value: segment::data_types::vectors::BatchVectorStructInternal) -> Self {
        match value {
            segment::data_types::vectors::BatchVectorStructInternal::Single(vectors) => {
                BatchVectorStruct::Single(vectors)
            }
            segment::data_types::vectors::BatchVectorStructInternal::MultiDense(vectors) => {
                BatchVectorStruct::MultiDense(
                    vectors
                        .into_iter()
                        .map(|v| v.into_multi_vectors())
                        .collect(),
                )
            }
            segment::data_types::vectors::BatchVectorStructInternal::Named(vectors) => {
                BatchVectorStruct::Named(
                    vectors
                        .into_iter()
                        .map(|(k, v)| (k, v.into_iter().map(|v| v.into()).collect()))
                        .collect(),
                )
            }
        }
    }
}

impl From<BatchVectorStruct> for segment::data_types::vectors::BatchVectorStructInternal {
    fn from(value: BatchVectorStruct) -> Self {
        match value {
            BatchVectorStruct::Single(vector) => {
                segment::data_types::vectors::BatchVectorStructInternal::Single(vector)
            }
            BatchVectorStruct::MultiDense(vectors) => {
                segment::data_types::vectors::BatchVectorStructInternal::MultiDense(
                    vectors
                        .into_iter()
                        .map(|v| {
                            segment::data_types::vectors::MultiDenseVectorInternal::new_unchecked(v)
                        })
                        .collect(),
                )
            }
            BatchVectorStruct::Named(vectors) => {
                segment::data_types::vectors::BatchVectorStructInternal::Named(
                    vectors
                        .into_iter()
                        .map(|(k, v)| (k, v.into_iter().map(|v| v.into()).collect()))
                        .collect(),
                )
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
            vector: value.vector.map(From::from),
            shard_key: value.shard_key,
            order_value: value.order_value.map(From::from),
        }
    }
}

impl From<ScoredPoint> for segment::types::ScoredPoint {
    fn from(value: ScoredPoint) -> Self {
        segment::types::ScoredPoint {
            id: value.id,
            version: value.version,
            score: value.score,
            payload: value.payload,
            vector: value.vector.map(From::from),
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
            segment::data_types::facets::FacetValue::Keyword(keyword) => Self::Keyword(keyword),
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
