use itertools::Itertools;
use segment::common::operation_error::OperationError;
use segment::data_types::vectors::{
    DenseVector, MultiDenseVectorInternal, NamedVectorStruct, VectorInternal, VectorStructInternal,
};
use sparse::common::sparse_vector::SparseVector;
use tonic::Status;

use crate::grpc::qdrant as grpc;
use crate::rest::schema as rest;

fn convert_to_plain_multi_vector(
    data: Vec<f32>,
    vectors_count: usize,
) -> Result<rest::MultiDenseVector, OperationError> {
    let dim = data.len() / vectors_count;
    if dim * vectors_count != data.len() {
        return Err(OperationError::ValidationError {
            description: format!(
                "Data length is not divisible by vectors count. Data length: {}, vectors count: {}",
                data.len(),
                vectors_count
            ),
        });
    }

    Ok(data
        .into_iter()
        .chunks(dim)
        .into_iter()
        .map(Iterator::collect)
        .collect())
}

impl TryFrom<rest::VectorOutput> for grpc::VectorOutput {
    type Error = OperationError;

    fn try_from(value: rest::VectorOutput) -> Result<Self, Self::Error> {
        let vector = match value {
            rest::VectorOutput::Dense(dense) => {
                let internal_vector = VectorInternal::from(dense);
                grpc::VectorOutput::from(internal_vector)
            }
            rest::VectorOutput::Sparse(sparse) => {
                let internal_vector = VectorInternal::from(sparse);
                grpc::VectorOutput::from(internal_vector)
            }
            rest::VectorOutput::MultiDense(multi) => {
                let internal_vector = VectorInternal::try_from(multi)?;
                grpc::VectorOutput::from(internal_vector)
            }
        };
        Ok(vector)
    }
}

impl TryFrom<rest::VectorStructOutput> for grpc::VectorsOutput {
    type Error = OperationError;

    fn try_from(
        vector_struct: crate::rest::schema::VectorStructOutput,
    ) -> Result<Self, Self::Error> {
        let vectors = match vector_struct {
            crate::rest::schema::VectorStructOutput::Single(dense) => {
                let vector = VectorInternal::from(dense);
                Self {
                    vectors_options: Some(grpc::vectors_output::VectorsOptions::Vector(
                        grpc::VectorOutput::from(vector),
                    )),
                }
            }
            crate::rest::schema::VectorStructOutput::MultiDense(vector) => {
                let vector = VectorInternal::try_from(vector)?;
                Self {
                    vectors_options: Some(grpc::vectors_output::VectorsOptions::Vector(
                        grpc::VectorOutput::from(vector),
                    )),
                }
            }
            crate::rest::schema::VectorStructOutput::Named(vectors) => {
                let vectors: Result<_, _> = vectors
                    .into_iter()
                    .map(|(name, vector)| grpc::VectorOutput::try_from(vector).map(|v| (name, v)))
                    .collect();

                Self {
                    vectors_options: Some(grpc::vectors_output::VectorsOptions::Vectors(
                        grpc::NamedVectorsOutput { vectors: vectors? },
                    )),
                }
            }
        };
        Ok(vectors)
    }
}

impl From<VectorInternal> for grpc::VectorOutput {
    fn from(vector: VectorInternal) -> Self {
        match vector {
            VectorInternal::Dense(vector) => Self {
                data: vector,
                indices: None,
                vectors_count: None,
                vector: None,
            },
            VectorInternal::Sparse(vector) => Self {
                data: vector.values,
                indices: Some(grpc::SparseIndices {
                    data: vector.indices,
                }),
                vectors_count: None,
                vector: None,
            },
            VectorInternal::MultiDense(vector) => {
                let vector_count = vector.multi_vectors().count() as u32;
                Self {
                    data: vector.flattened_vectors,
                    indices: None,
                    vectors_count: Some(vector_count),
                    vector: None,
                }
            }
        }
    }
}

impl From<VectorStructInternal> for grpc::VectorsOutput {
    fn from(vector_struct: VectorStructInternal) -> Self {
        match vector_struct {
            VectorStructInternal::Single(vector) => {
                let vector = VectorInternal::from(vector);
                Self {
                    vectors_options: Some(grpc::vectors_output::VectorsOptions::Vector(
                        grpc::VectorOutput::from(vector),
                    )),
                }
            }
            VectorStructInternal::MultiDense(vector) => {
                let vector = VectorInternal::from(vector);
                Self {
                    vectors_options: Some(grpc::vectors_output::VectorsOptions::Vector(
                        grpc::VectorOutput::from(vector),
                    )),
                }
            }
            VectorStructInternal::Named(vectors) => Self {
                vectors_options: Some(grpc::vectors_output::VectorsOptions::Vectors(
                    grpc::NamedVectorsOutput {
                        vectors: vectors
                            .into_iter()
                            .map(|(name, vector)| (name, grpc::VectorOutput::from(vector)))
                            .collect(),
                    },
                )),
            },
        }
    }
}

impl TryFrom<grpc::Vectors> for rest::VectorStruct {
    type Error = Status;

    fn try_from(vectors: grpc::Vectors) -> Result<Self, Self::Error> {
        match vectors.vectors_options {
            Some(vectors_options) => Ok(match vectors_options {
                grpc::vectors::VectorsOptions::Vector(vector) => {
                    let grpc::Vector {
                        data,
                        indices,
                        vectors_count,
                        vector,
                    } = vector;

                    if let Some(vector) = vector {
                        return match vector {
                            grpc::vector::Vector::Dense(dense) => {
                                Ok(rest::VectorStruct::Single(dense.data))
                            }
                            grpc::vector::Vector::Sparse(_sparse) => {
                                return Err(Status::invalid_argument(
                                    "Sparse vector must be named".to_string(),
                                ));
                            }
                            grpc::vector::Vector::MultiDense(multi) => {
                                Ok(rest::VectorStruct::MultiDense(
                                    multi.vectors.into_iter().map(|v| v.data).collect(),
                                ))
                            }
                            grpc::vector::Vector::Document(document) => Ok(
                                rest::VectorStruct::Document(rest::Document::try_from(document)?),
                            ),
                            grpc::vector::Vector::Image(image) => {
                                Ok(rest::VectorStruct::Image(rest::Image::try_from(image)?))
                            }
                            grpc::vector::Vector::Object(object) => Ok(rest::VectorStruct::Object(
                                rest::InferenceObject::try_from(object)?,
                            )),
                        };
                    }

                    if indices.is_some() {
                        return Err(Status::invalid_argument(
                            "Sparse vector must be named".to_string(),
                        ));
                    }
                    if let Some(vectors_count) = vectors_count {
                        let multi = convert_to_plain_multi_vector(data, vectors_count as usize)
                            .map_err(|err| {
                                Status::invalid_argument(format!(
                                    "Unable to convert to multi-dense vector: {err}"
                                ))
                            })?;

                        rest::VectorStruct::MultiDense(multi)
                    } else {
                        rest::VectorStruct::Single(data)
                    }
                }
                grpc::vectors::VectorsOptions::Vectors(vectors) => {
                    let named_vectors: Result<_, _> = vectors
                        .vectors
                        .into_iter()
                        .map(|(k, v)| rest::Vector::try_from(v).map(|res| (k, res)))
                        .collect();

                    rest::VectorStruct::Named(named_vectors?)
                }
            }),
            None => Err(Status::invalid_argument("No Vector Provided")),
        }
    }
}

impl TryFrom<grpc::Vector> for rest::Vector {
    type Error = Status;

    fn try_from(vector: grpc::Vector) -> Result<Self, Self::Error> {
        let grpc::Vector {
            data,
            indices,
            vectors_count,
            vector,
        } = vector;

        if let Some(vector) = vector {
            return match vector {
                grpc::vector::Vector::Dense(dense) => Ok(rest::Vector::Dense(dense.data)),
                grpc::vector::Vector::Sparse(sparse) => Ok(rest::Vector::Sparse(
                    sparse::common::sparse_vector::SparseVector::from(sparse),
                )),
                grpc::vector::Vector::MultiDense(multi) => Ok(rest::Vector::MultiDense(
                    multi.vectors.into_iter().map(|v| v.data).collect(),
                )),
                grpc::vector::Vector::Document(document) => {
                    Ok(rest::Vector::Document(rest::Document::try_from(document)?))
                }
                grpc::vector::Vector::Image(image) => {
                    Ok(rest::Vector::Image(rest::Image::try_from(image)?))
                }
                grpc::vector::Vector::Object(object) => Ok(rest::Vector::Object(
                    rest::InferenceObject::try_from(object)?,
                )),
            };
        }

        if let Some(indices) = indices {
            return Ok(rest::Vector::Sparse(
                sparse::common::sparse_vector::SparseVector {
                    values: data,
                    indices: indices.data,
                },
            ));
        }

        if let Some(vectors_count) = vectors_count {
            let multi =
                convert_to_plain_multi_vector(data, vectors_count as usize).map_err(|err| {
                    Status::invalid_argument(format!(
                        "Unable to convert to multi-dense vector: {err}"
                    ))
                })?;
            Ok(rest::Vector::MultiDense(multi))
        } else {
            Ok(rest::Vector::Dense(data))
        }
    }
}

impl grpc::MultiDenseVector {
    pub fn into_matrix(self) -> Vec<Vec<f32>> {
        self.vectors.into_iter().map(|v| v.data).collect()
    }
}

impl TryFrom<grpc::VectorOutput> for VectorInternal {
    type Error = OperationError;

    fn try_from(vector: grpc::VectorOutput) -> Result<Self, Self::Error> {
        let grpc::VectorOutput {
            data,
            indices,
            vectors_count,
            vector,
        } = vector;

        if let Some(vector) = vector {
            return match vector {
                grpc::vector_output::Vector::Dense(dense) => Ok(VectorInternal::Dense(dense.data)),
                grpc::vector_output::Vector::Sparse(sparse) => Ok(VectorInternal::Sparse(
                    sparse::common::sparse_vector::SparseVector::from(sparse),
                )),
                grpc::vector_output::Vector::MultiDense(multi) => Ok(VectorInternal::MultiDense(
                    MultiDenseVectorInternal::try_from_matrix(multi.into_matrix())?,
                )),
            };
        }

        if let Some(indices) = indices {
            return Ok(VectorInternal::Sparse(
                sparse::common::sparse_vector::SparseVector {
                    values: data,
                    indices: indices.data,
                },
            ));
        }

        if let Some(vectors_count) = vectors_count {
            let dim = data.len() / vectors_count as usize;
            let multi = MultiDenseVectorInternal::try_from_flatten(data, dim)?;
            Ok(VectorInternal::MultiDense(multi))
        } else {
            Ok(VectorInternal::Dense(data))
        }
    }
}

impl TryFrom<grpc::VectorsOutput> for VectorStructInternal {
    type Error = OperationError;
    fn try_from(vectors_output: grpc::VectorsOutput) -> Result<Self, Self::Error> {
        match vectors_output.vectors_options {
            Some(vectors_options) => Ok(match vectors_options {
                grpc::vectors_output::VectorsOptions::Vector(vector) => {
                    let grpc::VectorOutput {
                        data,
                        indices,
                        vectors_count,
                        vector,
                    } = vector;

                    if let Some(vector) = vector {
                        return match vector {
                            grpc::vector_output::Vector::Dense(dense) => {
                                Ok(VectorStructInternal::Single(dense.data))
                            }
                            grpc::vector_output::Vector::Sparse(_sparse) => {
                                return Err(OperationError::ValidationError {
                                    description: "Sparse vector must be named".to_string(),
                                });
                            }
                            grpc::vector_output::Vector::MultiDense(multi) => {
                                Ok(VectorStructInternal::MultiDense(
                                    MultiDenseVectorInternal::try_from_matrix(multi.into_matrix())?,
                                ))
                            }
                        };
                    }

                    if indices.is_some() {
                        return Err(OperationError::ValidationError {
                            description: "Sparse vector must be named".to_string(),
                        });
                    }

                    if let Some(vectors_count) = vectors_count {
                        let dim = data.len() / vectors_count as usize;
                        let multi = MultiDenseVectorInternal::try_from_flatten(data, dim)?;
                        VectorStructInternal::MultiDense(multi)
                    } else {
                        VectorStructInternal::Single(data)
                    }
                }
                grpc::vectors_output::VectorsOptions::Vectors(vectors) => {
                    let named_vectors: Result<_, _> = vectors
                        .vectors
                        .into_iter()
                        .map(|(k, v)| VectorInternal::try_from(v).map(|res| (k, res)))
                        .collect();

                    VectorStructInternal::Named(named_vectors?)
                }
            }),
            None => Err(OperationError::ValidationError {
                description: "No Vector Provided".to_string(),
            }),
        }
    }
}

impl From<VectorInternal> for grpc::Vector {
    fn from(vector: VectorInternal) -> Self {
        match vector {
            VectorInternal::Dense(vector) => Self {
                data: vector,
                indices: None,
                vectors_count: None,
                vector: None,
            },
            VectorInternal::Sparse(vector) => Self {
                data: vector.values,
                indices: Some(grpc::SparseIndices {
                    data: vector.indices,
                }),
                vectors_count: None,
                vector: None,
            },
            VectorInternal::MultiDense(vector) => {
                let vector_count = vector.multi_vectors().count() as u32;
                Self {
                    data: vector.flattened_vectors,
                    indices: None,
                    vectors_count: Some(vector_count),
                    vector: None,
                }
            }
        }
    }
}

impl From<VectorStructInternal> for grpc::Vectors {
    fn from(vector_struct: VectorStructInternal) -> Self {
        match vector_struct {
            VectorStructInternal::Single(vector) => {
                let vector = VectorInternal::from(vector);
                Self {
                    vectors_options: Some(grpc::vectors::VectorsOptions::Vector(
                        grpc::Vector::from(vector),
                    )),
                }
            }
            VectorStructInternal::MultiDense(vector) => {
                let vector = VectorInternal::from(vector);
                Self {
                    vectors_options: Some(grpc::vectors::VectorsOptions::Vector(
                        grpc::Vector::from(vector),
                    )),
                }
            }
            VectorStructInternal::Named(vectors) => Self {
                vectors_options: Some(grpc::vectors::VectorsOptions::Vectors(grpc::NamedVectors {
                    vectors: vectors
                        .into_iter()
                        .map(|(name, vector)| (name, grpc::Vector::from(vector)))
                        .collect(),
                })),
            },
        }
    }
}

impl TryFrom<grpc::Vector> for VectorInternal {
    type Error = Status;

    fn try_from(vector: grpc::Vector) -> Result<Self, Self::Error> {
        // sparse vector
        if let Some(indices) = vector.indices {
            return Ok(VectorInternal::Sparse(
                SparseVector::new(indices.data, vector.data).map_err(|e| {
                    Status::invalid_argument(format!(
                        "Sparse indices does not match sparse vector conditions: {e}"
                    ))
                })?,
            ));
        }

        // multi vector
        if let Some(vector_count) = vector.vectors_count {
            if vector_count == 0 {
                return Err(Status::invalid_argument(
                    "Vector count should be greater than 0",
                ));
            }
            let dim = vector.data.len() / vector_count as usize;
            let multi = MultiDenseVectorInternal::new(vector.data, dim);
            return Ok(VectorInternal::MultiDense(multi));
        }

        // dense vector
        Ok(VectorInternal::Dense(vector.data))
    }
}

impl From<grpc::DenseVector> for DenseVector {
    fn from(value: grpc::DenseVector) -> Self {
        value.data
    }
}

impl From<DenseVector> for grpc::DenseVector {
    fn from(value: DenseVector) -> Self {
        Self { data: value }
    }
}

impl From<SparseVector> for grpc::SparseVector {
    fn from(value: SparseVector) -> Self {
        let SparseVector { indices, values } = value;

        Self { values, indices }
    }
}

impl From<grpc::SparseVector> for SparseVector {
    fn from(value: grpc::SparseVector) -> Self {
        let grpc::SparseVector { indices, values } = value;

        Self { indices, values }
    }
}

impl From<MultiDenseVectorInternal> for grpc::MultiDenseVector {
    fn from(value: MultiDenseVectorInternal) -> Self {
        let vectors = value
            .flattened_vectors
            .into_iter()
            .chunks(value.dim)
            .into_iter()
            .map(Iterator::collect::<Vec<_>>)
            .map(grpc::DenseVector::from)
            .collect();
        Self { vectors }
    }
}

impl From<grpc::MultiDenseVector> for MultiDenseVectorInternal {
    /// Uses the equivalent of [new_unchecked()](segment_vectors::MultiDenseVectorInternal::new_unchecked), but rewritten to avoid collecting twice
    fn from(value: grpc::MultiDenseVector) -> Self {
        let dim = value.vectors[0].data.len();
        let inner_vector = value
            .vectors
            .into_iter()
            .flat_map(DenseVector::from)
            .collect();
        Self {
            flattened_vectors: inner_vector,
            dim,
        }
    }
}

impl From<VectorInternal> for grpc::RawVector {
    fn from(value: VectorInternal) -> Self {
        use crate::grpc::qdrant::raw_vector::Variant;

        let variant = match value {
            VectorInternal::Dense(vector) => Variant::Dense(grpc::DenseVector::from(vector)),
            VectorInternal::Sparse(vector) => Variant::Sparse(grpc::SparseVector::from(vector)),
            VectorInternal::MultiDense(vector) => {
                Variant::MultiDense(grpc::MultiDenseVector::from(vector))
            }
        };

        Self {
            variant: Some(variant),
        }
    }
}

impl TryFrom<grpc::RawVector> for VectorInternal {
    type Error = Status;

    fn try_from(value: grpc::RawVector) -> Result<Self, Self::Error> {
        use crate::grpc::qdrant::raw_vector::Variant;

        let variant = value
            .variant
            .ok_or_else(|| Status::invalid_argument("No vector variant provided"))?;

        let vector = match variant {
            Variant::Dense(dense) => VectorInternal::Dense(DenseVector::from(dense)),
            Variant::Sparse(sparse) => {
                VectorInternal::Sparse(sparse::common::sparse_vector::SparseVector::from(sparse))
            }
            Variant::MultiDense(multi_dense) => {
                VectorInternal::MultiDense(MultiDenseVectorInternal::from(multi_dense))
            }
        };

        Ok(vector)
    }
}

impl From<NamedVectorStruct> for grpc::RawVector {
    fn from(value: NamedVectorStruct) -> Self {
        Self::from(value.to_vector())
    }
}
