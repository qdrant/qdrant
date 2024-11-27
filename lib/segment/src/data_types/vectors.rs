use std::collections::HashMap;
use std::slice::ChunksExactMut;

use half::f16;
use itertools::Itertools;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::SparseVector;
use validator::Validate;

use super::named_vectors::NamedVectors;
use super::primitive::PrimitiveVectorElement;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::transpose_map_into_named_vector;
use crate::vector_storage::query::{ContextQuery, DiscoveryQuery, RecoQuery, TransformInto};

#[derive(Clone, Debug, PartialEq)]
pub enum VectorInternal {
    Dense(DenseVector),
    Sparse(SparseVector),
    MultiDense(MultiDenseVectorInternal),
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum VectorRef<'a> {
    Dense(&'a [VectorElementType]),
    Sparse(&'a SparseVector),
    MultiDense(TypedMultiDenseVectorRef<'a, VectorElementType>),
}

impl<'a> TryFrom<VectorRef<'a>> for &'a [VectorElementType] {
    type Error = OperationError;

    fn try_from(value: VectorRef<'a>) -> Result<Self, Self::Error> {
        match value {
            VectorRef::Dense(v) => Ok(v),
            VectorRef::Sparse(_) => Err(OperationError::WrongSparse),
            VectorRef::MultiDense(_) => Err(OperationError::WrongMulti),
        }
    }
}

impl<'a> TryFrom<VectorRef<'a>> for &'a SparseVector {
    type Error = OperationError;

    fn try_from(value: VectorRef<'a>) -> Result<Self, Self::Error> {
        match value {
            VectorRef::Dense(_) => Err(OperationError::WrongSparse),
            VectorRef::Sparse(v) => Ok(v),
            VectorRef::MultiDense(_) => Err(OperationError::WrongMulti),
        }
    }
}

impl<'a> TryFrom<VectorRef<'a>> for TypedMultiDenseVectorRef<'a, f32> {
    type Error = OperationError;

    fn try_from(value: VectorRef<'a>) -> Result<Self, Self::Error> {
        match value {
            VectorRef::Dense(d) => Ok(TypedMultiDenseVectorRef {
                flattened_vectors: d,
                dim: d.len(),
            }),
            VectorRef::Sparse(_v) => Err(OperationError::WrongSparse),
            VectorRef::MultiDense(v) => Ok(v),
        }
    }
}

impl From<NamedVectorStruct> for VectorInternal {
    fn from(value: NamedVectorStruct) -> Self {
        match value {
            NamedVectorStruct::Default(v) => VectorInternal::Dense(v),
            NamedVectorStruct::Dense(v) => VectorInternal::Dense(v.vector),
            NamedVectorStruct::Sparse(v) => VectorInternal::Sparse(v.vector),
            NamedVectorStruct::MultiDense(v) => VectorInternal::MultiDense(v.vector),
        }
    }
}

impl TryFrom<VectorInternal> for DenseVector {
    type Error = OperationError;

    fn try_from(value: VectorInternal) -> Result<Self, Self::Error> {
        match value {
            VectorInternal::Dense(v) => Ok(v),
            VectorInternal::Sparse(_) => Err(OperationError::WrongSparse),
            VectorInternal::MultiDense(_) => Err(OperationError::WrongMulti),
        }
    }
}

impl TryFrom<VectorInternal> for SparseVector {
    type Error = OperationError;

    fn try_from(value: VectorInternal) -> Result<Self, Self::Error> {
        match value {
            VectorInternal::Dense(_) => Err(OperationError::WrongSparse),
            VectorInternal::Sparse(v) => Ok(v),
            VectorInternal::MultiDense(_) => Err(OperationError::WrongMulti),
        }
    }
}

impl TryFrom<VectorInternal> for MultiDenseVectorInternal {
    type Error = OperationError;

    fn try_from(value: VectorInternal) -> Result<Self, Self::Error> {
        match value {
            VectorInternal::Dense(v) => {
                // expand single dense vector into multivector with a single vector
                let len = v.len();
                Ok(MultiDenseVectorInternal::new(v, len))
            }
            VectorInternal::Sparse(_) => Err(OperationError::WrongSparse),
            VectorInternal::MultiDense(v) => Ok(v),
        }
    }
}

impl<'a> From<&'a [VectorElementType]> for VectorRef<'a> {
    fn from(val: &'a [VectorElementType]) -> Self {
        VectorRef::Dense(val)
    }
}

impl<'a> From<&'a DenseVector> for VectorRef<'a> {
    fn from(val: &'a DenseVector) -> Self {
        VectorRef::Dense(val.as_slice())
    }
}

impl<'a> From<&'a MultiDenseVectorInternal> for VectorRef<'a> {
    fn from(val: &'a MultiDenseVectorInternal) -> Self {
        VectorRef::MultiDense(TypedMultiDenseVectorRef::from(val))
    }
}

impl<'a> From<TypedMultiDenseVectorRef<'a, VectorElementType>> for VectorRef<'a> {
    fn from(val: TypedMultiDenseVectorRef<'a, VectorElementType>) -> Self {
        VectorRef::MultiDense(val)
    }
}

impl<'a> From<&'a SparseVector> for VectorRef<'a> {
    fn from(val: &'a SparseVector) -> Self {
        VectorRef::Sparse(val)
    }
}

impl From<DenseVector> for VectorInternal {
    fn from(val: DenseVector) -> Self {
        VectorInternal::Dense(val)
    }
}

impl From<SparseVector> for VectorInternal {
    fn from(val: SparseVector) -> Self {
        VectorInternal::Sparse(val)
    }
}

impl From<MultiDenseVectorInternal> for VectorInternal {
    fn from(val: MultiDenseVectorInternal) -> Self {
        VectorInternal::MultiDense(val)
    }
}

impl<'a> From<&'a VectorInternal> for VectorRef<'a> {
    fn from(val: &'a VectorInternal) -> Self {
        match val {
            VectorInternal::Dense(v) => VectorRef::Dense(v.as_slice()),
            VectorInternal::Sparse(v) => VectorRef::Sparse(v),
            VectorInternal::MultiDense(v) => {
                VectorRef::MultiDense(TypedMultiDenseVectorRef::from(v))
            }
        }
    }
}

/// Type of vector element.
pub type VectorElementType = f32;

pub type VectorElementTypeHalf = f16;

pub type VectorElementTypeByte = u8;

pub const DEFAULT_VECTOR_NAME: &str = "";

pub type TypedDenseVector<T> = Vec<T>;

/// Type for dense vector
pub type DenseVector = TypedDenseVector<VectorElementType>;

/// Type for multi dense vector
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct TypedMultiDenseVector<T> {
    pub flattened_vectors: TypedDenseVector<T>, // vectors are flattened into a single vector
    pub dim: usize,                             // dimension of each vector
}

impl<T> TypedMultiDenseVector<T> {
    pub fn try_from_flatten(vectors: Vec<T>, dim: usize) -> Result<Self, OperationError> {
        if vectors.len() % dim != 0 || vectors.is_empty() {
            return Err(OperationError::ValidationError {
                description: format!(
                    "Invalid multi-vector length: {}, expected multiple of {}",
                    vectors.len(),
                    dim
                ),
            });
        }

        Ok(TypedMultiDenseVector {
            flattened_vectors: vectors,
            dim,
        })
    }

    pub fn try_from_matrix(matrix: Vec<Vec<T>>) -> Result<Self, OperationError> {
        if matrix.is_empty() {
            return Err(OperationError::ValidationError {
                description: "MultiDenseVector cannot be empty".to_string(),
            });
        }
        let dim = matrix[0].len();
        // assert all vectors have the same dimension
        if let Some(bad_vec) = matrix.iter().find(|v| v.len() != dim) {
            return Err(OperationError::WrongVectorDimension {
                expected_dim: dim,
                received_dim: bad_vec.len(),
            });
        }

        let flattened_vectors = matrix.into_iter().flatten().collect_vec();
        let multi_dense = TypedMultiDenseVector {
            flattened_vectors,
            dim,
        };

        Ok(multi_dense)
    }
}

pub type MultiDenseVectorInternal = TypedMultiDenseVector<VectorElementType>;

impl<T: PrimitiveVectorElement> TypedMultiDenseVector<T> {
    pub fn new(flattened_vectors: TypedDenseVector<T>, dim: usize) -> Self {
        debug_assert_eq!(flattened_vectors.len() % dim, 0, "Invalid vector length");
        Self {
            flattened_vectors,
            dim,
        }
    }

    /// To be used when the input vectors are already validated to avoid double validation
    pub fn new_unchecked(vectors: Vec<Vec<T>>) -> Self {
        debug_assert!(!vectors.is_empty(), "MultiDenseVector cannot be empty");
        debug_assert!(
            vectors.iter().all(|v| !v.is_empty()),
            "Multi individual vectors cannot be empty"
        );
        let dim = vectors[0].len();
        let inner_vector = vectors.into_iter().flatten().collect();
        Self {
            flattened_vectors: inner_vector,
            dim,
        }
    }

    /// MultiDenseVector cannot be empty, so we use a placeholder vector instead
    pub fn placeholder(dim: usize) -> Self {
        Self {
            flattened_vectors: vec![Default::default(); dim],
            dim,
        }
    }

    /// Slices the multi vector into the underlying individual vectors
    pub fn multi_vectors(&self) -> impl Iterator<Item = &[T]> {
        self.flattened_vectors.chunks_exact(self.dim)
    }

    pub fn multi_vectors_mut(&mut self) -> ChunksExactMut<'_, T> {
        self.flattened_vectors.chunks_exact_mut(self.dim)
    }

    /// Consumes the multi vector and returns the underlying individual vectors
    pub fn into_multi_vectors(self) -> Vec<Vec<T>> {
        self.flattened_vectors
            .into_iter()
            .chunks(self.dim)
            .into_iter()
            .map(Iterator::collect)
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.flattened_vectors.is_empty()
    }

    pub fn vectors_count(&self) -> usize {
        self.flattened_vectors.len() / self.dim
    }
}

impl<T: PrimitiveVectorElement> TryFrom<Vec<TypedDenseVector<T>>> for TypedMultiDenseVector<T> {
    type Error = OperationError;

    fn try_from(value: Vec<TypedDenseVector<T>>) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(OperationError::ValidationError {
                description: "MultiDenseVector cannot be empty".to_string(),
            });
        }
        let dim = value[0].len();
        // assert all vectors have the same dimension
        if let Some(bad_vec) = value.iter().find(|v| v.len() != dim) {
            Err(OperationError::WrongVectorDimension {
                expected_dim: dim,
                received_dim: bad_vec.len(),
            })
        } else {
            let flattened_vectors = value.into_iter().flatten().collect_vec();
            let multi_dense = TypedMultiDenseVector {
                flattened_vectors,
                dim,
            };
            Ok(multi_dense)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TypedMultiDenseVectorRef<'a, T> {
    pub flattened_vectors: &'a [T],
    pub dim: usize,
}

impl<'a, T: PrimitiveVectorElement> TypedMultiDenseVectorRef<'a, T> {
    /// Slices the multi vector into the underlying individual vectors
    pub fn multi_vectors(self) -> impl Iterator<Item = &'a [T]> {
        self.flattened_vectors.chunks_exact(self.dim)
    }

    pub fn is_empty(self) -> bool {
        self.flattened_vectors.is_empty()
    }

    pub fn vectors_count(self) -> usize {
        self.flattened_vectors.len() / self.dim
    }

    // Cannot use `ToOwned` trait because of `Borrow` implementation for `TypedMultiDenseVector`
    pub fn to_owned(self) -> TypedMultiDenseVector<T> {
        TypedMultiDenseVector {
            flattened_vectors: self.flattened_vectors.to_owned(),
            dim: self.dim,
        }
    }
}

impl<'a, T: PrimitiveVectorElement> From<&'a TypedMultiDenseVector<T>>
    for TypedMultiDenseVectorRef<'a, T>
{
    fn from(val: &'a TypedMultiDenseVector<T>) -> Self {
        TypedMultiDenseVectorRef {
            flattened_vectors: &val.flattened_vectors,
            dim: val.dim,
        }
    }
}

impl TryFrom<Vec<DenseVector>> for VectorInternal {
    type Error = OperationError;

    fn try_from(value: Vec<DenseVector>) -> Result<Self, Self::Error> {
        MultiDenseVectorInternal::try_from(value).map(VectorInternal::MultiDense)
    }
}

impl VectorRef<'_> {
    // Cannot use `ToOwned` trait because of `Borrow` implementation for `Vector`
    pub fn to_owned(self) -> VectorInternal {
        match self {
            VectorRef::Dense(v) => VectorInternal::Dense(v.to_vec()),
            VectorRef::Sparse(v) => VectorInternal::Sparse(v.clone()),
            VectorRef::MultiDense(v) => VectorInternal::MultiDense(v.to_owned()),
        }
    }
}

impl<'a> TryInto<&'a [VectorElementType]> for &'a VectorInternal {
    type Error = OperationError;

    fn try_into(self) -> Result<&'a [VectorElementType], Self::Error> {
        match self {
            VectorInternal::Dense(v) => Ok(v),
            VectorInternal::Sparse(_) => Err(OperationError::WrongSparse),
            VectorInternal::MultiDense(_) => Err(OperationError::WrongMulti),
        }
    }
}

impl<'a> TryInto<&'a SparseVector> for &'a VectorInternal {
    type Error = OperationError;

    fn try_into(self) -> Result<&'a SparseVector, Self::Error> {
        match self {
            VectorInternal::Dense(_) => Err(OperationError::WrongSparse),
            VectorInternal::Sparse(v) => Ok(v),
            VectorInternal::MultiDense(_) => Err(OperationError::WrongMulti),
        }
    }
}

impl<'a> TryInto<&'a MultiDenseVectorInternal> for &'a VectorInternal {
    type Error = OperationError;

    fn try_into(self) -> Result<&'a MultiDenseVectorInternal, Self::Error> {
        match self {
            VectorInternal::Dense(_) => Err(OperationError::WrongMulti), // &Dense vector cannot be converted to &MultiDense
            VectorInternal::Sparse(_) => Err(OperationError::WrongSparse),
            VectorInternal::MultiDense(v) => Ok(v),
        }
    }
}

pub fn default_vector(vec: DenseVector) -> NamedVectors<'static> {
    NamedVectors::from_pairs([(DEFAULT_VECTOR_NAME.to_owned(), vec)])
}

pub fn default_multi_vector(vec: MultiDenseVectorInternal) -> NamedVectors<'static> {
    let mut named_vectors = NamedVectors::default();
    named_vectors.insert(
        DEFAULT_VECTOR_NAME.to_owned(),
        VectorInternal::MultiDense(vec),
    );
    named_vectors
}

pub fn only_default_vector(vec: &[VectorElementType]) -> NamedVectors {
    NamedVectors::from_ref(DEFAULT_VECTOR_NAME, VectorRef::from(vec))
}

pub fn only_default_multi_vector(vec: &MultiDenseVectorInternal) -> NamedVectors {
    NamedVectors::from_ref(
        DEFAULT_VECTOR_NAME,
        VectorRef::MultiDense(TypedMultiDenseVectorRef::from(vec)),
    )
}

/// Full vector data per point separator with single and multiple vector modes
#[derive(Clone, Debug, PartialEq)]
pub enum VectorStructInternal {
    Single(DenseVector),
    MultiDense(MultiDenseVectorInternal),
    Named(HashMap<String, VectorInternal>),
}

impl From<DenseVector> for VectorStructInternal {
    fn from(v: DenseVector) -> Self {
        VectorStructInternal::Single(v)
    }
}

impl From<&[VectorElementType]> for VectorStructInternal {
    fn from(v: &[VectorElementType]) -> Self {
        VectorStructInternal::Single(v.to_vec())
    }
}

impl From<NamedVectors<'_>> for VectorStructInternal {
    fn from(v: NamedVectors) -> Self {
        if v.len() == 1 && v.contains_key(DEFAULT_VECTOR_NAME) {
            let vector_ref = v.get(DEFAULT_VECTOR_NAME).unwrap();

            match vector_ref {
                VectorRef::Dense(v) => VectorStructInternal::Single(v.to_owned()),
                VectorRef::Sparse(v) => {
                    debug_assert!(false, "Sparse vector cannot be default");
                    let mut map = HashMap::new();
                    map.insert(
                        DEFAULT_VECTOR_NAME.to_owned(),
                        VectorInternal::Sparse(v.to_owned()),
                    );
                    VectorStructInternal::Named(map)
                }
                VectorRef::MultiDense(v) => VectorStructInternal::MultiDense(v.to_owned()),
            }
        } else {
            VectorStructInternal::Named(v.into_owned_map())
        }
    }
}

impl VectorStructInternal {
    pub fn get(&self, name: &str) -> Option<VectorRef> {
        match self {
            VectorStructInternal::Single(v) => {
                (name == DEFAULT_VECTOR_NAME).then_some(VectorRef::from(v))
            }
            VectorStructInternal::MultiDense(v) => {
                (name == DEFAULT_VECTOR_NAME).then_some(VectorRef::from(v))
            }
            VectorStructInternal::Named(v) => v.get(name).map(VectorRef::from),
        }
    }
}

/// Dense vector data with name
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct NamedVector {
    /// Name of vector data
    pub name: String,
    /// Vector data
    pub vector: DenseVector,
}

/// MultiDense vector data with name
#[derive(Debug, Clone, PartialEq)]
pub struct NamedMultiDenseVector {
    /// Name of vector data
    pub name: String,
    /// Vector data
    pub vector: MultiDenseVectorInternal,
}

/// Sparse vector data with name
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Validate, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct NamedSparseVector {
    /// Name of vector data
    pub name: String,
    /// Vector data
    #[validate(nested)]
    pub vector: SparseVector,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NamedVectorStruct {
    Default(DenseVector),
    Dense(NamedVector),
    Sparse(NamedSparseVector),
    MultiDense(NamedMultiDenseVector),
}

impl From<DenseVector> for NamedVectorStruct {
    fn from(v: DenseVector) -> Self {
        NamedVectorStruct::Default(v)
    }
}

impl From<NamedVector> for NamedVectorStruct {
    fn from(v: NamedVector) -> Self {
        NamedVectorStruct::Dense(v)
    }
}

impl From<NamedSparseVector> for NamedVectorStruct {
    fn from(v: NamedSparseVector) -> Self {
        NamedVectorStruct::Sparse(v)
    }
}

impl From<NamedMultiDenseVector> for NamedVectorStruct {
    fn from(v: NamedMultiDenseVector) -> Self {
        NamedVectorStruct::MultiDense(v)
    }
}

pub trait Named {
    fn get_name(&self) -> &str;
}

impl Named for NamedVectorStruct {
    fn get_name(&self) -> &str {
        match self {
            NamedVectorStruct::Default(_) => DEFAULT_VECTOR_NAME,
            NamedVectorStruct::Dense(v) => &v.name,
            NamedVectorStruct::Sparse(v) => &v.name,
            NamedVectorStruct::MultiDense(v) => &v.name,
        }
    }
}

impl NamedVectorStruct {
    pub fn new_from_vector(vector: VectorInternal, name: impl Into<String>) -> Self {
        let name = name.into();
        match vector {
            VectorInternal::Dense(vector) => NamedVectorStruct::Dense(NamedVector { name, vector }),
            VectorInternal::Sparse(vector) => {
                NamedVectorStruct::Sparse(NamedSparseVector { name, vector })
            }
            VectorInternal::MultiDense(vector) => {
                NamedVectorStruct::MultiDense(NamedMultiDenseVector { name, vector })
            }
        }
    }

    pub fn get_vector(&self) -> VectorRef {
        match self {
            NamedVectorStruct::Default(v) => v.as_slice().into(),
            NamedVectorStruct::Dense(v) => v.vector.as_slice().into(),
            NamedVectorStruct::Sparse(v) => (&v.vector).into(),
            NamedVectorStruct::MultiDense(v) => (&v.vector).into(),
        }
    }

    pub fn to_vector(self) -> VectorInternal {
        match self {
            NamedVectorStruct::Default(v) => v.into(),
            NamedVectorStruct::Dense(v) => v.vector.into(),
            NamedVectorStruct::Sparse(v) => v.vector.into(),
            NamedVectorStruct::MultiDense(v) => v.vector.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum BatchVectorStructInternal {
    Single(Vec<DenseVector>),
    MultiDense(Vec<MultiDenseVectorInternal>),
    Named(HashMap<String, Vec<VectorInternal>>),
}

impl From<Vec<DenseVector>> for BatchVectorStructInternal {
    fn from(v: Vec<DenseVector>) -> Self {
        BatchVectorStructInternal::Single(v)
    }
}

impl BatchVectorStructInternal {
    pub fn into_all_vectors(self, num_records: usize) -> Vec<NamedVectors<'static>> {
        match self {
            BatchVectorStructInternal::Single(vectors) => {
                vectors.into_iter().map(default_vector).collect()
            }
            BatchVectorStructInternal::MultiDense(vectors) => {
                vectors.into_iter().map(default_multi_vector).collect()
            }
            BatchVectorStructInternal::Named(named_vectors) => {
                if named_vectors.is_empty() {
                    vec![NamedVectors::default(); num_records]
                } else {
                    transpose_map_into_named_vector(named_vectors)
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamedQuery<TQuery> {
    pub query: TQuery,
    pub using: Option<String>,
}

impl<T> Named for NamedQuery<T> {
    fn get_name(&self) -> &str {
        self.using.as_deref().unwrap_or(DEFAULT_VECTOR_NAME)
    }
}

impl<T: Validate> Validate for NamedQuery<T> {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        self.query.validate()
    }
}

impl NamedQuery<RecoQuery<VectorInternal>> {
    pub fn new(query: RecoQuery<VectorInternal>, using: Option<String>) -> Self {
        // TODO: maybe validate there is no sparse vector without vector name
        NamedQuery { query, using }
    }
}

#[derive(Debug, Clone)]
pub enum QueryVector {
    Nearest(VectorInternal),
    Recommend(RecoQuery<VectorInternal>),
    Discovery(DiscoveryQuery<VectorInternal>),
    Context(ContextQuery<VectorInternal>),
}

impl TransformInto<QueryVector, VectorInternal, VectorInternal> for QueryVector {
    fn transform<F>(self, mut f: F) -> OperationResult<QueryVector>
    where
        F: FnMut(VectorInternal) -> OperationResult<VectorInternal>,
    {
        match self {
            QueryVector::Nearest(v) => f(v).map(QueryVector::Nearest),
            QueryVector::Recommend(v) => Ok(QueryVector::Recommend(v.transform(&mut f)?)),
            QueryVector::Discovery(v) => Ok(QueryVector::Discovery(v.transform(&mut f)?)),
            QueryVector::Context(v) => Ok(QueryVector::Context(v.transform(&mut f)?)),
        }
    }
}

impl From<DenseVector> for QueryVector {
    fn from(vec: DenseVector) -> Self {
        Self::Nearest(VectorInternal::Dense(vec))
    }
}

impl<'a> From<&'a [VectorElementType]> for QueryVector {
    fn from(vec: &'a [VectorElementType]) -> Self {
        Self::Nearest(VectorInternal::Dense(vec.to_vec()))
    }
}

impl<'a> From<&'a MultiDenseVectorInternal> for QueryVector {
    fn from(vec: &'a MultiDenseVectorInternal) -> Self {
        Self::Nearest(VectorInternal::MultiDense(vec.clone()))
    }
}

impl<const N: usize> From<[VectorElementType; N]> for QueryVector {
    fn from(vec: [VectorElementType; N]) -> Self {
        let vec: VectorRef = vec.as_slice().into();
        Self::Nearest(vec.to_owned())
    }
}

impl<'a> From<VectorRef<'a>> for QueryVector {
    fn from(vec: VectorRef<'a>) -> Self {
        Self::Nearest(vec.to_owned())
    }
}

impl From<VectorInternal> for QueryVector {
    fn from(vec: VectorInternal) -> Self {
        Self::Nearest(vec)
    }
}

impl From<SparseVector> for QueryVector {
    fn from(vec: SparseVector) -> Self {
        Self::Nearest(VectorInternal::Sparse(vec))
    }
}

impl From<MultiDenseVectorInternal> for QueryVector {
    fn from(vec: MultiDenseVectorInternal) -> Self {
        Self::Nearest(VectorInternal::MultiDense(vec))
    }
}
