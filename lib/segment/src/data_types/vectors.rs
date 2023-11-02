use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::SparseVector;

use super::named_vectors::NamedVectors;
use crate::common::operation_error::OperationError;
use crate::common::utils::transpose_map_into_named_vector;
use crate::vector_storage::query::context_query::ContextQuery;
use crate::vector_storage::query::discovery_query::DiscoveryQuery;
use crate::vector_storage::query::reco_query::RecoQuery;

#[derive(Debug, PartialEq, Clone)]
pub enum Vector {
    Dense(VectorType),
    Sparse(SparseVector),
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum VectorRef<'a> {
    Dense(&'a [VectorElementType]),
    Sparse(&'a SparseVector),
}

impl Vector {
    pub fn to_vec_ref(&self) -> VectorRef {
        match self {
            Vector::Dense(v) => VectorRef::Dense(v.as_slice()),
            Vector::Sparse(v) => VectorRef::Sparse(v),
        }
    }
}

impl<'a> VectorRef<'a> {
    pub fn to_vec(self) -> Vector {
        match self {
            VectorRef::Dense(v) => Vector::Dense(v.to_vec()),
            VectorRef::Sparse(v) => Vector::Sparse(v.clone()),
        }
    }
}

impl<'a> TryFrom<VectorRef<'a>> for &'a [VectorElementType] {
    type Error = OperationError;

    fn try_from(value: VectorRef<'a>) -> Result<Self, Self::Error> {
        match value {
            VectorRef::Dense(v) => Ok(v),
            VectorRef::Sparse(_) => Err(OperationError::WrongSparse),
        }
    }
}

impl<'a> TryFrom<VectorRef<'a>> for &'a SparseVector {
    type Error = OperationError;

    fn try_from(value: VectorRef<'a>) -> Result<Self, Self::Error> {
        match value {
            VectorRef::Dense(_) => Err(OperationError::WrongSparse),
            VectorRef::Sparse(v) => Ok(v),
        }
    }
}

impl From<NamedVectorStruct> for Vector {
    fn from(value: NamedVectorStruct) -> Self {
        match value {
            NamedVectorStruct::Default(v) => Vector::Dense(v),
            NamedVectorStruct::Named(v) => Vector::Dense(v.vector),
        }
    }
}

impl From<Vector> for VectorType {
    fn from(value: Vector) -> Self {
        match value {
            Vector::Dense(v) => v,
            Vector::Sparse(_) => panic!("Can't convert sparse vector to dense"), // TODO(sparse)
        }
    }
}

impl TryFrom<Vector> for SparseVector {
    type Error = OperationError;

    fn try_from(value: Vector) -> Result<Self, Self::Error> {
        match value {
            Vector::Dense(_) => Err(OperationError::WrongSparse),
            Vector::Sparse(v) => Ok(v),
        }
    }
}

impl<'a> From<&'a [VectorElementType]> for VectorRef<'a> {
    fn from(val: &'a [VectorElementType]) -> Self {
        VectorRef::Dense(val)
    }
}

impl<'a> From<&'a VectorType> for VectorRef<'a> {
    fn from(val: &'a VectorType) -> Self {
        VectorRef::Dense(val.as_slice())
    }
}

impl<'a> From<&'a SparseVector> for VectorRef<'a> {
    fn from(val: &'a SparseVector) -> Self {
        VectorRef::Sparse(val)
    }
}

impl From<VectorType> for Vector {
    fn from(val: VectorType) -> Self {
        Vector::Dense(val)
    }
}

impl From<SparseVector> for Vector {
    fn from(val: SparseVector) -> Self {
        Vector::Sparse(val)
    }
}

impl<'a> From<&'a Vector> for VectorRef<'a> {
    fn from(val: &'a Vector) -> Self {
        match val {
            Vector::Dense(v) => VectorRef::Dense(v.as_slice()),
            Vector::Sparse(v) => VectorRef::Sparse(v),
        }
    }
}

/// Type of vector element.
pub type VectorElementType = f32;

pub const DEFAULT_VECTOR_NAME: &str = "";

/// Type for vector
pub type VectorType = Vec<VectorElementType>;

pub fn default_vector(vec: Vec<VectorElementType>) -> NamedVectors<'static> {
    NamedVectors::from([(DEFAULT_VECTOR_NAME.to_owned(), vec)])
}

pub fn only_default_vector(vec: &[VectorElementType]) -> NamedVectors {
    NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vec)
}

/// Full vector data per point separator with single and multiple vector modes
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorStruct {
    Single(VectorType),
    Multi(HashMap<String, VectorType>),
}

impl VectorStruct {
    /// Check if this vector struct is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            VectorStruct::Single(vector) => vector.is_empty(),
            VectorStruct::Multi(vectors) => vectors.values().all(|v| v.is_empty()),
        }
    }
}

impl From<VectorType> for VectorStruct {
    fn from(v: VectorType) -> Self {
        VectorStruct::Single(v)
    }
}

impl From<&[VectorElementType]> for VectorStruct {
    fn from(v: &[VectorElementType]) -> Self {
        VectorStruct::Single(v.to_vec())
    }
}

impl<'a> From<NamedVectors<'a>> for VectorStruct {
    fn from(v: NamedVectors) -> Self {
        if v.len() == 1 && v.contains_key(DEFAULT_VECTOR_NAME) {
            VectorStruct::Single(v.into_default_vector().unwrap())
        } else {
            VectorStruct::Multi(v.into_owned_map())
        }
    }
}

impl VectorStruct {
    pub fn get(&self, name: &str) -> Option<&VectorType> {
        match self {
            VectorStruct::Single(v) => (name == DEFAULT_VECTOR_NAME).then_some(v),
            VectorStruct::Multi(v) => v.get(name),
        }
    }

    pub fn into_all_vectors(self) -> NamedVectors<'static> {
        match self {
            VectorStruct::Single(v) => default_vector(v),
            VectorStruct::Multi(v) => NamedVectors::from_map(v),
        }
    }
}

/// Vector data with name
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct NamedVector {
    /// Name of vector data
    pub name: String,
    /// Vector data
    pub vector: VectorType,
}

/// Vector data separator for named and unnamed modes
/// Unnamed mode:
///
/// {
///   "vector": [1.0, 2.0, 3.0]
/// }
///
/// or named mode:
///
/// {
///   "vector": {
///     "vector": [1.0, 2.0, 3.0],
///     "name": "image-embeddings"
///   }
/// }
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum NamedVectorStruct {
    Default(VectorType),
    Named(NamedVector),
}

impl From<VectorType> for NamedVectorStruct {
    fn from(v: VectorType) -> Self {
        NamedVectorStruct::Default(v)
    }
}

impl From<NamedVectorStruct> for NamedVector {
    fn from(v: NamedVectorStruct) -> Self {
        match v {
            NamedVectorStruct::Default(v) => NamedVector {
                name: DEFAULT_VECTOR_NAME.to_owned(),
                vector: v,
            },
            NamedVectorStruct::Named(v) => v,
        }
    }
}

impl From<NamedVector> for NamedVectorStruct {
    fn from(v: NamedVector) -> Self {
        NamedVectorStruct::Named(v)
    }
}
pub trait Named {
    fn get_name(&self) -> &str;
}

impl Named for NamedVectorStruct {
    fn get_name(&self) -> &str {
        match self {
            NamedVectorStruct::Default(_) => DEFAULT_VECTOR_NAME,
            NamedVectorStruct::Named(v) => &v.name,
        }
    }
}

impl NamedVectorStruct {
    pub fn get_vector(&self) -> &VectorType {
        match self {
            NamedVectorStruct::Default(v) => v,
            NamedVectorStruct::Named(v) => &v.vector,
        }
    }
    pub fn to_vector(self) -> VectorType {
        match self {
            NamedVectorStruct::Default(v) => v,
            NamedVectorStruct::Named(v) => v.vector,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum BatchVectorStruct {
    Single(Vec<VectorType>),
    Multi(HashMap<String, Vec<VectorType>>),
}

impl From<Vec<VectorType>> for BatchVectorStruct {
    fn from(v: Vec<VectorType>) -> Self {
        BatchVectorStruct::Single(v)
    }
}

impl From<HashMap<String, Vec<VectorType>>> for BatchVectorStruct {
    fn from(v: HashMap<String, Vec<VectorType>>) -> Self {
        if v.len() == 1 && v.contains_key(DEFAULT_VECTOR_NAME) {
            BatchVectorStruct::Single(v.into_iter().next().unwrap().1)
        } else {
            BatchVectorStruct::Multi(v)
        }
    }
}

impl BatchVectorStruct {
    pub fn single(&mut self) -> &mut Vec<VectorType> {
        match self {
            BatchVectorStruct::Single(v) => v,
            BatchVectorStruct::Multi(v) => v.get_mut(DEFAULT_VECTOR_NAME).unwrap(),
        }
    }

    pub fn multi(&mut self) -> &mut HashMap<String, Vec<VectorType>> {
        match self {
            BatchVectorStruct::Single(_) => panic!("BatchVectorStruct is not Single"),
            BatchVectorStruct::Multi(v) => v,
        }
    }

    pub fn into_all_vectors(self, num_records: usize) -> Vec<NamedVectors<'static>> {
        match self {
            BatchVectorStruct::Single(vectors) => vectors.into_iter().map(default_vector).collect(),
            BatchVectorStruct::Multi(named_vectors) => {
                if named_vectors.is_empty() {
                    vec![NamedVectors::default(); num_records]
                } else {
                    transpose_map_into_named_vector(named_vectors)
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct NamedQuery<TQuery> {
    pub query: TQuery,
    pub using: Option<String>,
}

impl<T> Named for NamedQuery<T> {
    fn get_name(&self) -> &str {
        self.using.as_deref().unwrap_or(DEFAULT_VECTOR_NAME)
    }
}

#[derive(Debug, Clone)]
pub enum QueryVector {
    Nearest(Vector),
    Recommend(RecoQuery<Vector>),
    Discovery(DiscoveryQuery<Vector>),
    Context(ContextQuery<Vector>),
}

impl From<VectorType> for QueryVector {
    fn from(vec: VectorType) -> Self {
        Self::Nearest(Vector::Dense(vec))
    }
}

impl<'a> From<&'a [VectorElementType]> for QueryVector {
    fn from(vec: &'a [VectorElementType]) -> Self {
        Self::Nearest(Vector::Dense(vec.to_vec()))
    }
}

impl<const N: usize> From<[VectorElementType; N]> for QueryVector {
    fn from(vec: [VectorElementType; N]) -> Self {
        Self::Nearest(Vector::Dense(vec.to_vec()))
    }
}

impl<'a> From<VectorRef<'a>> for QueryVector {
    fn from(vec: VectorRef<'a>) -> Self {
        Self::Nearest(vec.to_vec())
    }
}

impl From<SparseVector> for QueryVector {
    fn from(vec: SparseVector) -> Self {
        Self::Nearest(Vector::Sparse(vec))
    }
}
