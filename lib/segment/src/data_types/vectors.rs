use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::SparseVector;

use super::named_vectors::NamedVectors;
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

// TODO(ivan) temporary conversion while sparse vectors are under development
impl<'a> From<VectorRef<'a>> for &'a [VectorElementType] {
    fn from(val: VectorRef<'a>) -> Self {
        match val {
            VectorRef::Dense(v) => v,
            VectorRef::Sparse(_) => unreachable!(),
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VectorOrSparseRef<'a> {
    Vector(&'a [VectorElementType]),
    Sparse(&'a SparseVector),
}

impl<'a> VectorOrSparseRef<'a> {
    // Cannot use `ToOwned` trait because of `Borrow` implementation for `VectorOrSparse`
    pub fn to_owned(self) -> VectorOrSparse {
        match self {
            VectorOrSparseRef::Vector(v) => VectorOrSparse::Vector(v.to_vec()),
            VectorOrSparseRef::Sparse(v) => VectorOrSparse::Sparse(v.clone()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            VectorOrSparseRef::Vector(v) => v.len(),
            VectorOrSparseRef::Sparse(v) => v.indices.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a> From<&'a VectorType> for VectorOrSparseRef<'a> {
    fn from(v: &'a VectorType) -> Self {
        VectorOrSparseRef::Vector(v)
    }
}

impl From<VectorType> for VectorOrSparse {
    fn from(v: VectorType) -> Self {
        VectorOrSparse::Vector(v)
    }
}

impl<'a> From<&'a [VectorElementType]> for VectorOrSparseRef<'a> {
    fn from(v: &'a [VectorElementType]) -> Self {
        VectorOrSparseRef::Vector(v)
    }
}

impl From<SparseVector> for VectorOrSparse {
    fn from(v: SparseVector) -> Self {
        VectorOrSparse::Sparse(v)
    }
}

impl<'a> From<&'a SparseVector> for VectorOrSparseRef<'a> {
    fn from(v: &'a SparseVector) -> Self {
        VectorOrSparseRef::Sparse(v)
    }
}

impl<'a> From<&'a VectorOrSparse> for VectorOrSparseRef<'a> {
    fn from(val: &'a VectorOrSparse) -> Self {
        match val {
            VectorOrSparse::Vector(v) => VectorOrSparseRef::Vector(v),
            VectorOrSparse::Sparse(v) => VectorOrSparseRef::Sparse(v),
        }
    }
}

impl<'a> TryInto<&'a [VectorElementType]> for &'a VectorOrSparse {
    type Error = OperationError;

    fn try_into(self) -> Result<&'a [VectorElementType], Self::Error> {
        match self {
            VectorOrSparse::Vector(v) => Ok(v),
            VectorOrSparse::Sparse(_) => Err(OperationError::WrongSparse),
        }
    }
}

impl TryInto<VectorType> for VectorOrSparse {
    type Error = OperationError;

    fn try_into(self) -> Result<VectorType, Self::Error> {
        match self {
            VectorOrSparse::Vector(v) => Ok(v),
            VectorOrSparse::Sparse(_) => Err(OperationError::WrongSparse),
        }
    }
}

impl<'a> TryInto<&'a SparseVector> for &'a VectorOrSparse {
    type Error = OperationError;

    fn try_into(self) -> Result<&'a SparseVector, Self::Error> {
        match self {
            VectorOrSparse::Vector(_) => Err(OperationError::WrongSparse),
            VectorOrSparse::Sparse(v) => Ok(v),
        }
    }
}

impl TryInto<SparseVector> for VectorOrSparse {
    type Error = OperationError;

    fn try_into(self) -> Result<SparseVector, Self::Error> {
        match self {
            VectorOrSparse::Vector(_) => Err(OperationError::WrongSparse),
            VectorOrSparse::Sparse(v) => Ok(v),
        }
    }
}

impl<'a> TryInto<&'a [VectorElementType]> for VectorOrSparseRef<'a> {
    type Error = OperationError;

    fn try_into(self) -> Result<&'a [VectorElementType], Self::Error> {
        match self {
            VectorOrSparseRef::Vector(v) => Ok(v),
            VectorOrSparseRef::Sparse(_) => Err(OperationError::WrongSparse),
        }
    }
}

impl<'a> TryInto<&'a SparseVector> for VectorOrSparseRef<'a> {
    type Error = OperationError;

    fn try_into(self) -> Result<&'a SparseVector, Self::Error> {
        match self {
            VectorOrSparseRef::Vector(_) => Err(OperationError::WrongSparse),
            VectorOrSparseRef::Sparse(v) => Ok(v),
        }
    }
}

pub fn default_vector(vec: Vec<VectorElementType>) -> NamedVectors<'static> {
    NamedVectors::from([(DEFAULT_VECTOR_NAME.to_owned(), vec)])
}

pub fn default_sparse_vector(vec: SparseVector) -> NamedVectors<'static> {
    let mut result = NamedVectors::default();
    result.insert(DEFAULT_VECTOR_NAME.to_owned(), vec.into());
    result
}

pub fn only_default_vector(vec: &[VectorElementType]) -> NamedVectors {
    NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vec.into())
}

pub fn only_default_sparse_vector(vec: &SparseVector) -> NamedVectors {
    NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vec.into())
}

pub fn only_default_mixed_vector(vec: &VectorOrSparse) -> NamedVectors {
    NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vec.into())
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorOrSparse {
    Vector(VectorType),
    Sparse(SparseVector),
}

/// Full vector data per point separator with single and multiple vector modes
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorStruct {
    Single(VectorOrSparse),
    Multi(HashMap<String, VectorOrSparse>),
}

impl VectorStruct {
    /// Check if this vector struct is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            VectorStruct::Single(vector) => match vector {
                VectorOrSparse::Vector(vector) => vector.is_empty(),
                VectorOrSparse::Sparse(vector) => vector.indices.is_empty(),
            },
            VectorStruct::Multi(vectors) => vectors.values().all(|v| match v {
                VectorOrSparse::Vector(vector) => vector.is_empty(),
                VectorOrSparse::Sparse(vector) => vector.indices.is_empty(),
            }),
        }
    }
}

impl From<VectorType> for VectorStruct {
    fn from(v: VectorType) -> Self {
        VectorStruct::Single(v.into())
    }
}

impl From<SparseVector> for VectorStruct {
    fn from(v: SparseVector) -> Self {
        VectorStruct::Single(v.into())
    }
}

impl From<&[VectorElementType]> for VectorStruct {
    fn from(v: &[VectorElementType]) -> Self {
        VectorStruct::Single(v.to_vec().into())
    }
}

impl<'a> From<NamedVectors<'a>> for VectorStruct {
    // TODO(ivan): add conversion for sparse vectors
    fn from(v: NamedVectors) -> Self {
        if v.len() == 1 && v.contains_key(DEFAULT_VECTOR_NAME) {
            VectorStruct::Single(v.into_default_vector().unwrap())
        } else {
            VectorStruct::Multi(v.into_owned_map())
        }
    }
}

impl VectorStruct {
    pub fn get(&self, name: &str) -> Option<VectorOrSparseRef> {
        match self {
            VectorStruct::Single(v) => (name == DEFAULT_VECTOR_NAME).then_some(v.into()),
            VectorStruct::Multi(v) => v.get(name).map(|v| v.into()),
        }
    }

    pub fn into_all_vectors(self) -> NamedVectors<'static> {
        match self {
            VectorStruct::Single(v) => match v {
                VectorOrSparse::Vector(v) => default_vector(v),
                VectorOrSparse::Sparse(v) => default_sparse_vector(v),
            },
            VectorStruct::Multi(v) => NamedVectors::from_mixed_map(v),
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

/// Vector data with name
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SparseNamedVector {
    /// Name of vector data
    pub name: String,
    /// Vector data
    pub vector: SparseVector,
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
    DefaultSparse(SparseVector),
    NamedSparse(SparseNamedVector),
}

impl From<VectorType> for NamedVectorStruct {
    fn from(v: VectorType) -> Self {
        NamedVectorStruct::Default(v)
    }
}

impl From<SparseVector> for NamedVectorStruct {
    fn from(v: SparseVector) -> Self {
        NamedVectorStruct::DefaultSparse(v)
    }
}

impl From<NamedVector> for NamedVectorStruct {
    fn from(v: NamedVector) -> Self {
        NamedVectorStruct::Named(v)
    }
}

impl From<SparseNamedVector> for NamedVectorStruct {
    fn from(v: SparseNamedVector) -> Self {
        NamedVectorStruct::NamedSparse(v)
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
            NamedVectorStruct::DefaultSparse(_) => DEFAULT_VECTOR_NAME,
            NamedVectorStruct::NamedSparse(v) => &v.name,
        }
    }
}

impl NamedVectorStruct {
    pub fn get_vector(&self) -> VectorOrSparseRef {
        match self {
            NamedVectorStruct::Default(v) => v.as_slice().into(),
            NamedVectorStruct::Named(v) => v.vector.as_slice().into(),
            NamedVectorStruct::DefaultSparse(v) => v.into(),
            NamedVectorStruct::NamedSparse(v) => (&v.vector).into(),
        }
    }

    pub fn to_vector(self) -> VectorOrSparse {
        match self {
            NamedVectorStruct::Default(v) => v.into(),
            NamedVectorStruct::Named(v) => v.vector.into(),
            NamedVectorStruct::DefaultSparse(v) => v.into(),
            NamedVectorStruct::NamedSparse(v) => v.vector.into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum BatchVectorStruct {
    Single(Vec<VectorType>),
    Multi(HashMap<String, Vec<VectorType>>),
    Sparse(Vec<SparseVector>),
    MultiSparse(HashMap<String, Vec<SparseVector>>),
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
            BatchVectorStruct::Sparse(vectors) => {
                vectors.into_iter().map(default_sparse_vector).collect()
            }
            BatchVectorStruct::MultiSparse(named_vectors) => {
                if named_vectors.is_empty() {
                    vec![NamedVectors::default(); num_records]
                } else {
                    transpose_map_into_sparse_named_vector(named_vectors)
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct NamedRecoQuery {
    pub query: RecoQuery<VectorOrSparse>,
    pub using: Option<String>,
}

impl Named for NamedRecoQuery {
    fn get_name(&self) -> &str {
        self.using.as_deref().unwrap_or(DEFAULT_VECTOR_NAME)
    }
}

#[derive(Debug, Clone)]
pub enum QueryVector {
    Nearest(VectorType),
    Recommend(RecoQuery<VectorType>),
    Discovery(DiscoveryQuery<VectorType>),
    Context(ContextQuery<VectorType>),
}

impl From<VectorOrSparse> for QueryVector {
    fn from(vec: VectorOrSparse) -> Self {
        Self::Nearest(vec)
    }
}

impl<'a> From<&'a [VectorElementType]> for QueryVector {
    fn from(vec: &'a [VectorElementType]) -> Self {
        let v: VectorOrSparseRef = vec.into();
        Self::Nearest(v.to_owned())
    }
}

impl<'a> From<VectorOrSparseRef<'a>> for QueryVector {
    fn from(vec: VectorOrSparseRef<'a>) -> Self {
        Self::Nearest(vec.to_owned())
    }
}

impl<const N: usize> From<[VectorElementType; N]> for QueryVector {
    fn from(vec: [VectorElementType; N]) -> Self {
        let vec: VectorOrSparseRef = vec.as_slice().into();
        Self::Nearest(vec.to_owned())
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn vector_struct_deserialization() {
        // single vector case
        let s = serde_json::json!([0.1, 1.1, 2.1, 3.1]);
        let s = serde_json::to_string(&s).unwrap();
        let v: super::VectorStruct = serde_json::from_str(&s).unwrap();
        println!("{:?}", v);

        // named vector case
        let s = serde_json::json!(
            {
                "named1": [0.1, 1.1, 2.1, 3.1],
                "named2": [0.1, 1.1, 2.1, 3.1]
            }
        );
        let s = serde_json::to_string(&s).unwrap();
        let v: super::VectorStruct = serde_json::from_str(&s).unwrap();
        println!("{:?}", v);

        // sparse vector case
        let s = serde_json::json!(
            {
                "named1": {
                    "weights": [0.1, 1.1, 2.1, 3.1],
                    "indices": [0, 1, 2, 3],
                },
                "named2": {
                    "weights": [0.1, 1.1, 2.1, 3.1],
                    "indices": [0, 1, 2, 3],
                }
            }
        );
        let s = serde_json::to_string(&s).unwrap();
        let v: super::VectorStruct = serde_json::from_str(&s).unwrap();
        println!("{:?}", v);

        // mixed vector case
        let s = serde_json::json!(
            {
                "named1": {
                    "weights": [0.1, 1.1, 2.1, 3.1],
                    "indices": [0, 1, 2, 3],
                },
                "named2": [0.1, 1.1, 2.1, 3.1]
            }
        );
        let s = serde_json::to_string(&s).unwrap();
        let v: super::VectorStruct = serde_json::from_str(&s).unwrap();
        println!("{:?}", v);
    }
}

impl<'a> From<VectorRef<'a>> for QueryVector {
    fn from(vec: VectorRef<'a>) -> Self {
        let vec: &[_] = vec.into();
        Self::Nearest(vec.to_vec())
    }
}
