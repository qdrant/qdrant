use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::SparseVector;
use validator::Validate;

use super::named_vectors::NamedVectors;
use crate::common::operation_error::OperationError;
use crate::common::utils::transpose_map_into_named_vector;
use crate::vector_storage::query::context_query::ContextQuery;
use crate::vector_storage::query::discovery_query::DiscoveryQuery;
use crate::vector_storage::query::reco_query::RecoQuery;

#[derive(Clone, Debug, PartialEq)]
pub enum Vector {
    Dense(DenseVector),
    Sparse(SparseVector),
    MultiDense(MultiDenseVector),
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum VectorRef<'a> {
    Dense(&'a [VectorElementType]),
    Sparse(&'a SparseVector),
    MultiDense(&'a [DenseVector]),
}

impl Vector {
    pub fn to_vec_ref(&self) -> VectorRef {
        match self {
            Vector::Dense(v) => VectorRef::Dense(v.as_slice()),
            Vector::Sparse(v) => VectorRef::Sparse(v),
            Vector::MultiDense(v) => VectorRef::MultiDense(v),
        }
    }
}

impl<'a> VectorRef<'a> {
    pub fn to_vec(self) -> Vector {
        match self {
            VectorRef::Dense(v) => Vector::Dense(v.to_vec()),
            VectorRef::Sparse(v) => Vector::Sparse(v.clone()),
            VectorRef::MultiDense(v) => Vector::MultiDense(v.to_vec()),
        }
    }
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

impl<'a> TryFrom<VectorRef<'a>> for &'a [DenseVector] {
    type Error = OperationError;

    fn try_from(value: VectorRef<'a>) -> Result<Self, Self::Error> {
        match value {
            VectorRef::Dense(_) => Err(OperationError::WrongMulti),
            VectorRef::Sparse(_v) => Err(OperationError::WrongSparse),
            VectorRef::MultiDense(v) => Ok(v),
        }
    }
}

impl From<NamedVectorStruct> for Vector {
    fn from(value: NamedVectorStruct) -> Self {
        match value {
            NamedVectorStruct::Default(v) => Vector::Dense(v),
            NamedVectorStruct::Dense(v) => Vector::Dense(v.vector),
            NamedVectorStruct::Sparse(v) => Vector::Sparse(v.vector),
        }
    }
}

impl TryFrom<Vector> for DenseVector {
    type Error = OperationError;

    fn try_from(value: Vector) -> Result<Self, Self::Error> {
        match value {
            Vector::Dense(v) => Ok(v),
            Vector::Sparse(_) => Err(OperationError::WrongSparse),
            Vector::MultiDense(_) => Err(OperationError::WrongMulti),
        }
    }
}

impl TryFrom<Vector> for SparseVector {
    type Error = OperationError;

    fn try_from(value: Vector) -> Result<Self, Self::Error> {
        match value {
            Vector::Dense(_) => Err(OperationError::WrongSparse),
            Vector::Sparse(v) => Ok(v),
            Vector::MultiDense(_) => Err(OperationError::WrongMulti),
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

impl<'a> From<&'a SparseVector> for VectorRef<'a> {
    fn from(val: &'a SparseVector) -> Self {
        VectorRef::Sparse(val)
    }
}

impl From<DenseVector> for Vector {
    fn from(val: DenseVector) -> Self {
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
            Vector::MultiDense(v) => VectorRef::MultiDense(v),
        }
    }
}

/// Type of vector element.
pub type VectorElementType = f32;

pub const DEFAULT_VECTOR_NAME: &str = "";

/// Type for dense vector
pub type DenseVector = Vec<VectorElementType>;

/// Type for multi dense vector
pub type MultiDenseVector = Vec<DenseVector>;

impl<'a> VectorRef<'a> {
    // Cannot use `ToOwned` trait because of `Borrow` implementation for `Vector`
    pub fn to_owned(self) -> Vector {
        match self {
            VectorRef::Dense(v) => Vector::Dense(v.to_vec()),
            VectorRef::Sparse(v) => Vector::Sparse(v.clone()),
            VectorRef::MultiDense(v) => Vector::MultiDense(v.to_vec()),
        }
    }
}

impl<'a> TryInto<&'a [VectorElementType]> for &'a Vector {
    type Error = OperationError;

    fn try_into(self) -> Result<&'a [VectorElementType], Self::Error> {
        match self {
            Vector::Dense(v) => Ok(v),
            Vector::Sparse(_) => Err(OperationError::WrongSparse),
            Vector::MultiDense(_) => Err(OperationError::WrongMulti),
        }
    }
}

impl<'a> TryInto<&'a SparseVector> for &'a Vector {
    type Error = OperationError;

    fn try_into(self) -> Result<&'a SparseVector, Self::Error> {
        match self {
            Vector::Dense(_) => Err(OperationError::WrongSparse),
            Vector::Sparse(v) => Ok(v),
            Vector::MultiDense(_) => Err(OperationError::WrongMulti),
        }
    }
}

pub fn default_vector(vec: DenseVector) -> NamedVectors<'static> {
    NamedVectors::from([(DEFAULT_VECTOR_NAME.to_owned(), vec)])
}

pub fn only_default_vector(vec: &[VectorElementType]) -> NamedVectors {
    NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vec.into())
}

/// Full vector data per point separator with single and multiple vector modes
/// TODO(colbert) try to remove this enum and use NamedVectors instead
#[derive(Clone, Debug, PartialEq)]
pub enum VectorStruct {
    Single(DenseVector),
    Multi(HashMap<String, Vector>),
}

impl VectorStruct {
    /// Merge `other` into this
    ///
    /// Other overwrites vectors we already have in this.
    pub fn merge(&mut self, other: Self) {
        match (self, other) {
            // If other is empty, merge nothing
            (_, VectorStruct::Multi(other)) if other.is_empty() => {}
            // Single overwrites single
            (VectorStruct::Single(this), VectorStruct::Single(other)) => {
                *this = other;
            }
            // If multi into single, convert this to multi and merge
            (this @ VectorStruct::Single(_), other @ VectorStruct::Multi(_)) => {
                let VectorStruct::Single(single) = this.clone() else {
                    unreachable!();
                };
                *this = VectorStruct::Multi(HashMap::from([(String::new(), single.into())]));
                this.merge(other);
            }
            // Single into multi
            (VectorStruct::Multi(this), VectorStruct::Single(other)) => {
                this.insert(String::new(), other.into());
            }
            // Multi into multi
            (VectorStruct::Multi(this), VectorStruct::Multi(other)) => this.extend(other),
        }
    }
}

impl From<DenseVector> for VectorStruct {
    fn from(v: DenseVector) -> Self {
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
            let vector: &[_] = v.get(DEFAULT_VECTOR_NAME).unwrap().try_into().unwrap();
            VectorStruct::Single(vector.to_owned())
        } else {
            VectorStruct::Multi(v.into_owned_map())
        }
    }
}

impl VectorStruct {
    pub fn get(&self, name: &str) -> Option<VectorRef> {
        match self {
            VectorStruct::Single(v) => (name == DEFAULT_VECTOR_NAME).then_some(v.into()),
            VectorStruct::Multi(v) => v.get(name).map(|v| v.into()),
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
    pub vector: DenseVector,
}

/// Sparse vector data with name
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Validate)]
#[serde(rename_all = "snake_case")]
pub struct NamedSparseVector {
    /// Name of vector data
    pub name: String,
    /// Vector data
    #[validate]
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
    Default(DenseVector),
    Dense(NamedVector),
    Sparse(NamedSparseVector),
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

pub trait Named {
    fn get_name(&self) -> &str;
}

impl Named for NamedVectorStruct {
    fn get_name(&self) -> &str {
        match self {
            NamedVectorStruct::Default(_) => DEFAULT_VECTOR_NAME,
            NamedVectorStruct::Dense(v) => &v.name,
            NamedVectorStruct::Sparse(v) => &v.name,
        }
    }
}

impl NamedVectorStruct {
    pub fn new_from_vector(vector: Vector, name: String) -> Self {
        match vector {
            Vector::Dense(vector) => NamedVectorStruct::Dense(NamedVector { name, vector }),
            Vector::Sparse(vector) => NamedVectorStruct::Sparse(NamedSparseVector { name, vector }),
            Vector::MultiDense(_) => {
                // TODO(colbert)
                unimplemented!("MultiDenseVector cannot be converted to NamedVectorStruct")
            }
        }
    }

    pub fn get_vector(&self) -> VectorRef {
        match self {
            NamedVectorStruct::Default(v) => v.as_slice().into(),
            NamedVectorStruct::Dense(v) => v.vector.as_slice().into(),
            NamedVectorStruct::Sparse(v) => (&v.vector).into(),
        }
    }

    pub fn to_vector(self) -> Vector {
        match self {
            NamedVectorStruct::Default(v) => v.into(),
            NamedVectorStruct::Dense(v) => v.vector.into(),
            NamedVectorStruct::Sparse(v) => v.vector.into(),
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

#[derive(Clone, Debug, PartialEq)]
pub enum BatchVectorStruct {
    Single(Vec<DenseVector>),
    Multi(HashMap<String, Vec<Vector>>),
}

impl From<Vec<DenseVector>> for BatchVectorStruct {
    fn from(v: Vec<DenseVector>) -> Self {
        BatchVectorStruct::Single(v)
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

impl<T: Validate> Validate for NamedQuery<T> {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        self.query.validate()
    }
}

#[derive(Debug, Clone)]
pub enum QueryVector {
    Nearest(Vector),
    Recommend(RecoQuery<Vector>),
    Discovery(DiscoveryQuery<Vector>),
    Context(ContextQuery<Vector>),
}

impl From<DenseVector> for QueryVector {
    fn from(vec: DenseVector) -> Self {
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
        let vec: VectorRef = vec.as_slice().into();
        Self::Nearest(vec.to_owned())
    }
}

impl<'a> From<VectorRef<'a>> for QueryVector {
    fn from(vec: VectorRef<'a>) -> Self {
        Self::Nearest(vec.to_vec())
    }
}

impl From<Vector> for QueryVector {
    fn from(vec: Vector) -> Self {
        Self::Nearest(vec)
    }
}

impl From<SparseVector> for QueryVector {
    fn from(vec: SparseVector) -> Self {
        Self::Nearest(Vector::Sparse(vec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_struct_merge_single_into_single() {
        let mut a = VectorStruct::Single(vec![0.2, 0.1, 0.0, 0.9]);
        let b = VectorStruct::Single(vec![0.1, 0.9, 0.6, 0.3]);
        a.merge(b);
        assert_eq!(a, VectorStruct::Single(vec![0.1, 0.9, 0.6, 0.3]));
    }

    #[test]
    fn vector_struct_merge_single_into_multi() {
        // Single into multi without default vector
        let mut a = VectorStruct::Multi(HashMap::from([
            ("a".into(), vec![0.8, 0.3, 0.0, 0.1].into()),
            ("b".into(), vec![0.4, 0.5, 0.8, 0.3].into()),
        ]));
        let b = VectorStruct::Single(vec![0.5, 0.3, 0.0, 0.4]);
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("a".into(), vec![0.8, 0.3, 0.0, 0.1].into()),
                ("b".into(), vec![0.4, 0.5, 0.8, 0.3].into()),
                ("".into(), vec![0.5, 0.3, 0.0, 0.4].into()),
            ])),
        );

        // Single into multi with default vector
        let mut a = VectorStruct::Multi(HashMap::from([
            ("a".into(), vec![0.2, 0.0, 0.5, 0.1].into()),
            ("".into(), vec![0.3, 0.7, 0.6, 0.4].into()),
        ]));
        let b = VectorStruct::Single(vec![0.4, 0.4, 0.8, 0.5]);
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("a".into(), vec![0.2, 0.0, 0.5, 0.1].into()),
                ("".into(), vec![0.4, 0.4, 0.8, 0.5].into()),
            ])),
        );
    }

    #[test]
    fn vector_struct_merge_multi_into_multi() {
        // Empty multi into multi shouldn't do anything
        let mut a = VectorStruct::Multi(HashMap::from([(
            "a".into(),
            vec![0.0, 0.5, 0.9, 0.0].into(),
        )]));
        let b = VectorStruct::Multi(HashMap::new());
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([(
                "a".into(),
                vec![0.0, 0.5, 0.9, 0.0].into()
            ),])),
        );

        // Multi into empty multi
        let mut a = VectorStruct::Multi(HashMap::new());
        let b = VectorStruct::Multi(HashMap::from([(
            "a".into(),
            vec![0.2, 0.0, 0.6, 0.5].into(),
        )]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([(
                "a".into(),
                vec![0.2, 0.0, 0.6, 0.5].into(),
            )]))
        );

        // Non-overlapping multi into multi
        let mut a = VectorStruct::Multi(HashMap::from([(
            "a".into(),
            vec![0.8, 0.6, 0.2, 0.1].into(),
        )]));
        let b = VectorStruct::Multi(HashMap::from([(
            "b".into(),
            vec![0.1, 0.9, 0.8, 0.2].into(),
        )]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("a".into(), vec![0.8, 0.6, 0.2, 0.1].into()),
                ("b".into(), vec![0.1, 0.9, 0.8, 0.2].into()),
            ])),
        );

        // Overlapping multi into multi
        let mut a = VectorStruct::Multi(HashMap::from([
            ("a".into(), vec![0.3, 0.2, 0.7, 0.5].into()),
            ("b".into(), vec![0.6, 0.3, 0.8, 0.3].into()),
        ]));
        let b = VectorStruct::Multi(HashMap::from([
            ("b".into(), vec![0.8, 0.2, 0.4, 0.9].into()),
            ("c".into(), vec![0.4, 0.8, 0.9, 0.6].into()),
        ]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("a".into(), vec![0.3, 0.2, 0.7, 0.5].into()),
                ("b".into(), vec![0.8, 0.2, 0.4, 0.9].into()),
                ("c".into(), vec![0.4, 0.8, 0.9, 0.6].into()),
            ])),
        );
    }

    #[test]
    fn vector_struct_merge_multi_into_single() {
        // Empty multi into single shouldn't do anything
        let mut a = VectorStruct::Single(vec![0.0, 0.8, 0.4, 0.1]);
        let b = VectorStruct::Multi(HashMap::new());
        a.merge(b);
        assert_eq!(a, VectorStruct::Single(vec![0.0, 0.8, 0.4, 0.1]),);

        // Non-overlapping multi into single
        let mut a = VectorStruct::Single(vec![0.2, 0.5, 0.5, 0.1]);
        let b = VectorStruct::Multi(HashMap::from([(
            "a".into(),
            vec![0.1, 0.9, 0.7, 0.6].into(),
        )]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("".into(), vec![0.2, 0.5, 0.5, 0.1].into()),
                ("a".into(), vec![0.1, 0.9, 0.7, 0.6].into()),
            ])),
        );

        // Overlapping multi ("") into single
        // This becomes a multi even if other has a multi with only a default vector
        let mut a = VectorStruct::Single(vec![0.3, 0.1, 0.8, 0.1]);
        let b = VectorStruct::Multi(HashMap::from([(
            "".into(),
            vec![0.6, 0.1, 0.3, 0.4].into(),
        )]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([(
                "".into(),
                vec![0.6, 0.1, 0.3, 0.4].into()
            )])),
        );

        // Overlapping multi into single
        let mut a = VectorStruct::Single(vec![0.6, 0.9, 0.7, 0.6]);
        let b = VectorStruct::Multi(HashMap::from([
            ("".into(), vec![0.7, 0.5, 0.8, 0.1].into()),
            ("a".into(), vec![0.2, 0.9, 0.7, 0.0].into()),
        ]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("".into(), vec![0.7, 0.5, 0.8, 0.1].into()),
                ("a".into(), vec![0.2, 0.9, 0.7, 0.0].into()),
            ])),
        );
    }
}
