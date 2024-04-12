use std::borrow::Cow;
use std::collections::HashMap;
use std::slice::ChunksExactMut;

#[cfg(feature = "f16")]
use half::f16;
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
    MultiDense(&'a MultiDenseVector),
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

impl<'a> TryFrom<VectorRef<'a>> for &'a MultiDenseVector {
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
            NamedVectorStruct::MultiDense(v) => Vector::MultiDense(v.vector),
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

impl TryFrom<Vector> for MultiDenseVector {
    type Error = OperationError;

    fn try_from(value: Vector) -> Result<Self, Self::Error> {
        match value {
            Vector::Dense(_) => Err(OperationError::WrongMulti),
            Vector::Sparse(_) => Err(OperationError::WrongSparse),
            Vector::MultiDense(v) => Ok(v),
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

impl<'a> From<&'a MultiDenseVector> for VectorRef<'a> {
    fn from(val: &'a MultiDenseVector) -> Self {
        VectorRef::MultiDense(val)
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

impl From<MultiDenseVector> for Vector {
    fn from(val: MultiDenseVector) -> Self {
        Vector::MultiDense(val)
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
#[cfg(not(feature = "f16"))]
pub type VectorElementType = f32;

/// Type of vector element.
#[cfg(feature = "f16")]
pub type VectorElementType = f16;

pub trait IntoVectorElement: Sized {
    fn into_vector_element(self) -> VectorElementType;
}

impl IntoVectorElement for f32 {
    #[cfg(not(feature = "f16"))]
    fn into_vector_element(self) -> VectorElementType {
        self
    }
    #[cfg(feature = "f16")]
    fn into_vector_element(self) -> VectorElementType {
        f16::from_f32(self)
    }
}

impl<T: IntoVectorElement + Copy> IntoVectorElement for &T {
    fn into_vector_element(self) -> VectorElementType {
        (*self).into_vector_element()
    }
}

pub trait FromVectorElement {
    fn from_vector_element(x: VectorElementType) -> Self;
}

impl FromVectorElement for f32 {
    #[cfg(not(feature = "f16"))]
    fn from_vector_element(x: VectorElementType) -> Self {
        x
    }
    #[cfg(feature = "f16")]
    fn from_vector_element(x: VectorElementType) -> Self {
        x.to_f32()
    }
}

pub trait IntoVectorElementArray<const N: usize>: Sized {
    fn into_vector_element_array(self) -> [VectorElementType; N];
}

impl<const N: usize> IntoVectorElementArray<N> for [f32; N] {
    #[cfg(not(feature = "f16"))]
    fn into_vector_element_array(self) -> [VectorElementType; N] {
        self
    }
    #[cfg(feature = "f16")]
    fn into_vector_element_array(self) -> [VectorElementType; N] {
        use half::slice::HalfFloatSliceExt;

        let mut output = [VectorElementType::default(); N];
        output.convert_from_f32_slice(&self);
        output
    }
}

pub trait FromVectorElementArray<const N: usize>: Sized {
    fn from_vector_element_array(x: [VectorElementType; N]) -> Self;
}

impl<const N: usize> FromVectorElementArray<N> for [f32; N] {
    #[cfg(not(feature = "f16"))]
    fn from_vector_element_array(x: [VectorElementType; N]) -> Self {
        x
    }
    #[cfg(feature = "f16")]
    fn from_vector_element_array(x: [VectorElementType; N]) -> Self {
        use half::slice::HalfFloatSliceExt;

        let mut output = [0f32; N];
        x.convert_to_f32_slice(&mut output);
        output
    }
}

pub type VectorElementTypeByte = u8;

pub const DEFAULT_VECTOR_NAME: &str = "";

pub type TypedDenseVector<T> = Vec<T>;

/// Type for dense vector
pub type DenseVector = TypedDenseVector<VectorElementType>;

pub trait IntoDenseVector {
    fn into_dense_vector(self) -> DenseVector;
}

impl<const N: usize> IntoDenseVector for [f32; N] {
    #[cfg(not(feature = "f16"))]
    fn into_dense_vector(self) -> DenseVector {
        self.to_vec()
    }
    #[cfg(feature = "f16")]
    fn into_dense_vector(self) -> DenseVector {
        use half::vec::HalfFloatVecExt;

        DenseVector::from_f32_slice(&self)
    }
}

impl IntoDenseVector for &[f32] {
    #[cfg(not(feature = "f16"))]
    fn into_dense_vector(self) -> DenseVector {
        self.to_vec()
    }
    #[cfg(feature = "f16")]
    fn into_dense_vector(self) -> DenseVector {
        use half::vec::HalfFloatVecExt;

        DenseVector::from_f32_slice(self)
    }
}

impl IntoDenseVector for &[u8] {
    #[cfg(not(feature = "f16"))]
    fn into_dense_vector(self) -> DenseVector {
        self.iter().map(|x| *x as f32).collect()
    }
    #[cfg(feature = "f16")]
    fn into_dense_vector(self) -> DenseVector {
        use half::slice::HalfFloatSliceExt;

        let mut output = Vec::with_capacity(self.len());
        let mut array_f32 = [0f32; 8];
        let mut array_f16 = [f16::ZERO; 8];
        let mut x_iter = self.chunks_exact(8);
        for x in x_iter.by_ref() {
            for (x, a) in x.iter().copied().zip(array_f32.iter_mut()) {
                *a = x as f32;
            }
            array_f16.convert_from_f32_slice(&array_f32);
            output.extend(array_f16);
        }
        output.extend(x_iter.remainder().iter().map(|x| f16::from(*x)));
        output
    }
}

pub trait FromDenseVector: Sized {
    fn from_dense_vector(x: DenseVector) -> Self;
}

impl FromDenseVector for Vec<f32> {
    #[cfg(not(feature = "f16"))]
    fn from_dense_vector(x: DenseVector) -> Self {
        x
    }
    #[cfg(feature = "f16")]
    fn from_dense_vector(x: DenseVector) -> Self {
        use half::slice::HalfFloatSliceExt;

        x.to_f32_vec()
    }
}

pub trait FromVectorElementSlice<'a>: Sized {
    fn from_vector_element_slice(x: &'a [VectorElementType]) -> Self;
}

impl<'a> FromVectorElementSlice<'a> for Vec<f32> {
    #[cfg(not(feature = "f16"))]
    fn from_vector_element_slice(x: &'a [VectorElementType]) -> Self {
        x.to_vec()
    }
    #[cfg(feature = "f16")]
    fn from_vector_element_slice(x: &'a [VectorElementType]) -> Self {
        use half::slice::HalfFloatSliceExt;

        x.to_f32_vec()
    }
}

impl<'a> FromVectorElementSlice<'a> for Vec<u8> {
    #[cfg(not(feature = "f16"))]
    fn from_vector_element_slice(x: &'a [VectorElementType]) -> Self {
        x.iter().map(|x| *x as u8).collect()
    }
    #[cfg(feature = "f16")]
    fn from_vector_element_slice(x: &'a [VectorElementType]) -> Self {
        use half::slice::HalfFloatSliceExt;

        let mut output = Vec::with_capacity(x.len());
        let mut array = [0f32; 8];
        let mut x_iter = x.chunks_exact(array.len());
        for x in x_iter.by_ref() {
            x.convert_to_f32_slice(&mut array);
            output.extend(array.map(|a| a as u8));
        }
        output.extend(x_iter.remainder().iter().map(|x| x.to_f32() as u8));
        output
    }
}

impl<'a> FromVectorElementSlice<'a> for Cow<'a, [f32]> {
    #[cfg(not(feature = "f16"))]
    fn from_vector_element_slice(x: &'a [VectorElementType]) -> Self {
        x.into()
    }
    #[cfg(feature = "f16")]
    fn from_vector_element_slice(x: &'a [VectorElementType]) -> Self {
        Vec::from_vector_element_slice(x).into()
    }
}

/// Type for multi dense vector
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct MultiDenseVector {
    pub inner_vector: DenseVector, // vectors are flattened into a single vector
    pub dim: usize,                // dimension of each vector
}

impl MultiDenseVector {
    pub fn new(flattened_vectors: DenseVector, dim: usize) -> Self {
        Self {
            inner_vector: flattened_vectors,
            dim,
        }
    }

    /// MultiDenseVector cannot be empty, so we use a placeholder vector instead
    pub fn placeholder(dim: usize) -> Self {
        Self {
            inner_vector: vec![num_traits::One::one(); dim],
            dim,
        }
    }

    /// Slices the multi vector into the underlying individual vectors
    pub fn multi_vectors(&self) -> impl Iterator<Item = &[VectorElementType]> {
        self.inner_vector.chunks_exact(self.dim)
    }

    pub fn multi_vectors_mut(&mut self) -> ChunksExactMut<'_, VectorElementType> {
        self.inner_vector.chunks_exact_mut(self.dim)
    }

    pub fn is_empty(&self) -> bool {
        self.inner_vector.is_empty()
    }
}

impl TryFrom<Vec<DenseVector>> for MultiDenseVector {
    type Error = OperationError;

    fn try_from(value: Vec<DenseVector>) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(OperationError::ValidationError {
                description: "MultiDenseVector cannot be empty".to_string(),
            });
        }
        let dim = value[0].len();
        // assert all vectors have the same dimension
        if let Some(bad_vec) = value.iter().find(|v| v.len() != dim) {
            Err(OperationError::WrongVector {
                expected_dim: dim,
                received_dim: bad_vec.len(),
            })
        } else {
            let inner_vector = value.into_iter().flatten().collect();
            let multi_dense = MultiDenseVector { inner_vector, dim };
            Ok(multi_dense)
        }
    }
}

impl TryFrom<Vec<DenseVector>> for Vector {
    type Error = OperationError;

    fn try_from(value: Vec<DenseVector>) -> Result<Self, Self::Error> {
        MultiDenseVector::try_from(value).map(Vector::MultiDense)
    }
}

impl<'a> VectorRef<'a> {
    // Cannot use `ToOwned` trait because of `Borrow` implementation for `Vector`
    pub fn to_owned(self) -> Vector {
        match self {
            VectorRef::Dense(v) => Vector::Dense(v.to_vec()),
            VectorRef::Sparse(v) => Vector::Sparse(v.clone()),
            VectorRef::MultiDense(v) => Vector::MultiDense(v.clone()),
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

impl<'a> TryInto<&'a MultiDenseVector> for &'a Vector {
    type Error = OperationError;

    fn try_into(self) -> Result<&'a MultiDenseVector, Self::Error> {
        match self {
            Vector::Dense(_) => Err(OperationError::WrongMulti),
            Vector::Sparse(_) => Err(OperationError::WrongSparse),
            Vector::MultiDense(v) => Ok(v),
        }
    }
}

pub fn default_vector(vec: DenseVector) -> NamedVectors<'static> {
    NamedVectors::from([(DEFAULT_VECTOR_NAME.to_owned(), vec)])
}

pub fn only_default_vector(vec: &[VectorElementType]) -> NamedVectors {
    NamedVectors::from_ref(DEFAULT_VECTOR_NAME, VectorRef::from(vec))
}

pub fn only_default_multi_vector(vec: &MultiDenseVector) -> NamedVectors {
    NamedVectors::from_ref(DEFAULT_VECTOR_NAME, VectorRef::MultiDense(vec))
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

/// Dense vector data with name
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[cfg_attr(not(feature = "f16"), derive(JsonSchema))]
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
    pub vector: MultiDenseVector,
}

/// Sparse vector data with name
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Validate, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct NamedSparseVector {
    /// Name of vector data
    pub name: String,
    /// Vector data
    #[validate]
    pub vector: SparseVector,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NamedVectorStruct {
    Default(DenseVector),
    Dense(NamedVector),
    Sparse(NamedSparseVector),
    MultiDense(NamedMultiDenseVector),
}

#[cfg(feature = "f16")]
const _: () = {
    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct NamedVectorF32 {
        /// Name of vector data
        pub name: String,
        /// Vector data
        pub vector: Vec<f32>,
    }

    impl JsonSchema for NamedVector {
        fn schema_name() -> String {
            "NamedVector".to_string()
        }
        fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
            NamedVectorF32::json_schema(gen)
        }
    }

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    enum NamedVectorStructF32 {
        Default(Vec<f32>),
        Dense(NamedVectorF32),
        Sparse(NamedSparseVector),
    }

    impl JsonSchema for NamedVectorStruct {
        fn schema_name() -> String {
            "NamedVector".to_string()
        }
        fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
            NamedVectorStructF32::json_schema(gen)
        }
    }
};

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
            NamedVectorStruct::MultiDense(v) => &v.name,
        }
    }
}

impl NamedVectorStruct {
    pub fn new_from_vector(vector: Vector, name: String) -> Self {
        match vector {
            Vector::Dense(vector) => NamedVectorStruct::Dense(NamedVector { name, vector }),
            Vector::Sparse(vector) => NamedVectorStruct::Sparse(NamedSparseVector { name, vector }),
            Vector::MultiDense(vector) => {
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

    pub fn to_vector(self) -> Vector {
        match self {
            NamedVectorStruct::Default(v) => v.into(),
            NamedVectorStruct::Dense(v) => v.vector.into(),
            NamedVectorStruct::Sparse(v) => v.vector.into(),
            NamedVectorStruct::MultiDense(v) => v.vector.into(),
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

impl<'a> From<&'a MultiDenseVector> for QueryVector {
    fn from(vec: &'a MultiDenseVector) -> Self {
        Self::Nearest(Vector::MultiDense(vec.clone()))
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

impl From<MultiDenseVector> for QueryVector {
    fn from(vec: MultiDenseVector) -> Self {
        Self::Nearest(Vector::MultiDense(vec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_struct_merge_single_into_single() {
        let mut a = VectorStruct::Single([0.2, 0.1, 0.0, 0.9].into_dense_vector());
        let b = VectorStruct::Single([0.1, 0.9, 0.6, 0.3].into_dense_vector());
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Single([0.1, 0.9, 0.6, 0.3].into_dense_vector())
        );
    }

    #[test]
    fn vector_struct_merge_single_into_multi() {
        // Single into multi without default vector
        let mut a = VectorStruct::Multi(HashMap::from([
            ("a".into(), [0.8, 0.3, 0.0, 0.1].into_dense_vector().into()),
            ("b".into(), [0.4, 0.5, 0.8, 0.3].into_dense_vector().into()),
        ]));
        let b = VectorStruct::Single([0.5, 0.3, 0.0, 0.4].into_dense_vector());
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("a".into(), [0.8, 0.3, 0.0, 0.1].into_dense_vector().into(),),
                ("b".into(), [0.4, 0.5, 0.8, 0.3].into_dense_vector().into(),),
                ("".into(), [0.5, 0.3, 0.0, 0.4].into_dense_vector().into(),),
            ])),
        );

        // Single into multi with default vector
        let mut a = VectorStruct::Multi(HashMap::from([
            ("a".into(), [0.2, 0.0, 0.5, 0.1].into_dense_vector().into()),
            ("".into(), [0.3, 0.7, 0.6, 0.4].into_dense_vector().into()),
        ]));
        let b = VectorStruct::Single([0.4, 0.4, 0.8, 0.5].into_dense_vector());
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("a".into(), [0.2, 0.0, 0.5, 0.1].into_dense_vector().into()),
                ("".into(), [0.4, 0.4, 0.8, 0.5].into_dense_vector().into()),
            ])),
        );
    }

    #[test]
    fn vector_struct_merge_multi_into_multi() {
        // Empty multi into multi shouldn't do anything
        let mut a = VectorStruct::Multi(HashMap::from([(
            "a".into(),
            [0.0, 0.5, 0.9, 0.0].into_dense_vector().into(),
        )]));
        let b = VectorStruct::Multi(HashMap::new());
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([(
                "a".into(),
                [0.0, 0.5, 0.9, 0.0].into_dense_vector().into(),
            ),])),
        );

        // Multi into empty multi
        let mut a = VectorStruct::Multi(HashMap::new());
        let b = VectorStruct::Multi(HashMap::from([(
            "a".into(),
            [0.2, 0.0, 0.6, 0.5].into_dense_vector().into(),
        )]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([(
                "a".into(),
                [0.2, 0.0, 0.6, 0.5].into_dense_vector().into(),
            )]))
        );

        // Non-overlapping multi into multi
        let mut a = VectorStruct::Multi(HashMap::from([(
            "a".into(),
            [0.8, 0.6, 0.2, 0.1].into_dense_vector().into(),
        )]));
        let b = VectorStruct::Multi(HashMap::from([(
            "b".into(),
            [0.1, 0.9, 0.8, 0.2].into_dense_vector().into(),
        )]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("a".into(), [0.8, 0.6, 0.2, 0.1].into_dense_vector().into()),
                ("b".into(), [0.1, 0.9, 0.8, 0.2].into_dense_vector().into(),),
            ])),
        );

        // Overlapping multi into multi
        let mut a = VectorStruct::Multi(HashMap::from([
            ("a".into(), [0.3, 0.2, 0.7, 0.5].into_dense_vector().into()),
            ("b".into(), [0.6, 0.3, 0.8, 0.3].into_dense_vector().into()),
        ]));
        let b = VectorStruct::Multi(HashMap::from([
            ("b".into(), [0.8, 0.2, 0.4, 0.9].into_dense_vector().into()),
            ("c".into(), [0.4, 0.8, 0.9, 0.6].into_dense_vector().into()),
        ]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("a".into(), [0.3, 0.2, 0.7, 0.5].into_dense_vector().into(),),
                (
                    "b".into(),
                    [0.8, 0.2, 0.4, 0.9].into_dense_vector().to_vec().into(),
                ),
                ("c".into(), [0.4, 0.8, 0.9, 0.6].into_dense_vector().into(),),
            ])),
        );
    }

    #[test]
    fn vector_struct_merge_multi_into_single() {
        // Empty multi into single shouldn't do anything
        let mut a = VectorStruct::Single([0.0, 0.8, 0.4, 0.1].into_dense_vector());
        let b = VectorStruct::Multi(HashMap::new());
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Single([0.0, 0.8, 0.4, 0.1].into_dense_vector()),
        );

        // Non-overlapping multi into single
        let mut a = VectorStruct::Single([0.2, 0.5, 0.5, 0.1].into_dense_vector());
        let b = VectorStruct::Multi(HashMap::from([(
            "a".into(),
            [0.1, 0.9, 0.7, 0.6].into_dense_vector().into(),
        )]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("".into(), [0.2, 0.5, 0.5, 0.1].into_dense_vector().into()),
                ("a".into(), [0.1, 0.9, 0.7, 0.6].into_dense_vector().into()),
            ])),
        );

        // Overlapping multi ("") into single
        // This becomes a multi even if other has a multi with only a default vector
        let mut a = VectorStruct::Single([0.3, 0.1, 0.8, 0.1].into_dense_vector());
        let b = VectorStruct::Multi(HashMap::from([(
            "".into(),
            [0.6, 0.1, 0.3, 0.4].into_dense_vector().into(),
        )]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([(
                "".into(),
                [0.6, 0.1, 0.3, 0.4].into_dense_vector().into()
            )])),
        );

        // Overlapping multi into single
        let mut a = VectorStruct::Single([0.6, 0.9, 0.7, 0.6].into_dense_vector());
        let b = VectorStruct::Multi(HashMap::from([
            ("".into(), [0.7, 0.5, 0.8, 0.1].into_dense_vector().into()),
            ("a".into(), [0.2, 0.9, 0.7, 0.0].into_dense_vector().into()),
        ]));
        a.merge(b);
        assert_eq!(
            a,
            VectorStruct::Multi(HashMap::from([
                ("".into(), [0.7, 0.5, 0.8, 0.1].into_dense_vector().into()),
                ("a".into(), [0.2, 0.9, 0.7, 0.0].into_dense_vector().into()),
            ])),
        );
    }
}
