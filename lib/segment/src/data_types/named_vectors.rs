use std::borrow::Cow;
use std::collections::HashMap;

use sparse::common::sparse_vector::SparseVector;

use super::primitive::PrimitiveVectorElement;
use super::tiny_map;
use super::vectors::{
    DenseVector, MultiDenseVectorInternal, TypedMultiDenseVector, TypedMultiDenseVectorRef,
    VectorElementType, VectorElementTypeByte, VectorElementTypeHalf, VectorInternal, VectorRef,
};
use crate::common::operation_error::OperationError;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::{Distance, VectorDataConfig, VectorName, VectorNameBuf, VectorStorageDatatype};

type CowKey<'a> = Cow<'a, VectorName>;

#[derive(Clone, PartialEq, Debug)]
pub enum CowMultiVector<'a, TElement: PrimitiveVectorElement> {
    Owned(TypedMultiDenseVector<TElement>),
    Borrowed(TypedMultiDenseVectorRef<'a, TElement>),
}

impl<TElement> CowMultiVector<'_, TElement>
where
    TElement: PrimitiveVectorElement,
{
    pub fn dim(&self) -> usize {
        match self {
            CowMultiVector::Owned(typed_multi_dense_vector) => typed_multi_dense_vector.dim,
            CowMultiVector::Borrowed(typed_multi_dense_vector_ref) => {
                typed_multi_dense_vector_ref.dim
            }
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum CowVector<'a> {
    Dense(Cow<'a, [VectorElementType]>),
    Sparse(Cow<'a, SparseVector>),
    MultiDense(CowMultiVector<'a, VectorElementType>),
}

impl Default for CowVector<'_> {
    fn default() -> Self {
        CowVector::Dense(Cow::Owned(Vec::new()))
    }
}

impl CowVector<'_> {
    pub fn dim(&self) -> usize {
        match self {
            CowVector::Dense(cow) => cow.len(),
            CowVector::Sparse(cow) => cow.indices.len(),
            CowVector::MultiDense(cow_multi_vector) => cow_multi_vector.dim(),
        }
    }

    pub fn estimate_size_in_bytes(&self) -> usize {
        self.dim() * size_of::<VectorElementType>()
    }
}

type TinyMap<'a> = tiny_map::TinyMap<CowKey<'a>, CowVector<'a>>;

#[derive(Clone, Default, Debug, PartialEq)]
pub struct NamedVectors<'a> {
    map: TinyMap<'a>,
}

impl NamedVectors<'_> {
    pub fn estimate_size_in_bytes(&self) -> usize {
        self.map.iter().map(|i| i.1.estimate_size_in_bytes()).sum()
    }
}

impl<'a, TElement: PrimitiveVectorElement> CowMultiVector<'a, TElement> {
    pub fn to_owned(self) -> TypedMultiDenseVector<TElement> {
        match self {
            CowMultiVector::Owned(v) => v,
            CowMultiVector::Borrowed(v) => v.to_owned(),
        }
    }

    pub fn as_vec_ref(&'a self) -> TypedMultiDenseVectorRef<'a, TElement> {
        match self {
            CowMultiVector::Owned(v) => TypedMultiDenseVectorRef {
                flattened_vectors: &v.flattened_vectors,
                dim: v.dim,
            },
            CowMultiVector::Borrowed(v) => *v,
        }
    }
}

impl CowVector<'_> {
    pub fn default_sparse() -> Self {
        CowVector::Sparse(Cow::Owned(SparseVector::default()))
    }

    pub fn to_owned(self) -> VectorInternal {
        match self {
            CowVector::Dense(v) => VectorInternal::Dense(v.into_owned()),
            CowVector::Sparse(v) => VectorInternal::Sparse(v.into_owned()),
            CowVector::MultiDense(v) => VectorInternal::MultiDense(v.to_owned()),
        }
    }

    pub fn as_vec_ref(&self) -> VectorRef {
        match self {
            CowVector::Dense(v) => VectorRef::Dense(v.as_ref()),
            CowVector::Sparse(v) => VectorRef::Sparse(v.as_ref()),
            CowVector::MultiDense(v) => VectorRef::MultiDense(v.as_vec_ref()),
        }
    }
}

impl<'a> From<Cow<'a, [VectorElementType]>> for CowVector<'a> {
    fn from(v: Cow<'a, [VectorElementType]>) -> Self {
        match v {
            Cow::Borrowed(v) => CowVector::Dense(Cow::Borrowed(v)),
            Cow::Owned(v) => CowVector::Dense(Cow::Owned(v)),
        }
    }
}

impl From<VectorInternal> for CowVector<'_> {
    fn from(v: VectorInternal) -> Self {
        match v {
            VectorInternal::Dense(v) => CowVector::Dense(Cow::Owned(v)),
            VectorInternal::Sparse(v) => CowVector::Sparse(Cow::Owned(v)),
            VectorInternal::MultiDense(v) => CowVector::MultiDense(CowMultiVector::Owned(v)),
        }
    }
}

impl From<SparseVector> for CowVector<'_> {
    fn from(v: SparseVector) -> Self {
        CowVector::Sparse(Cow::Owned(v))
    }
}

impl From<DenseVector> for CowVector<'_> {
    fn from(v: DenseVector) -> Self {
        CowVector::Dense(Cow::Owned(v))
    }
}

impl From<MultiDenseVectorInternal> for CowVector<'_> {
    fn from(v: MultiDenseVectorInternal) -> Self {
        CowVector::MultiDense(CowMultiVector::Owned(v))
    }
}

impl<'a> From<Cow<'a, MultiDenseVectorInternal>> for CowVector<'a> {
    fn from(v: Cow<'a, MultiDenseVectorInternal>) -> Self {
        match v {
            Cow::Borrowed(v) => {
                CowVector::MultiDense(CowMultiVector::Borrowed(TypedMultiDenseVectorRef::from(v)))
            }
            Cow::Owned(v) => CowVector::MultiDense(CowMultiVector::Owned(v)),
        }
    }
}

impl<'a> From<&'a SparseVector> for CowVector<'a> {
    fn from(v: &'a SparseVector) -> Self {
        CowVector::Sparse(Cow::Borrowed(v))
    }
}

impl<'a> From<&'a [VectorElementType]> for CowVector<'a> {
    fn from(v: &'a [VectorElementType]) -> Self {
        CowVector::Dense(Cow::Borrowed(v))
    }
}

impl<'a> From<&'a MultiDenseVectorInternal> for CowVector<'a> {
    fn from(v: &'a MultiDenseVectorInternal) -> Self {
        CowVector::MultiDense(CowMultiVector::Borrowed(TypedMultiDenseVectorRef::from(v)))
    }
}

impl<'a> TryFrom<CowVector<'a>> for SparseVector {
    type Error = OperationError;

    fn try_from(value: CowVector<'a>) -> Result<Self, Self::Error> {
        match value {
            CowVector::Dense(_) => Err(OperationError::WrongSparse),
            CowVector::Sparse(v) => Ok(v.into_owned()),
            CowVector::MultiDense(_) => Err(OperationError::WrongSparse),
        }
    }
}

impl<'a> TryFrom<CowVector<'a>> for DenseVector {
    type Error = OperationError;

    fn try_from(value: CowVector<'a>) -> Result<Self, Self::Error> {
        match value {
            CowVector::Dense(v) => Ok(v.into_owned()),
            CowVector::Sparse(_) => Err(OperationError::WrongSparse),
            CowVector::MultiDense(_) => Err(OperationError::WrongMulti),
        }
    }
}

impl<'a> TryFrom<CowVector<'a>> for Cow<'a, [VectorElementType]> {
    type Error = OperationError;

    fn try_from(value: CowVector<'a>) -> Result<Self, Self::Error> {
        match value {
            CowVector::Dense(v) => Ok(v),
            CowVector::Sparse(_) => Err(OperationError::WrongSparse),
            CowVector::MultiDense(_) => Err(OperationError::WrongMulti),
        }
    }
}

impl<'a> From<VectorRef<'a>> for CowVector<'a> {
    fn from(v: VectorRef<'a>) -> Self {
        match v {
            VectorRef::Dense(v) => CowVector::Dense(Cow::Borrowed(v)),
            VectorRef::Sparse(v) => CowVector::Sparse(Cow::Borrowed(v)),
            VectorRef::MultiDense(v) => CowVector::MultiDense(CowMultiVector::Borrowed(v)),
        }
    }
}

impl<'a> NamedVectors<'a> {
    pub fn from_ref(key: &'a VectorName, value: VectorRef<'a>) -> Self {
        let mut map = TinyMap::new();
        map.insert(Cow::Borrowed(key), CowVector::from(value));
        Self { map }
    }

    pub fn from_pairs<const N: usize>(arr: [(VectorNameBuf, DenseVector); N]) -> Self {
        NamedVectors {
            map: arr
                .into_iter()
                .map(|(k, v)| (CowKey::from(k), CowVector::Dense(Cow::Owned(v))))
                .collect(),
        }
    }

    pub fn from_map(map: HashMap<VectorNameBuf, VectorInternal>) -> Self {
        Self {
            map: map
                .into_iter()
                .map(|(k, v)| (CowKey::from(k), v.into()))
                .collect(),
        }
    }

    pub fn from_map_ref(map: &'a HashMap<VectorNameBuf, DenseVector>) -> Self {
        Self {
            map: map
                .iter()
                .map(|(k, v)| (CowKey::from(k), CowVector::Dense(Cow::Borrowed(v))))
                .collect(),
        }
    }

    pub fn merge(&mut self, other: NamedVectors<'a>) {
        for (key, value) in other {
            self.map.insert(key, value);
        }
    }

    pub fn insert(&mut self, name: VectorNameBuf, vector: VectorInternal) {
        self.map
            .insert(CowKey::Owned(name), CowVector::from(vector));
    }

    pub fn insert_ref(&mut self, name: &'a VectorName, vector: VectorRef<'a>) {
        self.map
            .insert(CowKey::Borrowed(name), CowVector::from(vector));
    }

    pub fn contains_key(&self, key: &VectorName) -> bool {
        self.map.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &VectorName> {
        self.map.iter().map(|(k, _)| k.as_ref())
    }

    pub fn into_owned_map(self) -> HashMap<VectorNameBuf, VectorInternal> {
        self.map
            .into_iter()
            .map(|(k, v)| (k.into_owned(), v.to_owned()))
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&VectorName, VectorRef<'_>)> {
        self.map.iter().map(|(k, v)| (k.as_ref(), v.as_vec_ref()))
    }

    pub fn get(&self, key: &VectorName) -> Option<VectorRef<'_>> {
        self.map.get(key).map(|v| v.as_vec_ref())
    }

    pub fn preprocess<'b>(
        &mut self,
        get_vector_data: impl Fn(&VectorName) -> &'b VectorDataConfig,
    ) {
        for (name, vector) in self.map.iter_mut() {
            match vector {
                CowVector::Dense(v) => {
                    let config = get_vector_data(name.as_ref());
                    let preprocessed_vector = Self::preprocess_dense_vector(v.to_vec(), config);
                    *vector = CowVector::Dense(Cow::Owned(preprocessed_vector))
                }
                CowVector::Sparse(v) => {
                    // sort by indices to enable faster dot product and overlap checks
                    if !v.is_sorted() {
                        v.to_mut().sort_by_indices();
                    }
                }
                CowVector::MultiDense(multi_vector) => {
                    // invalid temp value to swap with multi_vector and reduce reallocations
                    let mut tmp_multi_vector = CowMultiVector::Borrowed(TypedMultiDenseVectorRef {
                        flattened_vectors: &[],
                        dim: 1,
                    });
                    // `multi_vector` is empty invalid and `tmp_multi_vector` owns the real data
                    std::mem::swap(&mut tmp_multi_vector, multi_vector);
                    let mut owned_multi_vector = tmp_multi_vector.to_owned();
                    let config = get_vector_data(name.as_ref());
                    for dense_vector in owned_multi_vector.multi_vectors_mut() {
                        let preprocessed_vector =
                            Self::preprocess_dense_vector(dense_vector.to_vec(), config);
                        // replace dense vector with preprocessed vector
                        dense_vector.copy_from_slice(&preprocessed_vector);
                    }
                    *multi_vector = CowMultiVector::Owned(owned_multi_vector);
                }
            }
        }
    }

    fn preprocess_dense_vector(
        dense_vector: DenseVector,
        config: &VectorDataConfig,
    ) -> DenseVector {
        match config.datatype {
            Some(VectorStorageDatatype::Float32) | None => match config.distance {
                Distance::Cosine => {
                    <CosineMetric as Metric<VectorElementType>>::preprocess(dense_vector)
                }
                Distance::Euclid => {
                    <EuclidMetric as Metric<VectorElementType>>::preprocess(dense_vector)
                }
                Distance::Dot => {
                    <DotProductMetric as Metric<VectorElementType>>::preprocess(dense_vector)
                }
                Distance::Manhattan => {
                    <ManhattanMetric as Metric<VectorElementType>>::preprocess(dense_vector)
                }
            },
            Some(VectorStorageDatatype::Uint8) => match config.distance {
                Distance::Cosine => {
                    <CosineMetric as Metric<VectorElementTypeByte>>::preprocess(dense_vector)
                }
                Distance::Euclid => {
                    <EuclidMetric as Metric<VectorElementTypeByte>>::preprocess(dense_vector)
                }
                Distance::Dot => {
                    <DotProductMetric as Metric<VectorElementTypeByte>>::preprocess(dense_vector)
                }
                Distance::Manhattan => {
                    <ManhattanMetric as Metric<VectorElementTypeByte>>::preprocess(dense_vector)
                }
            },
            Some(VectorStorageDatatype::Float16) => match config.distance {
                Distance::Cosine => {
                    <CosineMetric as Metric<VectorElementTypeHalf>>::preprocess(dense_vector)
                }
                Distance::Euclid => {
                    <EuclidMetric as Metric<VectorElementTypeHalf>>::preprocess(dense_vector)
                }
                Distance::Dot => {
                    <DotProductMetric as Metric<VectorElementTypeHalf>>::preprocess(dense_vector)
                }
                Distance::Manhattan => {
                    <ManhattanMetric as Metric<VectorElementTypeHalf>>::preprocess(dense_vector)
                }
            },
        }
    }
}

impl<'a> IntoIterator for NamedVectors<'a> {
    type Item = (CowKey<'a>, CowVector<'a>);

    type IntoIter =
        tinyvec::TinyVecIterator<[(CowKey<'a>, CowVector<'a>); super::tiny_map::CAPACITY]>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}
