use std::borrow::Cow;
use std::collections::HashMap;

use sparse::common::sparse_vector::SparseVector;

use super::tiny_map;
use super::vectors::{DenseVector, MultiDenseVector, Vector, VectorElementType, VectorRef};
use crate::common::operation_error::OperationError;
use crate::types::Distance;

type CowKey<'a> = Cow<'a, str>;

#[derive(Clone, PartialEq, Debug)]
pub enum CowVector<'a> {
    Dense(Cow<'a, [VectorElementType]>),
    Sparse(Cow<'a, SparseVector>),
    MultiDense(Cow<'a, [DenseVector]>),
}

impl<'a> Default for CowVector<'a> {
    fn default() -> Self {
        CowVector::Dense(Cow::Owned(Vec::new()))
    }
}

type TinyMap<'a> = tiny_map::TinyMap<CowKey<'a>, CowVector<'a>>;

#[derive(Clone, Default, Debug, PartialEq)]
pub struct NamedVectors<'a> {
    map: TinyMap<'a>,
}

impl<'a> CowVector<'a> {
    pub fn to_owned(self) -> Vector {
        match self {
            CowVector::Dense(v) => Vector::Dense(v.into_owned()),
            CowVector::Sparse(v) => Vector::Sparse(v.into_owned()),
            CowVector::MultiDense(v) => Vector::MultiDense(v.into_owned()),
        }
    }

    pub fn as_vec_ref(&self) -> VectorRef {
        match self {
            CowVector::Dense(v) => VectorRef::Dense(v.as_ref()),
            CowVector::Sparse(v) => VectorRef::Sparse(v.as_ref()),
            CowVector::MultiDense(v) => VectorRef::MultiDense(v.as_ref()),
        }
    }
}

impl<'a> From<Vector> for CowVector<'a> {
    fn from(v: Vector) -> Self {
        match v {
            Vector::Dense(v) => CowVector::Dense(Cow::Owned(v)),
            Vector::Sparse(v) => CowVector::Sparse(Cow::Owned(v)),
            Vector::MultiDense(v) => CowVector::MultiDense(Cow::Owned(v)),
        }
    }
}

impl<'a> From<SparseVector> for CowVector<'a> {
    fn from(v: SparseVector) -> Self {
        CowVector::Sparse(Cow::Owned(v))
    }
}

impl<'a> From<DenseVector> for CowVector<'a> {
    fn from(v: DenseVector) -> Self {
        CowVector::Dense(Cow::Owned(v))
    }
}

impl<'a> From<MultiDenseVector> for CowVector<'a> {
    fn from(v: MultiDenseVector) -> Self {
        CowVector::MultiDense(Cow::Owned(v))
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

impl<'a> From<&'a [DenseVector]> for CowVector<'a> {
    fn from(v: &'a [DenseVector]) -> Self {
        CowVector::MultiDense(Cow::Borrowed(v))
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

impl<'a> From<VectorRef<'a>> for CowVector<'a> {
    fn from(v: VectorRef<'a>) -> Self {
        match v {
            VectorRef::Dense(v) => CowVector::Dense(Cow::Borrowed(v)),
            VectorRef::Sparse(v) => CowVector::Sparse(Cow::Borrowed(v)),
            VectorRef::MultiDense(v) => CowVector::MultiDense(Cow::Borrowed(v)),
        }
    }
}

impl<'a> NamedVectors<'a> {
    pub fn from_ref(key: &'a str, value: VectorRef<'a>) -> Self {
        let mut map = TinyMap::new();
        map.insert(
            Cow::Borrowed(key),
            match value {
                VectorRef::Dense(v) => CowVector::Dense(Cow::Borrowed(v)),
                VectorRef::Sparse(v) => CowVector::Sparse(Cow::Borrowed(v)),
                VectorRef::MultiDense(v) => CowVector::MultiDense(Cow::Borrowed(v)),
            },
        );
        Self { map }
    }

    pub fn from<const N: usize>(arr: [(String, DenseVector); N]) -> Self {
        NamedVectors {
            map: arr
                .into_iter()
                .map(|(k, v)| (CowKey::from(k), CowVector::Dense(Cow::Owned(v))))
                .collect(),
        }
    }

    pub fn from_map(map: HashMap<String, Vector>) -> Self {
        Self {
            map: map
                .into_iter()
                .map(|(k, v)| (CowKey::from(k), v.into()))
                .collect(),
        }
    }

    pub fn from_map_ref(map: &'a HashMap<String, DenseVector>) -> Self {
        Self {
            map: map
                .iter()
                .map(|(k, v)| (CowKey::from(k), CowVector::Dense(Cow::Borrowed(v))))
                .collect(),
        }
    }

    pub fn insert(&mut self, name: String, vector: Vector) {
        self.map.insert(
            CowKey::Owned(name),
            match vector {
                Vector::Dense(v) => CowVector::Dense(Cow::Owned(v)),
                Vector::Sparse(v) => CowVector::Sparse(Cow::Owned(v)),
                Vector::MultiDense(v) => CowVector::MultiDense(Cow::Owned(v)),
            },
        );
    }

    pub fn insert_ref(&mut self, name: &'a str, vector: VectorRef<'a>) {
        self.map.insert(
            CowKey::Borrowed(name),
            match vector {
                VectorRef::Dense(v) => CowVector::Dense(Cow::Borrowed(v)),
                VectorRef::Sparse(v) => CowVector::Sparse(Cow::Borrowed(v)),
                VectorRef::MultiDense(v) => CowVector::MultiDense(Cow::Borrowed(v)),
            },
        );
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.map.iter().map(|(k, _)| k.as_ref())
    }

    pub fn into_owned_map(self) -> HashMap<String, Vector> {
        self.map
            .into_iter()
            .map(|(k, v)| (k.into_owned(), v.to_owned()))
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, VectorRef<'_>)> {
        self.map.iter().map(|(k, v)| (k.as_ref(), v.as_vec_ref()))
    }

    pub fn get(&self, key: &str) -> Option<VectorRef<'_>> {
        self.map.get(key).map(|v| v.as_vec_ref())
    }

    pub fn preprocess<F>(&mut self, distance_map: F)
    where
        F: Fn(&str) -> Distance,
    {
        for (name, vector) in self.map.iter_mut() {
            let distance = distance_map(name);
            match vector {
                CowVector::Dense(v) => {
                    let preprocessed_vector = distance.preprocess_vector(v.to_vec());
                    *vector = CowVector::Dense(Cow::Owned(preprocessed_vector))
                }
                CowVector::Sparse(v) => {
                    // sort by indices to enable faster dot product and overlap checks
                    v.to_mut().sort_by_indices();
                }
                CowVector::MultiDense(v) => {
                    for dense_vector in v.to_mut() {
                        let preprocessed_vector = distance.preprocess_vector(dense_vector.to_vec());
                        *dense_vector = preprocessed_vector;
                    }
                }
            }
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
