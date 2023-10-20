use std::borrow::Cow;
use std::collections::HashMap;

use sparse::common::sparse_vector::SparseVector;

use super::tiny_map;
use super::vectors::{Vector, VectorElementType, VectorRef, DEFAULT_VECTOR_NAME};
use crate::types::Distance;

type CowKey<'a> = Cow<'a, str>;

#[derive(Clone, PartialEq, Debug)]
pub enum CowValue<'a> {
    Vector(Cow<'a, [VectorElementType]>),
    Sparse(Cow<'a, SparseVector>),
}

impl<'a> Default for CowValue<'a> {
    fn default() -> Self {
        CowValue::Vector(Cow::Owned(Vec::new()))
    }
}

type TinyMap<'a> = tiny_map::TinyMap<CowKey<'a>, CowValue<'a>>;

#[derive(Clone, Default, Debug, PartialEq)]
pub struct NamedVectors<'a> {
    map: TinyMap<'a>,
}

impl<'a> CowValue<'a> {
    pub fn to_owned(self) -> Vector {
        match self {
            CowValue::Vector(v) => Vector::Dense(v.into_owned()),
            CowValue::Sparse(v) => Vector::Sparse(v.into_owned()),
        }
    }
}

impl<'a> NamedVectors<'a> {
    pub fn from_ref(key: &'a str, value: VectorRef<'a>) -> Self {
        let mut map = TinyMap::new();
        map.insert(
            Cow::Borrowed(key),
            match value {
                VectorRef::Dense(v) => CowValue::Vector(Cow::Borrowed(v)),
                VectorRef::Sparse(v) => CowValue::Sparse(Cow::Borrowed(v)),
            },
        );
        Self { map }
    }

    pub fn from<const N: usize>(arr: [(String, Vec<VectorElementType>); N]) -> Self {
        NamedVectors {
            map: arr
                .into_iter()
                .map(|(k, v)| (CowKey::from(k), CowValue::Vector(Cow::Owned(v))))
                .collect(),
        }
    }

    pub fn from_map(map: HashMap<String, Vec<VectorElementType>>) -> Self {
        Self {
            map: map
                .into_iter()
                .map(|(k, v)| (CowKey::from(k), CowValue::Vector(Cow::Owned(v))))
                .collect(),
        }
    }

    pub fn from_sparse_map(map: HashMap<String, SparseVector>) -> Self {
        Self {
            map: map
                .into_iter()
                .map(|(k, v)| (CowKey::from(k), CowValue::Sparse(Cow::Owned(v))))
                .collect(),
        }
    }

    pub fn from_mixed_map(map: HashMap<String, Vector>) -> Self {
        Self {
            map: map
                .into_iter()
                .map(|(k, v)| {
                    (
                        CowKey::from(k),
                        match v {
                            Vector::Dense(v) => CowValue::Vector(Cow::Owned(v)),
                            Vector::Sparse(v) => CowValue::Sparse(Cow::Owned(v)),
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn from_map_ref(map: &'a HashMap<String, Vec<VectorElementType>>) -> Self {
        Self {
            map: map
                .iter()
                .map(|(k, v)| (CowKey::from(k), CowValue::Vector(Cow::Borrowed(v))))
                .collect(),
        }
    }

    pub fn from_sparse_map_ref(map: &'a HashMap<String, SparseVector>) -> Self {
        Self {
            map: map
                .iter()
                .map(|(k, v)| (CowKey::from(k), CowValue::Sparse(Cow::Borrowed(v))))
                .collect(),
        }
    }

    pub fn from_mixed_map_ref(map: &'a HashMap<String, Vector>) -> Self {
        Self {
            map: map
                .iter()
                .map(|(k, v)| {
                    (
                        CowKey::from(k),
                        match v {
                            Vector::Dense(v) => CowValue::Vector(Cow::Borrowed(v)),
                            Vector::Sparse(v) => CowValue::Sparse(Cow::Borrowed(v)),
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn insert(&mut self, name: String, vector: Vector) {
        self.map.insert(
            CowKey::Owned(name),
            match vector {
                Vector::Dense(v) => CowValue::Vector(Cow::Owned(v)),
                Vector::Sparse(v) => CowValue::Sparse(Cow::Owned(v)),
            },
        );
    }

    pub fn insert_ref(&mut self, name: &'a str, vector: VectorRef<'a>) {
        self.map.insert(
            CowKey::Borrowed(name),
            match vector {
                VectorRef::Dense(v) => CowValue::Vector(Cow::Borrowed(v)),
                VectorRef::Sparse(v) => CowValue::Sparse(Cow::Borrowed(v)),
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

    pub fn into_default_vector(mut self) -> Option<Vector> {
        self.map.get_mut(DEFAULT_VECTOR_NAME).map(|src| match src {
            CowValue::Vector(src) => Vector::Dense(std::mem::take(src).into_owned()),
            CowValue::Sparse(src) => Vector::Sparse(std::mem::take(src).into_owned()),
        })
    }

    pub fn into_owned_map(self) -> HashMap<String, Vector> {
        self.map
            .into_iter()
            .map(|(k, v)| {
                (
                    k.into_owned(),
                    match v {
                        CowValue::Vector(src) => Vector::Dense(src.into_owned()),
                        CowValue::Sparse(src) => Vector::Sparse(src.into_owned()),
                    },
                )
            })
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, VectorRef<'_>)> {
        self.map.iter().map(|(k, v)| {
            (
                k.as_ref(),
                match v {
                    CowValue::Vector(v) => VectorRef::Dense(v.as_ref()),
                    CowValue::Sparse(v) => VectorRef::Sparse(v.as_ref()),
                },
            )
        })
    }

    pub fn get(&self, key: &str) -> Option<VectorRef<'_>> {
        match self.map.get(key) {
            Some(CowValue::Vector(v)) => Some(VectorRef::Dense(v.as_ref())),
            Some(CowValue::Sparse(v)) => Some(VectorRef::Sparse(v.as_ref())),
            None => None,
        }
    }

    pub fn preprocess<F>(&mut self, distance_map: F)
    where
        F: Fn(&str) -> Option<Distance>,
    {
        for (name, vector) in self.map.iter_mut() {
            if let Some(distance) = distance_map(name) {
                match vector {
                    CowValue::Vector(v) => {
                        let preprocessed_vector = distance.preprocess_vector(v.to_vec());
                        *vector = CowValue::Vector(Cow::Owned(preprocessed_vector))
                    }
                    CowValue::Sparse(_) => {}
                }
            }
        }
    }
}

impl<'a> IntoIterator for NamedVectors<'a> {
    type Item = (CowKey<'a>, CowValue<'a>);

    type IntoIter =
        tinyvec::TinyVecIterator<[(CowKey<'a>, CowValue<'a>); super::tiny_map::CAPACITY]>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}
