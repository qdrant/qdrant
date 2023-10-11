use std::borrow::Cow;
use std::collections::HashMap;

use sparse::common::sparse_vector::SparseVector;

use super::tiny_map;
use super::vectors::{VectorElementType, VectorOrSparse, VectorOrSparseRef, DEFAULT_VECTOR_NAME};
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
    pub fn to_owned(self) -> VectorOrSparse {
        match self {
            CowValue::Vector(v) => VectorOrSparse::Vector(v.into_owned()),
            CowValue::Sparse(v) => VectorOrSparse::Sparse(v.into_owned()),
        }
    }
}

impl<'a> NamedVectors<'a> {
    pub fn from_ref(key: &'a str, value: VectorOrSparseRef<'a>) -> Self {
        let mut map = TinyMap::new();
        map.insert(
            Cow::Borrowed(key),
            match value {
                VectorOrSparseRef::Vector(v) => CowValue::Vector(Cow::Borrowed(v)),
                VectorOrSparseRef::Sparse(v) => CowValue::Sparse(Cow::Borrowed(v)),
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

    pub fn from_mixed_map(map: HashMap<String, VectorOrSparse>) -> Self {
        Self {
            map: map
                .into_iter()
                .map(|(k, v)| {
                    (
                        CowKey::from(k),
                        match v {
                            VectorOrSparse::Vector(v) => CowValue::Vector(Cow::Owned(v)),
                            VectorOrSparse::Sparse(v) => CowValue::Sparse(Cow::Owned(v)),
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

    pub fn from_mixed_map_ref(map: &'a HashMap<String, VectorOrSparse>) -> Self {
        Self {
            map: map
                .iter()
                .map(|(k, v)| {
                    (
                        CowKey::from(k),
                        match v {
                            VectorOrSparse::Vector(v) => CowValue::Vector(Cow::Borrowed(v)),
                            VectorOrSparse::Sparse(v) => CowValue::Sparse(Cow::Borrowed(v)),
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn insert(&mut self, name: String, vector: VectorOrSparse) {
        self.map.insert(
            CowKey::Owned(name),
            match vector {
                VectorOrSparse::Vector(v) => CowValue::Vector(Cow::Owned(v)),
                VectorOrSparse::Sparse(v) => CowValue::Sparse(Cow::Owned(v)),
            },
        );
    }

    pub fn insert_ref(&mut self, name: &'a str, vector: VectorOrSparseRef<'a>) {
        self.map.insert(
            CowKey::Borrowed(name),
            match vector {
                VectorOrSparseRef::Vector(v) => CowValue::Vector(Cow::Borrowed(v)),
                VectorOrSparseRef::Sparse(v) => CowValue::Sparse(Cow::Borrowed(v)),
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

    pub fn into_default_vector(mut self) -> Option<VectorOrSparse> {
        self.map.get_mut(DEFAULT_VECTOR_NAME).map(|src| match src {
            CowValue::Vector(src) => VectorOrSparse::Vector(std::mem::take(src).into_owned()),
            CowValue::Sparse(src) => VectorOrSparse::Sparse(std::mem::take(src).into_owned()),
        })
    }

    pub fn into_owned_map(self) -> HashMap<String, VectorOrSparse> {
        self.map
            .into_iter()
            .map(|(k, v)| {
                (
                    k.into_owned(),
                    match v {
                        CowValue::Vector(src) => VectorOrSparse::Vector(src.into_owned()),
                        CowValue::Sparse(src) => VectorOrSparse::Sparse(src.into_owned()),
                    },
                )
            })
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, VectorOrSparseRef<'_>)> {
        self.map.iter().map(|(k, v)| {
            (
                k.as_ref(),
                match v {
                    CowValue::Vector(v) => VectorOrSparseRef::Vector(v.as_ref()),
                    CowValue::Sparse(v) => VectorOrSparseRef::Sparse(v.as_ref()),
                },
            )
        })
    }

    pub fn get(&self, key: &str) -> Option<VectorOrSparseRef<'_>> {
        match self.map.get(key) {
            Some(CowValue::Vector(v)) => Some(VectorOrSparseRef::Vector(v.as_ref())),
            Some(CowValue::Sparse(v)) => Some(VectorOrSparseRef::Sparse(v.as_ref())),
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
