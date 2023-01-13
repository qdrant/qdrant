use std::borrow::Cow;
use std::collections::HashMap;

use super::tiny_map;
use super::vectors::{VectorElementType, DEFAULT_VECTOR_NAME};

type CowKey<'a> = Cow<'a, str>;
type CowValue<'a> = Cow<'a, [VectorElementType]>;
type TinyMap<'a> = tiny_map::TinyMap<CowKey<'a>, CowValue<'a>>;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NamedVectors<'a> {
    map: TinyMap<'a>,
}

impl<'a> NamedVectors<'a> {
    pub fn from_ref(key: &'a str, value: &'a [VectorElementType]) -> Self {
        let mut map = TinyMap::new();
        map.insert(Cow::Borrowed(key), Cow::Borrowed(value));
        Self { map }
    }

    pub fn from<const N: usize>(arr: [(String, Vec<VectorElementType>); N]) -> Self {
        NamedVectors {
            map: arr
                .into_iter()
                .map(|(k, v)| (CowKey::from(k), CowValue::from(v)))
                .collect(),
        }
    }

    pub fn from_map(map: HashMap<String, Vec<VectorElementType>>) -> Self {
        Self {
            map: map
                .into_iter()
                .map(|(k, v)| (CowKey::from(k), CowValue::from(v)))
                .collect(),
        }
    }

    pub fn from_map_ref(map: &'a HashMap<String, Vec<VectorElementType>>) -> Self {
        Self {
            map: map
                .iter()
                .map(|(k, v)| (CowKey::from(k), CowValue::from(v)))
                .collect(),
        }
    }

    pub fn insert(&mut self, name: String, vector: Vec<VectorElementType>) {
        self.map
            .insert(CowKey::Owned(name), CowValue::Owned(vector));
    }

    pub fn insert_ref(&mut self, name: &'a str, vector: &'a [VectorElementType]) {
        self.map
            .insert(CowKey::Borrowed(name), CowValue::Borrowed(vector));
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

    pub fn into_default_vector(mut self) -> Option<Vec<VectorElementType>> {
        self.map
            .get_mut(DEFAULT_VECTOR_NAME)
            .map(|src| std::mem::take(src).into_owned())
    }

    pub fn into_owned_map(self) -> HashMap<String, Vec<VectorElementType>> {
        self.map
            .into_iter()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &[VectorElementType])> {
        self.map.iter().map(|(k, v)| (k.as_ref(), v.as_ref()))
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
