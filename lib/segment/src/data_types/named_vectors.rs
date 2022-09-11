use std::borrow::Cow;
use std::collections::HashMap;

use super::vectors::{VectorElementType, DEFAULT_VECTOR_NAME};

type CowKey<'a> = Cow<'a, str>;
type CowValue<'a> = Cow<'a, [VectorElementType]>;
type HashMapType<'a> = HashMap<CowKey<'a>, CowValue<'a>>;

#[derive(Clone, PartialEq, Default)]
pub struct NamedVectors<'a> {
    pub map: HashMapType<'a>,
}

impl<'a> NamedVectors<'a> {
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

    pub fn insert(&mut self, name: String, vector: Vec<VectorElementType>) {
        self.map.insert(CowKey::from(name), CowValue::from(vector));
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

    pub fn keys(&self) -> Vec<String> {
        self.map
            .iter()
            .map(|(k, _)| k.clone().into_owned())
            .collect()
    }

    pub fn into_default_vector(mut self) -> Option<Vec<VectorElementType>> {
        let mut result = CowValue::default();
        let src = self.map.get_mut(DEFAULT_VECTOR_NAME)?;
        std::mem::swap(&mut result, src);
        Some(result.into_owned())
    }

    pub fn into_owned_map(self) -> HashMap<String, Vec<VectorElementType>> {
        self.map
            .into_iter()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect()
    }
}

#[allow(clippy::into_iter_on_ref)]
impl<'a> IntoIterator for &'a NamedVectors<'a> {
    type Item = (&'a CowKey<'a>, &'a CowValue<'a>);

    type IntoIter = std::collections::hash_map::Iter<'a, CowKey<'a>, CowValue<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        let map: &'a HashMapType = &self.map;
        map.into_iter()
    }
}

#[allow(clippy::into_iter_on_ref)]
impl<'a> IntoIterator for &'a mut NamedVectors<'a> {
    type Item = (&'a CowKey<'a>, &'a mut CowValue<'a>);

    type IntoIter = std::collections::hash_map::IterMut<'a, CowKey<'a>, CowValue<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        let map: &'a mut HashMapType = &mut self.map;
        map.into_iter()
    }
}

impl<'a> IntoIterator for NamedVectors<'a> {
    type Item = (CowKey<'a>, CowValue<'a>);

    type IntoIter = std::collections::hash_map::IntoIter<CowKey<'a>, CowValue<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}
