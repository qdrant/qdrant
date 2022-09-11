use std::collections::hash_map::Keys;
use std::collections::HashMap;

use super::vectors::{VectorElementType, DEFAULT_VECTOR_NAME};

#[derive(Clone, PartialEq, Default)]
pub struct NamedVectors {
    pub map: HashMap<String, Vec<VectorElementType>>,
}

impl NamedVectors {
    pub fn from<const N: usize>(arr: [(String, Vec<VectorElementType>); N]) -> Self {
        NamedVectors {
            map: HashMap::from(arr),
        }
    }

    pub fn from_map(map: HashMap<String, Vec<VectorElementType>>) -> Self {
        Self { map }
    }

    pub fn insert(
        &mut self,
        name: String,
        vector: Vec<VectorElementType>,
    ) -> Option<Vec<VectorElementType>> {
        self.map.insert(name, vector)
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

    pub fn keys(&self) -> Keys<String, Vec<VectorElementType>> {
        self.map.keys()
    }

    pub fn into_default_vector(mut self) -> Option<Vec<VectorElementType>> {
        let mut result = Vec::new();
        let src = self.map.get_mut(DEFAULT_VECTOR_NAME)?;
        std::mem::swap(&mut result, src);
        Some(result)
    }

    pub fn into_owned_map(self) -> HashMap<String, Vec<VectorElementType>> {
        self.map
    }
}

#[allow(clippy::into_iter_on_ref)]
impl<'a> IntoIterator for &'a NamedVectors {
    type Item = (&'a String, &'a Vec<VectorElementType>);

    type IntoIter = std::collections::hash_map::Iter<'a, String, Vec<VectorElementType>>;

    fn into_iter(self) -> Self::IntoIter {
        let map = &self.map;
        map.into_iter()
    }
}

#[allow(clippy::into_iter_on_ref)]
impl<'a> IntoIterator for &'a mut NamedVectors {
    type Item = (&'a String, &'a mut Vec<VectorElementType>);

    type IntoIter = std::collections::hash_map::IterMut<'a, String, Vec<VectorElementType>>;

    fn into_iter(self) -> Self::IntoIter {
        let map = &mut self.map;
        map.into_iter()
    }
}

impl IntoIterator for NamedVectors {
    type Item = (String, Vec<VectorElementType>);

    type IntoIter = std::collections::hash_map::IntoIter<String, Vec<VectorElementType>>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}
