use std::collections::HashMap;
use std::{borrow, iter, mem, slice};

use tinyvec::TinyVec;

pub const CAPACITY: usize = 3;

#[derive(Clone, Debug, Default)]
pub struct TinyMap<K, V>
where
    K: Default,
    V: Default,
{
    list: TinyVec<[(K, V); CAPACITY]>,
}

impl<K, V> TinyMap<K, V>
where
    K: Default,
    V: Default,
{
    pub fn new() -> Self {
        Self {
            list: TinyVec::new(),
        }
    }

    pub fn insert_no_check(&mut self, key: K, value: V) {
        self.list.push((key, value));
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    pub fn iter(&self) -> slice::Iter<'_, (K, V)> {
        self.list.iter()
    }

    pub fn iter_mut(&mut self) -> slice::IterMut<'_, (K, V)> {
        self.list.iter_mut()
    }

    pub fn clear(&mut self) {
        self.list.clear();
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.list.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.list.iter().map(|(_, v)| v)
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.list.iter_mut().map(|(_, v)| v)
    }
}

impl<K, V> TinyMap<K, V>
where
    K: Default + Eq,
    V: Default,
{
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let found = self.list.iter_mut().find(|(k, _)| k == &key);
        match found {
            Some((_, v)) => {
                let old = mem::replace(v, value);
                Some(old)
            }
            None => {
                self.list.push((key, value));
                None
            }
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: borrow::Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.list
            .iter()
            .find(|(k, _)| k.borrow() == key)
            .map(|(_, v)| v)
    }

    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: borrow::Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.list
            .iter_mut()
            .find(|(k, _)| k.borrow() == key)
            .map(|(_, v)| v)
    }

    /// Returns the (mutable) value assigned to `key`, if such an entry exists.
    /// Otherwise the default value for `V` is inserted and returned as mutable reference.
    ///
    /// This method automatically clones `key` if required. Therefore Q must implement `ToOwned<Owned = K>`.
    pub fn get_or_insert_default<Q>(&mut self, key: &Q) -> &mut V
    where
        V: Sized,
        K: borrow::Borrow<Q>,
        Q: Eq + ToOwned<Owned = K> + ?Sized,
    {
        // Try to locate an existing entry for the key.
        let existing_position = self.list.iter().position(|(k, _)| k.borrow() == key);

        // Insert default value if not existing and get the new index.
        let index = match existing_position {
            Some(existing_pos) => existing_pos,
            None => {
                let new_index = self.list.len();
                self.list.push((key.to_owned(), V::default()));
                new_index
            }
        };

        &mut self.list[index].1
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: borrow::Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let found = self.list.iter().position(|(k, _)| k.borrow() == key);
        match found {
            Some(i) => {
                let (_, v) = self.list.remove(i);
                Some(v)
            }
            None => None,
        }
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: borrow::Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.list.iter().any(|(k, _)| k.borrow() == key)
    }
}

impl<K, V> PartialEq for TinyMap<K, V>
where
    K: Default + Eq,
    V: Default + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        for (k, v) in self.iter() {
            if other.get(k) != Some(v) {
                return false;
            }
        }

        true
    }
}

impl<K, V> IntoIterator for TinyMap<K, V>
where
    K: Default,
    V: Default,
{
    type Item = (K, V);

    type IntoIter = tinyvec::TinyVecIterator<[(K, V); CAPACITY]>;

    fn into_iter(self) -> Self::IntoIter {
        self.list.into_iter()
    }
}

impl<K, V> From<TinyMap<K, V>> for HashMap<K, V>
where
    K: Default + std::hash::Hash + Eq,
    V: Default,
{
    #[inline]
    fn from(value: TinyMap<K, V>) -> Self {
        value.into_iter().collect()
    }
}

impl<K, V> FromIterator<(K, V)> for TinyMap<K, V>
where
    K: Default,
    V: Default,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            list: iter::FromIterator::from_iter(iter),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tiny_map_basic_operations() {
        // Create dummy data
        let mut map: TinyMap<String, String> = TinyMap::new();
        let key = "key".to_string();
        let mut value = "value".to_string();
        let key2 = "key2".to_string();
        let value2 = "value2".to_string();
        let key3 = "key3".to_string();
        let value3 = "value3".to_string();

        // Test insert
        map.insert(key.clone(), value.clone());
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&key), Some(&value));

        // Test insert_no_check
        map.insert_no_check(key2.clone(), value2.clone());
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&key2), Some(&value2));

        // Test insert overwrite
        map.insert(key.clone(), value3.clone());
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&key), Some(&value3));

        // Test remove
        map.remove(&key);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&key), None);

        // Test get_mut
        map.clear();
        map.insert(key.clone(), value.clone());
        assert_eq!(map.get_mut(&key), Some(&mut value));
        map.get_mut(&key).unwrap().clone_from(&value3);
        assert_eq!(map.get(&key), Some(&value3));

        // Test iter
        map.clear();
        map.insert(key2.clone(), value2.clone());
        map.insert(key3.clone(), value3.clone());
        let mut iter = map.iter();
        assert_eq!(iter.next(), Some(&(key2, value2)));
        assert_eq!(iter.next(), Some(&(key3, value3)));
    }

    #[test]
    fn test_tiny_map_get_or_insert() {
        let mut map: TinyMap<String, usize> = TinyMap::new();

        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 2);
        assert_eq!(map.len(), 2);

        assert_eq!(*map.get_or_insert_default("a"), 1);
        assert_eq!(map.len(), 2);

        assert_eq!(*map.get_or_insert_default("b"), 2);
        assert_eq!(map.len(), 2);

        assert_eq!(*map.get_or_insert_default("c"), 0);
        assert_eq!(map.len(), 3);

        let mut map: TinyMap<usize, usize> = TinyMap::new();
        map.insert(1, 1);
        map.insert(2, 4);
        assert_eq!(map.len(), 2);

        assert_eq!(*map.get_or_insert_default(&1), 1);
        assert_eq!(map.len(), 2);

        assert_eq!(*map.get_or_insert_default(&2), 4);
        assert_eq!(map.len(), 2);

        assert_eq!(*map.get_or_insert_default(&3), 0);
        assert_eq!(map.len(), 3);

        *map.get_or_insert_default(&3) = 6;
        assert_eq!(map.len(), 3); // This call should not add an additional item.

        assert_eq!(map.get(&3), Some(&6));
    }
}
