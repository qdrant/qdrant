use tinyvec::TinyVec;

const CAPACITY: usize = 3;

pub struct TinyMap<K: Default, V: Default> {
    list: TinyVec<[(K, V); CAPACITY]>,
}

impl<K: Default + std::cmp::PartialEq, V: Default> TinyMap<K, V> {
    pub fn new() -> Self {
        Self {
            list: TinyVec::new(),
        }
    }

    pub fn insert_no_check(&mut self, key: K, value: V) {
        self.list.push((key, value));
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let found = self.list.iter_mut().find(|(k, _)| k == &key);
        match found {
            Some((_, v)) => {
                let old = std::mem::replace(v, value);
                Some(old)
            }
            None => {
                self.list.push((key, value));
                None
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.list.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.list.iter_mut().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let found = self.list.iter().position(|(k, _)| k == key);
        match found {
            Some(i) => {
                let (_, v) = self.list.remove(i);
                Some(v)
            }
            None => None,
        }
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, (K, V)> {
        self.list.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, (K, V)> {
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

    pub fn contains_key(&self, key: &K) -> bool {
        self.list.iter().any(|(k, _)| k == key)
    }
}

impl<K: Default + std::cmp::PartialEq, V: Default> Default for TinyMap<K, V> {
    fn default() -> Self {
        Self::new()
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
        *map.get_mut(&key).unwrap() = value3.clone();
        assert_eq!(map.get(&key), Some(&value3));

        // Test iter
        map.clear();
        map.insert(key2.clone(), value2.clone());
        map.insert(key3.clone(), value3.clone());
        let mut iter = map.iter();
        assert_eq!(iter.next(), Some(&(key2, value2)));
        assert_eq!(iter.next(), Some(&(key3, value3)));
    }
}
