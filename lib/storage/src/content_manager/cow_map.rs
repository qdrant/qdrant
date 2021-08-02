use core::mem;
use std::borrow::Borrow;
use std::collections::hash_map::Keys;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_epoch::{pin, Atomic, Owned};

/// Thread-safe lock-free implementation of hash table that re-creates an underlying `HashMap` on
/// each modification. Memory reclamation is implemented by epoch-based GC from `crossbeam-epoch`
/// crate (https://github.com/crossbeam-rs/crossbeam).
pub(crate) struct CopyOnWriteMap<K: Hash + Eq + Clone, V> {
    ptr: Atomic<Arc<HashMap<K, Arc<V>>>>,
}

impl<K: Hash + Eq + Clone, V> CopyOnWriteMap<K, V> {
    pub fn new(new_map: HashMap<K, Arc<V>>) -> Self {
        Self {
            ptr: Atomic::new(Arc::new(new_map)),
        }
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        self.load().get(key).cloned()
    }

    pub fn insert(&self, key: K, value: V) {
        let arc_value = Arc::new(value);
        self.cas_modify(|new_map| {
            new_map.insert(key.clone(), arc_value.clone());
        });
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<Arc<V>>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        self.cas_modify(|new_map| new_map.remove(key))
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        self.load().contains_key(key)
    }

    pub fn clone_keys(&self) -> Vec<K> {
        self.load().keys().cloned().collect()
    }

    pub fn visit_keys<F, R>(&self, visitor: F) -> R
    where
        F: Fn(Keys<K, Arc<V>>) -> R,
    {
        visitor(self.load().keys())
    }

    fn cas_modify<F, R>(&self, updater: F) -> R
    where
        F: Fn(&mut HashMap<K, Arc<V>>) -> R,
    {
        loop {
            let guard = &pin();
            let current_shared = self.ptr.load(Ordering::Acquire, guard);
            // Safety: After we pinned current thread it's safe to deref current shared pointer
            // since no de-allocations are allowed now.
            let current_map = unsafe { current_shared.deref() };
            let mut cloned_map = HashMap::clone(current_map);
            let update_result = updater(&mut cloned_map);
            match self.ptr.compare_exchange_weak(
                current_shared,
                Owned::new(Arc::new(cloned_map)),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => {
                    // Safety: We atomically successfully exchanged value. Only one thread proceeds to
                    // this point for particular `current_shared`, hence it's safe to schedule it for destroying.
                    unsafe { guard.defer_destroy(current_shared) }
                    guard.flush();
                    break update_result;
                }
                Err(_) => {
                    std::thread::yield_now();
                }
            }
        }
    }

    fn load(&self) -> Arc<HashMap<K, Arc<V>>> {
        let guard = &pin();
        let shared = self.ptr.load(Ordering::Acquire, guard);
        // Safety: After we pinned current thread it's safe to deref current shared pointer
        // since no de-allocations are allowed now.
        unsafe { shared.deref() }.clone()
    }
}

impl<K: Hash + Eq + Clone, V> Drop for CopyOnWriteMap<K, V> {
    fn drop(&mut self) {
        let replaced = mem::replace(&mut self.ptr, Atomic::null());
        // Safety: If we drop our instance it means no other threads can access it, so we can drop the inner value.
        unsafe {
            drop(replaced.into_owned());
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct Droppable(String);

    impl Drop for Droppable {
        fn drop(&mut self) {
            println!("dropping {:?}", self.0)
        }
    }

    #[test]
    fn insert_and_delete_works() {
        //given
        let cow_map = CopyOnWriteMap::new(HashMap::default());
        let key1 = "key1".to_owned();
        let key2 = "key2".to_owned();
        let value1 = Droppable("value1".to_owned());
        let value2 = Droppable("value2".to_owned());

        //when
        cow_map.insert(key1.clone(), value1.clone());
        cow_map.insert(key2.clone(), value2.clone());

        //then
        assert_eq!(*cow_map.get(&key1).unwrap(), value1);
        cow_map.remove(&key1);
        assert_eq!(cow_map.get(&key1), None);
        assert_eq!(*cow_map.get(&key2).unwrap(), value2);
    }

    #[test]
    fn keys_works() {
        //given
        let cow_map = CopyOnWriteMap::new(HashMap::default());
        let key1 = "key1".to_owned();
        let key2 = "key2".to_owned();
        let value1 = Droppable("value1".to_owned());
        let value2 = Droppable("value2".to_owned());
        cow_map.insert(key1.clone(), value1);
        cow_map.insert(key2.clone(), value2);

        //when
        let keys = cow_map.clone_keys();
        let keys2 = cow_map.clone_keys();
        cow_map.remove(&key1);
        cow_map.remove(&key2);

        //then
        assert_eq!(keys.contains(&&key1), true);
        assert_eq!(keys.contains(&&key2), true);
        assert_eq!(keys2.contains(&&key1), true);
        assert_eq!(keys2.contains(&&key2), true);

        assert_eq!(cow_map.clone_keys().is_empty(), true);
    }

    #[test]
    fn keys_lives_enough() {
        //given
        let cow_map = CopyOnWriteMap::new(HashMap::default());
        let key1 = "key1".to_owned();
        let value = Droppable("value1".to_owned());
        cow_map.insert(key1.clone(), value);

        //when
        let keys = cow_map.clone_keys();

        //then
        assert_eq!(keys.contains(&key1), true);
        cow_map.remove(&key1);
        drop(cow_map);
        assert_eq!(keys.contains(&key1), true);
    }

    #[test]
    fn contains_key_works() {
        //given
        let cow_map = CopyOnWriteMap::new(HashMap::default());
        let key1 = "key1".to_owned();
        let key2 = "key2".to_owned();
        let value = Droppable("value1".to_owned());

        //when
        cow_map.insert(key1.clone(), value);

        //then
        assert_eq!(cow_map.contains_key(&key1), true);
        assert_eq!(cow_map.contains_key(&key2), false);
        cow_map.remove(&key1);
        cow_map.remove(&key2);
        assert_eq!(cow_map.contains_key(&key1), false);
        assert_eq!(cow_map.contains_key(&key2), false);
    }

    #[test]
    fn collect_keys_via_visiting() {
        //given
        let cow_map = CopyOnWriteMap::new(HashMap::default());
        let key1 = "key1".to_owned();
        let key2 = "key2".to_owned();
        let value1 = Droppable("value1".to_owned());
        let value2 = Droppable("value2".to_owned());
        cow_map.insert(key1.clone(), value1);
        cow_map.insert(key2.clone(), value2);

        //when
        let collected = cow_map.visit_keys(|keys| keys.cloned().collect::<Vec<String>>());

        //then
        assert_eq!(collected.len(), 2);
        assert_eq!(collected.contains(&key1), true);
        assert_eq!(collected.contains(&key2), true);
    }
}
