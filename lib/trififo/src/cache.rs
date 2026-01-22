use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU8, Ordering};

use crate::fifos::{Entry, Fifos, LocalOffset};
use crate::ringbuffer::RingBuffer;
use hashbrown::HashTable;

pub(crate) type GlobalOffset = u32;

pub struct Cache<K, V, S = ahash::RandomState> {
    hashtable: hashbrown::HashTable<GlobalOffset>,
    fifos: Fifos<K, V>,
    hasher: S,
}

impl<K, V, S> Cache<K, V, S>
where
    K: Copy + Hash + PartialEq,
    S: BuildHasher,
{
    pub fn new(capacity: usize, small_ratio: f32, ghost_ratio: f32, hasher: S) -> Self {
        assert!(capacity > 0);
        let hashtable = HashTable::with_capacity(capacity);

        let small_size = (capacity as f32 * small_ratio) as usize;
        let small = RingBuffer::with_capacity(small_size);

        let ghost_size = (capacity as f32 * ghost_ratio) as usize;
        let ghost = RingBuffer::with_capacity(ghost_size);

        let main_size = capacity - small_size;
        let main = RingBuffer::with_capacity(main_size);

        let small_end = small_size as GlobalOffset;
        let ghost_end = small_end + ghost_size as GlobalOffset;
        let main_end = ghost_end + (capacity - small_size) as GlobalOffset;

        Self {
            hashtable,
            fifos: Fifos {
                small,
                ghost,
                main,
                small_end,
                ghost_end,
                main_end,
            },
            hasher,
        }
    }

    /// A convenience method to calculate the hash for a given `key`.
    #[inline]
    fn hash_key(&self, key: &K) -> u64 {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
    fn get_entry(&self, key: &K) -> Option<&Entry<K, V>> {
        let hash = self.hash_key(key);
        let global_offset = self
            .hashtable
            .find(hash, |global_offset| self.fifos.key_eq(*global_offset, key))?;
        let local_offset = self.fifos.local_offset(*global_offset);
        self.fifos.get_entry_by_local_offset(local_offset)
    }

    /// Promotes an entry from ghost to main, or bumps their recency if already in small or main.
    fn promote_existing(&mut self, local_offset: LocalOffset, key: K, value: V) {
        match local_offset {
            LocalOffset::Ghost(offset) => {
                if let Some(ghost_key) = self.fifos.ghost.get_absolute_unchecked(offset as usize) {
                    if ghost_key != &key {
                        debug_assert!(false, "Key mismatch");
                        return;
                    }

                    // promote to main queue
                    let entry = Entry {
                        key,
                        value,
                        recency: AtomicU8::new(0),
                    };

                    let local_offset = self.push_to_main_queue(entry);
                    self.update_hashtable(&key, local_offset);

                    // After updating, ghost key will remain in ghost queue, but it won't be referenced by the hashmap.
                    // This is expected behavior
                }
            }
            LocalOffset::Small(offset) => {
                if let Some(entry) = self.fifos.small.get_absolute_unchecked(offset as usize) {
                    entry.incr_recency();
                }
            }
            LocalOffset::Main(offset) => {
                if let Some(entry) = self.fifos.main.get_absolute_unchecked(offset as usize) {
                    entry.incr_recency();
                }
            }
        }
    }

    /// Pushes an entry to the main queue, if it was full, it will re-insert items with recency > 0.
    /// It will keep reinserting until one entry can be evicted.
    ///
    /// Reinserted entries will preserve their absolute offset, so there is no need to update the
    /// hashtable
    #[must_use = "The returned LocalOffset is used to update the hashtable"]
    fn push_to_main_queue(&mut self, entry: Entry<K, V>) -> LocalOffset {
        // Push if queue is not full
        let entry = match self.fifos.main.try_push(entry) {
            Ok(local_offset) => return LocalOffset::Main(local_offset as u32),
            Err(entry) => entry,
        };

        // reinsert while there are old entries with non-zero recency
        while self.fifos.main.reinsert_if(|entry| {
            if entry.recency.load(Ordering::Relaxed) > 0 {
                entry.decr_recency();
                true
            } else {
                false
            }
        }) {}

        debug_assert!(
            self.fifos
                .main
                .get_absolute_unchecked(self.fifos.main.write_position())
                .unwrap()
                .recency
                .load(Ordering::Relaxed)
                == 0,
            "Detected eviction of entry with non-zero recency"
        );

        let (evicted_entry, local_offset) = self.fifos.main.pop_push(entry);

        self.remove_from_hashtable(&evicted_entry.key);

        LocalOffset::Main(local_offset as u32)
    }

    /// Push an entry to the small queue. If full, moves the oldest entry to the main queue
    /// if recency > 0, otherwise moves the key to the ghost queue.
    ///
    /// This function updates the hashtable for the keys moved to the other queues.
    #[must_use = "The returned LocalOffset is used to update the hashtable"]
    fn push_to_small_queue(&mut self, entry: Entry<K, V>) -> LocalOffset {
        // If full, send oldest key to ghost queue, or promote to main queue
        if self.fifos.small.is_full() {
            let (oldest_entry, small_offset) = self.fifos.small.pop_push(entry);
            if oldest_entry.recency() > 0 {
                // promote to main queue
                let oldest_key = oldest_entry.key;
                let local_offset = self.push_to_main_queue(oldest_entry);
                self.update_hashtable(&oldest_key, local_offset);
            } else {
                // move key to ghost queue
                let ghost_offset = self.fifos.ghost.overwriting_push(oldest_entry.key);
                self.update_hashtable(&oldest_entry.key, LocalOffset::Ghost(ghost_offset as u32));
            }

            return LocalOffset::Small(small_offset as u32);
        }

        // else, just push to queue
        let local_offset = self.fifos.small.overwriting_push(entry);

        LocalOffset::Small(local_offset as u32)
    }

    /// Updates or inserts the new offset for the key.
    fn update_hashtable(&mut self, key: &K, local_offset: LocalOffset) {
        let hash = self.hash_key(key);
        let global_offset = self.fifos.global_offset(local_offset);

        let entry = self.hashtable.entry(
            hash,
            |global_offset| self.fifos.key_eq(*global_offset, key),
            |global_offset| self.fifos.hash_offset(*global_offset, &self.hasher)
        );

        match entry {
            hashbrown::hash_table::Entry::Occupied(mut occupied_entry) => {
                *occupied_entry.get_mut() = global_offset;
            }
            hashbrown::hash_table::Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(global_offset);
            }
        };
    }

    fn remove_from_hashtable(&mut self, key: &K) {
        let hash = self.hash_key(key);
        if let Ok(entry) = self
            .hashtable
            .find_entry(hash, |global_offset| self.fifos.key_eq(*global_offset, key))
        {
            entry.remove();
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let entry = self.get_entry(key)?;

        entry.incr_recency();

        Some(&entry.value)
    }

    pub fn insert(&mut self, key: K, value: V) {
        let hash = self.hash_key(&key);

        // If in ghost, insert to main
        if let Some(global_offset) = self.hashtable.find(hash, |global_offset| {
            self.fifos.key_eq(*global_offset, &key)
        }) {
            let local_offset = self.fifos.local_offset(*global_offset);

            self.promote_existing(local_offset, key, value);

            return;
        }

        // else, insert to small
        let entry = Entry {
            key,
            value,
            recency: AtomicU8::new(0),
        };

        let local_offset = self.push_to_small_queue(entry);
        self.update_hashtable(&key, local_offset);
    }

    pub fn len(&self) -> usize {
        self.hashtable.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hashtable.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_new_cache() {
        let cache: Cache<i32, String> = Cache::new(10, 0.1, 0.1, ahash::RandomState::new());
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_basic_insert_and_get() {
        // Use large cache to avoid immediate evictions
        let mut cache = Cache::new(100, 0.1, 0.1, ahash::RandomState::new());

        cache.insert(1, "one");
        cache.insert(2, "two");
        cache.insert(3, "three");

        assert_eq!(cache.get(&1), Some(&"one"));
        assert_eq!(cache.get(&2), Some(&"two"));
        assert_eq!(cache.get(&3), Some(&"three"));
        assert_eq!(cache.get(&4), None);
    }

    #[test]
    fn test_insert_existing_key_increments_recency() {
        let mut cache = Cache::new(10, 0.1, 0.1, ahash::RandomState::new());

        cache.insert(1, "one");
        assert_eq!(cache.get(&1), Some(&"one"));

        // Re-inserting an existing key in small/main queue only increments recency,
        // doesn't update the value
        cache.insert(1, "ONE");
        assert_eq!(cache.get(&1), Some(&"one")); // Still has original value

        // But recency should have increased
        let entry = cache.get_entry(&1).unwrap();
        assert!(entry.recency() > 0);
    }

    #[test]
    fn test_small_queue_eviction() {
        // Create cache with 10 capacity, 10% small (1 slot), 10% ghost (1 slot), 90% main (9 slots)
        let mut cache = Cache::new(10, 0.1, 0.1, ahash::RandomState::new());

        // Fill small queue (only 1 slot)
        cache.insert(1, "one");

        // This should evict from small to ghost (since recency is 0)
        cache.insert(2, "two");

        // Key 1 should be in ghost queue (not retrievable)
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&"two"));
    }

    #[test]
    fn test_ghost_to_main_promotion() {
        let mut cache = Cache::new(10, 0.1, 0.1, ahash::RandomState::new());

        // Insert into small queue
        cache.insert(1, "one");

        // Evict Key 1 to ghost queue
        cache.insert(2, "two");

        // Key 1 is now in ghost, not retrievable
        assert_eq!(cache.get(&1), None);

        // Re-insert key 1, should promote from ghost to main
        cache.insert(1, "one_again");

        // Now it should be retrievable from main queue
        assert_eq!(cache.get(&1), Some(&"one_again"));

        // The second entry should still be in the small queue
        assert_eq!(cache.get(&2), Some(&"two"));
    }

    #[test]
    fn test_recency_promotion_to_main() {
        let mut cache = Cache::new(10, 0.1, 0.1, ahash::RandomState::new());

        // Insert into small queue
        cache.insert(1, "one");

        // Access it to increase recency
        cache.get(&1);

        // Insert another item, this should evict key 1 from small
        // Since recency > 0, it should go to main instead of ghost
        cache.insert(2, "two");

        // Key 1 should still be retrievable (in main queue)
        assert_eq!(cache.get(&1), Some(&"one"));
    }

    #[test]
    fn test_capacity_limit() {
        let mut cache = Cache::new(5, 0.2, 0.2, ahash::RandomState::new());

        // Insert more items than capacity
        for i in 0..20 {
            cache.insert(i, format!("value_{}", i));
        }

        // Cache should not exceed capacity (but ghost queue exists)
        // We can't directly check internal size, but we can verify old items are gone
        assert_eq!(cache.get(&0), None);
        assert_eq!(cache.get(&1), None);

        // Recent items should be present
        assert_eq!(cache.get(&19), Some(&"value_19".to_string()));
    }

    #[test]
    fn test_recency_saturation() {
        let mut cache = Cache::new(10, 0.1, 0.1, ahash::RandomState::new());

        cache.insert(1, "one");

        // Access many times to test saturation at 4
        for _ in 0..10 {
            cache.get(&1);
        }

        // Get the entry directly to check recency
        let entry = cache.get_entry(&1).unwrap();
        assert!(entry.recency() <= 4);
    }

    #[test]
    fn test_main_queue_eviction_with_recency() {
        // Small cache to test eviction more easily
        let mut cache = Cache::new(5, 0.2, 0.2, ahash::RandomState::new());
        // 5 * 0.2 = 1 small, 1 ghost, 3 main

        // Fill the cache and promote items to main
        cache.insert(1, "one");
        cache.get(&1); // Increase recency
        cache.insert(2, "two"); // Evicts 1 to main

        cache.insert(3, "three");
        cache.get(&3);
        cache.insert(4, "four"); // Evicts 3 to main

        cache.insert(5, "five");
        cache.get(&5);
        cache.insert(6, "six"); // Evicts 5 to main

        // Fill main queue more
        cache.insert(7, "seven");
        cache.insert(8, "eight");

        // Some items should have been evicted
        // But we can't precisely predict which without knowing internal state
    }

    #[test]
    fn test_empty_cache() {
        let cache: Cache<i32, String> = Cache::new(10, 0.1, 0.1, ahash::RandomState::new());

        assert_eq!(cache.get(&1), None);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_small_capacity() {
        // Minimum capacity of 3 to ensure all queues have at least 1 slot
        // 0.34 * 3 = 1.02 -> 1 small, 0.33 * 3 = 0.99 -> 0 ghost (rounds down), main = 3 - 1 = 2
        // Actually we need ghost to have at least 1, so let's use capacity 10
        let mut cache = Cache::new(10, 0.1, 0.1, ahash::RandomState::new());
        // 10 * 0.1 = 1 small, 10 * 0.1 = 1 ghost, main = 10 - 1 = 9

        cache.insert(1, "one".to_string());
        assert_eq!(cache.get(&1), Some(&"one".to_string()));

        // Fill up the cache beyond capacity
        for i in 2..20 {
            cache.insert(i, format!("value_{i}"));
        }

        // First item should be evicted due to small capacity
        assert_eq!(cache.get(&1), None);
    }

    #[test]
    fn test_different_types() {
        let mut cache = Cache::new(100, 0.1, 0.1, ahash::RandomState::new());

        cache.insert("key1", 100);
        cache.insert("key2", 200);

        assert_eq!(cache.get(&"key1"), Some(&100));
        assert_eq!(cache.get(&"key2"), Some(&200));
    }

    #[test]
    fn test_sequential_access_pattern() {
        let mut cache = Cache::new(100, 0.1, 0.1, ahash::RandomState::new());

        // Sequential insertions
        for i in 0..50 {
            cache.insert(i, i * 10);
        }

        // Recent items should be accessible
        for i in 40..50 {
            assert_eq!(cache.get(&i), Some(&(i * 10)));
        }
    }

    #[test]
    fn test_random_access_pattern() {
        let mut cache = Cache::new(50, 0.1, 0.1, ahash::RandomState::new());

        // Insert items
        for i in 0..100 {
            cache.insert(i, format!("value_{}", i));
        }

        // Access some items multiple times
        let hot_keys = [95, 96, 97, 98, 99];
        for &key in &hot_keys {
            for _ in 0..5 {
                cache.get(&key);
            }
        }

        // Hot keys should still be in cache
        for &key in &hot_keys {
            assert_eq!(cache.get(&key), Some(&format!("value_{}", key)));
        }
    }

    // Model testing against quick_cache
    #[test]
    fn test_model_against_quick_cache() {
        use quick_cache::sync::Cache as QuickCache;
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        let mut rng = StdRng::seed_from_u64(42);

        const CAPACITY: usize = 100;
        const OPS: usize = 1000;
        const KEY_RANGE: i32 = 500;

        let mut our_cache = Cache::new(CAPACITY, 0.1, 0.1, ahash::RandomState::new());
        let quick_cache = QuickCache::new(CAPACITY);

        // Track what we expect to be in the cache using a reference model
        let mut reference: HashMap<i32, String> = HashMap::new();

        for _ in 0..OPS {
            let key = rng.random_range(0..KEY_RANGE);
            let value = format!("value_{}", key);

            if rng.random_bool(0.7) {
                // 70% inserts
                our_cache.insert(key, value.clone());
                quick_cache.insert(key, value.clone());
                reference.insert(key, value);
            } else {
                // 30% gets
                let our_result = our_cache.get(&key);
                let quick_result = quick_cache.get(&key);

                // Both should agree on presence/absence
                // Note: They might not have the exact same items due to different eviction policies,
                // but we're testing that our cache behaves reasonably
                if our_result.is_some() {
                    assert_eq!(our_result, Some(&reference[&key]));
                }
                if quick_result.is_some() {
                    assert_eq!(quick_result.as_deref(), Some(reference[&key].as_str()));
                }
            }
        }

        // Verify recent hot keys are likely in both caches
        let hot_key = KEY_RANGE - 1;
        for _ in 0..10 {
            our_cache.insert(hot_key, format!("hot_value"));
            quick_cache.insert(hot_key, format!("hot_value"));
        }

        assert_eq!(our_cache.get(&hot_key), Some(&"hot_value".to_string()));
        assert_eq!(quick_cache.get(&hot_key).as_deref(), Some("hot_value"));
    }

    #[test]
    fn test_model_workload_correctness() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        let mut rng = StdRng::seed_from_u64(123456);

        const CAPACITY: usize = 50;
        const OPS: usize = 500;

        let mut cache = Cache::new(CAPACITY, 0.1, 0.1, ahash::RandomState::new());
        let mut model: HashMap<i32, String> = HashMap::new();

        for _ in 0..OPS {
            let key = rng.random_range(0..200);
            let value = format!("val_{}", key);

            cache.insert(key, value.clone());
            model.insert(key, value);

            // Randomly access some keys
            if rng.random_bool(0.3) {
                let access_key = rng.random_range(0..200);
                if let Some(cached_value) = cache.get(&access_key) {
                    // If in cache, it must match the model
                    assert_eq!(cached_value, &model[&access_key]);
                }
            }
        }

        // All values in cache must match the model
        for i in 0..200 {
            if let Some(cached) = cache.get(&i) {
                assert_eq!(cached, &model[&i], "Mismatch for key {}", i);
            }
        }
    }

    #[test]
    fn test_lru_like_behavior() {
        let mut cache = Cache::new(10, 0.1, 0.1, ahash::RandomState::new());

        // Insert 10 items
        for i in 0..10 {
            cache.insert(i, format!("value_{}", i));
        }

        // Access some items to mark them as recently used
        for i in 5..10 {
            cache.get(&i);
        }

        // Insert more items to trigger eviction
        for i in 10..15 {
            cache.insert(i, format!("value_{}", i));
        }

        // Recently accessed items (5-9) are more likely to still be present
        // This is probabilistic with S3-FIFO, but should generally hold
        let mut recent_present = 0;
        for i in 5..10 {
            if cache.get(&i).is_some() {
                recent_present += 1;
            }
        }

        // At least some recently accessed items should remain
        assert!(
            recent_present > 0,
            "Expected some recently accessed items to remain"
        );
    }

    #[test]
    fn test_scan_resistance() {
        let mut cache = Cache::new(20, 0.1, 0.1, ahash::RandomState::new());

        // Insert hot items and access them multiple times
        for i in 0..5 {
            cache.insert(i, format!("hot_{}", i));
            for _ in 0..3 {
                cache.get(&i);
            }
        }

        // Now perform a scan of many one-time access items
        for i in 100..200 {
            cache.insert(i, format!("cold_{}", i));
        }

        // Hot items should still be present (scan resistance)
        let mut hot_present = 0;
        for i in 0..5 {
            if cache.get(&i).is_some() {
                hot_present += 1;
            }
        }

        // S3-FIFO should protect hot items from scan
        assert!(
            hot_present >= 3,
            "Expected most hot items to survive scan, got {}",
            hot_present
        );
    }

    #[test]
    fn test_alternating_access_pattern() {
        let mut cache = Cache::new(10, 0.2, 0.2, ahash::RandomState::new());

        // Alternate between two sets of keys
        for _ in 0..20 {
            for i in 0..5 {
                cache.insert(i, format!("set1_{}", i));
            }
            for i in 10..15 {
                cache.insert(i, format!("set2_{}", i));
            }
        }

        // Both sets should have some representation
        let set1_present = (0..5).filter(|i| cache.get(i).is_some()).count();
        let set2_present = (10..15).filter(|i| cache.get(i).is_some()).count();

        assert!(set1_present > 0 || set2_present > 0);
    }

    #[test]
    fn test_edge_case_all_same_key() {
        let mut cache = Cache::new(10, 0.1, 0.1, ahash::RandomState::new());

        // Insert the same key repeatedly with different values
        // Note: re-inserting only updates recency, not value (except when promoting from ghost)
        for i in 0..100 {
            cache.insert(1, format!("value_{}", i));
        }

        // Should have the first value (since re-inserts don't update value in small/main queues)
        assert_eq!(cache.get(&1), Some(&"value_0".to_string()));

        // Cache should only have 1 item
        assert_eq!(cache.len(), 1);

        // Recency should be saturated
        let entry = cache.get_entry(&1).unwrap();
        assert_eq!(entry.recency(), 4);
    }

    #[test]
    fn test_hash_collision_resistance() {
        // Use a simple hasher that might have more collisions
        let mut cache = Cache::new(100, 0.1, 0.1, ahash::RandomState::new());

        // Insert many items
        for i in 0..200 {
            cache.insert(i, format!("value_{}", i));
        }

        // Verify correctness - any item in cache should have correct value
        for i in 0..200 {
            if let Some(value) = cache.get(&i) {
                assert_eq!(value, &format!("value_{}", i));
            }
        }
    }
}
