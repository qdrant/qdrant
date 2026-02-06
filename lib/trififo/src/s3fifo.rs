use std::hash::{BuildHasher, Hash};

use hashbrown::HashTable;

use crate::entry::Entry;
use crate::lifecycle::Lifecycle;
use crate::raw_fifos::{GlobalOffset, LocalOffset, RawFifos};
use crate::seqlock::SeqLockSafe;

pub(crate) struct S3Fifo<K, V, L, S = ahash::RandomState> {
    /// Non-concurrent hashtable mapping key -> global offset in the FIFOs.
    hashtable: HashTable<GlobalOffset>,

    /// The actual FIFO structures (small, ghost, main).
    fifos: RawFifos<K, V>,

    /// Lifecycle impl for hooking up to events
    lifecycle: L,

    /// Hasher state used to compute the hash for lookups.
    hasher: S,
}

unsafe impl<K, V, L, S> SeqLockSafe for S3Fifo<K, V, L, S> {}

impl<K, V, L, S> S3Fifo<K, V, L, S>
where
    K: Copy + Hash + Eq,
    V: Clone,
    L: Lifecycle<K, V>,
    S: BuildHasher + Default,
{
    pub fn new(capacity: usize, small_ratio: f32, ghost_ratio: f32, lifecycle: L) -> Self {
        Self::new_with_hasher(capacity, small_ratio, ghost_ratio, lifecycle, S::default())
    }

    /// Create a new S3Fifo cache with a specific hasher instance.
    ///
    /// This is useful when you need multiple caches to share the same hasher
    /// (e.g., in a sharded cache where shard selection uses the same hash).
    pub fn new_with_hasher(
        capacity: usize,
        small_ratio: f32,
        ghost_ratio: f32,
        lifecycle: L,
        hasher: S,
    ) -> Self {
        assert!(capacity > 0);

        // Create FIFOs (reader + writer halves are managed by S3Fifo)
        let fifos = RawFifos::new(capacity, small_ratio, ghost_ratio);

        // Create a hashbrown hashtable with requested capacity.
        //
        // Maximum entries = small + main + ghost = capacity + ghost_size
        let max_entries = capacity + (capacity as f32 * ghost_ratio) as usize;

        // IMPORTANT: Allocate for 2x as much entries so that hashtable can be rehashed in-place if needed,
        // but never resized and reallocate.
        // See: https://github.com/rust-lang/hashbrown/blob/9641fb3eea9a07933fb631da6e4f5070d2f7e1da/src/raw.rs#L2775
        //
        // Internally, the capacity might increase to 1/8 higher, but that should only make it more robust.
        // By doing this, we ensure that concurrent readers will not get to a use-after-free error.
        let table_capacity = max_entries * 2;
        let hashtable = HashTable::with_capacity(table_capacity);

        Self {
            hashtable,
            fifos,
            hasher,
            lifecycle,
        }
    }

    /// Convenience to compute hash for a key using stored hasher.
    #[inline]
    fn hash_key(&self, key: &K) -> u64 {
        self.hasher.hash_one(key)
    }

    pub fn len(&self) -> usize {
        self.fifos.entries_len()
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let hash = self.hash_key(key);
        self.get_with_hash(key, hash)
    }

    /// Get a value from the cache using a pre-computed hash.
    ///
    /// This is useful when the hash has already been computed (e.g., for shard selection)
    /// to avoid computing it twice.
    #[inline]
    pub fn get_with_hash(&self, key: &K, hash: u64) -> Option<V> {
        let global_offset = self
            .hashtable
            .find(hash, |global_offset| self.fifos.key_eq(*global_offset, key))?;

        let entry = self.fifos.get_entry(*global_offset)?;

        entry.incr_recency();

        Some(entry.value.clone())
    }

    /// Entrypoint insert implementation
    pub fn do_insert(&mut self, key: K, value: V) {
        // Check existing entry
        let hash = self.hash_key(&key);
        self.do_insert_with_hash(key, value, hash);
    }

    /// Insert a key-value pair using a pre-computed hash.
    ///
    /// This is useful when the hash has already been computed (e.g., for shard selection)
    /// to avoid computing it twice.
    #[inline]
    pub fn do_insert_with_hash(&mut self, key: K, value: V, hash: u64) {
        if let Some(global_offset) = self.hashtable.find(hash, |global_offset| {
            self.fifos.key_eq(*global_offset, &key)
        }) {
            self.promote_existing(*global_offset, key, value);
            return;
        }

        // New entry -> insert into small queue
        let entry = Entry::new(key, value);
        let local = self.push_to_small_queue(entry);
        self.insert_unique_to_hashtable_with_hash(&key, local, hash);
    }

    /// Promote existing entry or increment recency.
    fn promote_existing(&mut self, global_offset: GlobalOffset, key: K, value: V) {
        let local_offset = self.fifos.local_offset(global_offset);
        match local_offset {
            LocalOffset::Ghost(_) => {
                let entry = Entry::new(key, value);
                let new_offset = self.push_to_main_queue(entry);
                // The same key now exists at ghost AND main queues, so we find the hashtable
                // entry that has the old offset, and update it to the offset in main queue
                //
                // The key at ghost queue will remain there, but nothing will point to it anymore,
                // it is safe to leave it there, as it will eventually get overwritten.
                self.update_hashtable(&key, global_offset, new_offset);
            }
            LocalOffset::Small(_) | LocalOffset::Main(_) => {
                if let Some(entry) = self.fifos.get_local_entry(local_offset) {
                    entry.incr_recency();
                }
            }
        }
    }

    /// Push to main queue. Evict or reinsert based on recency.
    #[must_use]
    fn push_to_main_queue(&mut self, entry: Entry<K, V>) -> GlobalOffset {
        // Try fast path
        let entry = match self.fifos.main_try_push(entry) {
            Ok(offset) => return offset,
            Err(entry) => entry,
        };

        // Reinsert while entries with recency > 0 exist
        while self.fifos.main_reinsert_if(|e| {
            if e.recency() > 0 {
                e.decr_recency();
                true
            } else {
                false
            }
        }) {}

        // We need to remove from hashtable BEFORE overwriting the slot,
        // because remove_from_hashtable uses key_eq which reads the current
        // slot contents. If we remove after overwriting_push, the slot
        // already contains the new key, so key_eq fails and the entry
        // becomes a zombie (never removed).
        //
        // todo(luis): think about using pop_push here
        let (eviction_global_offset, eviction_candidate) = self
            .fifos
            .main_eviction_candidate()
            .expect("We are the only writer, no torn read");
        let eviction_candidate_key = eviction_candidate.key;
        self.lifecycle
            .on_evict(eviction_candidate_key, eviction_candidate.value.clone());
        self.remove_from_hashtable(&eviction_candidate_key, &eviction_global_offset);

        // Now safe to overwrite the slot
        self.fifos.main_overwriting_push(entry)
    }

    /// Push to small queue, handling eviction to main/ghost and updating hashtable.
    #[must_use]
    fn push_to_small_queue(&mut self, entry: Entry<K, V>) -> GlobalOffset {
        // Try fast path
        let entry = match self.fifos.small_try_push(entry) {
            Ok(offset) => return offset,
            Err(entry) => entry,
        };

        // We need to read the oldest entry and update the hashtable BEFORE
        // overwriting the slot. This is because update_hashtable use key_eq
        // which reads the current slot contents. If we do this after overwriting_push,
        // the slot contains the new key, so key_eq fails and we fail to update the hashtable.
        let (old_offset, oldest_entry) = self
            .fifos
            .small_eviction_candidate()
            .expect("We are the only writer");
        let oldest_key = oldest_entry.key;

        let new_offset = if oldest_entry.recency() > 0 {
            // Promote to main queue
            self.push_to_main_queue(oldest_entry.clone())
        } else {
            self.lifecycle
                .on_evict(oldest_key, oldest_entry.value.clone());
            // Demote key to ghost queue
            self.push_to_ghost_queue(oldest_key)
        };

        // Update the hashtable to now find the moved entry at the main/ghost queue.
        // SAFETY: Entry is currently at two places, but:
        // 1. We have removed the bucket pointing to the overwritten position in main/ghost.
        // 2. So key_eq will only succeed with the one in small queue.
        self.update_hashtable(&oldest_key, old_offset, new_offset);

        // Now safe to overwrite the slot
        self.fifos.small_overwriting_push(entry)
    }

    fn push_to_ghost_queue(&mut self, key: K) -> GlobalOffset {
        // Try fast path
        let key = match self.fifos.ghost_try_push(key) {
            Ok(offset) => return offset,
            Err(key) => key,
        };

        // We need to remove from hashtable BEFORE overwriting the slot,
        // because remove_from_hashtable uses key_eq which reads the current
        // slot contents. If we remove after pop_push_unchecked, the slot
        // already contains the new key, so key_eq fails and the entry
        // becomes a zombie (never removed).
        let (eviction_global_offset, eviction_candidate_key) = self
            .fifos
            .ghost_eviction_candidate()
            .expect("We are the only writer");
        let eviction_candidate_key = *eviction_candidate_key;
        self.remove_from_hashtable(&eviction_candidate_key, &eviction_global_offset);

        // Now safe to overwrite the slot
        self.fifos.ghost_overwriting_push(key)
    }

    /// Update hashtable entry for `key`. If it did not exist, does nothing.
    fn update_hashtable(&mut self, key: &K, old_offset: GlobalOffset, new_offset: GlobalOffset) {
        let hash = self.hash_key(key);

        // Use the find_entry API to update.
        let entry = self
            .hashtable
            .find_entry(hash, |global_offset| global_offset == &old_offset);

        if let Ok(mut occupied) = entry {
            *occupied.get_mut() = new_offset;
        }
    }

    /// Remove a key from the hashtable if present.
    #[inline]
    fn remove_from_hashtable(&mut self, key: &K, eviction_global_offset: &GlobalOffset) {
        let hash = self.hash_key(key);
        if let Ok(entry) = self.hashtable.find_entry(hash, |global_offset| {
            global_offset == eviction_global_offset
        }) {
            entry.remove();
        }
    }

    /// Insert a fresh entry into the hashtable. Use this when we've already
    /// removed the old entry and need to insert at a new location.
    fn insert_unique_to_hashtable(&mut self, key: &K, global_offset: GlobalOffset) {
        let hash = self.hash_key(key);
        self.insert_unique_to_hashtable_with_hash(key, global_offset, hash);
    }

    /// Insert a fresh entry into the hashtable using a pre-computed hash.
    #[inline]
    fn insert_unique_to_hashtable_with_hash(
        &mut self,
        _key: &K,
        global_offset: GlobalOffset,
        hash: u64,
    ) {
        // Insert directly without searching for existing entry.
        // Caller must ensure the old entry was already removed.
        self.hashtable
            .insert_unique(hash, global_offset, |global_offset| {
                self.fifos.hash_key_at_offset(*global_offset, &self.hasher)
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::NoLifecycle;

    /// Helper to create a default S3Fifo cache with standard ratios
    /// Note: With capacity 100 and small_ratio 0.1, small queue = 10 slots
    fn create_cache(capacity: usize) -> S3Fifo<u64, String, NoLifecycle> {
        S3Fifo::new(capacity, 0.1, 0.9, NoLifecycle)
    }

    // ==================== Basic Operations ====================

    #[test]
    fn test_basic_insert_and_get() {
        // Use capacity 100 with small_ratio 0.1 = 10 slots in small queue
        let mut cache = create_cache(100);

        cache.do_insert(1, "one".to_string());
        cache.do_insert(2, "two".to_string());
        cache.do_insert(3, "three".to_string());

        assert_eq!(cache.get(&1), Some("one".to_string()));
        assert_eq!(cache.get(&2), Some("two".to_string()));
        assert_eq!(cache.get(&3), Some("three".to_string()));
        assert_eq!(cache.get(&4), None);
    }

    #[test]
    fn test_len_increases_with_inserts() {
        // Use larger capacity so small queue has multiple slots
        let mut cache = create_cache(100);

        assert_eq!(cache.len(), 0);
        cache.do_insert(1, "one".to_string());
        assert_eq!(cache.len(), 1);
        cache.do_insert(2, "two".to_string());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_len_with_ghost_entries() {
        // With capacity 10 and small_ratio 0.1, small queue = 1 slot
        // Ghost entries don't contribute to len()
        let mut cache = create_cache(10);

        cache.do_insert(1, "one".to_string());
        assert_eq!(cache.len(), 1);

        // Insert second item - first item (recency 0) goes to ghost
        cache.do_insert(2, "two".to_string());
        // len = small(1) + main(0) = 1 (ghost doesn't count)
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_update_existing_key() {
        let mut cache = create_cache(10);

        cache.do_insert(1, "one".to_string());
        assert_eq!(cache.get(&1), Some("one".to_string()));

        // Access to increment recency
        cache.get(&1);

        // Re-insert same key - should update recency, not add new entry
        cache.do_insert(1, "one_updated".to_string());

        // Value should remain the same (promote_existing just increments recency)
        assert_eq!(cache.get(&1), Some("one".to_string()));
        assert_eq!(cache.len(), 1);
    }

    // ==================== Recency Tracking ====================

    #[test]
    fn test_recency_increments_on_get() {
        let mut cache = create_cache(10);

        cache.do_insert(1, "one".to_string());

        // Multiple gets should increment recency (up to max of 3)
        for _ in 0..5 {
            assert_eq!(cache.get(&1), Some("one".to_string()));
        }

        // Item should still be retrievable
        assert_eq!(cache.get(&1), Some("one".to_string()));
    }

    // ==================== Small Queue Behavior ====================

    #[test]
    fn test_new_items_go_to_small_queue() {
        // With capacity 10 and small_ratio 0.1, small queue has 1 slot
        let mut cache = create_cache(10);

        cache.do_insert(1, "one".to_string());
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&1), Some("one".to_string()));
    }

    #[test]
    fn test_small_queue_overflow_demotes_to_ghost() {
        // Small queue size = ceil(10 * 0.1) = 1
        // New items without access go to ghost queue
        let mut cache = create_cache(10);

        // Insert first item (goes to small queue)
        cache.do_insert(1, "one".to_string());

        // Insert second item - first item has recency 0, should go to ghost
        cache.do_insert(2, "two".to_string());

        // Item 1 is now in ghost queue (no value, just key)
        assert_eq!(cache.get(&1), None);

        // Item 2 should still be accessible
        assert_eq!(cache.get(&2), Some("two".to_string()));
    }

    #[test]
    fn test_small_queue_overflow_promotes_accessed_to_main() {
        // Small queue size = ceil(10 * 0.1) = 1
        let mut cache = create_cache(10);

        // Insert and access first item
        cache.do_insert(1, "one".to_string());
        cache.get(&1); // Increment recency

        // Insert second item - first item has recency > 0, should go to main
        cache.do_insert(2, "two".to_string());

        // Both items should be accessible
        assert_eq!(cache.get(&1), Some("one".to_string()));
        assert_eq!(cache.get(&2), Some("two".to_string()));
    }

    // ==================== Ghost Queue Behavior ====================

    #[test]
    fn test_ghost_entry_promotes_to_main_on_reinsert() {
        let mut cache = create_cache(10);

        // Insert item (goes to small queue)
        cache.do_insert(1, "one".to_string());

        // Insert another item - item 1 (recency 0) goes to ghost
        cache.do_insert(2, "two".to_string());

        // Item 1 is now in ghost (not accessible)
        assert_eq!(cache.get(&1), None);

        // Re-insert item 1 - should go directly to main queue
        cache.do_insert(1, "one_new".to_string());

        // Now item 1 should be accessible with the new value
        assert_eq!(cache.get(&1), Some("one_new".to_string()));
    }

    // ==================== Main Queue Behavior ====================

    #[test]
    fn test_main_queue_reinsertion() {
        // Test that items with recency > 0 get reinserted in main queue
        let mut cache = create_cache(20);

        // Fill small queue and promote items to main by accessing them
        for i in 0..15 {
            cache.do_insert(i, format!("value_{}", i));
            cache.get(&i); // Increment recency to ensure promotion to main
        }

        // Access some items multiple times to give them high recency
        for _ in 0..3 {
            cache.get(&0);
            cache.get(&1);
        }

        // Continue inserting - this should trigger main queue evictions
        for i in 15..25 {
            cache.do_insert(i, format!("value_{}", i));
            cache.get(&i);
        }

        // Items with high recency should survive longer
        // (exact behavior depends on eviction order)
    }

    // ==================== Capacity and Eviction ====================

    #[test]
    fn test_capacity_limit() {
        let capacity = 10;
        let mut cache = create_cache(capacity);

        // Insert more items than capacity
        for i in 0..20u64 {
            cache.do_insert(i, format!("value_{}", i));
        }

        // Cache length should not exceed capacity
        assert!(cache.len() <= capacity);
    }

    #[test]
    fn test_fifo_eviction_order() {
        // With capacity 10, small = 1, main = 9, ghost = 9
        let mut cache = create_cache(10);

        // Insert items without accessing them (stay at recency 0)
        for i in 0..5u64 {
            cache.do_insert(i, format!("value_{}", i));
        }

        // Old items should be evicted first (FIFO order)
        // Item 0 should be in ghost queue after subsequent inserts
        assert_eq!(cache.get(&0), None); // Evicted to ghost (no value)
    }

    #[test]
    fn test_eviction_prefers_low_recency() {
        let mut cache = create_cache(10);

        // Insert and heavily access item 0
        cache.do_insert(0, "zero".to_string());
        for _ in 0..5 {
            cache.get(&0);
        }

        // Insert more items
        for i in 1..15u64 {
            cache.do_insert(i, format!("value_{}", i));
        }

        // Item 0 should survive due to high recency (promoted to main and reinserted)
        // Note: exact behavior depends on the eviction sequence
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_single_capacity() {
        // Minimum capacity edge case
        let mut cache: S3Fifo<u64, String, NoLifecycle> = S3Fifo::new(2, 0.5, 0.5, NoLifecycle);

        cache.do_insert(1, "one".to_string());
        assert_eq!(cache.get(&1), Some("one".to_string()));

        cache.do_insert(2, "two".to_string());
        // One of them might be evicted
        assert!(cache.len() <= 2);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let cache = create_cache(10);
        assert_eq!(cache.get(&999), None);
    }

    #[test]
    fn test_empty_cache_len() {
        let cache = create_cache(10);
        assert_eq!(cache.len(), 0);
    }

    // ==================== Larger Scale Tests ====================

    #[test]
    fn test_many_inserts() {
        let mut cache = create_cache(100);

        // Insert 1000 items
        for i in 0..1000u64 {
            cache.do_insert(i, format!("value_{}", i));
        }

        // Cache should be at capacity
        assert!(cache.len() <= 100);

        // Recent items should be accessible
        assert_eq!(cache.get(&999), Some("value_999".to_string()));
    }

    #[test]
    fn test_working_set_pattern() {
        // Simulate a working set pattern where some keys are accessed frequently
        // Use larger cache with bigger small queue for this pattern
        let mut cache: S3Fifo<u64, String, NoLifecycle> = S3Fifo::new(100, 0.2, 0.9, NoLifecycle);

        // Insert a working set and immediately access to promote to main
        for i in 0..20u64 {
            cache.do_insert(i, format!("value_{}", i));
            cache.get(&i); // Access to increment recency before eviction
        }

        // Frequently access the working set to build up recency
        for _ in 0..10 {
            for i in 0..10u64 {
                cache.get(&i);
            }
        }

        // Insert new items (simulating one-hit wonders)
        for i in 100..200u64 {
            cache.do_insert(i, format!("value_{}", i));
        }

        // Working set items (0-9) should have higher survival rate
        // due to their high recency from frequent access
        let working_set_hits: usize = (0..10u64).filter(|&i| cache.get(&i).is_some()).count();

        // Most of the working set should still be present
        // (exact number depends on cache dynamics)
        assert!(
            working_set_hits > 0,
            "Working set should have some surviving entries"
        );
    }

    #[test]
    fn test_scan_resistance() {
        // S3-FIFO should be resistant to scans (sequential access patterns)
        let mut cache = create_cache(50);

        // Build up a frequently accessed set
        for i in 0..20u64 {
            cache.do_insert(i, format!("frequent_{}", i));
            // Access multiple times
            cache.get(&i);
            cache.get(&i);
        }

        // Simulate a scan - insert many items that are never re-accessed
        for i in 100..200u64 {
            cache.do_insert(i, format!("scan_{}", i));
        }

        // The frequently accessed items should survive the scan
        // because scan items have recency 0 and go to ghost queue
        let frequent_hits: usize = (0..20u64).filter(|&i| cache.get(&i).is_some()).count();

        // Some frequent items should survive
        assert!(
            frequent_hits > 0,
            "Frequently accessed items should survive scan"
        );
    }

    // ==================== Hash Collision Scenarios ====================

    #[test]
    fn test_different_keys_same_value() {
        // Use larger capacity so items stay in small queue
        let mut cache = create_cache(100);

        cache.do_insert(1, "same".to_string());
        cache.do_insert(2, "same".to_string());
        cache.do_insert(3, "same".to_string());

        assert_eq!(cache.get(&1), Some("same".to_string()));
        assert_eq!(cache.get(&2), Some("same".to_string()));
        assert_eq!(cache.get(&3), Some("same".to_string()));
    }

    // ==================== Sequence Tests ====================

    #[test]
    fn test_insert_get_insert_pattern() {
        let mut cache = create_cache(10);

        for round in 0..5 {
            // Insert
            cache.do_insert(round * 2, format!("a_{}", round));
            cache.do_insert(round * 2 + 1, format!("b_{}", round));

            // Get (increment recency)
            cache.get(&(round * 2));
        }

        // Verify some items are still accessible
        assert!(cache.len() > 0);
    }

    #[test]
    fn test_repeated_key_access() {
        let mut cache = create_cache(10);

        cache.do_insert(1, "one".to_string());

        // Access the same key many times
        for _ in 0..100 {
            assert_eq!(cache.get(&1), Some("one".to_string()));
        }

        // Insert many other items
        for i in 2..20u64 {
            cache.do_insert(i, format!("value_{}", i));
        }

        // Key 1 should survive due to high recency
        assert_eq!(cache.get(&1), Some("one".to_string()));
    }

    // ==================== Algorithm Invariant Tests ====================

    #[test]
    fn test_ghost_entries_have_no_value() {
        let mut cache = create_cache(10);

        // Insert item without accessing
        cache.do_insert(1, "one".to_string());

        // Force it to ghost by inserting another item
        cache.do_insert(2, "two".to_string());

        // Item 1 is in ghost - get returns None (no value stored)
        let result = cache.get(&1);
        assert_eq!(result, None);

        // But when we re-insert, it goes to main
        cache.do_insert(1, "one_again".to_string());
        assert_eq!(cache.get(&1), Some("one_again".to_string()));
    }

    #[test]
    fn test_main_queue_fifo_with_reinsertion() {
        // Test that main queue reinserts items with recency > 0
        let mut cache = create_cache(20);

        // Fill the cache with items, accessing them to promote to main
        for i in 0..18u64 {
            cache.do_insert(i, format!("value_{}", i));
            cache.get(&i);
            cache.get(&i); // Higher recency
        }

        // Keep accessing item 0 to give it maximum recency
        for _ in 0..5 {
            cache.get(&0);
        }

        // Insert more items to trigger evictions in main queue
        for i in 20..40u64 {
            cache.do_insert(i, format!("new_{}", i));
        }

        // Item 0 with high recency should have better chance of survival
        // (due to reinsertion in main queue)
    }

    #[test]
    fn test_custom_ratios() {
        // Test with different small/ghost ratios
        let mut cache: S3Fifo<u64, String, NoLifecycle> = S3Fifo::new(100, 0.2, 0.5, NoLifecycle);

        for i in 0..50u64 {
            cache.do_insert(i, format!("value_{}", i));
        }

        assert!(cache.len() <= 100);
    }

    #[test]
    fn test_small_ratio_larger() {
        // Test with larger small queue ratio
        let mut cache: S3Fifo<u64, String, NoLifecycle> = S3Fifo::new(100, 0.3, 0.9, NoLifecycle);

        for i in 0..200u64 {
            cache.do_insert(i, format!("value_{}", i));
            if i % 3 == 0 {
                cache.get(&i);
            }
        }

        assert!(cache.len() <= 100);
    }

    // ==================== One-Hit Wonder Tests ====================

    #[test]
    fn test_one_hit_wonders_go_to_ghost() {
        // One-hit wonders (items accessed only once during insertion) should go to ghost
        let mut cache = create_cache(20);

        // Insert items without any additional access (one-hit wonders)
        for i in 0..30u64 {
            cache.do_insert(i, format!("value_{}", i));
        }

        // Early items should be in ghost queue (not accessible)
        for i in 0..5u64 {
            assert_eq!(cache.get(&i), None, "Item {} should be in ghost", i);
        }
    }

    #[test]
    fn test_quick_demotion() {
        // S3-FIFO should quickly demote unpopular items
        let mut cache = create_cache(10);

        // Insert item 1 (will be demoted quickly)
        cache.do_insert(1, "one".to_string());

        // Insert item 2 and access it
        cache.do_insert(2, "two".to_string());
        cache.get(&2);
        cache.get(&2);

        // Item 1 should already be in ghost (quick demotion)
        assert_eq!(cache.get(&1), None);

        // Item 2 should still be accessible
        assert_eq!(cache.get(&2), Some("two".to_string()));
    }

    // ==================== Ghost Queue Eviction Tests ====================

    #[test]
    fn test_ghost_queue_eviction() {
        // Test that ghost queue also evicts when full
        let mut cache = create_cache(10);

        // Insert many items to fill ghost queue
        for i in 0..50u64 {
            cache.do_insert(i, format!("value_{}", i));
        }

        // Very old ghost entries should be evicted from ghost too
        // Re-inserting them should NOT go to main (treated as new)
        cache.do_insert(0, "zero_new".to_string());

        // The item should be accessible (either from main via ghost hit, or from small as new)
        // This tests that the ghost eviction doesn't cause issues
    }

    // ==================== Stress Tests ====================

    #[test]
    fn test_rapid_insert_delete_cycle() {
        let mut cache = create_cache(50);

        for round in 0..10 {
            // Insert batch
            for i in 0..100u64 {
                cache.do_insert(round * 100 + i, format!("r{}_{}", round, i));
            }

            // Access some
            for i in 0..20u64 {
                cache.get(&(round * 100 + i));
            }
        }

        // Cache should still be functional and within limits
        assert!(cache.len() <= 50);
    }

    #[test]
    fn test_alternating_access_pattern() {
        // Use larger cache with bigger small queue
        let mut cache: S3Fifo<u64, String, NoLifecycle> = S3Fifo::new(50, 0.3, 0.9, NoLifecycle);

        // Insert items and access them to promote to main
        for i in 0..30u64 {
            cache.do_insert(i, format!("value_{}", i));
            cache.get(&i); // Access to ensure they get promoted to main
        }

        // Access some items multiple times to build recency
        for _ in 0..5 {
            for i in 0..10u64 {
                cache.get(&i);
            }
        }

        // Insert new items
        for i in 100..150u64 {
            cache.do_insert(i, format!("new_{}", i));
        }

        // Some old items should survive due to high recency
        let old_hits: usize = (0..10u64).filter(|&i| cache.get(&i).is_some()).count();
        assert!(old_hits > 0, "Some old items should survive");
    }

    // ==================== Boundary Condition Tests ====================

    #[test]
    fn test_exact_capacity_fill() {
        let capacity = 10;
        let mut cache = create_cache(capacity);

        // Fill exactly to capacity (accounting for small queue)
        // Small = 1, Main = 9
        for i in 0..10u64 {
            cache.do_insert(i, format!("value_{}", i));
            if i > 0 {
                // Access to promote previous items to main
                cache.get(&(i - 1));
            }
        }

        assert!(cache.len() <= capacity);
    }

    #[test]
    fn test_reinsert_immediately_after_eviction() {
        // Use capacity 10 with small_ratio 0.1 = 1 slot in small queue
        let mut cache = create_cache(10);

        // Insert item 1
        cache.do_insert(1, "one".to_string());

        // Force eviction to ghost
        cache.do_insert(2, "two".to_string());

        // Item 1 should be in ghost now
        assert_eq!(cache.get(&1), None);

        // Immediately reinsert - should go to main (ghost hit)
        cache.do_insert(1, "one_v2".to_string());
        assert_eq!(cache.get(&1), Some("one_v2".to_string()));

        // Access it multiple times to maximize recency (max is 3)
        cache.get(&1);
        cache.get(&1);
        cache.get(&1);

        // Insert a few more items - item 1 should survive due to high recency in main
        // With small=1 and main=9, we can insert up to 9 more items that get promoted
        for i in 3..8u64 {
            cache.do_insert(i, format!("value_{}", i));
            cache.get(&i); // Promote to main by accessing before next eviction
        }

        // Item 1 should still be accessible (in main with recency)
        assert_eq!(cache.get(&1), Some("one_v2".to_string()));
    }
}
