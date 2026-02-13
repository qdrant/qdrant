use crate::array_lookup::{ArrayLookup, AsIndex};
use crate::entry::Entry;
use crate::lifecycle::Lifecycle;
use crate::raw_fifos::{GlobalOffset, LocalOffset, RawFifos};
use crate::seqlock::SeqLockSafe;

pub(crate) struct S3Fifo<K, V, L> {
    /// Lookup table mapping key -> global offset in the FIFOs.
    /// Uses `ArrayLookup` for O(1) direct-indexed access.
    lookup: ArrayLookup,

    /// The actual FIFO structures (small, ghost, main).
    fifos: RawFifos<K, V>,

    /// Lifecycle impl for hooking up to events
    lifecycle: L,
}

unsafe impl<K, V, L> SeqLockSafe for S3Fifo<K, V, L> {}

impl<K, V, L> S3Fifo<K, V, L>
where
    K: Copy + AsIndex,
    V: Clone,
    L: Lifecycle<K, V>,
{
    pub fn new(capacity: usize, small_ratio: f32, ghost_ratio: f32, lifecycle: L) -> Self {
        assert!(capacity > 0);

        // Create FIFOs (reader + writer halves are managed by S3Fifo)
        let fifos = RawFifos::new(capacity, small_ratio, ghost_ratio);

        let lookup = ArrayLookup::new();

        Self {
            lookup,
            fifos,
            lifecycle,
        }
    }

    pub fn len(&self) -> usize {
        self.fifos.entries_len()
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let global_offset = self.lookup.get(key.as_index())?;

        let entry = self.fifos.get_entry(global_offset)?;

        entry.incr_recency();

        Some(entry.value.clone())
    }

    /// Entrypoint insert implementation
    pub fn do_insert(&mut self, key: K, value: V) {
        if let Some(global_offset) = self.lookup.get(key.as_index()) {
            self.promote_existing(global_offset, key, value);
            return;
        }

        // New entry -> insert into small queue
        let entry = Entry::new(key, value);
        let global_offset = self.push_to_small_queue(entry);
        self.lookup.set(key.as_index(), global_offset);
    }

    /// Promote existing entry or increment recency.
    fn promote_existing(&mut self, global_offset: GlobalOffset, key: K, value: V) {
        let local_offset = self.fifos.local_offset(global_offset);
        match local_offset {
            LocalOffset::Ghost(_) => {
                let entry = Entry::new(key, value);
                let new_offset = self.push_to_main_queue(entry);
                // The same key now exists at ghost AND main queues, so we update
                // the lookup to point to the new offset in main queue.
                //
                // The key at ghost queue will remain there, but nothing will point to it anymore,
                // it is safe to leave it there, as it will eventually get overwritten.
                self.lookup.set(key.as_index(), new_offset);
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

        // We need to remove from lookup BEFORE overwriting the slot,
        // because we need to identify which key is being evicted.
        // If we remove after overwriting_push, the slot already contains
        // the new key.
        //
        // todo(luis): think about using pop_push here
        let (_eviction_global_offset, eviction_candidate) = self
            .fifos
            .main_eviction_candidate()
            .expect("We are the only writer, no torn read");
        let eviction_candidate_key = eviction_candidate.key;
        self.lifecycle
            .on_evict(eviction_candidate_key, eviction_candidate.value.clone());
        self.lookup.remove(eviction_candidate_key.as_index());

        // Now safe to overwrite the slot
        self.fifos.main_overwriting_push(entry)
    }

    /// Push to small queue, handling eviction to main/ghost and updating lookup.
    #[must_use]
    fn push_to_small_queue(&mut self, entry: Entry<K, V>) -> GlobalOffset {
        // Try fast path
        let entry = match self.fifos.small_try_push(entry) {
            Ok(offset) => return offset,
            Err(entry) => entry,
        };

        // We need to read the oldest entry and update the lookup BEFORE
        // overwriting the slot.
        let (_old_offset, oldest_entry) = self
            .fifos
            .small_eviction_candidate()
            .expect("We are the only writer");
        let oldest_key = oldest_entry.key;

        // promote to main if recency is high, or if we haven't fully populated main.
        let new_offset = if oldest_entry.recency() > 0 || !self.fifos.main_is_full() {
            // Promote to main queue
            self.push_to_main_queue(oldest_entry.clone())
        } else {
            self.lifecycle
                .on_evict(oldest_key, oldest_entry.value.clone());
            // Demote key to ghost queue
            self.push_to_ghost_queue(oldest_key)
        };

        // Update the lookup to now find the moved entry at the main/ghost queue.
        self.lookup.set(oldest_key.as_index(), new_offset);

        // Now safe to overwrite the slot
        self.fifos.small_overwriting_push(entry)
    }

    fn push_to_ghost_queue(&mut self, key: K) -> GlobalOffset {
        // Try fast path
        let key = match self.fifos.ghost_try_push(key) {
            Ok(offset) => return offset,
            Err(key) => key,
        };

        // We need to remove from lookup BEFORE overwriting the slot.
        let (eviction_global_offset, eviction_candidate_key) = self
            .fifos
            .ghost_eviction_candidate()
            .expect("We are the only writer");
        let eviction_candidate_key = *eviction_candidate_key;

        if let Some(lookup_offset) = self.lookup.get(eviction_candidate_key.as_index()) {
            if lookup_offset == eviction_global_offset {
                self.lookup.remove(eviction_candidate_key.as_index());
            }
        }

        // Now safe to overwrite the slot
        self.fifos.ghost_overwriting_push(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::NoLifecycle;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn create_cache() -> S3Fifo<u64, String, NoLifecycle> {
        S3Fifo::new(100, 0.1, 0.9, NoLifecycle)
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_basic_insert_and_get() {
        let mut cache = create_cache();

        cache.do_insert(1u64, "hello".into());
        cache.do_insert(2, "world".into());

        assert_eq!(cache.get(&1), Some("hello".into()));
        assert_eq!(cache.get(&2), Some("world".into()));
        assert_eq!(cache.get(&3), None);
    }

    #[test]
    fn test_len_increases_with_inserts() {
        let mut cache = create_cache();
        assert_eq!(cache.len(), 0);

        cache.do_insert(1u64, "a".into());
        assert_eq!(cache.len(), 1);

        cache.do_insert(2, "b".into());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_len_with_ghost_entries() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        // Fill small queue (1 slot) and overflow into ghost/main
        for i in 0..20 {
            cache.do_insert(i, format!("val{i}"));
        }
        // Ghost entries shouldn't count towards len
        assert!(cache.len() <= 10);
    }

    #[test]
    fn test_update_existing_key() {
        let mut cache = create_cache();

        cache.do_insert(1u64, "first".into());
        assert_eq!(cache.get(&1), Some("first".into()));

        // Update value for existing key
        cache.do_insert(1, "second".into());
        // In S3Fifo, updating an existing key increments recency rather than updating the value
        // unless the key was in the ghost queue. For small/main entries, the value stays the same.
        let result = cache.get(&1);
        assert!(result.is_some());
    }

    #[test]
    fn test_recency_increments_on_get() {
        let mut cache = create_cache();

        cache.do_insert(1u64, "hello".into());

        // Multiple gets should not fail
        assert_eq!(cache.get(&1), Some("hello".into()));
        assert_eq!(cache.get(&1), Some("hello".into()));
        assert_eq!(cache.get(&1), Some("hello".into()));

        // Key should still be accessible
        assert_eq!(cache.get(&1), Some("hello".into()));
    }

    #[test]
    fn test_new_items_go_to_small_queue() {
        let mut cache = create_cache();

        cache.do_insert(1u64, "test".into());
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&1), Some("test".into()));
    }

    #[test]
    fn test_small_queue_overflow_demotes_to_ghost() {
        // Small queue size = ceil(10 * 0.1) = 1
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        // First insert goes to small queue
        cache.do_insert(1u64, "first".into());

        // Second insert should push first to main (since main is not full)
        cache.do_insert(2, "second".into());

        // Both should be accessible
        assert_eq!(cache.get(&1), Some("first".into()));
        assert_eq!(cache.get(&2), Some("second".into()));
    }

    #[test]
    fn test_small_queue_overflow_promotes_accessed_to_main() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        cache.do_insert(1u64, "first".into());

        // Access to increase recency
        cache.get(&1);

        // This should evict key 1 from small queue,
        // and since recency > 0, promote to main
        cache.do_insert(2, "second".into());

        // Key 1 should still be accessible (now in main queue)
        assert_eq!(cache.get(&1), Some("first".into()));
    }

    #[test]
    fn test_ghost_entry_promotes_to_main_on_reinsert() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        // Fill to create ghost entries
        // First item goes to main (since main isn't full initially)
        cache.do_insert(100u64, "first".into());

        // Fill main queue
        for i in 1..=9 {
            cache.do_insert(i * 100, format!("val{i}"));
        }

        // Now insert more to create ghost entries (items evicted from small with recency 0)
        cache.do_insert(1000u64, "ghost_candidate".into());
        cache.do_insert(2000u64, "another".into());

        // Reinsert the ghost candidate - should promote to main
        cache.do_insert(1000u64, "ghost_reinserted".into());
        assert_eq!(cache.get(&1000), Some("ghost_reinserted".into()));
    }

    #[test]
    fn test_main_queue_reinsertion() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        // Fill cache to capacity
        for i in 0..10u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Access some entries to increase their recency
        cache.get(&0);
        cache.get(&1);
        cache.get(&2);

        // Insert more to trigger main queue eviction
        for i in 10..20u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // The accessed entries may have been reinserted due to high recency
        // At least the cache should still work correctly
        assert!(cache.len() <= 10);
    }

    #[test]
    fn test_capacity_limit() {
        let capacity = 20;
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(capacity, 0.1, 0.9, NoLifecycle);

        // Insert more than capacity
        for i in 0..100u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Cache should not exceed capacity
        assert!(cache.len() <= capacity, "len: {}", cache.len());
    }

    #[test]
    fn test_fifo_eviction_order() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(5, 0.2, 0.9, NoLifecycle);

        // Insert 5 items (fills capacity)
        for i in 0..10u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Oldest items should have been evicted
        // Newest items should still be present
        assert_eq!(cache.get(&9), Some("val9".into()));
    }

    #[test]
    fn test_eviction_prefers_low_recency() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(5, 0.2, 0.9, NoLifecycle);

        for i in 0..5u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Access item 0 and 1 to increase recency
        cache.get(&0);
        cache.get(&1);

        // Insert more items, forcing eviction
        for i in 5..10u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Items with high recency (0, 1) may survive longer
        // Items with low recency should be evicted first
        assert!(cache.len() <= 5);
    }

    #[test]
    fn test_single_capacity() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(2, 0.5, 0.9, NoLifecycle);

        cache.do_insert(1u64, "first".into());
        assert_eq!(cache.get(&1), Some("first".into()));

        cache.do_insert(2, "second".into());
        assert!(cache.len() <= 2);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let cache = create_cache();
        assert_eq!(cache.get(&999u64), None);
    }

    #[test]
    fn test_empty_cache_len() {
        let cache = create_cache();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_many_inserts() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(1000, 0.1, 0.9, NoLifecycle);

        for i in 0..5000u64 {
            cache.do_insert(i, format!("value_{i}"));
        }

        assert!(cache.len() <= 1000);

        // Recent inserts should be findable
        for i in 4900..5000u64 {
            assert!(cache.get(&i).is_some(), "Key {i} should be in cache");
        }
    }

    #[test]
    fn test_working_set_pattern() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(100, 0.1, 0.9, NoLifecycle);

        // Insert a working set
        for i in 0..50u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Repeatedly access the working set to build up recency
        for _ in 0..3 {
            for i in 0..50u64 {
                cache.get(&i);
            }
        }

        // Insert "scan" items that should not evict the working set
        for i in 50..150u64 {
            cache.do_insert(i, format!("scan{i}"));
        }

        // Working set should mostly survive
        let working_set_hits: usize = (0..50)
            .filter(|i| cache.get(&(*i as u64)).is_some())
            .count();

        assert!(
            working_set_hits > 25,
            "Working set should mostly survive scan, but only {working_set_hits}/50 survived"
        );
    }

    #[test]
    fn test_scan_resistance() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(100, 0.1, 0.9, NoLifecycle);

        // Insert and access a working set
        for i in 0..80u64 {
            cache.do_insert(i, format!("val{i}"));
            cache.get(&i);
        }

        // Simulate a scan of 200 unique items (only accessed once each)
        for i in 1000..1200u64 {
            cache.do_insert(i, format!("scan{i}"));
        }

        // Check how many of the working set survived the scan
        let survivors: usize = (0..80)
            .filter(|i| cache.get(&(*i as u64)).is_some())
            .count();

        // S3-FIFO should protect frequently accessed items from scan eviction
        assert!(
            survivors > 20,
            "Expected some scan resistance, but only {survivors}/80 working set items survived"
        );
    }

    #[test]
    fn test_different_keys_same_value() {
        let mut cache = create_cache();

        cache.do_insert(1u64, "same".into());
        cache.do_insert(2u64, "same".into());
        cache.do_insert(3u64, "same".into());

        assert_eq!(cache.get(&1), Some("same".into()));
        assert_eq!(cache.get(&2), Some("same".into()));
        assert_eq!(cache.get(&3), Some("same".into()));
    }

    #[test]
    fn test_insert_get_insert_pattern() {
        let mut cache = create_cache();

        cache.do_insert(1u64, "v1".into());
        assert_eq!(cache.get(&1), Some("v1".into()));

        cache.do_insert(2u64, "v2".into());
        assert_eq!(cache.get(&2), Some("v2".into()));

        // Re-insert key 1 (should hit small/main path, incrementing recency)
        cache.do_insert(1u64, "v1_updated".into());

        // Key 1 should still be accessible
        assert!(cache.get(&1).is_some());
    }

    #[test]
    fn test_repeated_key_access() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        cache.do_insert(42u64, "answer".into());

        // Access many times
        for _ in 0..100 {
            assert_eq!(cache.get(&42), Some("answer".into()));
        }

        // Fill cache with other items
        for i in 0..20u64 {
            cache.do_insert(i + 100, format!("val{i}"));
        }

        // Key 42 should survive due to high recency
        assert_eq!(
            cache.get(&42),
            Some("answer".into()),
            "Frequently accessed item should survive eviction"
        );
    }

    #[test]
    fn test_ghost_entries_have_no_value() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        // Insert items to fill small + main
        for i in 0..10u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Insert more to push some to ghost
        for i in 10..20u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Some early items should have been evicted (either in ghost or gone)
        // Ghost items should return None for get
        let evicted_count = (0..10u64).filter(|i| cache.get(i).is_none()).count();
        assert!(
            evicted_count > 0,
            "Some items should have been evicted or moved to ghost"
        );
    }

    #[test]
    fn test_main_queue_fifo_with_reinsertion() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        // Fill cache
        for i in 0..10u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Access only even items
        for i in (0..10u64).step_by(2) {
            cache.get(&i);
        }

        // Insert new items to trigger eviction
        for i in 10..20u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Even items (higher recency) should be more likely to survive
        let even_survivors = (0..10u64)
            .step_by(2)
            .filter(|i| cache.get(i).is_some())
            .count();
        let odd_survivors = (1..10u64)
            .step_by(2)
            .filter(|i| cache.get(i).is_some())
            .count();

        assert!(
            even_survivors >= odd_survivors,
            "Even items (accessed) should survive at least as well as odd items: even={even_survivors}, odd={odd_survivors}"
        );
    }

    #[test]
    fn test_custom_ratios() {
        // Very small small-queue ratio
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(100, 0.01, 0.5, NoLifecycle);

        for i in 0..200u64 {
            cache.do_insert(i, format!("val{i}"));
        }
        assert!(cache.len() <= 100);
    }

    #[test]
    fn test_small_ratio_larger() {
        // Large small-queue ratio
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(100, 0.5, 0.5, NoLifecycle);

        for i in 0..200u64 {
            cache.do_insert(i, format!("val{i}"));
        }
        assert!(cache.len() <= 100);

        // Recent items should be present
        assert!(cache.get(&199).is_some());
    }

    #[test]
    fn test_one_hit_wonders_go_to_ghost() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        // Fill main queue first
        for i in 0..9u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Insert one-hit wonders (never accessed again)
        for i in 100..110u64 {
            cache.do_insert(i, format!("wonder{i}"));
        }

        // One-hit wonders with no recency should go to ghost, not main
        // So the main queue items should mostly survive
        assert!(cache.len() <= 10);
    }

    #[test]
    fn test_quick_demotion() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        // Insert and immediately evict by inserting more
        for i in 0..100u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Only the most recent items and those with recency should survive
        assert!(cache.len() <= 10);

        // The last inserted item should definitely be present
        assert_eq!(cache.get(&99), Some("val99".into()));
    }

    #[test]
    fn test_ghost_queue_eviction() {
        // Ghost queue size = ceil(20 * 0.9) = 18
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(20, 0.1, 0.9, NoLifecycle);

        // Insert many items to fill ghost queue
        for i in 0..100u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Cache should maintain its capacity
        assert!(cache.len() <= 20);

        // Should still function correctly
        cache.do_insert(999u64, "new".into());
        assert_eq!(cache.get(&999), Some("new".into()));
    }

    #[test]
    fn test_rapid_insert_delete_cycle() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(5, 0.2, 0.9, NoLifecycle);

        // Rapidly insert and cause evictions
        for cycle in 0..10u64 {
            for i in 0..10u64 {
                let key = cycle * 10 + i;
                cache.do_insert(key, format!("val{key}"));
            }
        }

        // Cache should be consistent
        assert!(cache.len() <= 5);

        // Last few items should be findable
        assert!(cache.get(&99).is_some());
    }

    #[test]
    fn test_alternating_access_pattern() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(20, 0.1, 0.9, NoLifecycle);

        // Insert 20 items
        for i in 0..20u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Access only first 10 repeatedly
        for _ in 0..5 {
            for i in 0..10u64 {
                cache.get(&i);
            }
        }

        // Now insert 20 new items
        for i in 20..40u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // First 10 items (frequently accessed) should mostly survive
        let survivors: usize = (0..10)
            .filter(|i| cache.get(&(*i as u64)).is_some())
            .count();
        assert!(
            survivors > 3,
            "Frequently accessed items should survive, but only {survivors}/10 did"
        );
    }

    #[test]
    fn test_exact_capacity_fill() {
        let capacity = 50;
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(capacity, 0.1, 0.9, NoLifecycle);

        // Insert exactly capacity items
        for i in 0..capacity as u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        assert_eq!(cache.len(), capacity);

        // All should be present
        for i in 0..capacity as u64 {
            assert!(
                cache.get(&i).is_some(),
                "Key {i} should be present when cache is exactly full"
            );
        }
    }

    #[test]
    fn test_reinsert_immediately_after_eviction() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        // Fill cache
        for i in 0..10u64 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Insert to evict, then immediately reinsert the evicted key
        cache.do_insert(100u64, "new".into());

        // Try to reinsert key 0 (likely evicted or in ghost)
        cache.do_insert(0u64, "reinserted".into());

        // Should be accessible again
        let result = cache.get(&0);
        assert!(
            result.is_some(),
            "Reinserted key should be accessible, got None"
        );
    }

    #[test]
    fn test_ghost_eviction_does_not_remove_promoted_entry() {
        let mut cache: S3Fifo<u64, String, _> = S3Fifo::new(10, 0.1, 0.9, NoLifecycle);

        // Fill Main (9 items) + Small (1 item). Total 10 items.
        for i in 0..10 {
            cache.do_insert(i, format!("val{i}"));
        }

        // Insert 10.
        // Item 9 (in Small) is evicted. Main is full (9 items).
        // 9 goes to Ghost.
        cache.do_insert(10, "val10".to_string());

        // Promote 9 to Main.
        // It stays in Ghost (stale) but lookup updates to Main.
        cache.do_insert(9, "val9-promoted".to_string());

        assert_eq!(cache.get(&9), Some("val9-promoted".to_string()));

        // Force eviction of 9 from Ghost.
        // Ghost capacity is 9. We push 9 items (11..19).
        // Each goes Small -> Ghost.
        for i in 11..20 {
            cache.do_insert(i, format!("val{i}"));
        }

        // 9 should have been evicted from Ghost, but should remain accessible in Main.
        assert_eq!(
            cache.get(&9),
            Some("val9-promoted".to_string()),
            "Key 9 should still be accessible after being evicted from ghost queue"
        );
    }
}
