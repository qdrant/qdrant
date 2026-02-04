use std::hash::{BuildHasher, Hash};

use hashbrown::HashTable;

use crate::entry::Entry;
use crate::raw_fifos::{GlobalOffset, LocalOffset, RawFifos};

pub(crate) struct S3Fifo<K, V, S = ahash::RandomState> {
    /// Non-concurrent hashtable mapping key -> global offset in the FIFOs.
    hashtable: HashTable<GlobalOffset>,

    /// The actual FIFO structures (small, ghost, main).
    fifos: RawFifos<K, V>,

    /// Hasher state used to compute the hash for lookups.
    hasher: S,
}

impl<K, V, S> S3Fifo<K, V, S>
where
    K: Copy + Hash + Eq,
    V: Clone,
    S: BuildHasher + Default,
{
    pub fn new(capacity: usize, small_ratio: f32, ghost_ratio: f32) -> Self {
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
            hasher: S::default(),
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
        if let Some(global_offset) = self.hashtable.find(hash, |global_offset| {
            self.fifos.key_eq(*global_offset, &key)
        }) {
            self.promote_existing(*global_offset, key, value);
            return;
        }

        // New entry -> insert into small queue
        let entry = Entry::new(key, value);
        let local = self.push_to_small_queue(entry);
        self.insert_unique_to_hashtable(&key, local);
    }

    /// Promote existing entry or increment recency.
    fn promote_existing(&mut self, global_offset: GlobalOffset, key: K, value: V) {
        let local_offset = self.fifos.local_offset(global_offset);
        match local_offset {
            LocalOffset::Ghost(_) => {
                let entry = Entry::new(key, value);
                let new_offset = self.push_to_main_queue(entry);
                self.update_hashtable(&key, new_offset);
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
        let eviction_candidate_key = self
            .fifos
            .main_eviction_candidate()
            .expect("We are the only writer, no torn read")
            .key;
        self.remove_from_hashtable(&eviction_candidate_key);

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
        let oldest_entry = self
            .fifos
            .small_eviction_candidate()
            .expect("We are the only writer");
        let oldest_key = oldest_entry.key;

        let new_offset = if oldest_entry.recency() > 0 {
            // Promote to main queue
            self.push_to_main_queue(oldest_entry.clone())
        } else {
            // Demote key to ghost queue
            self.push_to_ghost_queue(oldest_key)
        };

        // Update the hashtable to now find the moved entry at the main/ghost queue.
        // SAFETY: Entry is currently at two places, but:
        // 1. We have removed the bucket pointing to the overwritten position in main/ghost.
        // 2. So key_eq will only succeed with the one in small queue.
        self.update_hashtable(&oldest_key, new_offset);

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
        let eviction_candidate = *self
            .fifos
            .ghost_eviction_candidate()
            .expect("We are the only writer");
        self.remove_from_hashtable(&eviction_candidate);

        // Now safe to overwrite the slot
        self.fifos.ghost_overwriting_push(key)
    }

    /// Update hashtable entry for `key`. If it did not exist, does nothing.
    fn update_hashtable(&mut self, key: &K, global_offset: GlobalOffset) {
        let hash = self.hash_key(key);

        // Use the find_entry API to update.
        let entry = self
            .hashtable
            .find_entry(hash, |global_offset| self.fifos.key_eq(*global_offset, key));

        if let Ok(mut occupied) = entry {
            *occupied.get_mut() = global_offset;
        }
    }

    /// Remove a key from the hashtable if present.
    #[inline]
    fn remove_from_hashtable(&mut self, key: &K) {
        let hash = self.hash_key(key);
        if let Ok(entry) = self
            .hashtable
            .find_entry(hash, |global_offset| self.fifos.key_eq(*global_offset, key))
        {
            entry.remove();
        }
    }

    /// Insert a fresh entry into the hashtable. Use this when we've already
    /// removed the old entry and need to insert at a new location.
    fn insert_unique_to_hashtable(&mut self, key: &K, global_offset: GlobalOffset) {
        let hash = self.hash_key(key);

        // Insert directly without searching for existing entry.
        // Caller must ensure the old entry was already removed.
        self.hashtable
            .insert_unique(hash, global_offset, |global_offset| {
                self.fifos.hash_key_at_offset(*global_offset, &self.hasher)
            });
    }
}
