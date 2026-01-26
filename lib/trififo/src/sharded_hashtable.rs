//! A sharded hash table for concurrent access with minimal lock contention.
//!
//! This module provides a thread-safe hash table that distributes entries across
//! multiple shards, each protected by its own mutex. This design minimizes lock
//! contention by allowing operations on different shards to proceed concurrently.
//!
//! The key benefits of this design:
//! - Operations on different shards don't block each other
//! - Uses hashbrown's HashTable for efficient probing
//! - High shard count (128 by default) minimizes contention
//! - All operations take `&self`, making it easy to share across threads

use std::hash::{BuildHasher, Hash};

use hashbrown::HashTable;
use parking_lot::{Mutex, MutexGuard};

/// Number of shards for the hash table.
/// 128 provides a good balance between memory overhead and lock contention reduction.
const DEFAULT_NUM_SHARDS: usize = 128;

/// A sharded hash table for concurrent access.
///
/// This hash table distributes entries across multiple shards based on their hash,
/// allowing concurrent access to different shards. Each shard is an independent
/// `HashTable` protected by a `Mutex`.
///
/// All operations take `&self` (not `&mut self`), making it easy to share
/// across threads without additional synchronization.
pub struct ShardedHashTable<T, S = ahash::RandomState> {
    shards: Box<[Mutex<HashTable<T>>]>,
    shard_mask: usize,
    hasher: S,
}

impl<T, S> ShardedHashTable<T, S>
where
    S: BuildHasher + Clone,
{
    /// Creates a new sharded hash table with the default number of shards (128).
    ///
    /// # Arguments
    /// * `capacity` - Total capacity across all shards
    /// * `hasher` - Hash builder for key hashing
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self::with_capacity_hasher_and_shards(capacity, hasher, DEFAULT_NUM_SHARDS)
    }

    /// Creates a new sharded hash table with a custom number of shards.
    ///
    /// # Arguments
    /// * `capacity` - Total capacity across all shards
    /// * `hasher` - Hash builder for key hashing
    /// * `num_shards` - Number of shards (will be rounded up to next power of 2)
    ///
    /// # Panics
    /// Panics if num_shards is 0.
    pub fn with_capacity_hasher_and_shards(
        capacity: usize,
        hasher: S,
        num_shards: usize,
    ) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");

        // Round up to next power of 2 for efficient masking
        let num_shards = num_shards.next_power_of_two();
        let shard_mask = num_shards - 1;

        // Distribute capacity across shards
        let capacity_per_shard = (capacity / num_shards).max(1);

        let shards: Vec<_> = (0..num_shards)
            .map(|_| Mutex::new(HashTable::with_capacity(capacity_per_shard)))
            .collect();

        Self {
            shards: shards.into_boxed_slice(),
            shard_mask,
            hasher,
        }
    }

    /// Returns the hasher used by this hash table.
    #[inline]
    #[allow(dead_code)]
    pub fn hasher(&self) -> &S {
        &self.hasher
    }

    /// Returns the number of shards.
    #[inline]
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Computes the hash for a given key.
    #[inline]
    pub fn hash_one<K: Hash>(&self, key: &K) -> u64 {
        self.hasher.hash_one(key)
    }

    /// Computes the shard index for a given hash.
    #[inline]
    fn shard_index(&self, hash: u64) -> usize {
        // Use upper bits for shard selection (often better distributed than lower bits)
        ((hash >> 7) as usize) & self.shard_mask
    }

    /// Gets a locked shard for the given hash.
    #[inline]
    fn get_shard(&self, hash: u64) -> MutexGuard<'_, HashTable<T>> {
        let shard_idx = self.shard_index(hash);
        self.shards[shard_idx].lock()
    }

    /// Finds an entry in the hash table.
    ///
    /// # Arguments
    /// * `hash` - The hash of the key to find
    /// * `eq` - A function that returns true if the entry matches the key
    ///
    /// # Returns
    /// A copy of the value if found, None otherwise.
    #[inline]
    pub fn find<F>(&self, hash: u64, eq: F) -> Option<T>
    where
        T: Copy,
        F: Fn(&T) -> bool,
    {
        let shard = self.get_shard(hash);
        shard.find(hash, eq).copied()
    }

    /// Finds an entry and applies a function to it.
    ///
    /// # Arguments
    /// * `hash` - The hash of the key to find
    /// * `eq` - A function that returns true if the entry matches the key
    /// * `f` - A function to apply to the found entry
    ///
    /// # Returns
    /// The result of `f` if the entry was found, None otherwise.
    #[inline]
    pub fn find_with<F, G, R>(&self, hash: u64, eq: F, f: G) -> Option<R>
    where
        F: Fn(&T) -> bool,
        G: FnOnce(&T) -> R,
    {
        let shard = self.get_shard(hash);
        shard.find(hash, eq).map(f)
    }

    /// Inserts or updates an entry in the hash table.
    ///
    /// If an entry with the same key exists (according to `eq`), it is replaced.
    /// Otherwise, a new entry is inserted.
    ///
    /// # Arguments
    /// * `hash` - The hash of the key
    /// * `value` - The value to insert
    /// * `eq` - A function that returns true if an existing entry matches the key
    /// * `get_hash` - A function to compute the hash for rehashing (used when table grows)
    #[inline]
    pub fn insert<F, H>(&self, hash: u64, value: T, eq: F, get_hash: H)
    where
        F: Fn(&T) -> bool,
        H: Fn(&T) -> u64,
    {
        let mut shard = self.get_shard(hash);

        // Try to find existing entry
        if let Some(entry) = shard.find_mut(hash, &eq) {
            *entry = value;
            return;
        }

        // Insert new entry
        shard.insert_unique(hash, value, get_hash);
    }

    /// Removes an entry from the hash table.
    ///
    /// # Arguments
    /// * `hash` - The hash of the key to remove
    /// * `eq` - A function that returns true if the entry matches the key
    ///
    /// # Returns
    /// The removed value if found, None otherwise.
    #[inline]
    pub fn remove<F>(&self, hash: u64, eq: F) -> Option<T>
    where
        F: Fn(&T) -> bool,
    {
        let mut shard = self.get_shard(hash);
        match shard.find_entry(hash, eq) {
            Ok(entry) => Some(entry.remove().0),
            Err(_) => None,
        }
    }

    /// Returns the total number of entries across all shards.
    ///
    /// Note: This acquires locks on all shards sequentially,
    /// so the count may not be perfectly consistent in concurrent scenarios.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().len()).sum()
    }

    /// Returns true if the hash table is empty.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.lock().is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_and_find() {
        let table: ShardedHashTable<(u64, String)> =
            ShardedHashTable::with_capacity_and_hasher(100, ahash::RandomState::default());

        let hash = table.hash_one(&1u64);
        table.insert(
            hash,
            (1, "one".to_string()),
            |(k, _)| *k == 1,
            |(k, _)| table.hash_one(k),
        );

        let result = table.find_with(hash, |(k, _)| *k == 1, |(_, v)| v.clone());
        assert_eq!(result, Some("one".to_string()));
    }

    #[test]
    fn test_update_existing() {
        let table: ShardedHashTable<(u64, i32)> =
            ShardedHashTable::with_capacity_and_hasher(100, ahash::RandomState::default());

        let hash = table.hash_one(&1u64);

        table.insert(hash, (1, 100), |(k, _)| *k == 1, |(k, _)| table.hash_one(k));
        table.insert(hash, (1, 200), |(k, _)| *k == 1, |(k, _)| table.hash_one(k));

        let result = table.find(hash, |(k, _)| *k == 1);
        assert_eq!(result, Some((1, 200)));
    }

    #[test]
    fn test_remove() {
        let table: ShardedHashTable<(u64, i32)> =
            ShardedHashTable::with_capacity_and_hasher(100, ahash::RandomState::default());

        let hash = table.hash_one(&1u64);

        table.insert(hash, (1, 100), |(k, _)| *k == 1, |(k, _)| table.hash_one(k));

        let removed = table.remove(hash, |(k, _)| *k == 1);
        assert_eq!(removed, Some((1, 100)));

        let result = table.find(hash, |(k, _)| *k == 1);
        assert_eq!(result, None);
    }

    #[test]
    fn test_len_and_is_empty() {
        let table: ShardedHashTable<(u64, i32)> =
            ShardedHashTable::with_capacity_and_hasher(100, ahash::RandomState::default());

        assert!(table.is_empty());
        assert_eq!(table.len(), 0);

        let hash1 = table.hash_one(&1u64);
        let hash2 = table.hash_one(&2u64);

        table.insert(hash1, (1, 100), |(k, _)| *k == 1, |(k, _)| table.hash_one(k));
        assert!(!table.is_empty());
        assert_eq!(table.len(), 1);

        table.insert(hash2, (2, 200), |(k, _)| *k == 2, |(k, _)| table.hash_one(k));
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn test_custom_num_shards() {
        let table: ShardedHashTable<(u64, i32)> =
            ShardedHashTable::with_capacity_hasher_and_shards(
                100,
                ahash::RandomState::default(),
                8,
            );

        assert_eq!(table.num_shards(), 8);

        // Non-power-of-2 should be rounded up
        let table2: ShardedHashTable<(u64, i32)> =
            ShardedHashTable::with_capacity_hasher_and_shards(
                100,
                ahash::RandomState::default(),
                5,
            );

        assert_eq!(table2.num_shards(), 8); // 5 rounded up to 8
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let table: Arc<ShardedHashTable<(u64, u64)>> = Arc::new(
            ShardedHashTable::with_capacity_and_hasher(10000, ahash::RandomState::default()),
        );

        const THREADS: usize = 4;
        const OPS_PER_THREAD: u64 = 1000;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let table = Arc::clone(&table);
                thread::spawn(move || {
                    let start = t as u64 * OPS_PER_THREAD;
                    for i in start..(start + OPS_PER_THREAD) {
                        let hash = table.hash_one(&i);
                        table.insert(hash, (i, i * 2), |(k, _)| *k == i, |(k, _)| table.hash_one(k));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all entries were inserted
        assert_eq!(table.len(), (THREADS as u64 * OPS_PER_THREAD) as usize);

        // Verify some random entries
        for i in [0u64, 500, 1500, 2500, 3500] {
            let hash = table.hash_one(&i);
            let result = table.find(hash, |(k, _)| *k == i);
            assert_eq!(result, Some((i, i * 2)));
        }
    }
}