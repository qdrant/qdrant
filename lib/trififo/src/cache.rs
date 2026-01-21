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

    fn update_hashtable(&mut self, key: &K, local_offset: LocalOffset) {
        let hash = self.hash_key(key);
        let global_offset = self.fifos.global_offset(local_offset);
        self.hashtable
            .find_mut(hash, |global_offset| self.fifos.key_eq(*global_offset, key))
            .map(|offset| *offset = global_offset);
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
        if let Some(global_offset) = self
            .hashtable
            .find(hash, |global_offset| self.fifos.key_eq(*global_offset, &key))
        {
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
}
