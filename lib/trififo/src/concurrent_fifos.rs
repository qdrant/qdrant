//! Concurrent FIFO queues for the S3-FIFO cache algorithm.
//!
//! This module provides separate writer and reader types for the three FIFO queues
//! (small, ghost, main) used in the S3-FIFO cache algorithm.
//!
//! The separation enforces at compile time that:
//! - Only the single writer thread can perform write operations
//! - Multiple reader threads can perform lock-free read operations

use std::sync::atomic::{AtomicU8, Ordering};
use std::hash::{BuildHasher, Hash};

use crate::cache::GlobalOffset;
use crate::concurrent_ringbuffer::RingBuffer;

/// Entry stored in the small and main queues.
pub struct Entry<K, V> {
    pub key: K,
    pub value: V,
    recency: AtomicU8,
}

impl<K: std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug for Entry<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("key", &self.key)
            .field("value", &self.value)
            .field("recency", &self.recency.load(Ordering::Relaxed))
            .finish()
    }
}

impl<K, V> Entry<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            recency: AtomicU8::new(0),
        }
    }

    #[inline]
    pub fn recency(&self) -> u8 {
        self.recency.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn incr_recency(&self) {
        // Saturating increment - cap at 3 as per S3-FIFO algorithm
        let current = self.recency.load(Ordering::Relaxed);
        if current < 3 {
            self.recency.store(current + 1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn decr_recency(&self) {
        let current = self.recency.load(Ordering::Relaxed);
        if current > 0 {
            self.recency.store(current - 1, Ordering::Relaxed);
        }
    }
}

/// Local offset within one of the three queues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalOffset {
    Small(u32),
    Ghost(u32),
    Main(u32),
}

/// Shared state between writer and readers.
///
/// Contains read-only accessor methods that can be used by both
/// `FifosWriter` (via `Deref`) and readers (via `Arc<SharedFifos<K, V>>`).
pub struct S3Fifo<K, V> {
    pub small: RingBuffer<Entry<K, V>>,
    pub ghost: RingBuffer<K>,
    pub main: RingBuffer<Entry<K, V>>,

    small_end: GlobalOffset,
    ghost_end: GlobalOffset,
}

impl<K, V> S3Fifo<K, V> {
    /// Creates a new pair of FIFO writer and reader handles.
    ///
    /// # Arguments
    /// * `capacity` - Total capacity for small + main queues
    /// * `small_ratio` - Fraction of capacity for small queue (typically 0.1)
    /// * `ghost_ratio` - Fraction of capacity for ghost queue (typically 0.9)
    pub fn new(capacity: usize, small_ratio: f32, ghost_ratio: f32) -> Self {
        let small_size = (capacity as f32 * small_ratio) as usize;
        let small = RingBuffer::new(small_size.max(1));

        let ghost_size = (capacity as f32 * ghost_ratio) as usize;
        let ghost = RingBuffer::new(ghost_size.max(1));

        let main_size = capacity - small_size;
        let main = RingBuffer::new(main_size.max(1));

        let small_end = small_size as GlobalOffset;
        let ghost_end = small_end + ghost_size as GlobalOffset;

        S3Fifo {
            small,
            ghost,
            main,
            small_end,
            ghost_end,
        }
    }

    /// Converts a global offset to a local offset.
    #[inline]
    pub fn local_offset(&self, global_offset: GlobalOffset) -> LocalOffset {
        if global_offset < self.small_end {
            LocalOffset::Small(global_offset)
        } else if global_offset < self.ghost_end {
            LocalOffset::Ghost(global_offset - self.small_end)
        } else {
            LocalOffset::Main(global_offset - self.ghost_end)
        }
    }

    /// Converts a local offset to a global offset.
    #[inline]
    pub fn global_offset(&self, local_offset: LocalOffset) -> GlobalOffset {
        match local_offset {
            LocalOffset::Small(offset) => offset,
            LocalOffset::Ghost(offset) => offset + self.small_end,
            LocalOffset::Main(offset) => offset + self.ghost_end,
        }
    }

    /// Gets an entry from the small or main queue by local offset.
    ///
    /// Returns None for ghost queue offsets (ghost only stores keys, not full entries).
    #[inline]
    pub fn get_entry(&self, local_offset: LocalOffset) -> Option<&Entry<K, V>> {
        match local_offset {
            LocalOffset::Small(offset) => Some(self.small.get_absolute_unchecked(offset as usize)),
            LocalOffset::Ghost(_) => None,
            LocalOffset::Main(offset) => Some(self.main.get_absolute_unchecked(offset as usize)),
        }
    }

    /// Gets a key from the ghost queue.
    #[inline]
    pub fn get_ghost_key(&self, offset: u32) -> &K {
        self.ghost.get_absolute_unchecked(offset as usize)
    }

    /// Gets an entry from the small queue by offset.
    #[inline]
    pub fn get_small_entry(&self, offset: u32) -> &Entry<K, V> {
        self.small.get_absolute_unchecked(offset as usize)
    }

    /// Gets an entry from the main queue by offset.
    #[inline]
    pub fn get_main_entry(&self, offset: u32) -> &Entry<K, V> {
        self.main.get_absolute_unchecked(offset as usize)
    }

    /// Compare the key stored at a global offset with the provided `key`.
    ///
    /// This is a convenience used by hashtable lookup helpers to verify whether
    /// an entry at `global_offset` corresponds to `key`. It resolves the global
    /// offset into the appropriate queue (small/main/ghost) and compares the
    /// stored key.
    #[inline]
    pub fn key_eq(&self, global_offset: GlobalOffset, key: &K) -> bool
    where
        K: PartialEq,
    {
        match self.local_offset(global_offset) {
            LocalOffset::Small(off) => self.get_small_entry(off).key == *key,
            LocalOffset::Main(off) => self.get_main_entry(off).key == *key,
            LocalOffset::Ghost(off) => self.get_ghost_key(off) == key,
        }
    }

    /// Compute the hash for the key stored at `global_offset` using the given
    /// `hasher`. This mirrors the `hash_key_at_offset` helper semantics used by
    /// other hashtable-adapter code.
    #[inline]
    pub fn hash_key_at_offset<S>(&self, global_offset: GlobalOffset, hasher: &S) -> u64
    where
        S: BuildHasher,
        K: Copy + Hash,
    {
        // Extract the key from the appropriate queue and hash it with the provided hasher.
        let key: K = match self.local_offset(global_offset) {
            LocalOffset::Small(off) => self.get_small_entry(off).key,
            LocalOffset::Main(off) => self.get_main_entry(off).key,
            LocalOffset::Ghost(off) => *self.get_ghost_key(off),
        };

        // Many hash builder helpers in this crate expose `hash_one`.
        // Call it to compute the 64-bit hash for the key.
        hasher.hash_one(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_fifos() {
        let mut fifos = S3Fifo::<u64, String>::new(100, 0.1, 0.9);

        // Push to small queue
        let entry = Entry::new(1u64, "hello".to_string());
        let offset = fifos.small.overwriting_push(entry);
        assert_eq!(offset, 0);

        // Read back via fifos
        let local_offset = fifos.local_offset(offset as u32);
        assert_eq!(local_offset, LocalOffset::Small(0));

        let entry = fifos.get_entry(local_offset).unwrap();
        assert_eq!(entry.key, 1);
        assert_eq!(entry.value, "hello");
    }

    #[test]
    fn test_ghost_queue() {
        let mut fifos = S3Fifo::<u64, String>::new(100, 0.1, 0.9);

        // Push key to ghost queue
        let offset = fifos.ghost.overwriting_push(42u64);
        let global_offset = fifos.global_offset(LocalOffset::Ghost(offset as u32));

        // Read back via fifos
        let local_offset = fifos.local_offset(global_offset);
        assert!(matches!(local_offset, LocalOffset::Ghost(_)));

        // Ghost entries return None for get_entry
        assert!(fifos.get_entry(local_offset).is_none());

        // But we can get the key directly
        if let LocalOffset::Ghost(off) = local_offset {
            assert_eq!(*fifos.get_ghost_key(off), 42);
        }
    }

    #[test]
    fn test_main_queue() {
        let mut fifos = S3Fifo::<u64, String>::new(100, 0.1, 0.9);

        // Push to main queue
        let entry = Entry::new(99u64, "main".to_string());
        let offset = fifos.main.try_push(entry).unwrap();
        let global_offset = fifos.global_offset(LocalOffset::Main(offset as u32));

        // Read back via fifos
        let local_offset = fifos.local_offset(global_offset);
        assert!(matches!(local_offset, LocalOffset::Main(_)));

        let entry = fifos.get_entry(local_offset).unwrap();
        assert_eq!(entry.key, 99);
        assert_eq!(entry.value, "main");
    }

    #[test]
    fn test_recency() {
        let mut fifos = S3Fifo::<u64, String>::new(100, 0.1, 0.9);

        let entry = Entry::new(1u64, "test".to_string());
        let offset = fifos.small.overwriting_push(entry);

        let local_offset = fifos.local_offset(offset as u32);
        let entry = fifos.get_entry(local_offset).unwrap();

        assert_eq!(entry.recency(), 0);
        entry.incr_recency();
        assert_eq!(entry.recency(), 1);
        entry.incr_recency();
        entry.incr_recency();
        entry.incr_recency(); // Should saturate at 3
        assert_eq!(entry.recency(), 3);

        entry.decr_recency();
        assert_eq!(entry.recency(), 2);
    }
}
