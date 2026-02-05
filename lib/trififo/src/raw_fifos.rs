use std::hash::{BuildHasher, Hash};
use std::num::NonZeroUsize;

use crate::entry::Entry;
use crate::ringbuffer::RingBuffer;

pub type GlobalOffset = u32;

/// Local offset within one of the three queues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalOffset {
    Small(u32),
    Ghost(u32),
    Main(u32),
}

/// Encapsulate the 3 FIFO queues used by S3-FIFO algorithm, where a global offset can be used to
/// index into any of them, as if they were consecutive arrays.
pub struct RawFifos<K, V> {
    small: RingBuffer<Entry<K, V>>,
    ghost: RingBuffer<K>,
    main: RingBuffer<Entry<K, V>>,

    small_end: u32,
    ghost_end: u32,
}

impl<K, V> RawFifos<K, V> {
    /// Initialize the queues with the provided sizes. The entire allocation is made upfront.
    ///
    /// # Arguments
    /// * `capacity` - Total capacity for small + main queues. Minimum is 2.
    /// * `small_ratio` - Fraction of capacity for small queue (typically 0.1)
    /// * `ghost_ratio` - Fraction of capacity for ghost queue (typically 0.9)
    pub fn new(capacity: usize, small_ratio: f32, ghost_ratio: f32) -> Self {
        assert!(small_ratio > 0.0 && small_ratio < 1.0);
        assert!(ghost_ratio > 0.0 && ghost_ratio < 1.0);

        let small_size = (capacity as f32 * small_ratio).ceil() as usize;
        let small_size = NonZeroUsize::try_from(small_size).unwrap_or(NonZeroUsize::MIN);
        let small = RingBuffer::new(small_size);

        let ghost_size = (capacity as f32 * ghost_ratio).ceil() as usize;
        let ghost_size = NonZeroUsize::try_from(ghost_size).unwrap_or(NonZeroUsize::MIN);
        let ghost = RingBuffer::new(ghost_size);

        let main_size = capacity - small_size.get();
        let main_size = NonZeroUsize::try_from(main_size).unwrap_or(NonZeroUsize::MIN);
        let main = RingBuffer::new(main_size);

        let small_end = small_size.get() as GlobalOffset;
        let ghost_end = small_end + ghost_size.get() as GlobalOffset;

        RawFifos {
            small,
            ghost,
            main,
            small_end,
            ghost_end,
        }
    }

    /// Returns number of entries currently cached.
    ///
    /// I.e. the ones which have a value, not including the ones in ghost queue.
    pub fn entries_len(&self) -> usize {
        self.small.len() + self.main.len()
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
    fn global_offset(&self, local_offset: LocalOffset) -> GlobalOffset {
        match local_offset {
            LocalOffset::Small(offset) => offset,
            LocalOffset::Ghost(offset) => offset + self.small_end,
            LocalOffset::Main(offset) => offset + self.ghost_end,
        }
    }

    pub fn get_entry(&self, global_offset: GlobalOffset) -> Option<&Entry<K, V>> {
        let local_offset = self.local_offset(global_offset);
        self.get_local_entry(local_offset)
    }

    /// Gets an entry from the small or main queue by local offset.
    ///
    /// Returns None for ghost queue offsets (ghost only stores keys, not full entries).
    #[inline]
    pub fn get_local_entry(&self, local_offset: LocalOffset) -> Option<&Entry<K, V>> {
        match local_offset {
            LocalOffset::Small(position) => Some(self.small.get_absolute(position as usize)?),
            LocalOffset::Ghost(_) => None,
            LocalOffset::Main(position) => Some(self.main.get_absolute(position as usize)?),
        }
    }

    fn get_local_key(&self, local_offset: LocalOffset) -> Option<&K> {
        match local_offset {
            LocalOffset::Small(position) => Some(&self.small.get_absolute(position as usize)?.key),
            LocalOffset::Main(position) => Some(&self.main.get_absolute(position as usize)?.key),
            LocalOffset::Ghost(position) => self.ghost.get_absolute(position as usize),
        }
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
        let local_offset = self.local_offset(global_offset);
        let Some(stored_key) = self.get_local_key(local_offset) else {
            return false;
        };
        stored_key == key
    }

    /// Compute the hash for the key stored at `global_offset` using the given
    /// `hasher`. This function is used by a hashtable for insertion.
    #[inline]
    pub fn hash_key_at_offset<S>(&self, global_offset: GlobalOffset, hasher: &S) -> u64
    where
        S: BuildHasher,
        K: Copy + Hash,
    {
        // Extract the key from the appropriate queue and hash it with the provided hasher.
        let local_offset = self.local_offset(global_offset);
        let key = self
            .get_local_key(local_offset)
            // Since we are the only writer, the offset is valid.
            .expect("This is used for rehashing, which means we must be the only writer");

        hasher.hash_one(key)
    }

    pub fn small_overwriting_push(&mut self, entry: Entry<K, V>) -> GlobalOffset {
        let local_offset = self.small.overwriting_push(entry);
        self.global_offset(LocalOffset::Small(local_offset as u32))
    }

    pub fn ghost_overwriting_push(&mut self, key: K) -> GlobalOffset {
        let local_offset = self.ghost.overwriting_push(key);
        self.global_offset(LocalOffset::Ghost(local_offset as u32))
    }

    pub fn main_overwriting_push(&mut self, entry: Entry<K, V>) -> GlobalOffset {
        let local_offset = self.main.overwriting_push(entry);
        self.global_offset(LocalOffset::Main(local_offset as u32))
    }

    pub fn small_try_push(&mut self, entry: Entry<K, V>) -> Result<GlobalOffset, Entry<K, V>> {
        let local_offset = self.small.try_push(entry)?;
        Ok(self.global_offset(LocalOffset::Small(local_offset as u32)))
    }

    pub fn ghost_try_push(&mut self, key: K) -> Result<GlobalOffset, K> {
        let local_offset = self.ghost.try_push(key)?;
        Ok(self.global_offset(LocalOffset::Ghost(local_offset as u32)))
    }

    pub fn main_try_push(&mut self, entry: Entry<K, V>) -> Result<GlobalOffset, Entry<K, V>> {
        let local_offset = self.main.try_push(entry)?;
        Ok(self.global_offset(LocalOffset::Main(local_offset as u32)))
    }

    pub fn main_reinsert_if(&mut self, f: impl FnOnce(&Entry<K, V>) -> bool) -> bool {
        self.main.reinsert_if(f)
    }

    pub fn small_eviction_candidate(&self) -> Option<&Entry<K, V>> {
        let position = self.small.write_position();
        self.small.get_absolute(position)
    }

    pub fn ghost_eviction_candidate(&self) -> Option<(GlobalOffset, &K)> {
        let position = self.ghost.write_position();

        let key = self.ghost.get_absolute(position)?;
        let global_offset = self.global_offset(LocalOffset::Ghost(position as u32));

        Some((global_offset, key))
    }

    pub fn main_eviction_candidate(&self) -> Option<(GlobalOffset, &Entry<K, V>)> {
        let position = self.main.write_position();
        let entry = self.main.get_absolute(position)?;
        let global_offset = self.global_offset(LocalOffset::Main(position as u32));

        Some((global_offset, entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_fifos() {
        let mut fifos = RawFifos::<u64, String>::new(100, 0.1, 0.9);

        // Push to small queue
        let entry = Entry::new(1u64, "hello".to_string());
        let offset = fifos.small.overwriting_push(entry);
        assert_eq!(offset, 0);

        // Read back via fifos
        let local_offset = fifos.local_offset(offset as u32);
        assert_eq!(local_offset, LocalOffset::Small(0));

        let entry = fifos.get_local_entry(local_offset).unwrap();
        assert_eq!(entry.key, 1);
        assert_eq!(entry.value, "hello");
    }

    #[test]
    fn test_ghost_queue() {
        let mut fifos = RawFifos::<u64, String>::new(100, 0.1, 0.9);

        // Push key to ghost queue
        let offset = fifos.ghost.overwriting_push(42u64);
        let global_offset = fifos.global_offset(LocalOffset::Ghost(offset as u32));

        // Read back via fifos
        let local_offset = fifos.local_offset(global_offset);
        assert!(matches!(local_offset, LocalOffset::Ghost(_)));

        // Ghost entries return None for get_entry
        assert!(fifos.get_local_entry(local_offset).is_none());

        // But the key is still there
        assert_eq!(*fifos.get_local_key(local_offset).unwrap(), 42);
    }

    #[test]
    fn test_main_queue() {
        let mut fifos = RawFifos::<u64, String>::new(100, 0.1, 0.9);

        // Push to main queue
        let entry = Entry::new(99u64, "main".to_string());
        let offset = fifos.main.try_push(entry).unwrap();
        let global_offset = fifos.global_offset(LocalOffset::Main(offset as u32));

        // Read back via fifos
        let local_offset = fifos.local_offset(global_offset);
        assert!(matches!(local_offset, LocalOffset::Main(_)));

        let entry = fifos.get_local_entry(local_offset).unwrap();
        assert_eq!(entry.key, 99);
        assert_eq!(entry.value, "main");
    }
}
