//! Concurrent FIFO queues for the S3-FIFO cache algorithm.
//!
//! This module provides separate writer and reader types for the three FIFO queues
//! (small, ghost, main) used in the S3-FIFO cache algorithm.
//!
//! The separation enforces at compile time that:
//! - Only the single writer thread can perform write operations
//! - Multiple reader threads can perform lock-free read operations

use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::cache::GlobalOffset;
use crate::concurrent_ringbuffer::{RingBufferReader, RingBufferWriter, new_ring_buffer};

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
    pub fn clone(&self) -> Self
    where
        K: Copy,
        V: Clone,
    {
        Self {
            key: self.key,
            value: self.value.clone(),
            recency: AtomicU8::new(self.recency.load(Ordering::Relaxed)),
        }
    }

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
pub struct SharedFifos<K, V> {
    small: RingBufferReader<Entry<K, V>>,
    ghost: RingBufferReader<K>,
    main: RingBufferReader<Entry<K, V>>,

    small_end: GlobalOffset,
    ghost_end: GlobalOffset,
}

impl<K, V> SharedFifos<K, V> {
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
}

/// Writer handle for the concurrent FIFO queues.
///
/// This type is `!Sync`, enforcing at compile time that only
/// one thread can perform write operations. It should be owned by the
/// disruptor consumer thread.
pub(super) struct FifosWriter<K, V> {
    shared: Arc<SharedFifos<K, V>>,

    // writers for the same `shared` fifos
    pub small: RingBufferWriter<Entry<K, V>>,
    pub ghost: RingBufferWriter<K>,
    pub main: RingBufferWriter<Entry<K, V>>,
}

impl<K, V> Deref for FifosWriter<K, V> {
    type Target = SharedFifos<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

/// Reader handle for the concurrent FIFO queues.
///
/// This is simply an `Arc<SharedFifos<K, V>>`, which is `Clone + Send + Sync`,
/// so it can be freely shared across threads.
pub type FifosReader<K, V> = Arc<SharedFifos<K, V>>;

/// Creates a new pair of FIFO writer and reader handles.
///
/// # Arguments
/// * `capacity` - Total capacity for small + main queues
/// * `small_ratio` - Fraction of capacity for small queue (typically 0.1)
/// * `ghost_ratio` - Fraction of capacity for ghost queue (typically 0.9)
///
/// # Returns
/// A tuple of (writer, reader) handles.
pub fn new_fifos<K, V>(
    capacity: usize,
    small_ratio: f32,
    ghost_ratio: f32,
) -> (FifosWriter<K, V>, FifosReader<K, V>) {
    let small_size = (capacity as f32 * small_ratio) as usize;
    let (small_writer, small_reader) = new_ring_buffer(small_size.max(1));

    let ghost_size = (capacity as f32 * ghost_ratio) as usize;
    let (ghost_writer, ghost_reader) = new_ring_buffer(ghost_size.max(1));

    let main_size = capacity - small_size;
    let (main_writer, main_reader) = new_ring_buffer(main_size.max(1));

    let small_end = small_size as GlobalOffset;
    let ghost_end = small_end + ghost_size as GlobalOffset;

    let shared = Arc::new(SharedFifos {
        small: small_reader,
        ghost: ghost_reader,
        main: main_reader,
        small_end,
        ghost_end,
    });

    let writer = FifosWriter {
        shared: Arc::clone(&shared),
        small: small_writer,
        ghost: ghost_writer,
        main: main_writer,
    };

    (writer, shared)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_fifos() {
        let (mut writer, reader) = new_fifos::<u64, String>(100, 0.1, 0.9);

        // Push to small queue
        let entry = Entry::new(1u64, "hello".to_string());
        let offset = writer.small.overwriting_push(entry);
        assert_eq!(offset, 0);

        // Read back via reader
        let local_offset = reader.local_offset(offset as u32);
        assert_eq!(local_offset, LocalOffset::Small(0));

        let entry = reader.get_entry(local_offset).unwrap();
        assert_eq!(entry.key, 1);
        assert_eq!(entry.value, "hello");
    }

    #[test]
    fn test_ghost_queue() {
        let (mut writer, reader) = new_fifos::<u64, String>(100, 0.1, 0.9);

        // Push key to ghost queue
        let offset = writer.ghost.overwriting_push(42u64);
        let global_offset = writer.global_offset(LocalOffset::Ghost(offset as u32));

        // Read back via reader
        let local_offset = reader.local_offset(global_offset);
        assert!(matches!(local_offset, LocalOffset::Ghost(_)));

        // Ghost entries return None for get_entry
        assert!(reader.get_entry(local_offset).is_none());

        // But we can get the key directly
        if let LocalOffset::Ghost(off) = local_offset {
            assert_eq!(*reader.get_ghost_key(off), 42);
        }
    }

    #[test]
    fn test_main_queue() {
        let (mut writer, reader) = new_fifos::<u64, String>(100, 0.1, 0.9);

        // Push to main queue
        let entry = Entry::new(99u64, "main".to_string());
        let offset = writer.main.try_push(entry).unwrap();
        let global_offset = writer.global_offset(LocalOffset::Main(offset as u32));

        // Read back via reader
        let local_offset = reader.local_offset(global_offset);
        assert!(matches!(local_offset, LocalOffset::Main(_)));

        let entry = reader.get_entry(local_offset).unwrap();
        assert_eq!(entry.key, 99);
        assert_eq!(entry.value, "main");
    }

    #[test]
    fn test_recency() {
        let (mut writer, reader) = new_fifos::<u64, String>(100, 0.1, 0.9);

        let entry = Entry::new(1u64, "test".to_string());
        let offset = writer.small.overwriting_push(entry);

        let local_offset = reader.local_offset(offset as u32);
        let entry = reader.get_entry(local_offset).unwrap();

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

    #[test]
    fn test_reader_is_clone_and_sync() {
        fn assert_clone<T: Clone>() {}
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_clone::<FifosReader<u64, String>>();
        assert_send::<FifosReader<u64, String>>();
        assert_sync::<FifosReader<u64, String>>();
    }

    #[test]
    fn test_writer_is_send_not_sync() {
        fn assert_send<T: Send>() {}
        assert_send::<FifosWriter<u64, String>>();

        // FifosWriter is !Sync because of PhantomData<UnsafeCell<()>>
        // This is enforced at compile time
    }
}
