use std::sync::atomic::AtomicU8;

use crate::cache::GlobalOffset;
use crate::ringbuffer::RingBuffer;

pub(crate) struct Fifos<K, V> {
    pub small: RingBuffer<Entry<K, V>>,
    pub ghost: RingBuffer<K>,
    pub main: RingBuffer<Entry<K, V>>,

    pub small_end: GlobalOffset,
    pub ghost_end: GlobalOffset,
    pub main_end: GlobalOffset,
}

pub(crate) struct Entry<K, V> {
    pub key: K,
    pub value: V,
    pub recency: AtomicU8,
}

pub(crate) enum LocalOffset {
    Small(u32),
    Ghost(u32),
    Main(u32),
}

impl<K, V> Fifos<K, V>
where
    K: PartialEq,
{
    #[inline]
    pub fn local_offset(&self, global_offset: GlobalOffset) -> LocalOffset {
        if global_offset < self.small_end {
            LocalOffset::Small(global_offset)
        } else if global_offset < self.ghost_end {
            LocalOffset::Ghost(global_offset - self.small_end)
        } else {
            debug_assert!(global_offset < self.main_end);
            LocalOffset::Main(global_offset - self.ghost_end)
        }
    }

    #[inline]
    pub fn global_offset(&self, local_offset: LocalOffset) -> GlobalOffset {
        match local_offset {
            LocalOffset::Small(offset) => offset,
            LocalOffset::Ghost(offset) => offset + self.small_end,
            LocalOffset::Main(offset) => offset + self.ghost_end,
        }
    }

    #[inline]
    pub fn get_entry_by_local_offset(&self, local_offset: LocalOffset) -> Option<&Entry<K, V>> {
        match local_offset {
            LocalOffset::Small(offset) => self.small.get_absolute_unchecked(offset as usize),
            LocalOffset::Ghost(_offset) => None,
            LocalOffset::Main(offset) => self.main.get_absolute_unchecked(offset as usize),
        }
    }

    #[inline]
    pub fn get_key_by_local_offset(&self, local_offset: LocalOffset) -> Option<&K> {
        match local_offset {
            LocalOffset::Small(offset) => self
                .small
                .get_absolute_unchecked(offset as usize)
                .map(|entry| &entry.key),
            LocalOffset::Ghost(offset) => self.ghost.get_absolute_unchecked(offset as usize),
            LocalOffset::Main(offset) => self
                .main
                .get_absolute_unchecked(offset as usize)
                .map(|entry| &entry.key),
        }
    }

    #[inline]
    pub fn key_eq(&self, global_offset: GlobalOffset, key: &K) -> bool {
        let local_offset = self.local_offset(global_offset);
        let Some(entry_key) = self.get_key_by_local_offset(local_offset) else {
            return false;
        };
        entry_key == key
    }
}

impl<K, V> Entry<K, V> {
    pub fn recency(&self) -> u8 {
        self.recency.load(std::sync::atomic::Ordering::Relaxed)
    }

    // Increase recency by one, but saturate at 4
    pub fn incr_recency(&self) {
        let _ = self.recency.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |current| if current < 4 { Some(current + 1) } else { None },
        );
    }

    // Decrease recency by one
    pub fn decr_recency(&self) {
        let _ = self.recency.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |current| if current > 0 { Some(current - 1) } else { None },
        );
    }
}
