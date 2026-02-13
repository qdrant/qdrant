use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

use memmap2::MmapMut;

use crate::raw_fifos::GlobalOffset;

// ---------------------------------------------------------------------------
// AsIndex — trait for keys usable as flat-array indices
// ---------------------------------------------------------------------------

/// Trait for key types that can be used as flat-array indices in
/// [`ArrayLookup`].
///
/// Implement this for any key type that maps to a dense `usize` range.
pub trait AsIndex {
    fn as_index(&self) -> usize;
}

impl AsIndex for u32 {
    #[inline]
    fn as_index(&self) -> usize {
        *self as usize
    }
}

impl AsIndex for u64 {
    #[inline]
    fn as_index(&self) -> usize {
        *self as usize
    }
}

impl AsIndex for usize {
    #[inline]
    fn as_index(&self) -> usize {
        *self
    }
}

// ---------------------------------------------------------------------------
// StoredOffset – sentinel encoding
// ---------------------------------------------------------------------------
// We use `u32::MAX` as a sentinel for "empty". This avoids the +1/−1
// arithmetic of the previous encoding. The trade-off is that freshly
// mmap'd pages (zero-initialized) must be filled with `0xFF` bytes so
// that every slot reads as `EMPTY`.

/// A stored value in the lookup array.
/// `u32::MAX` means empty, any other value is a `GlobalOffset` stored directly.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct StoredOffset(u32);

impl StoredOffset {
    const EMPTY: Self = Self(u32::MAX);

    #[inline]
    fn from_global(offset: GlobalOffset) -> Self {
        // GlobalOffset is u32. `u32::MAX` is reserved as the EMPTY sentinel,
        // so it cannot be stored. In practice offsets are bounded by total
        // FIFO capacity (order of millions), so this is never hit.
        debug_assert!(
            offset != u32::MAX,
            "GlobalOffset u32::MAX is reserved as EMPTY sentinel"
        );
        Self(offset)
    }

    #[inline]
    fn to_global(self) -> Option<GlobalOffset> {
        if self.0 == u32::MAX {
            None
        } else {
            Some(self.0)
        }
    }
}

// ---------------------------------------------------------------------------
// LookupView – immutable snapshot
// ---------------------------------------------------------------------------
// Heap-allocated via `Box` and kept alive for the entire lifetime of the
// `ArrayLookup`. This guarantees that a SeqLock reader who loaded a stale
// `current` pointer still dereferences valid, immutable memory.

struct LookupView {
    /// Points into an `MmapMut` that is kept alive in `ArrayLookup::mmaps`.
    data: *const StoredOffset,
    /// Number of `StoredOffset` entries (not bytes).
    len: usize,
}

// LookupView is read-only once published; the mmap it points into is never
// unmapped during the lifetime of ArrayLookup.
unsafe impl Send for LookupView {}
unsafe impl Sync for LookupView {}

// ---------------------------------------------------------------------------
// ArrayLookup
// ---------------------------------------------------------------------------

/// Minimum capacity (in number of entries) when the first allocation happens.
const MIN_CAPACITY: usize = 1024;

/// Growable, mmap-backed flat array: `key (usize) → GlobalOffset`.
///
/// # Design
///
/// * **O(1) lookup** – no hashing, no probing. `get(key)` is a bounds check
///   plus a single indexed read.
/// * **Growth** – when `set()` is called with a key beyond the current
///   capacity, a new anonymous mmap is created (zero-initialized, pages
///   demand-paged), old data is copied, and the atomic pointer (`current`)
///   is swapped.
/// * **SeqLock safety** – `current` is an `AtomicPtr`, so loads and stores
///   are explicit and well-ordered. Old `LookupView`s and their backing
///   mmaps are *retired but never freed*, so a reader that loaded a stale
///   pointer still accesses valid, immutable memory. The `(data, len)` pair
///   lives in the same immutable `LookupView`, eliminating torn-pair issues.
pub(crate) struct ArrayLookup {
    /// Atomic pointer to the current view.
    /// Readers load this pointer, then read the immutable `(data, len)`.
    /// Old views are never freed, so stale loads always point to valid memory.
    current: AtomicPtr<LookupView>,

    /// Keeps mmap regions alive. Only touched by the writer.
    mmaps: Vec<MmapMut>,

    /// Keeps retired `LookupView` allocations alive so that stale reader
    /// pointers remain valid. `Box` handles cleanup on drop automatically.
    views: Vec<Box<LookupView>>,
}

// SAFETY:
// * Writer-exclusive mutation: only the writer (which holds `&mut self`) ever
//   modifies the array or swaps `current`.
// * Reader-visible memory is never freed: old mmaps and LookupViews are
//   retired into Vecs that live as long as the ArrayLookup.
// * `current` is an AtomicPtr; loads/stores are well-defined.
unsafe impl Send for ArrayLookup {}
unsafe impl Sync for ArrayLookup {}

impl ArrayLookup {
    /// Create a new, empty `ArrayLookup`.
    ///
    /// No mmap memory is allocated until the first `set()` call.
    pub fn new() -> Self {
        // Start with an empty view so that `get()` always has a valid pointer
        // to dereference (with len == 0, every lookup is an out-of-bounds miss).
        let view = Box::new(LookupView {
            data: ptr::null(),
            len: 0,
        });
        let ptr = &*view as *const LookupView as *mut LookupView;

        Self {
            current: AtomicPtr::new(ptr),
            mmaps: Vec::new(),
            views: vec![view],
        }
    }

    // -----------------------------------------------------------------------
    // Reader path (called inside SeqLock read closure)
    // -----------------------------------------------------------------------

    /// Look up the `GlobalOffset` associated with `key`.
    ///
    /// Returns `None` if the slot is empty or the key is out of range.
    ///
    /// # Safety contract (upheld by design, not by caller)
    ///
    /// This method only performs:
    /// 1. An atomic load of `self.current` (never torn).
    /// 2. A bounds check against the immutable `len`.
    /// 3. An indexed read of a `u32` from the immutable mmap region.
    ///
    /// All referenced memory is valid for the lifetime of the `ArrayLookup`.
    #[inline]
    pub fn get(&self, key: usize) -> Option<GlobalOffset> {
        // SAFETY: `current` always points to a valid, immutable LookupView
        // that is kept alive in `self.views`.
        let view = unsafe { &*self.current.load(Ordering::Relaxed) };

        if key >= view.len {
            return None;
        }

        // SAFETY: `key < view.len` and `view.data` points to at least
        // `view.len` valid `StoredOffset` values in a live mmap.
        let stored = unsafe { *view.data.add(key) };
        stored.to_global()
    }

    // -----------------------------------------------------------------------
    // Writer path (called inside SeqLock write closure, &mut self)
    // -----------------------------------------------------------------------

    /// Associate `key` with `offset`. Grows the backing array if necessary.
    #[inline]
    pub fn set(&mut self, key: usize, offset: GlobalOffset) {
        self.ensure_capacity(key);

        // SAFETY: after ensure_capacity, the current view covers `key`.
        let view = unsafe { &*self.current.load(Ordering::Relaxed) };
        debug_assert!(key < view.len);

        // SAFETY: writer has exclusive access (&mut self). The slot is within
        // bounds and the mmap region is writable.
        let slot = unsafe { &mut *(view.data as *mut StoredOffset).add(key) };
        *slot = StoredOffset::from_global(offset);
    }

    /// Remove the entry at `key` (set it to empty).
    ///
    /// Does nothing if `key` is out of range.
    #[inline]
    pub fn remove(&mut self, key: usize) {
        let view = unsafe { &*self.current.load(Ordering::Relaxed) };

        if key >= view.len {
            return; // nothing to remove
        }

        // SAFETY: writer has exclusive access (&mut self). The slot is within
        // bounds and the mmap region is writable.
        let slot = unsafe { &mut *(view.data as *mut StoredOffset).add(key) };
        *slot = StoredOffset::EMPTY;
    }

    // -----------------------------------------------------------------------
    // Growth
    // -----------------------------------------------------------------------

    /// Ensure the backing array can hold at least `key + 1` entries.
    /// If growth is needed, a new mmap is allocated, old data is copied,
    /// and a new `LookupView` is published.
    fn ensure_capacity(&mut self, key: usize) {
        let view = unsafe { &*self.current.load(Ordering::Relaxed) };

        if key < view.len {
            return; // already fits
        }

        // Compute new capacity: next power of two ≥ key+1, at least MIN_CAPACITY.
        let required = key + 1;
        let new_capacity = required.next_power_of_two().max(MIN_CAPACITY);

        // Allocate a new anonymous mmap (zero-initialized by OS).
        let byte_len = new_capacity * size_of::<StoredOffset>();
        let mut new_mmap = MmapMut::map_anon(byte_len)
            .expect("failed to mmap anonymous region for ArrayLookup growth");

        // Fill the entire region with 0xFF so every slot is `StoredOffset::EMPTY`
        // (u32::MAX). We do this *before* copying old data on top.
        // SAFETY: the mmap region is writable and `byte_len` bytes long.
        unsafe {
            ptr::write_bytes(new_mmap.as_mut_ptr(), 0xFF, byte_len);
        }

        // Copy old data into the new mmap (overwrites the 0xFF fill for
        // those slots).
        if view.len > 0 {
            let old_bytes = view.len * size_of::<StoredOffset>();
            // SAFETY: old data pointer is valid for `old_bytes` bytes, and
            // new mmap is at least that large.
            unsafe {
                ptr::copy_nonoverlapping(view.data as *const u8, new_mmap.as_mut_ptr(), old_bytes);
            }
        }

        let new_data = new_mmap.as_ptr() as *const StoredOffset;

        // Create a new immutable view. The Box keeps it at a stable heap
        // address even when `self.views` reallocates.
        let new_view = Box::new(LookupView {
            data: new_data,
            len: new_capacity,
        });
        let new_ptr = &*new_view as *const LookupView as *mut LookupView;

        // Retire old mmap and view; they must stay alive for stale readers.
        self.mmaps.push(new_mmap);
        self.views.push(new_view);

        // Swing the atomic pointer. The SeqLock's sequence counter ensures
        // readers see a consistent snapshot.
        self.current.store(new_ptr, Ordering::Relaxed);
    }

    /// Returns the current capacity (number of slots) — exposed for tests.
    #[cfg(test)]
    fn capacity(&self) -> usize {
        unsafe { &*self.current.load(Ordering::Relaxed) }.len
    }
}

// No manual Drop needed: `Vec<Box<LookupView>>` frees the views automatically,
// and `Vec<MmapMut>` unmaps all regions on drop.

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stored_offset_encoding() {
        assert_eq!(StoredOffset::EMPTY.to_global(), None);
        assert_eq!(StoredOffset::from_global(0).to_global(), Some(0));
        assert_eq!(StoredOffset::from_global(42).to_global(), Some(42));
        assert_eq!(
            StoredOffset::from_global(u32::MAX - 1).to_global(),
            Some(u32::MAX - 1)
        );
    }

    #[test]
    fn test_empty_lookup() {
        let lookup = ArrayLookup::new();
        assert_eq!(lookup.get(0), None);
        assert_eq!(lookup.get(1), None);
        assert_eq!(lookup.get(999), None);
        assert_eq!(lookup.get(usize::MAX), None);
    }

    #[test]
    fn test_basic_set_and_get() {
        let mut lookup = ArrayLookup::new();

        lookup.set(0, 100);
        lookup.set(5, 200);
        lookup.set(10, 300);

        assert_eq!(lookup.get(0), Some(100));
        assert_eq!(lookup.get(5), Some(200));
        assert_eq!(lookup.get(10), Some(300));

        // Unset keys return None
        assert_eq!(lookup.get(1), None);
        assert_eq!(lookup.get(6), None);
    }

    #[test]
    fn test_overwrite() {
        let mut lookup = ArrayLookup::new();

        lookup.set(3, 10);
        assert_eq!(lookup.get(3), Some(10));

        lookup.set(3, 20);
        assert_eq!(lookup.get(3), Some(20));

        lookup.set(3, 0);
        assert_eq!(lookup.get(3), Some(0));
    }

    #[test]
    fn test_remove() {
        let mut lookup = ArrayLookup::new();

        lookup.set(7, 42);
        assert_eq!(lookup.get(7), Some(42));

        lookup.remove(7);
        assert_eq!(lookup.get(7), None);

        // Removing a non-existent key is a no-op
        lookup.remove(9999);
    }

    #[test]
    fn test_remove_out_of_range() {
        let mut lookup = ArrayLookup::new();
        // Should not panic even when the array is empty.
        lookup.remove(0);
        lookup.remove(1_000_000);
    }

    #[test]
    fn test_growth() {
        let mut lookup = ArrayLookup::new();

        // Insert a key within default MIN_CAPACITY
        lookup.set(100, 1);
        assert_eq!(lookup.get(100), Some(1));

        // Insert a key that forces growth beyond MIN_CAPACITY
        lookup.set(2000, 2);
        assert_eq!(lookup.get(2000), Some(2));

        // Old data must survive the growth
        assert_eq!(lookup.get(100), Some(1));

        // Insert an even larger key
        lookup.set(100_000, 3);
        assert_eq!(lookup.get(100_000), Some(3));
        assert_eq!(lookup.get(100), Some(1));
        assert_eq!(lookup.get(2000), Some(2));
    }

    #[test]
    fn test_growth_preserves_data() {
        let mut lookup = ArrayLookup::new();

        // Fill a bunch of sequential keys
        for i in 0..500u32 {
            lookup.set(i as usize, i * 10);
        }

        // Force a growth
        lookup.set(5000, 99);

        // Verify all old data is intact
        for i in 0..500u32 {
            assert_eq!(
                lookup.get(i as usize),
                Some(i * 10),
                "key {i} lost after growth"
            );
        }
        assert_eq!(lookup.get(5000), Some(99));
    }

    #[test]
    fn test_power_of_two_capacity() {
        let mut lookup = ArrayLookup::new();

        // Key 1023 → capacity should be max(1024, 1024) = 1024
        lookup.set(1023, 1);
        assert_eq!(lookup.capacity(), 1024);

        // Key 1024 → needs 1025 entries → next_power_of_two = 2048
        lookup.set(1024, 2);
        assert_eq!(lookup.capacity(), 2048);
    }

    #[test]
    fn test_large_key() {
        let mut lookup = ArrayLookup::new();

        let big_key = 1_000_000;
        lookup.set(big_key, 7);
        assert_eq!(lookup.get(big_key), Some(7));

        // Capacity should be a power of two ≥ 1_000_001
        let cap = lookup.capacity();
        assert!(cap >= big_key + 1);
        assert!(cap.is_power_of_two());
    }

    #[test]
    fn test_zero_global_offset() {
        // GlobalOffset 0 must be storable (it encodes as StoredOffset(1), not empty).
        let mut lookup = ArrayLookup::new();

        lookup.set(42, 0);
        assert_eq!(lookup.get(42), Some(0));

        lookup.remove(42);
        assert_eq!(lookup.get(42), None);
    }

    #[test]
    fn test_retired_mmaps_accumulate() {
        let mut lookup = ArrayLookup::new();

        // Each growth adds one mmap
        assert_eq!(lookup.mmaps.len(), 0);

        lookup.set(0, 1); // first growth → 1 mmap
        assert_eq!(lookup.mmaps.len(), 1);

        lookup.set(2000, 2); // second growth → 2 mmaps
        assert_eq!(lookup.mmaps.len(), 2);

        lookup.set(100_000, 3); // third growth → 3 mmaps
        assert_eq!(lookup.mmaps.len(), 3);

        // All data still accessible
        assert_eq!(lookup.get(0), Some(1));
        assert_eq!(lookup.get(2000), Some(2));
        assert_eq!(lookup.get(100_000), Some(3));
    }

    #[test]
    fn test_sequential_insert_and_read() {
        let mut lookup = ArrayLookup::new();
        let n = 10_000;

        for i in 0..n {
            lookup.set(i, i as u32);
        }

        for i in 0..n {
            assert_eq!(lookup.get(i), Some(i as u32));
        }
    }

    #[test]
    fn test_set_remove_set() {
        let mut lookup = ArrayLookup::new();

        lookup.set(5, 10);
        assert_eq!(lookup.get(5), Some(10));

        lookup.remove(5);
        assert_eq!(lookup.get(5), None);

        lookup.set(5, 20);
        assert_eq!(lookup.get(5), Some(20));
    }
}
