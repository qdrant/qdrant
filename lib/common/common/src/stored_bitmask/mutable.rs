use std::borrow::Cow;
use std::path::Path;

use roaring::RoaringBitmap;

use super::format::MAX_LOGICAL_LEN;
use super::read::StoredBitmask;
use super::write::bitmask_file_bytes;
use crate::universal_io::{OpenOptions, UioResult, UniversalReadFs, UniversalWriteFileOps};

/// Mutable in-RAM handle over a bitmask persisted by [`save_bitmask`].
///
/// [`Self::open`] materializes the whole mask into a bitmap of the set
/// positions and drops the file handle; reads and mutations then operate on
/// RAM only. Changes are tracked per position, and [`Self::save`] rewrites
/// the whole file atomically — or skips the write entirely when the mask has
/// not effectively changed since it was opened or last saved.
///
/// Not a concurrency primitive: mutations and saves take `&mut self`, and
/// there is no interior locking. Sharing and flush orchestration belong to a
/// wrapper.
///
/// [`save_bitmask`]: super::save_bitmask
#[derive(Debug)]
pub struct MutableStoredBitmask {
    /// Number of logical flags (bits) in the mask.
    logical_len: u64,
    /// Authoritative set positions. Invariant: every position is below
    /// `logical_len`.
    ones: RoaringBitmap,
    /// Whether this structures has any in-memory changes that are not yet persisted.
    changed: bool,
    /// `logical_len` of the last persisted snapshot; `None` when the mask
    /// has never been persisted (fresh [`Self::new`]).
    persisted_len: Option<u64>,
}

impl MutableStoredBitmask {
    /// Fresh all-zeros mask of `logical_len` bits, not persisted anywhere
    /// yet: the first [`Self::save`] writes the file even with no set bits.
    ///
    /// # Panics
    ///
    /// If `logical_len` exceeds the `u32` position space.
    pub fn new(logical_len: u64) -> Self {
        assert!(
            logical_len <= MAX_LOGICAL_LEN,
            "bitmask of {logical_len} bits exceeds the u32 position space",
        );
        Self {
            logical_len,
            ones: RoaringBitmap::new(),
            changed: false,
            persisted_len: None,
        }
    }

    /// Open a persisted bitmask and materialize it into RAM; the file handle
    /// is dropped before returning. The result starts clean.
    ///
    /// A missing file is an error; compose with
    /// [`ok_not_found`](crate::universal_io::OkNotFound::ok_not_found) and
    /// [`Self::new`] to start from an empty mask instead.
    ///
    /// The mask is read once, whole: pass a non-writeable, sequential
    /// [`OpenOptions`] with blocking populate.
    pub fn open<Fs: UniversalReadFs>(
        fs: &Fs,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Fs::OpenExtra,
    ) -> UioResult<Self> {
        let stored = StoredBitmask::<Fs::File>::open(fs, path, options, extra)?;
        Ok(Self {
            logical_len: stored.bit_len(),
            ones: stored.read_ones()?,
            changed: false,
            persisted_len: Some(stored.bit_len()),
        })
    }

    /// Number of logical flags (bits) in the mask.
    pub fn bit_len(&self) -> u64 {
        self.logical_len
    }

    /// Number of set bits.
    pub fn count_ones(&self) -> u64 {
        self.ones.len()
    }

    /// The set positions; everything else in `0..bit_len` is unset.
    pub fn ones(&self) -> &RoaringBitmap {
        &self.ones
    }

    /// Value of the bit at `index`; `false` at and beyond [`Self::bit_len`].
    pub fn get(&self, index: u32) -> bool {
        self.ones.contains(index)
    }

    /// Set the bit at `index`, returning its previous value.
    ///
    /// # Panics
    ///
    /// If `index` is at or beyond [`Self::bit_len`].
    pub fn set(&mut self, index: u32, value: bool) -> bool {
        assert!(
            u64::from(index) < self.logical_len,
            "position {index} beyond bitmask of {} bits",
            self.logical_len,
        );
        let previous = if value {
            !self.ones.insert(index)
        } else {
            self.ones.remove(index)
        };
        if previous != value {
            self.changed = true;
        }
        previous
    }

    /// Grow the mask to `new_len` bits; the new bits are unset. Passing the
    /// current length is a no-op.
    ///
    /// # Panics
    ///
    /// If `new_len` would shrink the mask or exceeds the `u32` position
    /// space.
    pub fn set_len(&mut self, new_len: u64) {
        assert!(
            new_len >= self.logical_len,
            "cannot shrink bitmask of {} bits to {new_len}",
            self.logical_len,
        );
        assert!(
            new_len <= MAX_LOGICAL_LEN,
            "bitmask of {new_len} bits exceeds the u32 position space",
        );
        self.logical_len = new_len;
    }

    /// Whether the in-RAM state differs from the last persisted snapshot.
    pub fn is_dirty(&self) -> bool {
        self.changed || self.persisted_len != Some(self.logical_len)
    }

    /// Persist the mask at `path` in one atomic whole-file write, or skip
    /// the write entirely when nothing changed since the mask was opened or
    /// last saved.
    ///
    /// A failed save leaves the mask dirty, so a later retry writes again.
    pub fn save(&mut self, fs: &impl UniversalWriteFileOps, path: &Path) -> UioResult<()> {
        if !self.is_dirty() {
            return Ok(());
        }

        // Run-optimize in place so the encoder can serialize a borrow.
        self.ones.optimize();
        let bytes = bitmask_file_bytes(self.logical_len, Cow::Borrowed(&self.ones))?;
        fs.atomic_save(path, &bytes)?;

        self.changed = false;
        self.persisted_len = Some(self.logical_len);
        Ok(())
    }
}
