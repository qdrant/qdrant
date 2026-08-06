mod storage;
#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashMap;
use common::bitvec::BitVec;
use common::universal_io::{OpenOptions, UniversalWrite};
use parking_lot::RwLock;

use self::storage::BitmaskStorage;
pub use self::storage::{BitmaskFormat, BitmaskPaths};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

/// A fixed-size set of persisted flags that delays writing changes to the
/// underlying storage until they get flushed manually.
///
/// The flags are stored in one of two formats — see [`BitmaskFormat`] — which
/// differ in how a flush persists them, and in which of the [`BitmaskPaths`]
/// they occupy. [`Self::open`] accepts both, [`Self::create`] picks one.
///
/// This expects the underlying storage not to grow in size.
#[derive(Debug)]
pub struct BufferedUpdateBitSlice<S: UniversalWrite> {
    storage: Arc<RwLock<BitmaskStorage<S>>>,
    len: usize,
    pending_updates: Arc<RwLock<AHashMap<usize, bool>>>,
    /// Lock to prevent concurrent flush and drop
    is_alive_flush_lock: common::is_alive_lock::IsAliveLock,
}

impl<S: UniversalWrite + Send + Sync + 'static> BufferedUpdateBitSlice<S> {
    /// Open the flags persisted at whichever of `paths` exists, in the
    /// [`BitmaskFormat`] that file implies.
    ///
    /// `options` are honored by [`BitmaskFormat::Raw`], which keeps the file
    /// open; the compact format reads its file once and closes it.
    pub fn open(fs: &S::Fs, paths: &BitmaskPaths, options: OpenOptions) -> OperationResult<Self> {
        Ok(Self::new(BitmaskStorage::open(fs, paths, options)?))
    }

    /// Persist `bit_len` flags with `ones` set in the requested `format`,
    /// replacing whatever was at either of `paths`, and open them.
    ///
    /// `ones` must be ascending and within `0..bit_len`.
    pub fn create(
        fs: &S::Fs,
        paths: &BitmaskPaths,
        options: OpenOptions,
        format: BitmaskFormat,
        bit_len: usize,
        ones: impl IntoIterator<Item = u64>,
    ) -> OperationResult<Self> {
        Ok(Self::new(BitmaskStorage::create(
            fs, paths, options, format, bit_len, ones,
        )?))
    }

    fn new(storage: BitmaskStorage<S>) -> Self {
        let len = storage.bit_len() as usize;
        Self {
            storage: Arc::new(RwLock::new(storage)),
            len,
            pending_updates: Arc::new(RwLock::new(AHashMap::new())),
            is_alive_flush_lock: common::is_alive_lock::IsAliveLock::new(),
        }
    }

    /// The format the flags are persisted in.
    pub fn format(&self) -> BitmaskFormat {
        self.storage.read().format()
    }

    /// Path of the file the flags are actually stored in — the one of
    /// [`BitmaskPaths`] that matches [`Self::format`].
    pub fn path(&self) -> PathBuf {
        self.storage.read().path().to_path_buf()
    }

    /// Sets the bit at `index` to `value` buffered.
    ///
    /// ## Panics
    /// Panics if the index is out of bounds.
    pub fn set(&self, index: usize, value: bool) {
        assert!(index < self.len, "index {index} out of range: {}", self.len);
        self.pending_updates.write().insert(index, value);
    }

    /// Current value of the flag at `index`: the value [`set`](Self::set)
    /// since the last flush if there is one, else the persisted value.
    ///
    /// `None` when `index` is past [`len`](Self::len), and also when reading
    /// the storage fails — that error is logged, not propagated.
    ///
    /// Only for tests and benchmarks. Callers of this type keep their own
    /// resident mirror of the flags (seeded from [`read_all`](Self::read_all))
    /// and read that instead, so a per-flag read never touches the storage.
    #[cfg(any(test, feature = "testing"))]
    pub fn get(&self, index: usize) -> Option<bool> {
        if index >= self.len {
            return None;
        }
        if let Some(value) = self.pending_updates.read().get(&index) {
            Some(*value)
        } else {
            self.storage.read().get_bit(index).unwrap_or_else(|err| {
                log::error!("Error reading bit at index {index}: {err}");
                debug_assert!(false, "Error reading bit at index {index}: {err}");
                None
            })
        }
    }

    /// Read every flag, pending updates included, into an owned bitvec of
    /// [`Self::len`] bits.
    pub fn read_all(&self) -> OperationResult<BitVec> {
        let mut bits = self.storage.read().read_all()?;
        for (index, value) in self.pending_updates.read().iter() {
            bits.set(*index, *value);
        }
        Ok(bits)
    }

    /// Number of addressable flags.
    ///
    /// [`BitmaskFormat::Compact`] records the exact count it was created with;
    /// [`BitmaskFormat::Raw`] only knows its file length, so it rounds up to a
    /// whole `u64` word.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Removes from `pending_updates` all results that are flushed.
    /// If values in `pending_updates` are changed, do not remove them.
    fn reconcile_persisted_updates(
        pending_updates: &RwLock<AHashMap<usize, bool>>,
        persisted: AHashMap<usize, bool>,
    ) {
        pending_updates
            .write()
            .retain(|point_id, a| persisted.get(point_id).is_none_or(|b| a != b));
    }

    /// Hint to the OS that pages backing the underlying storage can be
    /// reclaimed.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            storage,
            len: _,
            pending_updates: _,
            is_alive_flush_lock: _,
        } = self;
        storage.read().clear_ram_cache()?;
        Ok(())
    }

    pub fn flusher(&self) -> Flusher {
        let updates = {
            let updates_guard = self.pending_updates.read();
            if updates_guard.is_empty() {
                return Box::new(|| Ok(()));
            }
            updates_guard.clone()
        };

        let storage = Arc::downgrade(&self.storage);
        let pending_updates_weak = Arc::downgrade(&self.pending_updates);
        let is_alive_flush_lock = self.is_alive_flush_lock.handle();

        Box::new(move || {
            let (Some(is_alive_flush_guard), Some(storage), Some(pending_updates_arc)) = (
                is_alive_flush_lock.lock_if_alive(),
                storage.upgrade(),
                pending_updates_weak.upgrade(),
            ) else {
                // Already dropped, skip flush
                log::trace!("BufferedUpdateBitSlice was dropped, cancelling flush");
                return Err(OperationError::cancelled(
                    "Aborted flushing on a dropped BufferedUpdateBitSlice instance",
                ));
            };

            storage.write().persist_updates(&updates)?;

            // Keep the guard till here to prevent concurrent drop/flushes
            // We don't touch files from here on and can drop the alive guard
            drop(is_alive_flush_guard);

            Self::reconcile_persisted_updates(&pending_updates_arc, updates);

            Ok(())
        })
    }
}
