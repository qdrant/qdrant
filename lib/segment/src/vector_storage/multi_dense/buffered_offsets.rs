//! Write-back buffer over the multi-vector `offsets` store.
//!
//! It prevents an inconsistent on-disk state from ever becoming durable, rather
//! than patching it up after the fact on reload.
//!
//! The structure deliberately follows the Gridstore flusher convention
//! (`lib/blobstore/src/blobstore/mod.rs` + `tracker/mod.rs`):
//!
//! - pending writes live *inside* the lock-guarded store (one `Arc<RwLock<_>>`),
//!   so a read consults the pending overlay and the durable bytes under a single
//!   lock, exactly like `Tracker::get`;
//! - the flusher clones the pending set at creation time, then applies + reconciles
//!   it under the write lock and builds the backing flusher there, but runs the
//!   durable msync *after* releasing the lock — Gridstore's `flush_tracker` never
//!   holds the store lock across the sync, and neither do we;
//! - `is_alive` + `Weak` upgrades cancel the flush if the storage was dropped.
//!
//! ## The skew it closes
//!
//! Each `ChunkedVectors` flusher snapshots its `status.len` at *creation* time but
//! msyncs chunk bytes at *execution* time. A re-upsert that grows a point past its
//! reserved capacity rewrites its `offsets` entry in place to a freshly-appended
//! row region. If that lands inside a flush's creation-to-execution window, the
//! relocated entry becomes durable while the `vectors` store's recorded length
//! still predates the rows it references: a durable forward reference into rows
//! the length does not cover. On reload that point is unreadable, and a WAL append
//! reuses the rows and overwrites another point.
//!
//! ## How buffering closes it
//!
//! Offset entries are staged in `pending` and only written into the durable store
//! while a flush executes. The flusher snapshots the pending set at creation time,
//! so any entry written after the flusher is created stays buffered for the *next*
//! flush. Combined with the `vectors` store persisting its own (creation-time)
//! length snapshot, the durable offsets can never reference rows beyond the durable
//! `vectors` length:
//!
//! - In memory the two stores are always consistent: `insert_multi_native` writes
//!   the rows (bumping `vectors.len`) before it sets the offset entry, so at every
//!   instant `entry.offset + entry.capacity <= vectors.len`.
//! - Both flushers snapshot at the same instant (the storage `flusher()` call), so
//!   the durable offsets and the durable `vectors.len` are a consistent cut of that
//!   instant. Rows written after the cut are durable-but-unreferenced garbage that
//!   the next append harmlessly overwrites.
//!
//! As a bonus, the backing store is flushed synchronously at apply time, so the
//! offsets store has no length smear of its own either — closing the residual
//! `offsets`-length skew the repair approach left open.

use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::is_alive_lock::IsAliveLock;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
use parking_lot::RwLock;

use super::appendable_mmap_multi_dense_vector_storage::MultivectorMmapOffset;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::ChunkedVectors;

type OffsetsStore = ChunkedVectors<MultivectorMmapOffset, MmapFile>;

/// Durable store plus the not-yet-flushed writes, guarded by a single lock so that
/// reads see a consistent overlay (mirrors Gridstore's `Tracker`).
#[derive(Debug)]
struct Inner {
    /// Durable backing store. Mutated only while a flush executes.
    store: OffsetsStore,
    /// Writes not yet flushed. Reads consult this before `store`.
    pending: AHashMap<VectorOffsetType, MultivectorMmapOffset>,
    /// Logical length, including not-yet-flushed appends (always `>= store.len()`).
    len: usize,
}

impl Inner {
    /// Read the offset entry for `key`: pending overlay first, then the durable store.
    fn get<P: AccessPattern>(&self, key: VectorOffsetType) -> Option<MultivectorMmapOffset> {
        if let Some(entry) = self.pending.get(&key) {
            return Some(*entry);
        }
        let &[entry] = self.store.get::<P>(key)?.as_ref() else {
            unreachable!("multi-vector offsets are stored as vectors of length 1");
        };
        Some(entry)
    }
}

#[derive(Debug)]
pub(super) struct BufferedOffsets {
    inner: Arc<RwLock<Inner>>,
    is_alive: IsAliveLock,
}

impl BufferedOffsets {
    pub(super) fn new(store: OffsetsStore) -> Self {
        let len = store.len();
        Self {
            inner: Arc::new(RwLock::new(Inner {
                store,
                pending: AHashMap::new(),
                len,
            })),
            is_alive: IsAliveLock::new(),
        }
    }

    /// Read the offset entry for `key` (pending overlay then durable store).
    pub(super) fn get<P: AccessPattern>(
        &self,
        key: VectorOffsetType,
    ) -> Option<MultivectorMmapOffset> {
        self.inner.read().get::<P>(key)
    }

    /// Resolve a batch of keys to `(user_data, row_offset, count)` under a single
    /// read lock, ready to hand to `ChunkedVectors::iter_vectors`. Holding the lock
    /// once for the whole batch mirrors `Tracker::iter`.
    pub(super) fn resolve_rows<P, U, I>(&self, keys: I) -> Vec<(U, PointOffsetType, u32)>
    where
        P: AccessPattern,
        I: IntoIterator<Item = (U, PointOffsetType)>,
    {
        let inner = self.inner.read();
        keys.into_iter()
            .map(|(user_data, key)| {
                let entry = inner
                    .get::<P>(key as VectorOffsetType)
                    .expect("offset entry exists");
                (user_data, entry.offset, entry.count)
            })
            .collect()
    }

    /// Stage a write for `key`. It becomes durable on the first flush whose
    /// flusher is created after this call returns.
    pub(super) fn set(&self, key: VectorOffsetType, entry: MultivectorMmapOffset) {
        let mut inner = self.inner.write();
        inner.pending.insert(key, entry);
        inner.len = inner.len.max(key + 1);
    }

    pub(super) fn len(&self) -> usize {
        self.inner.read().len
    }

    pub(super) fn populate(&self) -> OperationResult<()> {
        self.inner.read().store.populate()
    }

    pub(super) fn clear_cache(&self) -> OperationResult<()> {
        self.inner.read().store.clear_cache()
    }

    pub(super) fn files(&self) -> Vec<PathBuf> {
        self.inner.read().store.files()
    }

    pub(super) fn immutable_files(&self) -> Vec<PathBuf> {
        self.inner.read().store.immutable_files()
    }

    pub(super) fn flusher(&self) -> Flusher {
        // Snapshot the pending writes at *creation* time. Anything written after
        // this clone stays buffered for the next flush — that is what keeps the
        // durable offsets from racing ahead of the durable `vectors` length.
        let snapshot: AHashMap<VectorOffsetType, MultivectorMmapOffset> =
            self.inner.read().pending.clone();

        let inner = Arc::downgrade(&self.inner);
        let is_alive = self.is_alive.handle();

        Box::new(move || {
            let (Some(_guard), Some(inner)) = (is_alive.lock_if_alive(), inner.upgrade()) else {
                // Storage was dropped before the flusher ran; nothing to persist.
                return Ok(());
            };

            // Apply + reconcile + build the backing flusher under the write lock,
            // then release the lock and run the durable sync — Gridstore's
            // `flush_tracker` likewise never holds the store lock across the msync,
            // so concurrent reads are not stalled by the sync.
            //
            // The backing flusher runs on every flush, even with an empty snapshot:
            // the underlying mmap sync only writes still-dirty pages, so a sync that
            // failed on an earlier flush is retried on the next one. This matches
            // Gridstore (its tracker store is synced unconditionally) and the sibling
            // `vectors` store; short-circuiting an empty snapshot would instead leave
            // a failed offsets sync stranded while `vectors` recovered, reopening the
            // very skew this buffer closes.
            let store_flusher = {
                // Apply in ascending key order so the backing store grows its
                // chunks/length monotonically (appends sit at the current end).
                // Dense key allocation (`new_id = len`) guarantees no gaps: any
                // append key in the snapshot has every lower key already durable
                // or applied in this same pass.
                let mut items: Vec<(VectorOffsetType, MultivectorMmapOffset)> =
                    snapshot.iter().map(|(key, entry)| (*key, *entry)).collect();
                items.sort_unstable_by_key(|(key, _)| *key);

                let mut inner = inner.write();

                let hw_counter = HardwareCounterCell::disposable();
                for (key, entry) in &items {
                    inner.store.insert(*key, &[*entry], &hw_counter)?;
                }

                // Drop every key we just persisted from the live pending set, unless
                // it was overwritten with a newer value after the snapshot was taken.
                inner.pending.retain(|key, value| {
                    snapshot.get(key).is_none_or(|persisted| persisted != value)
                });

                // Create the flusher here so its `status.len` snapshot covers the
                // entries just applied; the inner guard drops before we sync below.
                inner.store.flusher()
            };

            store_flusher()
        })
    }
}

#[cfg(test)]
mod tests {
    use common::generic_consts::Sequential;
    use common::mmap::AdviceSetting;
    use common::universal_io::{MmapFs, Populate};
    use tempfile::Builder;

    use super::*;

    fn offset(o: u32, c: u32) -> MultivectorMmapOffset {
        MultivectorMmapOffset {
            offset: o,
            count: c,
            capacity: c,
        }
    }

    fn open_inner(dir: &std::path::Path) -> OffsetsStore {
        ChunkedVectors::open(MmapFs, dir, 1, AdviceSetting::Global, Populate::No).unwrap()
    }

    /// The core property: a write that lands *after* a flusher is created must not
    /// become durable through that flush. It stays buffered for the next one.
    ///
    /// This is exactly the creation-to-execution window that corrupts the storage
    /// today; here it is a no-op on disk.
    #[test]
    fn post_snapshot_write_does_not_leak_into_durable_store() {
        let dir = Builder::new().prefix("buffered_offsets").tempdir().unwrap();

        let store = BufferedOffsets::new(open_inner(dir.path()));
        // Two points appended and durable.
        store.set(0, offset(0, 2));
        store.set(1, offset(2, 3));

        // A flush is started: its flusher snapshots {0, 1} now.
        let flush = store.flusher();

        // Point 1 then grows and relocates — the dangerous in-window write.
        store.set(1, offset(5, 5));

        // The flush executes. It must persist only the snapshot ({0,1-old}), not
        // the post-snapshot relocation.
        flush().unwrap();

        // Reopen the durable store directly and inspect what actually landed.
        drop(store);
        let durable = open_inner(dir.path());
        assert_eq!(durable.len(), 2, "both points are durable");
        let &[e1] = durable.get::<Sequential>(1).unwrap().as_ref() else {
            unreachable!()
        };
        assert_eq!(
            e1,
            offset(2, 3),
            "durable entry for point 1 must be the pre-snapshot value, not the relocation",
        );

        // The in-memory view still reflects the latest write (read-through overlay),
        // and the relocation persists on the *next* flush.
        let store = BufferedOffsets::new(durable);
        assert_eq!(store.get::<Sequential>(1), Some(offset(2, 3)));
    }

    /// A second flush commits what the first one deferred — no data is lost, the
    /// commit point just moves forward consistently.
    #[test]
    fn deferred_write_commits_on_next_flush() {
        let dir = Builder::new().prefix("buffered_offsets").tempdir().unwrap();

        let store = BufferedOffsets::new(open_inner(dir.path()));
        store.set(0, offset(0, 2));
        let flush = store.flusher();
        store.set(1, offset(2, 3)); // appended after the snapshot
        flush().unwrap();

        // Read-through sees it immediately even though it is not durable yet.
        assert_eq!(store.get::<Sequential>(1), Some(offset(2, 3)));
        assert_eq!(store.len(), 2);

        // Next flush persists it.
        store.flusher()().unwrap();
        drop(store);

        let durable = open_inner(dir.path());
        assert_eq!(durable.len(), 2);
        let &[e1] = durable.get::<Sequential>(1).unwrap().as_ref() else {
            unreachable!()
        };
        assert_eq!(e1, offset(2, 3));
    }
}
