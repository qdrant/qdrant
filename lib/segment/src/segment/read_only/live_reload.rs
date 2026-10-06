use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalReadFs, UniversalReadFsAsync};
use futures::future::{BoxFuture, join_all};

use super::{ReadOnlySegment, ReadOnlyVectorData};
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;
use crate::id_tracker::point_moves::MoveResolution;
use crate::index::UniversalReadExt;

impl<S: UniversalReadExt<Fs: UniversalReadFsAsync> + 'static> ReadOnlySegment<S> {
    /// Stage every component's next [`Self::live_reload`] under shared access:
    /// re-snapshot the retained caching filesystem's listing, schedule every
    /// fetch the reload will need, then drive them all to completion — so the
    /// reload only applies ready data.
    pub async fn live_preload(
        &self,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Option<PointOffsetType>> {
        let Self {
            uuid: _,
            segment_path: _,
            id_tracker,
            vector_data,
            payload_index,
            payload_storage,
            pending_reload,
            reload_fs,
            segment_type: _,
            segment_config: _,
        } = self;

        let mut reload_fs = reload_fs.borrow_mut();

        // 1. Probe the tracker files on the inner fs before taking the directory listing snapshot,
        // anchoring max_committed_id and live-reloading held handles in place.
        let probe = id_tracker
            .borrow()
            .probe_committed(reload_fs.inner())
            .await?;
        let max_committed_id = probe.max_committed_id();

        // 2. If nothing changed and there are no unapplied pending changes from a previous
        // failed reload, skip the expensive directory LIST and preloading entirely.
        if probe.is_unchanged() && pending_reload.borrow().is_empty() {
            return Ok(max_committed_id);
        }

        // 3. Take directory listing snapshot now that max_committed_id is anchored.
        reload_fs.cache_file_info_async().await?;

        check_process_stopped(is_stopped)?;

        let fs = &*reload_fs;

        let mut preloads = id_tracker.borrow().live_preload(fs)?;
        preloads.extend(payload_storage.borrow().live_preload(fs)?);
        preloads.extend(payload_index.borrow().live_preload(fs)?);
        for vector_data in vector_data.values() {
            preloads.extend(vector_data.live_preload(fs)?);
        }

        futures::join!(fs.wait_all(), join_all(preloads));
        Ok(max_committed_id)
    }

    /// Refresh every component to the current on-disk state (id-tracker delta → all components).
    ///
    /// Must follow a [`Self::live_preload`]: opens resolve against (and consume)
    /// what it staged, and files that appeared since its listing snapshot are
    /// not visible.
    ///
    /// Draining the id-tracker advances its internal state and cannot be replayed,
    /// so the delta is accumulated into `pending_reload` and only cleared once
    /// every component has reloaded successfully. If a component fails mid-way the
    /// delta is retained, and a later reload folds in the tracker's new changes and
    /// replays the union — no component is left drifting on a partial reload.
    ///
    /// New inserts are published to the id-tracker's readers only after that, so
    /// readers never reach an offset a component cannot serve.
    pub fn live_reload(
        &mut self,
        max_committed_id: Option<PointOffsetType>,
    ) -> OperationResult<()> {
        let Self {
            uuid: _,
            segment_path: _,
            id_tracker,
            vector_data,
            payload_index,
            payload_storage,
            pending_reload,
            reload_fs,
            segment_type: _,
            segment_config: _,
        } = self;

        let fs = &mut *reload_fs.get_mut();

        // Drain the tracker delta and fold it into whatever a previous reload left
        // unapplied. This must happen before any component reload can fail, so the
        // accumulated delta survives an error and is replayed on the next call.
        let fresh = id_tracker.borrow_mut().live_reload(fs, max_committed_id)?;
        let mut pending = pending_reload.borrow_mut();
        pending.merge(fresh);

        log::trace!(target: "live-reload", "Pending live-reload in {} changes: {:?}", self.uuid, pending);

        if pending.is_empty() {
            id_tracker.borrow_mut().publish_staged();
            fs.rotate_cache_file_info();
            return Ok(());
        }

        // Replay the full accumulated delta to every component. Bail on the first
        // error without clearing `pending`, so the next reload retries the union.
        {
            // SAFETY: `merge` keeps both lists sorted ascending.
            let deleted = unsafe { SortedSlice::new_unchecked(&pending.deleted) };
            let inserted = unsafe { SortedSlice::new_unchecked(&pending.inserted) };

            payload_storage
                .borrow_mut()
                .live_reload(fs, &deleted, &inserted)?;
            payload_index
                .borrow_mut()
                .live_reload(fs, &deleted, &inserted)?;

            for vector_data in vector_data.values() {
                vector_data.live_reload(fs, &deleted, &inserted)?;
            }
        }

        // Every component is now in sync; publish the inserts, discard the applied
        // delta and rotate file info.
        id_tracker.borrow_mut().publish_staged();
        *pending = LiveReloadResult::default();
        fs.rotate_cache_file_info();

        Ok(())
    }
}

/// Point moves, see [`point_moves`](crate::id_tracker::point_moves). A segment whose id tracker
/// does not resolve moves reports nothing to read and nothing to delete.
impl<S: UniversalReadExt<Fs: UniversalReadFsAsync> + 'static> ReadOnlySegment<S> {
    /// The raw backend the segment was opened on, for reads that must bypass the listing snapshot.
    pub fn raw_fs(&self) -> S::Fs {
        self.reload_fs.borrow().inner().clone()
    }

    /// Where a tail read of the move log has to start, if some tombstone waits for one to be
    /// classified. The read itself is the caller's, so no lock is held across it.
    pub fn point_moves_tail_to_read(&self) -> Option<(PathBuf, u64)> {
        self.id_tracker.borrow().point_moves_tail_to_read()
    }

    /// Take in a tail read of the move log from byte offset `start`.
    pub fn ingest_point_moves_tail(&mut self, start: u64, bytes: &[u8]) {
        self.id_tracker
            .borrow_mut()
            .ingest_point_moves_tail(start, bytes);
    }

    /// Delete what `resolution` names, in the id tracker and in every component.
    ///
    /// Components only apply the deletions, they touch no file, so this does no IO. The delta goes
    /// through `pending_reload` like a reload's: if a component fails, the next reload replays it.
    pub fn apply_point_moves(&mut self, resolution: &MoveResolution) -> OperationResult<()> {
        if resolution.is_empty() {
            return Ok(());
        }

        let Self {
            uuid: _,
            segment_path: _,
            id_tracker,
            vector_data,
            payload_index,
            payload_storage,
            pending_reload,
            reload_fs: _,
            segment_type: _,
            segment_config: _,
        } = self;

        let fresh = id_tracker.borrow_mut().apply_point_moves(resolution);
        let mut pending = pending_reload.borrow_mut();
        pending.merge(fresh);

        {
            // SAFETY: `merge` keeps both lists sorted ascending.
            let deleted = unsafe { SortedSlice::new_unchecked(&pending.deleted) };

            payload_storage.borrow_mut().apply_deletions(&deleted)?;
            payload_index.borrow_mut().apply_deletions(&deleted)?;
            for vector_data in vector_data.values() {
                vector_data.apply_deletions(&deleted)?;
            }
        }

        // Inserts a failed reload left unapplied stay, for the next reload to replay with the
        // deletions, which is idempotent
        if pending.inserted.is_empty() {
            *pending = LiveReloadResult::default();
        }
        Ok(())
    }
}

impl<S: UniversalReadExt<Fs: UniversalReadFsAsync> + 'static> ReadOnlyVectorData<S> {
    /// Apply `deleted` to this vector's storage, index and quantized vectors, without IO.
    fn apply_deletions(&self, deleted: &SortedSlice<'_, PointOffsetType>) -> OperationResult<()> {
        let Self {
            vector_index,
            vector_storage,
            quantized_vectors,
        } = self;

        vector_storage.borrow_mut().apply_deletions(deleted)?;
        vector_index.borrow_mut().apply_deletions(deleted)?;
        if let Some(quantized_vectors) = quantized_vectors.borrow_mut().as_mut() {
            quantized_vectors.apply_deletions(deleted)?;
        }
        Ok(())
    }

    /// Stage this vector's next [`Self::live_reload`]. Shared access only.
    fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        let Self {
            vector_index,
            vector_storage,
            quantized_vectors,
        } = self;

        let mut futs = vector_storage.borrow().live_preload(fs)?;
        futs.extend(vector_index.borrow().live_preload(fs)?);
        if let Some(quantized_vectors) = quantized_vectors.borrow().as_ref() {
            futs.extend(quantized_vectors.live_preload(fs)?);
        }
        Ok(futs)
    }

    /// Refresh this vector's storage, index and quantized vectors to the current
    /// on-disk state.
    ///
    /// `Self` is destructured so that every component is covered: adding a field
    /// without reloading it won't compile. Each component mutates through its own
    /// `Arc<AtomicRefCell<_>>`, so `&self` is enough — no `&mut` is needed.
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &self,
        fs: &Fs,
        deleted: &SortedSlice<'_, PointOffsetType>,
        inserted: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        let Self {
            vector_index,
            vector_storage,
            quantized_vectors,
        } = self;

        // Storage strictly before index: the mutable-RAM sparse index reads
        // the newly inserted vectors from this very storage to fold them into
        // its inverted index (see `ReadOnlySparseVectorIndex::live_reload`).
        vector_storage
            .borrow_mut()
            .live_reload(fs, deleted, inserted)?;
        vector_index
            .borrow_mut()
            .live_reload(fs, deleted, inserted)?;
        if let Some(quantized_vectors) = quantized_vectors.borrow_mut().as_mut() {
            quantized_vectors.live_reload(fs, deleted, inserted)?;
        }

        Ok(())
    }
}
