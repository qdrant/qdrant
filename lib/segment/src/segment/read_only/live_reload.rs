use std::sync::atomic::AtomicBool;

use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalReadFs, UniversalReadFsAsync};
use futures::future::{BoxFuture, join_all};

use super::{ReadOnlySegment, ReadOnlyVectorData};
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;
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
            deferred_deletes,
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
        // failed reload, nor deletions components still have to drop, skip the expensive
        // directory LIST and preloading entirely.
        if probe.is_unchanged()
            && pending_reload.borrow().is_empty()
            && deferred_deletes.borrow().is_empty()
        {
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
    /// A failed reload must leave readers on the previous view, so nothing a
    /// component applies before the publish may be observable:
    /// - New inserts are published to the id-tracker's readers only at the end,
    ///   so readers never reach an offset a component cannot serve.
    /// - Deletions reach components one successful reload late (see
    ///   [`ReadOnlySegment::deferred_deletes`]): until this reload publishes, an
    ///   offset it reports deleted may still be the one the tracker serves.
    pub fn live_reload(
        &mut self,
        max_committed_id: Option<PointOffsetType>,
    ) -> OperationResult<()> {
        self.live_reload_with_fault(max_committed_id, |_| Ok(()))
    }

    /// [`Self::live_reload`] with a hook called before each component reloads,
    /// so tests can fail a reload part-way.
    pub(super) fn live_reload_with_fault(
        &mut self,
        max_committed_id: Option<PointOffsetType>,
        fault: impl Fn(ReloadStage) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let Self {
            uuid: _,
            segment_path: _,
            id_tracker,
            vector_data,
            payload_index,
            payload_storage,
            pending_reload,
            deferred_deletes,
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
        let deferred = deferred_deletes.get_mut();

        log::trace!(target: "live-reload", "Pending live-reload in {} changes: {:?}, deferred deletes: {}", self.uuid, pending, deferred.len());

        if pending.is_empty() && deferred.is_empty() {
            id_tracker.borrow_mut().publish_staged();
            fs.rotate_cache_file_info();
            return Ok(());
        }

        // Replay the accumulated inserts, and the deletions an earlier reload already
        // published, to every component. Bail on the first error without clearing
        // either, so the next reload retries them; every component reload is
        // idempotent.
        {
            // SAFETY: `merge` keeps the lists sorted ascending; `deferred` is a list
            // `merge` produced.
            let deleted = unsafe { SortedSlice::new_unchecked(deferred) };
            let inserted = unsafe { SortedSlice::new_unchecked(&pending.inserted) };

            fault(ReloadStage::PayloadStorage)?;
            payload_storage
                .borrow_mut()
                .live_reload(fs, &deleted, &inserted)?;
            fault(ReloadStage::PayloadIndex)?;
            payload_index
                .borrow_mut()
                .live_reload(fs, &deleted, &inserted)?;

            for vector_data in vector_data.values() {
                fault(ReloadStage::Vectors)?;
                vector_data.live_reload(fs, &deleted, &inserted)?;
            }
        }

        // Every component is now in sync; publish the inserts. The offsets this
        // reload deleted are dead in the tracker from here on, so components drop
        // them on the next reload.
        id_tracker.borrow_mut().publish_staged();
        *deferred = std::mem::take(&mut pending.deleted);
        *pending = LiveReloadResult::default();
        fs.rotate_cache_file_info();

        Ok(())
    }
}

/// A component reload [`ReadOnlySegment::live_reload_with_fault`] is about to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReloadStage {
    PayloadStorage,
    PayloadIndex,
    Vectors,
}

impl<S: UniversalReadExt<Fs: UniversalReadFsAsync> + 'static> ReadOnlyVectorData<S> {
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
    pub(super) fn live_reload<Fs: UniversalReadFs<File = S>>(
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
