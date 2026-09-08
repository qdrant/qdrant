use std::collections::HashMap;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::{
    MmapFs, UniversalAppendFs, UniversalWriteFsAsync,
};
use futures::StreamExt as _;
use parking_lot::RwLock;
use rayon::prelude::*;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::entry::{NonAppendableSegmentEntry as _, StorageSegmentEntry as _};
use segment::json_path::JsonPath;
use segment::segment::update_only::{LookupSegment, UpdateOnlySegmentEnum};
use segment::segment_constructor::build_segment;
use segment::types::{PayloadFieldSchema, SegmentConfig};
use shard::files::SEGMENTS_PATH;
use uuid::Uuid;

use crate::read_only::{ListedSegment, LocalSegmentEnumerator, SegmentEnumerator};
use crate::read_view::build_segment_pool;
use crate::update_only::UpdateOnlyEdgeShard;
use crate::update_only::holder::LookupSegmentHolder;

impl UpdateOnlyEdgeShard<MmapFs> {
    /// Open a writer over local memory-mapped files, discovering segments by
    /// scanning the `segments/` directory — the writer owns the directory it
    /// writes to, so there is no manifest to agree with.
    pub fn open_mmap(path: &Path) -> OperationResult<Self> {
        Self::open(MmapFs, path, LocalSegmentEnumerator::new(path))
    }
}

impl<Fs: UniversalAppendFs> UpdateOnlyEdgeShard<Fs> {
    /// Open a writer over the shard directory at `path`, using `fs` as the
    /// backend and `enumerator` to discover the segments.
    ///
    /// Segments are opened in parallel on the shard's thread pool, each over
    /// its own prefetching [`CachedFs`](common::universal_io::CachedFs) (see
    /// [`LookupSegment::open`]) — the same shape as the read-only
    /// follower's load — and entirely cold: no point data is fetched until a
    /// batch reads a point. Each segment's writer is opened here too, resuming
    /// from the state its lookup half just observed; the store components stay
    /// unopened until a point is actually stored. A segment that fails to load
    /// is an error, not a skip — a writer that misses a segment would resolve
    /// a point against a stale copy of itself, or duplicate it.
    pub fn open(
        fs: Fs,
        path: &Path,
        enumerator: impl SegmentEnumerator + 'static,
    ) -> OperationResult<Self> {
        // Sized like the search pools: over-provisioned relative to the CPU
        // count, since on a remote backend the threads mostly wait on IO.
        let pool = build_segment_pool(
            "edge-update",
            common::defaults::search_thread_count(0),
            None,
        )?;

        let segments: Vec<(Uuid, ListedSegment)> =
            enumerator.list_segments()?.into_iter().collect();
        let opened: Vec<_> = pool.install(|| {
            segments
                .into_par_iter()
                .map(|(uuid, listing)| {
                    let ListedSegment { path, writable } = listing;
                    // No deferred threshold yet: it belongs to the coordination
                    // with an external rebuilder, which does not exist in this
                    // iteration.
                    let segment = LookupSegment::open(fs.clone(), &path, None)?;
                    let writer = UpdateOnlySegmentEnum::open(
                        fs.clone(),
                        &path,
                        &segment.segment_config,
                        segment.writer_state(),
                    )?;
                    Ok((uuid, segment, writer, writable))
                })
                .collect::<OperationResult<Vec<_>>>()
        })?;

        let mut holder = LookupSegmentHolder::default();
        let mut writers = HashMap::new();
        for (uuid, segment, writer, writable) in opened {
            holder.insert(uuid, segment, writable);
            writers.insert(uuid, writer);
        }

        Ok(Self {
            path: path.to_path_buf(),
            fs,
            segments: RwLock::new(holder),
            writers,
            pool,
        })
    }
}

impl<Fs> UpdateOnlyEdgeShard<Fs>
where
    Fs: UniversalAppendFs + UniversalWriteFsAsync,
{
    /// [`create_appendable`](Self::create_appendable) with `source`'s config.
    #[cfg(test)]
    pub(crate) fn create_appendable_from(
        self,
        source: Uuid,
        indexed_fields: &HashMap<JsonPath, PayloadFieldSchema>,
        temp_path: &Path,
    ) -> OperationResult<(Self, Uuid)> {
        let config = self
            .segments
            .read()
            .get(source)?
            .read()
            .segment_config
            .clone();
        self.create_appendable(&config, indexed_fields, temp_path)
    }

    /// Build an empty appendable and adopt it as the write target; the
    /// manifest entry stays the caller's job. Also bootstraps a segmentless
    /// shard (an empty manifest opens with no write target).
    ///
    /// `temp_path` is a local directory the segment is built in before it is
    /// copied to the backend (conventionally `<shard>/temp_segments`); it is
    /// created if missing and left empty afterwards.
    pub fn create_appendable(
        mut self,
        config: &SegmentConfig,
        indexed_fields: &HashMap<JsonPath, PayloadFieldSchema>,
        temp_path: &Path,
    ) -> OperationResult<(Self, Uuid)> {
        // Built locally: the append-only components have no create path over a backend.
        fs_err::create_dir_all(temp_path)?;
        let scratch = tempfile::Builder::new()
            .prefix("appendable-")
            .tempdir_in(temp_path)
            .map_err(|err| OperationError::service_error(format!("create scratch dir: {err}")))?;
        let (mut segment, token) = build_segment(scratch.path(), config, None, true)?;
        let uuid = token.id();
        let hw_counter = HardwareCounterCell::disposable();
        for (key, schema) in indexed_fields {
            segment.create_field_index(0, key, Some(schema), &hw_counter)?;
        }
        segment.flush(true)?;
        let local = segment.data_path();
        drop(segment);

        let remote = self.path.join(SEGMENTS_PATH).join(uuid.to_string());
        futures::executor::block_on(copy_dir(&self.fs, &local, &remote))?;

        let lookup = LookupSegment::<Fs>::open(self.fs.clone(), &remote, None)?;
        let writer = UpdateOnlySegmentEnum::open(
            self.fs.clone(),
            &remote,
            &lookup.segment_config,
            lookup.writer_state(),
        )?;
        self.segments.write().insert(uuid, lookup, true);
        self.writers.insert(uuid, writer);
        Ok((self, uuid))
    }
}

/// Copy a locally built segment directory to the backend: directories first,
/// then every file as one whole-object save, as many at a time as the backend
/// takes ([`UniversalWriteFsAsync::max_concurrent_saves`]).
///
/// Every save is awaited even after one fails, so nothing is left in flight and
/// the cleanup covers every object that landed.
pub(crate) async fn copy_dir<F: UniversalWriteFsAsync>(
    fs: &F,
    local: &Path,
    remote: &Path,
) -> OperationResult<()> {
    let mut dirs = vec![remote.to_path_buf()];
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(local) {
        let entry = entry.map_err(|err| {
            OperationError::service_error(format!("walk {}: {err}", local.display()))
        })?;
        let path = entry.path();
        let rel = path.strip_prefix(local).map_err(|err| {
            OperationError::service_error(format!("relativize {}: {err}", path.display()))
        })?;
        if entry.file_type().is_dir() {
            if !rel.as_os_str().is_empty() {
                dirs.push(remote.join(rel));
            }
        } else if entry.file_type().is_file() {
            files.push((path.to_path_buf(), remote.join(rel)));
        }
    }

    // walkdir is pre-order, so parents precede children.
    for dir in dirs {
        fs.create_dir_async(dir.clone()).await.map_err(|err| {
            OperationError::service_error(format!("create dir {}: {err}", dir.display()))
        })?;
    }

    // Read inside the wave, so only the in-flight files are in memory.
    let saved: Vec<(PathBuf, OperationResult<()>)> = futures::stream::iter(files)
        .map(|(source, target)| async move {
            let result = match fs_err::read(&source) {
                Ok(bytes) => fs
                    .atomic_save_async(target.clone(), bytes)
                    .await
                    .map_err(|err| {
                        OperationError::service_error(format!("save {}: {err}", target.display()))
                    }),
                Err(err) => Err(OperationError::service_error(format!(
                    "read {}: {err}",
                    source.display()
                ))),
            };
            (target, result)
        })
        .buffer_unordered(fs.max_concurrent_saves().max(1))
        .collect()
        .await;

    let mut first_error = None;
    let mut landed = Vec::with_capacity(saved.len());
    for (path, result) in saved {
        match result {
            Ok(()) => landed.push(path),
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    if let Some(err) = first_error {
        for object in landed {
            if let Err(cleanup) = fs.remove_async(object.clone()).await {
                log::warn!(
                    "failed segment copy left {} behind: {cleanup}",
                    object.display()
                );
            }
        }
        return Err(err);
    }
    Ok(())
}
