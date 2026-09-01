use std::collections::HashMap;
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::universal_io::{
    MmapFile, MmapFs, OpenOptions, Populate, UniversalAppend, UniversalFlush as _, UniversalReadFs,
    UniversalReadFsAsync, UniversalWriteFileOps,
};
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

impl UpdateOnlyEdgeShard<MmapFile> {
    /// Open a writer over local memory-mapped files, discovering segments by
    /// scanning the `segments/` directory — the writer owns the directory it
    /// writes to, so there is no manifest to agree with.
    pub fn open_mmap(path: &Path) -> OperationResult<Self> {
        Self::open(MmapFs, path, LocalSegmentEnumerator::new(path))
    }
}

impl<S: UniversalAppend + 'static> UpdateOnlyEdgeShard<S> {
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
        fs: S::Fs,
        path: &Path,
        enumerator: impl SegmentEnumerator + 'static,
    ) -> OperationResult<Self>
    where
        S::Fs: UniversalReadFs<File = S> + UniversalReadFsAsync,
    {
        // Sized like the search pools: over-provisioned relative to the CPU
        // count, since on a remote backend the threads mostly wait on IO.
        let pool = build_segment_pool(
            "edge-update",
            common::defaults::search_thread_count(0),
            None,
        )?;

        let segments: Vec<(Uuid, ListedSegment)> =
            enumerator.list_segments()?.into_iter().collect();
        let opened: Vec<(Uuid, LookupSegment<S>, UpdateOnlySegmentEnum<S>, bool)> =
            pool.install(|| {
                segments
                    .into_par_iter()
                    .map(|(uuid, listing)| {
                        // No deferred threshold yet: it belongs to the coordination
                        // with an external rebuilder, which does not exist in this
                        // iteration.
                        let segment = LookupSegment::<S>::open(&fs, &listing.path, None)?;
                        let writer = UpdateOnlySegmentEnum::open(
                            &fs,
                            &listing.path,
                            &segment.segment_config,
                            segment.writer_state(),
                        )?;
                        Ok((uuid, segment, writer, listing.writable))
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

impl<S: UniversalAppend + 'static> UpdateOnlyEdgeShard<S>
where
    S::Fs: UniversalReadFs<File = S> + UniversalReadFsAsync,
{
    /// [`create_appendable`](Self::create_appendable) with `source`'s config.
    pub fn create_appendable_from(
        self,
        source: Uuid,
        indexed_fields: &HashMap<JsonPath, PayloadFieldSchema>,
    ) -> OperationResult<(Self, Uuid)> {
        let config = self
            .segments
            .read()
            .get(source)?
            .read()
            .segment_config
            .clone();
        self.create_appendable(&config, indexed_fields)
    }

    /// Build an empty appendable and adopt it as the write target; the
    /// manifest entry stays the caller's job. Also bootstraps a segmentless
    /// shard (an empty manifest opens with no write target).
    pub fn create_appendable(
        mut self,
        config: &SegmentConfig,
        indexed_fields: &HashMap<JsonPath, PayloadFieldSchema>,
    ) -> OperationResult<(Self, Uuid)> {
        // Built locally: the append-only components have no create path over a backend.
        let scratch = tempfile::tempdir()
            .map_err(|err| OperationError::service_error(format!("create scratch dir: {err}")))?;
        let (mut segment, _token) = build_segment(scratch.path(), config, None, true)?;
        let hw_counter = HardwareCounterCell::disposable();
        for (key, schema) in indexed_fields {
            segment.create_field_index(0, key, Some(schema), &hw_counter)?;
        }
        segment.flush(true)?;
        let local = segment.data_path();
        let uuid: Uuid = local
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse().ok())
            .ok_or_else(|| {
                OperationError::service_error(format!(
                    "built segment path carries no uuid: {}",
                    local.display(),
                ))
            })?;
        drop(segment);

        let remote = self.path.join(SEGMENTS_PATH).join(uuid.to_string());
        copy_dir_via(&self.fs, &local, &remote)?;

        let lookup = LookupSegment::<S>::open(&self.fs, &remote, None)?;
        let writer = UpdateOnlySegmentEnum::open(
            &self.fs,
            &remote,
            &lookup.segment_config,
            lookup.writer_state(),
        )?;
        self.segments.write().insert(uuid, lookup, true);
        self.writers.insert(uuid, writer);
        Ok((self, uuid))
    }
}

fn copy_dir_via<F: UniversalWriteFileOps>(
    fs: &F,
    local: &Path,
    remote: &Path,
) -> OperationResult<()> {
    let mut created = std::collections::HashSet::new();
    for entry in walkdir::WalkDir::new(local) {
        let entry = entry.map_err(|err| {
            OperationError::service_error(format!("walk {}: {err}", local.display()))
        })?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let rel = path.strip_prefix(local).map_err(|err| {
            OperationError::service_error(format!("relativize {}: {err}", path.display()))
        })?;
        let bytes = fs_err::read(path).map_err(|err| {
            OperationError::service_error(format!("read {}: {err}", path.display()))
        })?;
        let target = remote.join(rel);
        if let Some(parent) = target.parent()
            && created.insert(parent.to_path_buf())
        {
            fs.create_dir(parent)?;
        }
        // Length 0: a pre-sized file would conflict with the offset-0 append.
        fs.create(&target, 0)?;
        let options = OpenOptions {
            writeable: true,
            need_sequential: true,
            populate: Populate::No,
            advice: AdviceSetting::Global,
        };
        let mut file = fs.open_append(&target, options.for_append())?;
        file.append(0, bytes.as_slice())?;
        file.flusher()()?;
    }
    Ok(())
}
