use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::universal_io::{MmapFile, MmapFs, UniversalRead, UniversalReadFs};
use parking_lot::RwLock;
use rayon::prelude::*;
use segment::common::operation_error::OperationResult;
use segment::segment::update_only::LookupSegment;
use uuid::Uuid;

use crate::read_only::{LocalSegmentEnumerator, SegmentEnumerator};
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

impl<S: UniversalRead + 'static> UpdateOnlyEdgeShard<S> {
    /// Open a writer over the shard directory at `path`, using `fs` as the
    /// read backend and `enumerator` to discover the segments.
    ///
    /// Segments are opened in parallel on the shard's thread pool, each over
    /// its own prefetching [`CachedFs`](common::universal_io::CachedFs) (see
    /// [`LookupSegment::open`]) — the same shape as the read-only
    /// follower's load — and entirely cold: no point data is fetched until a
    /// batch reads a point. A segment that fails to load is an error, not a
    /// skip — a writer that misses a segment would resolve a point against a
    /// stale copy of itself, or duplicate it.
    pub fn open(
        fs: S::Fs,
        path: &Path,
        enumerator: impl SegmentEnumerator + 'static,
    ) -> OperationResult<Self>
    where
        S::Fs: UniversalReadFs<File = S>,
    {
        // Sized like the search pools: over-provisioned relative to the CPU
        // count, since on a remote backend the threads mostly wait on IO.
        let pool = build_segment_pool(
            "edge-update",
            common::defaults::search_thread_count(0),
            None,
        )?;

        let segments: Vec<(Uuid, PathBuf)> = enumerator.list_segments()?.into_iter().collect();
        let opened: Vec<(Uuid, LookupSegment<S>)> = pool.install(|| {
            segments
                .into_par_iter()
                .map(|(uuid, segment_path)| {
                    // No deferred threshold yet: it belongs to the coordination
                    // with an external rebuilder, which does not exist in this
                    // iteration.
                    let segment = LookupSegment::<S>::open(&fs, &segment_path, uuid, None)?;
                    Ok((uuid, segment))
                })
                .collect::<OperationResult<Vec<_>>>()
        })?;

        let mut holder = LookupSegmentHolder::default();
        for (uuid, segment) in opened {
            holder.insert(uuid, segment);
        }

        if holder.is_empty() {
            // Creating the first appendable segment needs the append-only
            // components the writer cannot build yet, so an empty directory is
            // not something this iteration can bootstrap.
            todo!("creating the initial appendable segment needs the append-only components");
        }

        Ok(Self {
            path: path.to_path_buf(),
            fs,
            segments: RwLock::new(holder),
            pool,
            applied: AtomicBool::new(false),
        })
    }
}
