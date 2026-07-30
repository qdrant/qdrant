use std::path::Path;

use common::universal_io::{MmapFile, MmapFs, UniversalRead, UniversalReadFs};
use parking_lot::RwLock;
use segment::common::operation_error::OperationResult;
use segment::segment::update_only::UpdateOnlySegment;

use crate::read_only::{LocalSegmentEnumerator, SegmentEnumerator};
use crate::read_view::build_segment_pool;
use crate::update_only::UpdateOnlyEdgeShard;
use crate::update_only::holder::UpdateOnlySegmentHolder;

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
    /// Every discovered segment is opened, narrowly (see
    /// [`UpdateOnlySegment::open`]) and entirely cold: no data is fetched
    /// until a batch reads a point. A segment that fails to load is an error,
    /// not a skip — a writer that misses a segment would resolve a point
    /// against a stale copy of itself, or duplicate it.
    pub fn open(
        fs: S::Fs,
        path: &Path,
        enumerator: impl SegmentEnumerator + 'static,
    ) -> OperationResult<Self>
    where
        S::Fs: UniversalReadFs<File = S>,
    {
        let mut holder = UpdateOnlySegmentHolder::default();
        for (uuid, segment_path) in enumerator.list_segments()? {
            let segment = UpdateOnlySegment::<S>::open(&fs, &fs, &segment_path, uuid)?;
            holder.insert(uuid, segment);
        }

        if holder.is_empty() {
            // Creating the first appendable segment needs the append-only
            // components the writer cannot build yet, so an empty directory is
            // not something this iteration can bootstrap.
            todo!("creating the initial appendable segment needs the append-only components");
        }

        // Sized like the search pools: over-provisioned relative to the CPU
        // count, since on a remote backend the threads mostly wait on IO.
        let pool = build_segment_pool(
            "edge-update",
            common::defaults::search_thread_count(0),
            None,
        )?;

        Ok(Self {
            path: path.to_path_buf(),
            fs,
            segments: RwLock::new(holder),
            pool,
        })
    }
}
