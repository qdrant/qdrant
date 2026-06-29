use std::path::PathBuf;

use rayon::ThreadPool;
use rayon::prelude::*;
use segment::common::operation_error::OperationResult;
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;
use uuid::Uuid;

/// Open the given segments in parallel on `pool` and return them in input order.
///
/// Reuses the shard's long-lived search thread pool instead of spawning a fresh OS thread per
/// segment, bounding concurrency to the pool size.
pub(crate) fn load_segments_parallel<S>(
    pool: &ThreadPool,
    fs: &S::Fs,
    segments: impl IntoIterator<Item = (Uuid, PathBuf)>,
) -> OperationResult<Vec<(Uuid, ReadOnlySegment<S>)>>
where
    S: UniversalReadExt + 'static,
    S::Fs: Send + Sync + Clone + 'static,
{
    let segments: Vec<(Uuid, PathBuf)> = segments.into_iter().collect();

    pool.install(|| {
        segments
            .into_par_iter()
            .map(|(uuid, segment_path)| {
                let segment = ReadOnlySegment::<S>::open(fs, &segment_path, uuid, None)?;
                Ok((uuid, segment))
            })
            .collect()
    })
}
