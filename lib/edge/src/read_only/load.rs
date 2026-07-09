use std::path::PathBuf;

use rayon::ThreadPool;
use rayon::prelude::*;
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;
use uuid::Uuid;

/// Open the given segments in parallel on `pool` and return the ones that loaded, in input order.
///
/// Reuses the shard's long-lived search thread pool instead of spawning a fresh OS thread per
/// segment, bounding concurrency to the pool size.
///
/// The segment manifest is superset-biased, so it may list segments a read-only follower cannot
/// load — a not-yet-finalized segment, one already deleted, or an appendable write-buffer segment
/// that has no disk-resident id tracker. Per the manifest's reader contract these are skipped (with
/// a warning) instead of failing the whole open.
pub(crate) fn load_segments_parallel<S>(
    pool: &ThreadPool,
    fs: &S::Fs,
    segments: impl IntoIterator<Item = (Uuid, PathBuf)>,
) -> Vec<(Uuid, ReadOnlySegment<S>)>
where
    S: UniversalReadExt + 'static,
    S::Fs: Send + Sync + Clone + 'static,
{
    let segments: Vec<(Uuid, PathBuf)> = segments.into_iter().collect();

    pool.install(|| {
        segments
            .into_par_iter()
            .filter_map(|(uuid, segment_path)| {
                match ReadOnlySegment::<S>::open(fs, &segment_path, uuid, None) {
                    Ok(segment) => Some((uuid, segment)),
                    Err(err) => {
                        log::warn!("read-only open: skipping unloadable segment {uuid}: {err}");
                        None
                    }
                }
            })
            .collect()
    })
}
