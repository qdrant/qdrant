use std::path::PathBuf;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use futures::future::join_all;
use parking_lot::RwLock;
use rayon::ThreadPool;
use rayon::prelude::*;
use segment::common::operation_error::OperationResult;
use segment::data_types::load_profile::LoadProfile;
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;
use uuid::Uuid;

/// Open the given segments and return the ones that loaded, in input order.
///
/// The IO never rides `pool` (the shard's search pool): every segment's
/// fetches are staged up front and driven to completion on the calling
/// thread, so the pool only runs the CPU-bound assembly and searches are
/// not stalled behind parked IO threads.
///
/// A `load_profile` (see [`LoadProfile`]) parks the components the shard's request won't touch
/// cold instead of warming them per the segment configs.
///
/// The segment manifest is superset-biased, so it may list segments a read-only follower cannot
/// load — a not-yet-finalized segment, one already deleted, or an appendable write-buffer segment
/// that has no disk-resident id tracker. Per the manifest's reader contract these are skipped (with
/// a warning) instead of failing the whole open.
pub(crate) fn load_segments_parallel<S>(
    pool: &ThreadPool,
    fs: &S::Fs,
    segments: impl IntoIterator<Item = (Uuid, PathBuf)>,
    load_profile: Option<&LoadProfile>,
) -> Vec<(Uuid, ReadOnlySegment<S>)>
where
    S: UniversalReadExt + 'static,
    S::Fs: Send + Sync + Clone + 'static,
{
    // Stage every open: per-segment LIST + config reads, with the bulk
    // fetches going in flight as scheduled.
    let staged: Vec<_> = segments
        .into_iter()
        .filter_map(|(uuid, segment_path)| {
            match ReadOnlySegment::<S>::schedule_open(fs, &segment_path, uuid, None, load_profile) {
                Ok(staged) => Some((uuid, staged)),
                Err(err) => {
                    log::warn!("read-only open: skipping unloadable segment {uuid}: {err}");
                    None
                }
            }
        })
        .collect();

    // Drive all segments' fetches to completion here, overlapped, off the pool.
    futures::executor::block_on(join_all(staged.iter().map(|(_, staged)| staged.wait())));

    // Assemble from the resolved handles on the pool.
    pool.install(|| {
        staged
            .into_par_iter()
            .filter_map(|(uuid, staged)| match staged.finish(fs) {
                Ok(segment) => Some((uuid, segment)),
                Err(err) => {
                    log::warn!("read-only open: skipping unloadable segment {uuid}: {err}");
                    None
                }
            })
            .collect()
    })
}

/// Live-reload the given segments in two phases — stage every fetch under
/// shared access (`live_preload`), then apply under exclusive access
/// (`live_reload`) — so the exclusive phase never waits on IO.
///
/// Like [`load_segments_parallel`], the IO is driven to completion on the
/// calling thread; `pool` only runs the staging and the CPU-bound apply.
/// A failed preload is benign (warn): its reload still runs and surfaces
/// anything real. Returns each segment's reload result, in input order.
pub(crate) fn reload_segments_parallel<S>(
    pool: &ThreadPool,
    segments: Vec<(Uuid, Arc<RwLock<ReadOnlySegment<S>>>)>,
    hw_counter: &HardwareCounterCell,
) -> Vec<(Uuid, OperationResult<()>)>
where
    S: UniversalReadExt + 'static,
    S::Fs: Send + Sync + Clone + 'static,
{
    let io_futures = pool.install(|| {
        segments
            .par_iter()
            .filter_map(|(uuid, segment)| match segment.read().live_preload() {
                Ok(future) => Some(future),
                Err(err) => {
                    log::warn!("live_preload of segment {uuid} failed: {err}");
                    None
                }
            })
            .collect::<Vec<_>>()
    });

    futures::executor::block_on(join_all(io_futures));

    let reloads: Vec<_> = segments
        .into_iter()
        // The counter cell is not `Sync`, so fork one per reload outside the
        // pool; forks drain into the shared accumulator on drop.
        .map(|(uuid, segment)| (uuid, segment, hw_counter.fork()))
        .collect();
    pool.install(|| {
        reloads
            .into_par_iter()
            .map(|(uuid, segment, hw)| (uuid, segment.write().live_reload(&hw)))
            .collect()
    })
}
