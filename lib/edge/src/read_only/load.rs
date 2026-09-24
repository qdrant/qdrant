use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::uio_trace;
use common::universal_io::{IsNotFound as _, UniversalReadFsAsync};
use futures::future::join_all;
use parking_lot::RwLock;
use rayon::ThreadPool;
use rayon::prelude::*;
use segment::common::operation_error::{OperationError, OperationResult, check_process_stopped};
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
///
/// Cooperative cancellation: `is_stopped` is checked between stages here and between components
/// inside each segment's staging and assembly (see [`ReadOnlySegment::schedule_open`]); a set flag
/// yields [`OperationError::Cancelled`] rather than a partial list. The IO wait and a single
/// component's open are indivisible and may delay the observation.
pub(crate) fn load_segments_parallel<S>(
    pool: &ThreadPool,
    fs: &S::Fs,
    segments: impl IntoIterator<Item = (Uuid, PathBuf)>,
    load_profile: Option<&LoadProfile>,
    is_stopped: &AtomicBool,
) -> OperationResult<Vec<(Uuid, ReadOnlySegment<S>)>>
where
    S: UniversalReadExt + 'static,
    S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
{
    // Stage every open: LIST all segments concurrently, then preopen each one.
    let listed_futs = segments.into_iter().map(|(uuid, path)| async move {
        let cached_fs = ReadOnlySegment::<S>::build_cached_fs_async(fs, &path).await;
        (uuid, path, cached_fs)
    });
    check_process_stopped(is_stopped)?;
    let listed = futures::executor::block_on(join_all(listed_futs));
    check_process_stopped(is_stopped)?;

    let mut staged = Vec::new();
    for (uuid, segment_path, cached_fs) in listed {
        let staged_open = cached_fs.and_then(|cached_fs| {
            ReadOnlySegment::<S>::schedule_open_with_cached_fs(
                cached_fs,
                &segment_path,
                uuid,
                None,
                load_profile,
                is_stopped,
            )
        });
        match staged_open {
            Ok(segment) => staged.push((uuid, segment)),
            Err(err @ OperationError::Cancelled { .. }) => return Err(err),
            Err(err) => {
                log::log!(
                    skip_level(&err),
                    "read-only open: skipping unloadable segment {uuid}: {err}"
                );
            }
        }
    }

    // Drive all segments' fetches to completion here, overlapped, off the pool.
    check_process_stopped(is_stopped)?;
    futures::executor::block_on(join_all(staged.iter().map(|(_, staged)| staged.wait())));
    check_process_stopped(is_stopped)?;

    // Assemble from the resolved handles on the pool.
    let ctx = uio_trace::Context::current();
    let loaded = pool.install(|| {
        staged
            .into_par_iter()
            .filter_map(|(uuid, staged)| match ctx.in_scope(|| staged.finish(fs)) {
                Ok(segment) => Some(Ok((uuid, segment))),
                Err(err @ OperationError::Cancelled { .. }) => Some(Err(err)),
                Err(err) => {
                    log::log!(
                        skip_level(&err),
                        "read-only open: skipping unloadable segment {uuid}: {err}"
                    );
                    None
                }
            })
            .collect::<OperationResult<Vec<_>>>()
    })?;
    check_process_stopped(is_stopped)?;
    Ok(loaded)
}

/// The level at which a segment that failed to open is reported.
///
/// The manifest is superset-biased, so a listed segment may be one the leader has
/// not finalized yet or has already removed. Both surface as `FileNotFound`, both
/// resolve themselves once the follower catches up, and both are routine enough to
/// stay out of the log at default levels.
///
/// Anything else is a segment that should have loaded and did not. The shard opens
/// and serves without it, so queries silently return results computed over a subset
/// of the data — worth an error, and worth telling apart from the churn above, which
/// it previously shared a log line and a level with.
fn skip_level(err: &OperationError) -> log::Level {
    if err.is_not_found() {
        log::Level::Debug
    } else {
        log::Level::Error
    }
}

/// Live-reload the given segments in two phases — stage every fetch under
/// shared access (`live_preload`), then apply under exclusive access
/// (`live_reload`) — so the exclusive phase never waits on IO.
///
/// Like [`load_segments_parallel`], the preloads are driven to completion on
/// the calling thread; `pool` only runs the CPU-bound apply.
/// A failed preload is benign (warn): its reload still runs and surfaces
/// anything real. Returns each segment's reload result, in input order.
///
/// Cooperative cancellation: `is_stopped` is checked between segments and
/// between stages; a set flag yields [`OperationError::Cancelled`] as the
/// outer error. A segment's reload is applied atomically under its write
/// lock, so segments reloaded before the cancellation was observed stay
/// consistent, and the rest replay their delta on the next reload.
//
// Preloads hold segment read locks across IO; the only writer is the apply below, serialized by
// the shard's `live_reload_lock`, so no writer queues behind them to stall reads.
#[expect(clippy::await_holding_lock)]
pub(crate) fn reload_segments_parallel<S>(
    pool: &ThreadPool,
    segments: Vec<(Uuid, Arc<RwLock<ReadOnlySegment<S>>>)>,
    hw_counter: &HardwareCounterCell,
    is_stopped: &AtomicBool,
) -> OperationResult<Vec<(Uuid, OperationResult<()>)>>
where
    S: UniversalReadExt + 'static,
    S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
{
    // Preload every segment concurrently on this thread; only the apply rides the pool.
    check_process_stopped(is_stopped)?;
    futures::executor::block_on(join_all(segments.iter().map(
        |(uuid, segment)| async move {
            match segment.read().live_preload(is_stopped).await {
                Ok(()) => {}
                Err(OperationError::Cancelled { .. }) => {}
                Err(err) => {
                    log::warn!("live_preload of segment {uuid} failed: {err}");
                }
            }
        },
    )));
    check_process_stopped(is_stopped)?;

    let reloads: Vec<_> = segments
        .into_iter()
        // The counter cell is not `Sync`, so fork one per reload outside the
        // pool; forks drain into the shared accumulator on drop.
        .map(|(uuid, segment)| (uuid, segment, hw_counter.fork()))
        .collect();
    let ctx = uio_trace::Context::current();
    let results = pool.install(|| {
        reloads
            .into_par_iter()
            .map(|(uuid, segment, hw)| {
                check_process_stopped(is_stopped)?;
                Ok((uuid, ctx.in_scope(|| segment.write().live_reload(&hw))))
            })
            .collect::<OperationResult<Vec<_>>>()
    })?;
    check_process_stopped(is_stopped)?;
    Ok(results)
}
