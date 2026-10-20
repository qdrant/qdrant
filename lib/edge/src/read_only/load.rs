use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::universal_io::{IsNotFound as _, UniversalReadFsAsync};
use futures::StreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use parking_lot::{Mutex, RwLock};
use rayon::ThreadPool;
use rayon::prelude::*;
use segment::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use segment::data_types::load_profile::LoadProfile;
use segment::index::UniversalReadExt;
use segment::segment::read_only::{ReadOnlySegment, build_cached_fs_async};
use uuid::Uuid;

/// Open the given segments and return the ones that loaded, in input order.
///
/// Listings and the final fetch wait run on the calling thread. Staging and
/// assembly run on `pool` (the shard's dedicated load pool), so one segment's
/// config reads do not block staging on other workers or occupy search workers.
/// Each segment is staged as soon as its own listing pipeline completes.
///
/// A `load_profile` (see [`LoadProfile`]) parks the components the shard's request won't touch
/// cold instead of warming them per the segment configs.
///
/// The segment manifest is superset-biased, so it may list segments a read-only follower cannot
/// load yet — one not finalized, or one already deleted. Those surface as not-found and are
/// skipped (at debug level) so the next live-reload can pick them up. Any other open failure
/// fails the whole open: the shard must not serve a subset of its segments.
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
    check_process_stopped(is_stopped)?;
    let mut listed_futs: FuturesUnordered<_> = segments
        .into_iter()
        .enumerate()
        .map(|(index, (uuid, path))| async move {
            let cached_fs = build_cached_fs_async(fs, &path).await;
            (index, uuid, path, cached_fs)
        })
        .collect();
    let handoff = ambient::current();
    let staged_opens = Mutex::new(Vec::new());
    // Keep polling all listings on the caller while workers stage each ready segment.
    // An in-place scope leaves every pool worker available, including in a single-thread pool.
    pool.in_place_scope(|scope| {
        let handoff = &handoff;
        let staged_opens = &staged_opens;
        futures::executor::block_on(async {
            while let Some((index, uuid, segment_path, cached_fs)) = listed_futs.next().await {
                check_process_stopped(is_stopped)?;
                scope.spawn(move |_| {
                    let staged_open = handoff.enter(|| {
                        cached_fs.and_then(|cached_fs| {
                            ReadOnlySegment::<S>::schedule_open_with_cached_fs(
                                cached_fs,
                                &segment_path,
                                uuid,
                                None,
                                load_profile,
                                is_stopped,
                            )
                        })
                    });
                    staged_opens.lock().push((index, uuid, staged_open));
                });
            }
            OperationResult::Ok(())
        })
    })?;
    check_process_stopped(is_stopped)?;

    // Listing and staging completion order must not change the returned segment order.
    let mut staged_opens = staged_opens.into_inner();
    staged_opens.sort_unstable_by_key(|(index, _, _)| *index);
    let mut staged = Vec::new();
    for (_, uuid, staged_open) in staged_opens {
        match staged_open {
            Ok(segment) => staged.push((uuid, segment)),
            Err(err) => {
                if err.is_not_found() {
                    // The segment might not be well-formed yet, this follower can catch up next live-reload.
                    log::debug!("read-only open: skipping unloadable segment {uuid}: {err}");
                } else {
                    return Err(err);
                }
            }
        }
    }

    // Drive all segments' fetches to completion here, overlapped, off the pool.
    check_process_stopped(is_stopped)?;
    futures::executor::block_on(join_all(staged.iter().map(|(_, staged)| staged.wait())));
    check_process_stopped(is_stopped)?;

    // Assemble from the resolved handles on the pool.
    let loaded = ambient::parallel(|handoff| {
        pool.install(|| {
            staged
                .into_par_iter()
                .filter_map(|(uuid, staged)| match handoff.enter(|| staged.finish(fs)) {
                    Ok(segment) => Some(Ok((uuid, segment))),
                    Err(err) => {
                        if err.is_not_found() {
                            // The segment might not be well-formed yet, this follower can catch up next live-reload.
                            log::debug!(
                                "read-only open: skipping unloadable segment {uuid}: {err}"
                            );
                            None
                        } else {
                            Some(Err(err))
                        }
                    }
                })
                .collect::<OperationResult<Vec<_>>>()
        })
    })?;
    check_process_stopped(is_stopped)?;
    Ok(loaded)
}

/// Live-reload the given segments in two phases — stage every fetch under
/// shared access (`live_preload`), then apply under exclusive access
/// (`live_reload`) — so the exclusive phase never waits on IO.
///
/// Like [`load_segments_parallel`], the preloads are driven to completion on
/// the calling thread; `pool` only runs the CPU-bound apply.
/// A failed preload skips its segment's reload and becomes that segment's
/// result. A reload that fails after a successful preload reopens the segment
/// (see [`reopen_after_failed_reload`]). Returns each segment's reload result,
/// in input order.
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
    fs: &S::Fs,
    segments: Vec<(Uuid, Arc<RwLock<ReadOnlySegment<S>>>)>,
    load_profile: Option<&LoadProfile>,
    is_stopped: &AtomicBool,
) -> OperationResult<Vec<(Uuid, OperationResult<()>)>>
where
    S: UniversalReadExt + 'static,
    S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
{
    // Preload every segment concurrently on this thread; only the apply rides the pool.
    check_process_stopped(is_stopped)?;
    let preloads = futures::executor::block_on(join_all(segments.iter().map(
        |(uuid, segment)| async move {
            let res = segment.read().live_preload(is_stopped).await;
            if let Err(ref err) = res
                && !matches!(err, OperationError::Cancelled { .. })
                && !err.is_not_found()
            {
                log::warn!("live_preload of segment {uuid} failed: {err}");
            }
            (*uuid, res)
        },
    )));
    check_process_stopped(is_stopped)?;

    let reloads: Vec<_> = segments.into_iter().zip(preloads).collect();
    let results = ambient::parallel(|handoff| {
        pool.install(|| {
            reloads
                .into_par_iter()
                .map(|((uuid, segment), (_, max_committed_id_res))| {
                    check_process_stopped(is_stopped)?;
                    let _scope = handoff.enter_guard();
                    let result = max_committed_id_res.and_then(|max_committed_id| {
                        let mut segment = segment.write();
                        segment.live_reload(max_committed_id).or_else(|err| {
                            if requires_reopen(&err) {
                                reopen_after_failed_reload(&mut segment, err, fs, load_profile)
                            } else {
                                Err(err)
                            }
                        })
                    });
                    Ok((uuid, result))
                })
                .collect::<OperationResult<Vec<_>>>()
        })
    })?;
    check_process_stopped(is_stopped)?;
    Ok(results)
}

/// Whether a failed reload left the segment in a state only a reopen recovers.
/// A cancelled reload replays its delta next time, and a not-found one is
/// resolved against the manifest by the caller.
fn requires_reopen(err: &OperationError) -> bool {
    match err {
        OperationError::Cancelled { .. } | OperationError::FileNotFound { .. } => false,
        OperationError::WrongVectorDimension { .. }
        | OperationError::MalformedVectorBlob { .. }
        | OperationError::MalformedPayloadBlob { .. }
        | OperationError::VectorNameNotExists { .. }
        | OperationError::PointIdError { .. }
        | OperationError::TypeError { .. }
        | OperationError::TypeInferenceError { .. }
        | OperationError::ServiceError { .. }
        | OperationError::InconsistentStorage { .. }
        | OperationError::OutOfMemory { .. }
        | OperationError::Timeout { .. }
        | OperationError::ValidationError { .. }
        | OperationError::WrongSparse
        | OperationError::WrongMulti
        | OperationError::MissingRangeIndexForOrderBy { .. }
        | OperationError::MissingMapIndexForFacet { .. }
        | OperationError::VariableTypeError { .. }
        | OperationError::NonFiniteNumber { .. }
        | OperationError::OutOfAppendableCapacity { .. } => true,
    }
}

/// Replace a segment whose reload failed after a successful preload with a
/// fresh open of its files, under the segment's write lock.
///
/// The preload already reported files that could not be fetched, so this
/// reload met data it could not apply on top of the loaded state. It may have
/// failed after some components applied it, and retrying would fail the same
/// way. Reads of this segment wait for the reopen, so none sees the partially
/// applied reload. The reopen is not cancellable for the same reason. If it
/// fails, the segment keeps its partially applied state and the error is
/// returned.
pub(super) fn reopen_after_failed_reload<S>(
    segment: &mut ReadOnlySegment<S>,
    reload_err: OperationError,
    fs: &S::Fs,
    load_profile: Option<&LoadProfile>,
) -> OperationResult<()>
where
    S: UniversalReadExt + 'static,
    S::Fs: UniversalReadFsAsync,
{
    let uuid = segment.uuid;
    log::warn!("live_reload of segment {uuid} failed, reopening it: {reload_err}");
    match ReadOnlySegment::open(fs, &segment.segment_path, uuid, None, load_profile) {
        Ok(reopened) => {
            *segment = reopened;
            Ok(())
        }
        Err(err) => {
            log::error!("reopening segment {uuid} after a failed live_reload failed: {err}");
            Err(err)
        }
    }
}
