use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::universal_io::{IsNotFound as _, UniversalReadFsAsync};
use futures::future::join_all;
use parking_lot::RwLock;
use rayon::ThreadPool;
use rayon::prelude::*;
use segment::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use segment::data_types::load_profile::LoadProfile;
use segment::index::UniversalReadExt;
use segment::segment::read_only::{ReadOnlySegment, build_cached_fs_async};
use uuid::Uuid;

/// Open the given segments and return the ones that loaded, in input order.
///
/// 🤖 Listing, staging and the fetch wait run on the calling thread, one future
/// 🤖 per segment, so a fetch chained on an earlier one proceeds as soon as that
/// 🤖 one lands. Only the assembly runs on `pool` (the shard's dedicated load pool).
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
    // 🤖 Stage every segment and drive its fetches to completion, overlapped, off the pool.
    check_process_stopped(is_stopped)?;
    let staged = futures::executor::block_on(join_all(segments.into_iter().map(
        |(uuid, segment_path)| async move {
            let staged = async {
                let cached_fs = build_cached_fs_async(fs, &segment_path).await?;
                let staged = ReadOnlySegment::<S>::schedule_open_with_cached_fs(
                    cached_fs,
                    &segment_path,
                    uuid,
                    None,
                    load_profile,
                    is_stopped,
                )
                .await?;
                staged.wait().await;
                Ok(staged)
            }
            .await;
            (uuid, staged)
        },
    )));
    check_process_stopped(is_stopped)?;
    let staged = staged
        .into_iter()
        .filter_map(|(uuid, staged)| skip_not_found(uuid, staged))
        .collect::<OperationResult<Vec<_>>>()?;

    // Assemble from the resolved handles on the pool.
    let loaded = ambient::parallel(|handoff| {
        pool.install(|| {
            staged
                .into_par_iter()
                .filter_map(|(uuid, staged)| {
                    skip_not_found(uuid, handoff.enter(|| staged.finish(fs)))
                })
                .collect::<OperationResult<Vec<_>>>()
        })
    })?;
    check_process_stopped(is_stopped)?;
    Ok(loaded)
}

/// 🤖 The segment might not be well-formed yet; this follower catches up next live-reload.
fn skip_not_found<T>(uuid: Uuid, result: OperationResult<T>) -> Option<OperationResult<(Uuid, T)>> {
    match result {
        Ok(value) => Some(Ok((uuid, value))),
        Err(err) if err.is_not_found() => {
            log::debug!("read-only open: skipping unloadable segment {uuid}: {err}");
            None
        }
        Err(err) => Some(Err(err)),
    }
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
