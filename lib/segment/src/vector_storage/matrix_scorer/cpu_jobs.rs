//! [`MatrixPhase::for_each_top_k`] on the CPU kernels.
//!
//! [`MatrixPhase::for_each_top_k`]: super::MatrixPhase::for_each_top_k

use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::ambient;
use common::reason::reason;
use common::types::PointOffsetType;
use parking_lot::Mutex;
use rayon::prelude::*;

use super::top_k_pool::TopKPool;
use super::{Consume, MatrixCols, MatrixJob, MatrixPicks, MatrixTopK, Scored};
use crate::common::operation_error::{OperationResult, check_process_stopped};

/// Rows per matrix call of a job against other points. Every call packs the columns again, so
/// smaller calls would pay that more often.
pub(super) const ROWS_PER_CALL: usize = 1024;

/// One matrix call, as [`MatrixScorer::top_k`].
///
/// [`MatrixScorer::top_k`]: super::MatrixScorer::top_k
pub(super) type TopKCall<'a> =
    &'a (dyn Fn(&[PointOffsetType], MatrixCols<'_>, &mut MatrixTopK) -> OperationResult<()> + Sync);

/// [`MatrixPhase::for_each_top_k`] with `top_k` as the matrix call: the calls in parallel on
/// the current rayon pool, in the buffers of `tops`.
///
/// - A job within its rows ([`MatrixCols::Rows`]) is one call, since every row scores against
///   every other.
/// - A job against other points is cut into calls of [`ROWS_PER_CALL`] rows. The picks of a row
///   don't depend on the other rows of its call, so the calls together give the result of one
///   call over all rows.
///
/// Grab, then consume: a call copies its picks into the result of its job and gives its buffer
/// back, before anything of the caller's runs. So a buffer is only held during a matrix call,
/// which doesn't use rayon: `tops` never has more buffers than the pool has threads, whatever
/// `scored` and `consume` do, also if they wait for rayon or score again. Neither runs under
/// a lock of this loop.
///
/// A call reports its rows as `scored` and counts them as done only after that, and the call
/// that brings its job to all rows done consumes it. So every `scored` of a job has returned
/// before its `consume` starts.
///
/// Not generic over the scorer, so it's compiled once and not per kind of operands.
///
/// [`MatrixPhase::for_each_top_k`]: super::MatrixPhase::for_each_top_k
pub(super) fn for_each_top_k(
    top_k: TopKCall<'_>,
    tops: &TopKPool,
    jobs: &[MatrixJob<'_>],
    stopped: &AtomicBool,
    scored: Scored<'_>,
    consume: Consume<'_>,
) -> OperationResult<()> {
    let mut calls = Vec::with_capacity(jobs.len());
    // The results of the jobs cut into several calls. Boxed: with one job per leaf, most jobs
    // are one call and have none
    let cut: Vec<Option<Box<CutJob>>> = jobs
        .iter()
        .enumerate()
        .map(|(job, &MatrixJob { rows, cols, k: _ })| {
            let rows_per_call = match cols {
                MatrixCols::Rows => rows.len(),
                MatrixCols::Other(_) => ROWS_PER_CALL,
            };
            // Also a job without rows gets its call, so that it's consumed
            let count = rows.len().div_ceil(rows_per_call.max(1)).max(1);
            calls.extend((0..count).map(|call| Call {
                job,
                rows: call * rows_per_call..rows.len().min((call + 1) * rows_per_call),
            }));
            (count > 1).then(|| Box::new(CutJob::new(rows.len())))
        })
        .collect();

    calls
        .par_iter()
        // Every call on its own, stealable until the end: a call takes up to milliseconds,
        // sizes differ, and rayon's splitting can leave one thread with a long run of them
        // otherwise, see `HNSW_BUILD_MAX_PAR_LEN`
        .with_max_len(1)
        .try_for_each(|call| {
            check_process_stopped(stopped)?;
            let _scope = ambient::unmeasured_guard(reason("Internal operation"));
            let MatrixJob { rows, cols, k } = jobs[call.job];
            // The calls keep the `k` of the phase per row, so a job can't get more
            assert!(k <= tops.k(), "a job's k is at most its phase's");
            let cut = cut[call.job].as_deref();
            // The whole result of a job of one call
            let whole = tops.with(|top| {
                top_k(&rows[call.rows.clone()], cols, top)?;
                OperationResult::Ok(match cut {
                    Some(cut) => {
                        cut.fill(k, call.rows.start, top);
                        None
                    }
                    None => {
                        let mut picks = MatrixPicks::new(k, rows.len());
                        picks.fill(0, top);
                        Some(picks)
                    }
                })
            })?;
            scored(call.job, call.rows.len());
            let complete = match cut {
                Some(cut) => cut.done(call.rows.len()),
                None => whole,
            };
            match complete {
                Some(picks) => consume(call.job, &picks),
                None => Ok(()),
            }
        })
}

/// One matrix call: these rows of the job, against all its columns.
struct Call {
    job: usize,
    rows: Range<usize>,
}

/// The result of a job of several calls, while they run.
struct CutJob {
    rows: usize,
    /// Rows whose call hasn't reported them as scored yet.
    missing: AtomicUsize,
    /// Allocated by the first call and taken by the last, so that only jobs in progress hold
    /// one.
    picks: Mutex<Option<MatrixPicks>>,
}

impl CutJob {
    fn new(rows: usize) -> Self {
        Self {
            rows,
            missing: AtomicUsize::new(rows),
            picks: Mutex::new(None),
        }
    }

    /// Copies the first `k` picks per row of `part` into the job's result, as its rows from
    /// `at` on.
    fn fill(&self, k: usize, at: usize, part: &MatrixTopK) {
        self.picks
            .lock()
            .get_or_insert_with(|| MatrixPicks::new(k, self.rows))
            .fill(at, part);
    }

    /// Counts `rows` filled rows as done. Returns the job's result with its last ones.
    #[must_use]
    fn done(&self, rows: usize) -> Option<MatrixPicks> {
        let missing = self.missing.fetch_sub(rows, Ordering::AcqRel);
        (missing == rows).then(|| {
            let picks = self.picks.lock().take();
            picks.expect("a call has filled the result")
        })
    }
}
