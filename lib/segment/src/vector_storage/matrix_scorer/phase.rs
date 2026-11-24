//! The matrix calls of one phase of a build, see [`MatrixScorer::phase`].
//!
//! [`MatrixScorer::phase`]: super::MatrixScorer::phase

use std::sync::atomic::AtomicBool;

use parking_lot::RwLock;

use super::top_k_pool::TopKPool;
use super::{DynMatrix, MatrixJob, MatrixPicks, cpu_jobs};
use crate::common::operation_error::OperationResult;

/// The matrix calls of one phase of a build, whose jobs keep at most the `k` of the phase per
/// row, shared by all threads of the phase.
///
/// It owns what the calls reuse, on the CPU the buffers of the matrix calls, megabytes each
/// with large vectors. They are kept from one [`Self::for_each_top_k`] to the next, also for
/// several running at once, and freed when the phase is dropped. Drop it before the next phase
/// allocates.
pub struct MatrixPhase<'a> {
    inner: Box<dyn DynPhase + 'a>,
}

impl<'a> MatrixPhase<'a> {
    pub(super) fn new(inner: Box<dyn DynPhase + 'a>) -> Self {
        Self { inner }
    }

    /// Scores every job with the rules of [`MatrixScorer::top_k`], and calls
    /// `consume(payload, picks)` with the payload of each job and its result: all rows of the
    /// job at once, with the job's `k` best columns per row.
    ///
    /// - The scorer decides how the jobs are scheduled, and may cut a job's rows into several
    ///   matrix calls. A caller passes its jobs as they are, however large, and doesn't cut
    ///   them itself.
    /// - `scored(payload, rows)` runs as the rows of a job are scored, a part of them at a time,
    ///   for the progress of a large job. The parts add up to the job's rows, and all of them
    ///   have been reported before the job is consumed. Several can run at once for the same
    ///   job.
    /// - `consume` runs once per job, in no particular order, from the threads of the current
    ///   rayon pool, several at once, outside of hardware measurement. It owns the payload,
    ///   what the job needs for itself: where its result goes, its share of the progress.
    /// - `picks` is the job's own, and the scorer holds nothing for the job while it's
    ///   consumed. So `consume` may take its time, use rayon, and score again.
    /// - Returns an error of a matrix call or of `consume`, any one of them if several fail,
    ///   or a cancellation if `stopped` is set before the last job has started. After either,
    ///   `consume` has run for some of the jobs only, and the payloads of the others are
    ///   dropped.
    ///
    /// [`MatrixScorer::top_k`]: super::MatrixScorer::top_k
    pub fn for_each_top_k<'j, T: Send + Sync>(
        &self,
        jobs: impl IntoIterator<Item = (MatrixJob<'j>, T)>,
        stopped: &AtomicBool,
        scored: impl Fn(&T, usize) + Sync,
        consume: impl Fn(T, &MatrixPicks) -> OperationResult<()> + Sync,
    ) -> OperationResult<()> {
        // Behind the trait object both get the position of their job. Reports of a job share
        // its payload and are all done before it's consumed, and a job is consumed once, so
        // no lock ever waits
        let (jobs, payloads): (Vec<_>, Vec<_>) = jobs
            .into_iter()
            .map(|(job, payload)| (job, RwLock::new(Some(payload))))
            .unzip();
        self.inner.for_each_top_k(
            &jobs,
            stopped,
            &|job, rows| {
                let payload = payloads[job].read();
                let payload = payload.as_ref().expect("scored before it's consumed");
                scored(payload, rows);
            },
            &|job, picks| {
                let payload = payloads[job].write().take();
                let payload = payload.expect("a job is consumed once");
                consume(payload, picks)
            },
        )
    }
}

/// `scored` of [`MatrixPhase::for_each_top_k`], with the position of the job for its payload.
pub(crate) type Scored<'a> = &'a (dyn Fn(usize, usize) + Sync);

/// `consume` of [`MatrixPhase::for_each_top_k`], with the position of the job for its payload.
pub(crate) type Consume<'a> = &'a (dyn Fn(usize, &MatrixPicks) -> OperationResult<()> + Sync);

/// Object-safe face of a [`MatrixPhase`], see [`DynMatrix::phase`].
///
/// An implementation scores every job with the rules of [`MatrixScorer::top_k`], builds its
/// result with [`MatrixPicks::new`] and fills it with [`MatrixPicks::fill_raw`], and keeps the
/// promises of [`MatrixPhase::for_each_top_k`] for `scored` and `consume`.
///
/// [`MatrixScorer::top_k`]: super::MatrixScorer::top_k
pub(crate) trait DynPhase: Sync {
    /// [`MatrixPhase::for_each_top_k`] with the position of the job in `jobs` for its payload:
    /// `scored(j, rows)` for the parts of a job, all of them before its `consume(j, picks)`.
    fn for_each_top_k(
        &self,
        jobs: &[MatrixJob<'_>],
        stopped: &AtomicBool,
        scored: Scored<'_>,
        consume: Consume<'_>,
    ) -> OperationResult<()>;
}

/// A phase on the CPU kernels of `matrix`, see [`cpu_jobs::for_each_top_k`].
pub(super) struct CpuPhase<'a, M: ?Sized> {
    matrix: &'a M,
    tops: TopKPool,
}

impl<'a, M: DynMatrix + ?Sized> CpuPhase<'a, M> {
    pub fn new(matrix: &'a M, k: usize) -> Self {
        Self {
            matrix,
            tops: TopKPool::new(k),
        }
    }
}

impl<M: DynMatrix + ?Sized> DynPhase for CpuPhase<'_, M> {
    fn for_each_top_k(
        &self,
        jobs: &[MatrixJob<'_>],
        stopped: &AtomicBool,
        scored: Scored<'_>,
        consume: Consume<'_>,
    ) -> OperationResult<()> {
        cpu_jobs::for_each_top_k(
            &|rows, cols, out| self.matrix.top_k(rows, cols, out),
            &self.tops,
            jobs,
            stopped,
            scored,
            consume,
        )
    }
}
