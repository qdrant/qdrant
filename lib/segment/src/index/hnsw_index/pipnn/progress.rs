//! Progress of a build: one subtask per phase, and the total on the build's own tracker.
//!
//! [`BuildProgress`] tracks the tracker of the build, and adds the subtasks [`PHASES`] to it,
//! pending. It starts each subtask when its phase first runs, and finishes it once its last level
//! is done; the subtasks of phases that didn't get there finish when [`BuildProgress`] is dropped.
//!
//! The counts are in points, or in point-equivalents of work:
//! - Each phase has a total of the points of every level that gets links, summed over those
//!   levels, and each level's run of the phase adds that level's points.
//! - The build's own tracker counts all phases together, so its total is [`PHASES`] times as
//!   many. Every piece of work adds its points to its phase and then to the total, see
//!   [`Counter::add`]: two separate adds, so a snapshot can see a phase a piece ahead of the
//!   total.
//! - The final prune counts the points of each job as it's done, so its count is the points
//!   pruned. Partitioning and the leaf k-NN hand out a level's points as [`Share`]s instead, in
//!   proportion to the work of the pieces, and their counts are point-equivalents of the work
//!   done, not points whose work is done. [`Share::split`] rounds so that the pieces add up to
//!   the share exactly, in whatever order they finish:
//!   - A step of partitioning has to hand shares to its clusters before it knows their work. It
//!     keeps half of its share for its own matrix calls, see [`partition`](super::partition).
//!   - A leaf's work grows with the square of its size, so a leaf gets a share of the level's
//!     points by that. Counting leaves would be simpler, but how many leaves a level has is only
//!     known once its partitioning is done, and a tracker's total is set once, before the first
//!     level.
//!
//! So every count only rises, ends exactly at its total when the build succeeds, and stays at or
//! below its total when a phase fails or stops: a phase whose last level was done before sits at
//! its total, as can a failing one whose missing pieces had shares that rounded to 0. The build's
//! total stays below its own then. A piece is one atomic add per counter, for thousands of scored
//! pairs or dozens of pruned points.
//!
//! A phase spans all levels, which run one after another, each through all phases. So a phase
//! stays running from its first level to its last, also while the other phases of the levels in
//! between run, and its duration covers theirs too. Its count shows how far it got.
//!
//! [`build_links`](super::build_links) creates [`BuildProgress`] only once the sketches are done.
//! Until then the build can still end with `None` and leave its tracker to the HNSW fallback,
//! which tracks it itself; subtasks created before would stay under that tracker, as phases that
//! finished with no work, next to the fallback's progress.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common::progress_tracker::ProgressTracker;

/// A phase of each level that gets links.
#[derive(Clone, Copy, Debug)]
pub(super) enum Phase {
    Partition,
    LeafKnn,
    Prune,
}

/// The phases, in the order they run on each level and are listed under the build's tracker.
pub(super) const PHASES: [Phase; 3] = [Phase::Partition, Phase::LeafKnn, Phase::Prune];

impl Phase {
    /// Name of the phase's subtask.
    pub fn name(self) -> &'static str {
        match self {
            Phase::Partition => "partition",
            Phase::LeafKnn => "leaf_knn",
            Phase::Prune => "prune",
        }
    }
}

/// The subtask of a phase, until it's finished, and its counter.
struct PhaseProgress {
    tracker: Option<ProgressTracker>,
    done: Arc<AtomicU64>,
}

/// The progress of a build: the subtasks of the phases, see the module docs.
pub(super) struct BuildProgress {
    partition: PhaseProgress,
    leaf_knn: PhaseProgress,
    prune: PhaseProgress,
    /// Counter of the build's own tracker.
    total: Arc<AtomicU64>,
    /// Points of a phase, the total of each subtask.
    points: u64,
}

impl BuildProgress {
    /// Tracks `tracker` with a total of [`PHASES`] times `points`, and adds a pending subtask
    /// for each of [`PHASES`] to it, with a total of `points` each.
    ///
    /// The tracker takes each subtask and its total in separate steps, so a snapshot taken
    /// meanwhile can list fewer phases, or a phase without a total.
    pub fn new(tracker: &ProgressTracker, points: u64) -> Self {
        // The total first, so that no snapshot shows phases with totals under a root without one
        let total = tracker.track_progress(Some(PHASES.len() as u64 * points));
        let [partition, leaf_knn, prune] = PHASES.map(|phase| {
            let tracker = tracker.subtask(phase.name());
            let done = tracker.track_progress(Some(points));
            PhaseProgress {
                tracker: Some(tracker),
                done,
            }
        });
        Self {
            partition,
            leaf_knn,
            prune,
            total,
            points,
        }
    }

    fn phase(&self, phase: Phase) -> &PhaseProgress {
        match phase {
            Phase::Partition => &self.partition,
            Phase::LeafKnn => &self.leaf_knn,
            Phase::Prune => &self.prune,
        }
    }

    fn phase_mut(&mut self, phase: Phase) -> &mut PhaseProgress {
        match phase {
            Phase::Partition => &mut self.partition,
            Phase::LeafKnn => &mut self.leaf_knn,
            Phase::Prune => &mut self.prune,
        }
    }

    /// Starts the subtask of `phase`, if it isn't running yet, and returns the counter its work
    /// adds to.
    #[must_use]
    pub fn start(&self, phase: Phase) -> Counter<'_> {
        let PhaseProgress { tracker, done } = self.phase(phase);
        if let Some(tracker) = tracker {
            tracker.start();
        }
        Counter {
            phase: done,
            total: &self.total,
        }
    }

    /// Finishes the subtask of `phase` if its count is at its total, as it is after its last
    /// level.
    pub fn end(&mut self, phase: Phase) {
        let points = self.points;
        let PhaseProgress { tracker, done } = self.phase_mut(phase);
        if done.load(Ordering::Relaxed) == points {
            // Dropping a tracker finishes its task
            tracker.take();
        }
    }

    /// Whether every counter is at its total.
    pub fn is_done(&self) -> bool {
        let points = self.points;
        PHASES
            .iter()
            .all(|&phase| self.phase(phase).done.load(Ordering::Relaxed) == points)
            && self.total.load(Ordering::Relaxed) == PHASES.len() as u64 * points
    }
}

/// Where the work of a phase counts: the phase's counter, and the build's total.
///
/// `Copy`, unlike [`Share`]: it adds the amounts it's given, so the parallel jobs of a phase can
/// each hold one.
#[derive(Clone, Copy, Debug)]
pub(super) struct Counter<'a> {
    phase: &'a AtomicU64,
    total: &'a AtomicU64,
}

impl Counter<'_> {
    /// Adds `points` to the phase and to the total.
    pub fn add(self, points: u64) {
        if points > 0 {
            self.phase.fetch_add(points, Ordering::Relaxed);
            self.total.fetch_add(points, Ordering::Relaxed);
        }
    }
}

/// A share of a phase's points: `points` that some work adds to its [`Counter`] by the time it's
/// done, in proportion to the work.
///
/// Not `Copy` or `Clone`: [`Self::split`], [`Self::halves`] and [`Self::add`] consume it, so a
/// share is added at most once, and one that is made but not used is warned about.
#[derive(Debug)]
#[must_use = "a share must be added or split, or its phase won't reach its total"]
pub(super) struct Share<'a> {
    counter: Counter<'a>,
    points: u64,
}

impl<'a> Share<'a> {
    pub fn new(counter: Counter<'a>, points: u64) -> Self {
        Self { counter, points }
    }

    /// Splits the share among pieces of work of the given sizes, in proportion to them. The
    /// shares add up to this one exactly: with `end` the sizes summed up to a piece, it gets
    /// `points · end / total` rounded down, minus the same for the piece before. If all sizes are
    /// 0, the pieces split it evenly, and without pieces, nothing gets it.
    #[must_use]
    pub fn split(self, sizes: impl IntoIterator<Item = u64>) -> Vec<Self> {
        let mut sizes: Vec<u64> = sizes.into_iter().collect();
        if sizes.iter().all(|&size| size == 0) {
            sizes.fill(1);
        }
        // The sums saturate, so `points · end` fits into u128, and the last piece still ends at
        // `total`
        let total = sizes
            .iter()
            .fold(0, |total: u64, &size| total.saturating_add(size));
        let (mut end, mut before) = (0u64, 0u64);
        sizes
            .into_iter()
            .map(|size| {
                end = end.saturating_add(size);
                let upto = (u128::from(self.points) * u128::from(end) / u128::from(total)) as u64;
                let points = upto - before;
                before = upto;
                Self::new(self.counter, points)
            })
            .collect()
    }

    /// The share in two halves, the second one larger by the odd point. Must be used, as
    /// [`Share`] is.
    pub fn halves(self) -> (Self, Self) {
        let first = self.points / 2;
        (
            Self::new(self.counter, first),
            Self::new(self.counter, self.points - first),
        )
    }

    /// Adds the share to its counter.
    pub fn add(self) {
        self.counter.add(self.points);
    }
}

/// A phase's counter and a total, for tests of a single phase.
#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct TestCounter {
    phase: AtomicU64,
    total: AtomicU64,
}

#[cfg(test)]
impl TestCounter {
    pub fn counter(&self) -> Counter<'_> {
        Counter {
            phase: &self.phase,
            total: &self.total,
        }
    }

    pub fn share(&self, points: u64) -> Share<'_> {
        Share::new(self.counter(), points)
    }

    /// The points added, the same on the phase and the total. Asserts that they're equal, which
    /// only holds while no add is in flight: call it from the thread that adds, or after.
    pub fn load(&self) -> u64 {
        let phase = self.phase.load(Ordering::Relaxed);
        assert_eq!(phase, self.total.load(Ordering::Relaxed), "phase and total");
        phase
    }
}

/// Name, whether started and finished, and done and total of a progress node.
#[cfg(test)]
pub(super) type NodeState = (String, bool, bool, Option<u64>, Option<u64>);

/// The [`NodeState`] of `tree`'s root.
#[cfg(test)]
pub(super) fn node_state(tree: &common::progress_tracker::ProgressTree) -> NodeState {
    (
        tree.name.clone(),
        tree.started_at.is_some(),
        tree.finished_at.is_some(),
        tree.done,
        tree.total,
    )
}

#[cfg(test)]
mod tests {
    use common::progress_tracker::{ProgressTree, new_progress_tracker};
    use rand::rngs::StdRng;
    use rand::{RngExt as _, SeedableRng as _};

    use super::*;

    /// The shares of `points` split by `sizes`, as points.
    fn shares(points: u64, sizes: &[u64]) -> Vec<u64> {
        let counter = TestCounter::default();
        let shares = counter.share(points).split(sizes.iter().copied());
        shares.into_iter().map(|share| share.points).collect()
    }

    /// Shares add up to the whole for any points and sizes, including zeros, and each is within 1
    /// of its exact proportion.
    #[test]
    fn shares_add_up_to_the_whole() {
        let mut rng = StdRng::seed_from_u64(1);
        for case in 0..2000 {
            let points = match case % 5 {
                0 => rng.random_range(0..4),
                1 => u64::MAX - rng.random_range(0..4),
                _ => rng.random_range(0..1_000_000),
            };
            let len = rng.random_range(1..50);
            let sizes: Vec<u64> = (0..len)
                .map(|_| match rng.random_range(0..4) {
                    0 => 0,
                    1 => rng.random(),
                    _ => rng.random_range(0..2000),
                })
                .collect();
            let shares = shares(points, &sizes);
            assert_eq!(shares.len(), sizes.len());
            let sum = shares.iter().fold(0u128, |sum, &s| sum + u128::from(s));
            assert_eq!(sum, u128::from(points), "{points} by {sizes:?}: {shares:?}");

            let total: u128 = sizes.iter().map(|&s| u128::from(s)).sum();
            if total <= u128::from(u64::MAX) && total > 0 {
                for (&size, &share) in sizes.iter().zip(&shares) {
                    let exact = u128::from(points) * u128::from(size) / total;
                    assert!(
                        u128::from(share).abs_diff(exact) <= 1,
                        "{points} by {sizes:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn shares_by_size() {
        assert_eq!(shares(10, &[1, 1]), [5, 5]);
        assert_eq!(shares(7, &[1, 1]), [3, 4]);
        assert_eq!(shares(100, &[1, 0, 3]), [25, 0, 75]);
        assert_eq!(shares(2, &[1, 1, 1]), [0, 1, 1]);
        // Evenly, if all are 0
        assert_eq!(shares(9, &[0, 0, 0]), [3, 3, 3]);
        assert_eq!(shares(0, &[5, 6]), [0, 0]);
        assert!(shares(9, &[]).is_empty());
    }

    /// A share split again still adds up to its part of the whole, on the phase and the total.
    #[test]
    fn nested_shares_add_up() {
        let counter = TestCounter::default();
        let (first, second) = counter.share(1_000_003).halves();
        assert_eq!((first.points, second.points), (500_001, 500_002));
        for half in [first, second] {
            for share in half.split([3, 0, 7, 1, 1]) {
                share.add();
            }
        }
        assert_eq!(counter.load(), 1_000_003);
    }

    /// Each node of `tree`, depth first.
    fn nodes(tree: &ProgressTree) -> Vec<NodeState> {
        let mut all = vec![node_state(tree)];
        all.extend(tree.children.iter().flat_map(nodes));
        all
    }

    /// The phases are listed, pending, from the start. Each starts when it first runs, and
    /// finishes once it's done; the total adds every phase.
    #[test]
    fn phases_start_and_end() {
        let (view, tracker) = new_progress_tracker();
        let mut progress = BuildProgress::new(&tracker, 10);
        let state = || {
            nodes(&view.snapshot("main_graph"))
                .into_iter()
                .map(|(name, started, finished, done, total)| {
                    (name, started, finished, done.unwrap(), total.unwrap())
                })
                .collect::<Vec<_>>()
        };
        let node = |name: &str, started, finished, done, total| {
            (name.to_string(), started, finished, done, total)
        };
        assert_eq!(
            state(),
            [
                node("main_graph", true, false, 0, 30),
                node("partition", false, false, 0, 10),
                node("leaf_knn", false, false, 0, 10),
                node("prune", false, false, 0, 10),
            ],
        );

        // A level of 4 points, then one of 6
        progress.start(Phase::Partition).add(4);
        progress.end(Phase::Partition);
        progress.start(Phase::LeafKnn).add(1);
        progress.end(Phase::LeafKnn);
        assert!(!progress.is_done());
        assert_eq!(
            state(),
            [
                node("main_graph", true, false, 5, 30),
                node("partition", true, false, 4, 10),
                node("leaf_knn", true, false, 1, 10),
                node("prune", false, false, 0, 10),
            ],
        );
        progress.start(Phase::LeafKnn).add(3);
        progress.start(Phase::Prune).add(4);
        progress.end(Phase::LeafKnn);
        progress.end(Phase::Prune);
        for phase in PHASES {
            progress.start(phase).add(6);
            progress.end(phase);
        }
        assert!(progress.is_done());
        assert_eq!(
            state(),
            [
                node("main_graph", true, false, 30, 30),
                node("partition", true, true, 10, 10),
                node("leaf_knn", true, true, 10, 10),
                node("prune", true, true, 10, 10),
            ],
        );
    }
}
