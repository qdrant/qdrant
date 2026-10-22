//! Progress of a build: one counter, to which every phase of every level adds its share.
//!
//! [`build_links`](super::build_links) fixes the total before the first level, from the points of
//! each level that gets links, see [`LevelProgress`]. Each phase gets its share as a [`Progress`]
//! and has added all of it when it returns, piece by piece as its work gets done: partitioning
//! per matrix call and per cluster, the leaf k-NN per leaf, the final prune per job of points.
//! [`Progress::split`] hands a share out to the pieces in proportion to their work, rounded so
//! that the pieces add up to the share exactly, in whatever order they finish. So the counter
//! only grows and ends at the total, and a phase that fails or stops has added less.
//!
//! A piece is one atomic add, for thousands of scored pairs or dozens of pruned points.

use std::sync::atomic::{AtomicU64, Ordering};

/// Units per point of a level that gets links, by phase.
///
/// They split a level as its time splits on dbpedia-openai 100K (1536 dimensions, 12 threads) at
/// level 0: 74% leaf k-NN, 16% partitioning, 10% final prune. The upper levels take about as long
/// per point, so a level's share of the total is about its share of the time too. The fallback
/// links take under 1% of a build and count nothing. Neither do the sketches, which run before the
/// total is fixed: they read every original vector, which is quick from memory but can take long
/// from disk.
pub(super) const PARTITION_UNITS: u64 = 16;
pub(super) const LEAF_KNN_UNITS: u64 = 74;
const PRUNE_UNITS: u64 = 10;

/// A share of the progress counter: `units` that some work adds by the time it's done.
#[derive(Clone, Copy, Debug)]
pub(super) struct Progress<'a> {
    counter: &'a AtomicU64,
    units: u64,
}

impl<'a> Progress<'a> {
    pub fn new(counter: &'a AtomicU64, units: u64) -> Self {
        Self { counter, units }
    }

    /// Splits the share among pieces of work of the given sizes, in proportion to them. The
    /// shares add up to this one exactly: with `end` the sizes summed up to a piece, it gets
    /// `units · end / total` rounded down, minus the same for the piece before. If all sizes are
    /// 0, the pieces split it evenly, and without pieces, nothing gets it.
    pub fn split(self, sizes: impl IntoIterator<Item = u64>) -> Vec<Self> {
        let mut sizes: Vec<u64> = sizes.into_iter().collect();
        if sizes.iter().all(|&size| size == 0) {
            sizes.fill(1);
        }
        // The sums saturate, so `units · end` fits into u128, and the last piece still ends at
        // `total`
        let total = sizes
            .iter()
            .fold(0, |total: u64, &size| total.saturating_add(size));
        let (mut end, mut before) = (0u64, 0u64);
        sizes
            .into_iter()
            .map(|size| {
                end = end.saturating_add(size);
                let upto = (u128::from(self.units) * u128::from(end) / u128::from(total)) as u64;
                let units = upto - before;
                before = upto;
                Self::new(self.counter, units)
            })
            .collect()
    }

    /// The share in two halves, the second one larger by the odd unit.
    pub fn halves(self) -> (Self, Self) {
        let first = self.units / 2;
        (
            Self::new(self.counter, first),
            Self::new(self.counter, self.units - first),
        )
    }

    /// Adds the share to the counter.
    pub fn add(self) {
        if self.units > 0 {
            self.counter.fetch_add(self.units, Ordering::Relaxed);
        }
    }
}

/// The shares of the phases of one level that gets links, by [`PARTITION_UNITS`],
/// [`LEAF_KNN_UNITS`], and [`PRUNE_UNITS`] per point.
#[derive(Clone, Copy, Debug)]
pub(super) struct LevelProgress<'a> {
    pub partition: Progress<'a>,
    pub leaf_knn: Progress<'a>,
    pub prune: Progress<'a>,
}

impl<'a> LevelProgress<'a> {
    /// The shares of a level of `len` points, [`Self::units`] of `counter` in all.
    pub fn new(counter: &'a AtomicU64, len: usize) -> Self {
        let len = len as u64;
        Self {
            partition: Progress::new(counter, PARTITION_UNITS * len),
            leaf_knn: Progress::new(counter, LEAF_KNN_UNITS * len),
            prune: Progress::new(counter, PRUNE_UNITS * len),
        }
    }

    /// Units of a level of `len` points.
    pub fn units(len: usize) -> u64 {
        (PARTITION_UNITS + LEAF_KNN_UNITS + PRUNE_UNITS) * len as u64
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt as _, SeedableRng as _};

    use super::*;

    /// The shares of `units` split by `sizes`, as units.
    fn shares(units: u64, sizes: &[u64]) -> Vec<u64> {
        let counter = AtomicU64::new(0);
        let shares = Progress::new(&counter, units).split(sizes.iter().copied());
        shares.into_iter().map(|share| share.units).collect()
    }

    /// Shares add up to the whole for any units and sizes, including zeros, and each is within 1
    /// of its exact proportion.
    #[test]
    fn shares_add_up_to_the_whole() {
        let mut rng = StdRng::seed_from_u64(1);
        for case in 0..2000 {
            let units = match case % 5 {
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
            let shares = shares(units, &sizes);
            assert_eq!(shares.len(), sizes.len());
            let sum = shares.iter().fold(0u128, |sum, &s| sum + u128::from(s));
            assert_eq!(sum, u128::from(units), "{units} by {sizes:?}: {shares:?}");

            let total: u128 = sizes.iter().map(|&s| u128::from(s)).sum();
            if total <= u128::from(u64::MAX) && total > 0 {
                for (&size, &share) in sizes.iter().zip(&shares) {
                    let exact = u128::from(units) * u128::from(size) / total;
                    assert!(
                        u128::from(share).abs_diff(exact) <= 1,
                        "{units} by {sizes:?}"
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

    /// A share split again still adds up to its part of the whole.
    #[test]
    fn nested_shares_add_up() {
        let counter = AtomicU64::new(0);
        let (first, second) = Progress::new(&counter, 1_000_003).halves();
        assert_eq!((first.units, second.units), (500_001, 500_002));
        for half in [first, second] {
            for share in half.split([3, 0, 7, 1, 1]) {
                share.add();
            }
        }
        assert_eq!(counter.into_inner(), 1_000_003);
    }

    #[test]
    fn level_shares_add_up_to_its_units() {
        let counter = AtomicU64::new(0);
        let level = LevelProgress::new(&counter, 1234);
        level.partition.add();
        level.leaf_knn.add();
        assert_eq!(counter.load(Ordering::Relaxed), 90 * 1234);
        level.prune.add();
        assert_eq!(counter.into_inner(), LevelProgress::units(1234));
        assert_eq!(LevelProgress::units(1234), 100 * 1234);
    }
}
