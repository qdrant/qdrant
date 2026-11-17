//! Backlinks: every point is linked back by the points it links, where their lists have room.
//!
//! HNSW links back every point it inserts: each selected neighbor pushes the new point into its
//! list while the list has room (`connect_with_heuristic`). PiPNN prunes every list once, on its
//! own, so without this pass a point that no list keeps (such as the zero vector with Cosine) is
//! unreachable, and lists stay sparser than HNSW's.
//!
//! Each list keeps its pruned links in front and fills its free slots with the points that link
//! it, lowest id first: what HNSW's rule gives when the points are inserted in id order. No list
//! is pruned again or grows past the degree limit, and the result doesn't depend on the threads.
//! Only pruned links are linked back. Points without candidates of their own get none: a NaN
//! point linked from outside would come first in every search reaching it.
//!
//! Costs one parallel sort of all pruned links as `(target, source)` pairs, 8 bytes per link,
//! after the reservoirs are freed.

use common::types::PointOffsetType;
use rayon::prelude::*;

/// Adds the backlinks of `points` (the points of one level, sorted) to `links`, their lists in
/// that order, up to `level_m` per list, see the module docs. Only points that `may_link` by
/// position are linked back; it must reject points without candidates and deleted points.
/// Returns how many backlinks it added. Runs on the current rayon pool.
pub(super) fn add_backlinks(
    links: &mut [Vec<PointOffsetType>],
    points: &[PointOffsetType],
    level_m: usize,
    may_link: impl Fn(usize) -> bool + Sync,
) -> usize {
    debug_assert_eq!(links.len(), points.len());
    debug_assert!(
        points.is_sorted_by(|a, b| a < b),
        "points must be sorted and distinct",
    );
    let mut backlinks: Vec<(PointOffsetType, PointOffsetType)> = links
        .par_iter()
        .zip(points)
        .enumerate()
        .filter(|&(position, _)| may_link(position))
        .flat_map_iter(|(_, (list, &source))| list.iter().map(move |&target| (target, source)))
        .collect();
    backlinks.par_sort_unstable();

    links
        .par_iter_mut()
        .zip(points)
        .map(|(list, &point)| {
            let pruned = list.len();
            let start = backlinks.partition_point(|&(target, _)| target < point);
            for &(target, source) in &backlinks[start..] {
                if target != point || list.len() >= level_m {
                    break;
                }
                // Points linked both ways already
                if !list[..pruned].contains(&source) {
                    list.push(source);
                }
            }
            list.len() - pruned
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::seq::IndexedRandom as _;
    use rand::{RngExt as _, SeedableRng as _};

    use super::*;

    /// HNSW's rule, one point at a time in id order: each point is pushed into the lists of its
    /// pruned links that have room and don't hold it yet.
    fn reference(
        links: &[Vec<PointOffsetType>],
        points: &[PointOffsetType],
        level_m: usize,
        may_link: impl Fn(usize) -> bool,
    ) -> Vec<Vec<PointOffsetType>> {
        let pruned = links.to_vec();
        let mut links = links.to_vec();
        for (source, list) in pruned.iter().enumerate() {
            if !may_link(source) {
                continue;
            }
            for &link in list {
                let target = &mut links[points.binary_search(&link).unwrap()];
                if target.len() < level_m && !target.contains(&points[source]) {
                    target.push(points[source]);
                }
            }
        }
        links
    }

    /// Random pruned lists of up to `level_m` distinct other points, some empty, over sparse ids.
    fn random_links(
        seed: u64,
        count: usize,
        level_m: usize,
    ) -> (Vec<PointOffsetType>, Vec<Vec<PointOffsetType>>) {
        let mut rng = StdRng::seed_from_u64(seed);
        let points: Vec<PointOffsetType> = (0..count as PointOffsetType * 3)
            .filter(|_| rng.random_bool(0.4))
            .collect();
        let links = points
            .iter()
            .map(|&point| {
                let degree = rng.random_range(0..=level_m);
                let others: Vec<_> = points.iter().copied().filter(|&p| p != point).collect();
                others.sample(&mut rng, degree).copied().collect()
            })
            .collect();
        (points, links)
    }

    /// Random graphs, degree limits from 1 to 8, and points that may not be linked.
    #[test]
    fn same_as_inserting_in_id_order() {
        for seed in 0..20 {
            let level_m = 1 + seed as usize % 8;
            let (points, links) = random_links(seed, 300, level_m);
            let may_link = |position: usize| position % 7 != 3;
            let expected = reference(&links, &points, level_m, may_link);
            let mut got = links.clone();
            let added = add_backlinks(&mut got, &points, level_m, may_link);
            assert_eq!(got, expected, "seed {seed}");
            let before: usize = links.iter().map(Vec::len).sum();
            let after: usize = got.iter().map(Vec::len).sum();
            assert_eq!(added, after - before);
        }
    }

    #[test]
    fn same_on_any_number_of_threads() {
        let (points, links) = random_links(5, 2000, 16);
        let run = |threads| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            let mut links = links.clone();
            pool.install(|| add_backlinks(&mut links, &points, 16, |_| true));
            links
        };
        let expected = run(1);
        for threads in [2, 7] {
            assert_eq!(run(threads), expected, "{threads} threads");
        }
    }

    #[test]
    fn links_back_where_there_is_room() {
        let points = [1, 2, 3, 4];
        // 4 links 1 and 3: 1 is full, 3 has room. 2 and 3 already link 1 back.
        let mut links = vec![vec![2, 3], vec![1], vec![1], vec![1, 3]];
        let added = add_backlinks(&mut links, &points, 2, |_| true);
        assert_eq!(added, 1);
        assert_eq!(links, [vec![2, 3], vec![1], vec![1, 4], vec![1, 3]]);
    }
}
