//! Identical vectors: built once, then linked as a chain.
//!
//! Identical vectors share one HashPrune bucket in every reservoir, which keeps the lowest id, and
//! the final prune keeps one of them per list (`is_redundant`). So without this, all but one or
//! two copies of a vector would get no in-links. HNSW reaches copies through its backlinks only
//! while the lists around them have room, up to a few hundred copies.
//!
//! Each level groups its points by [`Fingerprints::same_vector`] and builds over the lowest point
//! of each group, the representative. The other copies skip the whole build and are chained
//! behind it, see [`link_copies`]: each copy is reachable from every point that links the
//! representative, and leads out of the group through the representative's links.

use std::num::NonZeroU64;

use common::types::PointOffsetType;
use rayon::prelude::*;

use super::sketches::{Fingerprints, Sketches};

/// The groups of identical vectors among `points` (sorted, distinct), each of at least two points,
/// sorted, and the groups sorted by their first point. Empty if no vector repeats.
///
/// Sorts the points by fingerprint, in parallel on the current rayon pool. `fingerprints` must be
/// those [`Sketches::new`] returned with `sketches`.
pub(super) fn find(
    points: &[PointOffsetType],
    sketches: &Sketches,
    fingerprints: &Fingerprints,
) -> Vec<Vec<PointOffsetType>> {
    debug_assert!(
        points.is_sorted_by(|a, b| a < b),
        "points must be sorted and distinct",
    );
    // Vectors with a NaN have no fingerprint, and are left to the fallback links
    let mut keyed: Vec<(NonZeroU64, PointOffsetType)> = points
        .par_iter()
        .filter_map(|&point| Some((fingerprints.get(point)?, point)))
        .collect();
    keyed.par_sort_unstable();
    if keyed.windows(2).all(|pair| pair[0].0 != pair[1].0) {
        return Vec::new();
    }

    let mut groups = Vec::new();
    for run in keyed
        .chunk_by(|a, b| a.0 == b.0)
        .filter(|run| run.len() > 1)
    {
        // Sorted by id within the run. A fingerprint collision of different vectors splits the
        // run into several groups.
        let mut rest: Vec<PointOffsetType> = run.iter().map(|&(_, point)| point).collect();
        while let Some(&first) = rest.first() {
            let (group, others): (Vec<_>, Vec<_>) = rest
                .iter()
                .partition(|&&point| fingerprints.same_vector(sketches, first, point));
            if group.len() > 1 {
                groups.push(group);
            }
            rest = others;
        }
    }
    groups.sort_unstable_by_key(|group| group[0]);
    groups
}

/// The groups of `groups` restricted to the points that pass `keep`, without those left with
/// fewer than two points.
pub(super) fn restrict(
    groups: &[Vec<PointOffsetType>],
    keep: impl Fn(PointOffsetType) -> bool,
) -> Vec<Vec<PointOffsetType>> {
    groups
        .iter()
        .map(|group| group.iter().copied().filter(|&point| keep(point)).collect())
        .filter(|group: &Vec<_>| group.len() > 1)
        .collect()
}

/// Points among `groups` that aren't the first of their group: the ones a level isn't built over.
pub(super) fn copies(groups: &[Vec<PointOffsetType>]) -> usize {
    groups.iter().map(|group| group.len() - 1).sum()
}

/// `points` (sorted, distinct) without the copies of `groups`, which must be groups of `points`
/// as [`find`] or [`restrict`] return them: the points to build a level over.
pub(super) fn representatives(
    points: &[PointOffsetType],
    groups: &[Vec<PointOffsetType>],
) -> Vec<PointOffsetType> {
    let mut copies: Vec<PointOffsetType> = groups
        .iter()
        .flat_map(|group| &group[1..])
        .copied()
        .collect();
    copies.sort_unstable();
    let mut copies = copies.into_iter().peekable();
    points
        .iter()
        .copied()
        .filter(|&point| copies.next_if_eq(&point).is_none())
        .collect()
}

/// Links of `points` (sorted, distinct) from `representative_links`, the links of
/// `representatives` in their order, and the chains of `groups`. At most `level_m` per point,
/// which must be at least 1.
///
/// The representative links the first copy, then its own links. Each copy links the next one,
/// then one about `√s` further down the chain for a group of `s`, so that any copy is a few hops
/// away, then the representative's links. The last copy links the representative's links, then
/// the representative. With a degree limit of 1 the chain still reaches every copy, and its last
/// copy leads out.
pub(super) fn link_copies(
    points: &[PointOffsetType],
    representatives: &[PointOffsetType],
    representative_links: Vec<Vec<PointOffsetType>>,
    groups: &[Vec<PointOffsetType>],
    level_m: usize,
) -> Vec<Vec<PointOffsetType>> {
    debug_assert!(level_m >= 1);
    debug_assert_eq!(representatives.len(), representative_links.len());
    let position = |point: PointOffsetType| {
        points
            .binary_search(&point)
            .expect("group points are points of the level")
    };
    let mut links = vec![Vec::new(); points.len()];
    for (&point, point_links) in std::iter::zip(representatives, representative_links) {
        links[position(point)] = point_links;
    }

    for group in groups {
        let representative = group[0];
        let own = std::mem::take(&mut links[position(representative)]);
        let step = group.len().isqrt();
        for (i, &point) in group.iter().enumerate() {
            let next = group.get(i + 1).copied();
            let skip = (step >= 2).then(|| group.get(i + step).copied()).flatten();
            let list: Vec<PointOffsetType> = if i == 0 {
                next.into_iter().chain(own.iter().copied()).collect()
            } else if let Some(next) = next {
                std::iter::once(next)
                    .chain(skip)
                    .chain(own.iter().copied())
                    .collect()
            } else {
                own.iter()
                    .copied()
                    .chain(std::iter::once(representative))
                    .collect()
            };
            links[position(point)] = list.into_iter().take(level_m).collect();
        }
    }
    links
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};

    use super::*;

    /// Points reachable from `from` over `links`, the lists of `points`.
    fn reachable(
        points: &[PointOffsetType],
        links: &[Vec<PointOffsetType>],
        from: PointOffsetType,
    ) -> HashSet<PointOffsetType> {
        let mut seen = HashSet::from([from]);
        let mut queue = VecDeque::from([from]);
        while let Some(point) = queue.pop_front() {
            let list = &links[points.binary_search(&point).unwrap()];
            for &link in list {
                if seen.insert(link) {
                    queue.push_back(link);
                }
            }
        }
        seen
    }

    #[test]
    fn representatives_drop_the_copies() {
        let points = [1, 2, 3, 5, 8, 9, 11];
        let groups = vec![vec![2, 5, 9], vec![3, 11]];
        assert_eq!(representatives(&points, &groups), [1, 2, 3, 8]);
        assert_eq!(copies(&groups), 3);
    }

    #[test]
    fn restrict_drops_small_groups() {
        let groups = vec![vec![2, 5, 9], vec![3, 11]];
        assert_eq!(restrict(&groups, |p| p != 11), [vec![2, 5, 9]]);
        assert_eq!(restrict(&groups, |p| p >= 5), [vec![5, 9]]);
    }

    /// A chain from the representative, with its links behind, and skip links in large groups.
    #[test]
    fn chain_layout() {
        let points: Vec<PointOffsetType> = (0..12).collect();
        // 0 and 1 are other points, 2..12 one group of 10, so a skip of 3
        let group: Vec<PointOffsetType> = (2..12).collect();
        let representatives = [0, 1, 2];
        let representative_links = vec![vec![1, 2], vec![0], vec![0, 1]];
        let links = link_copies(
            &points,
            &representatives,
            representative_links,
            std::slice::from_ref(&group),
            4,
        );
        assert_eq!(links[0], [1, 2]);
        assert_eq!(links[1], [0]);
        assert_eq!(links[2], [3, 0, 1]);
        assert_eq!(links[3], [4, 6, 0, 1]);
        assert_eq!(links[9], [10, 0, 1]);
        assert_eq!(links[11], [0, 1, 2]);
    }

    /// Every point is reachable from outside the group, and every copy leads out of it, for any
    /// degree limit and group size.
    #[test]
    fn every_copy_reachable_and_leads_out() {
        for level_m in [1, 2, 3, 32] {
            for size in [2, 3, 4, 9, 50, 1000] {
                let others: PointOffsetType = 3;
                let points: Vec<PointOffsetType> = (0..others + size).collect();
                let group: Vec<PointOffsetType> = (others..others + size).collect();
                let representatives: Vec<PointOffsetType> = (0..=others).collect();
                // A ring over the other points and the representative
                let representative_links = representatives
                    .iter()
                    .map(|&p| vec![(p + 1) % (others + 1)])
                    .collect();
                let links = link_copies(
                    &points,
                    &representatives,
                    representative_links,
                    std::slice::from_ref(&group),
                    level_m,
                );
                for (&point, list) in std::iter::zip(&points, &links) {
                    assert!(!list.is_empty(), "{point} has no links");
                    assert!(list.len() <= level_m);
                    assert!(!list.contains(&point));
                    let distinct: HashSet<_> = list.iter().collect();
                    assert_eq!(distinct.len(), list.len());
                }
                assert_eq!(
                    reachable(&points, &links, 0).len(),
                    points.len(),
                    "m {level_m}, size {size}",
                );
                for &copy in &group {
                    assert!(reachable(&points, &links, copy).contains(&0));
                }
            }
        }
    }

    /// A level of only copies of one vector links them as a ring.
    #[test]
    fn group_alone_on_its_level() {
        let points = [4, 7, 9];
        let groups = vec![points.to_vec()];
        let links = link_copies(&points, &[4], vec![vec![]], &groups, 2);
        assert_eq!(links, [vec![7], vec![9], vec![4]]);
    }
}
