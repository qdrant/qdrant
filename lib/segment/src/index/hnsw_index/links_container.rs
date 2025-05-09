use std::cell::Cell;
use std::iter::Copied;
use std::num::NonZeroU32;

use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};

use crate::common::vector_utils::TrySetCapacityExact as _;

pub struct LinksContainer {
    links: Vec<PointOffsetType>,
    /// Number of links that have been processed by the heuristic.
    processed_by_heuristic: u32,
}

impl LinksContainer {
    pub fn with_capacity(m: usize) -> Self {
        Self {
            links: Vec::with_capacity(m),
            processed_by_heuristic: 0,
        }
    }

    pub fn push(&mut self, link: PointOffsetType) {
        self.links.push(link);
    }

    pub fn links(&self) -> &[PointOffsetType] {
        &self.links
    }

    pub fn iter(&self) -> Copied<std::slice::Iter<'_, u32>> {
        self.links.iter().copied()
    }

    pub fn into_vec(self) -> Vec<PointOffsetType> {
        self.links
    }

    /// Put points into the container.
    pub fn fill_from(&mut self, points: impl Iterator<Item = PointOffsetType>) {
        self.links.clear();
        self.links.extend(points);
        self.processed_by_heuristic = 0;
    }

    /// Put `m` candidates selected by the heuristic into the container.
    pub fn fill_from_sorted_with_heuristic(
        &mut self,
        candidates: impl Iterator<Item = ScoredPointOffset>,
        level_m: usize,
        mut score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    ) {
        self.links.clear();
        if level_m == 0 {
            // Unlikely.
            self.processed_by_heuristic = 0;
            return;
        }
        'outer: for candidate in candidates {
            for &existing in &self.links {
                if score(candidate.idx, existing) > candidate.score {
                    continue 'outer;
                }
            }
            self.links.push(candidate.idx);
            if self.links.len() >= level_m {
                break;
            }
        }
        self.processed_by_heuristic = self.links.len() as u32;
    }

    /// Connect new point to links, so that links contains only closest points.
    pub fn connect(
        &mut self,
        new_point_id: PointOffsetType,
        target_point_id: PointOffsetType,
        level_m: usize,
        mut score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    ) {
        // Invalidate assumptions about the heuristic eagerly.
        self.processed_by_heuristic = 0;

        // ToDo: binary search here ? (most likely does not worth it)
        let new_to_target = score(target_point_id, new_point_id);

        let mut id_to_insert = self.links.len();
        for (i, &item) in self.links.iter().enumerate() {
            let target_to_link = score(target_point_id, item);
            if target_to_link < new_to_target {
                id_to_insert = i;
                break;
            }
        }

        if self.links.len() < level_m {
            self.links.insert(id_to_insert, new_point_id);
        } else if id_to_insert != self.links.len() {
            self.links.pop();
            self.links.insert(id_to_insert, new_point_id);
        }
    }

    /// Append one point to the container. If the container is full, run the heuristic.
    ///
    /// This is a reference implementation for testing.
    #[cfg(test)]
    fn connect_with_heuristic_simple(
        &mut self,
        new_point_id: PointOffsetType,
        target_point_id: PointOffsetType,
        level_m: usize,
        mut score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    ) {
        if self.links.len() < level_m {
            self.links.push(new_point_id);
        } else {
            let mut candidates = Vec::with_capacity(level_m + 1);
            for &idx in &self.links {
                candidates.push(ScoredPointOffset {
                    idx,
                    score: score(target_point_id, idx),
                });
            }
            candidates.push(ScoredPointOffset {
                idx: new_point_id,
                score: score(target_point_id, new_point_id),
            });
            candidates.sort_unstable_by(|a, b| b.score.total_cmp(&a.score));
            self.fill_from_sorted_with_heuristic(candidates.into_iter(), level_m, score);
        }
    }

    /// Append one point to the container. If the container is full, run the heuristic.
    ///
    /// The result is exactly the same as [`Self::connect_with_heuristic_simple`],
    /// but this implementation cuts some corners given that some of the links
    /// are already processed by the heuristic.
    pub fn connect_with_heuristic(
        &mut self,
        new_point_id: PointOffsetType,
        target_point_id: PointOffsetType,
        level_m: usize,
        mut score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
        items: &mut ItemsBuffer,
    ) {
        if level_m == 0 {
            // Unlikely.
            return;
        }

        if self.links.len() < level_m {
            self.links.push(new_point_id);
            return;
        }

        items.0.clear();
        items.0.try_set_capacity_exact(level_m + 1).unwrap();
        for (order, &link) in self.links.iter().enumerate() {
            items.0.push(Item {
                idx: link,
                score: Cell::new(None),
                order: if order < self.processed_by_heuristic as usize {
                    NonZeroU32::new(order as u32)
                } else {
                    None
                },
            });
        }
        items.0.push(Item {
            idx: new_point_id,
            score: Cell::new(None),
            order: None,
        });
        items.0.sort_unstable_by(|a, b| {
            if let (Some(a_order), Some(b_order)) = (a.order, b.order) {
                // Both items processed by the heuristic, compare their order
                // to avoid recomputing the score.
                return a_order.cmp(&b_order);
            }
            b.cached_score(target_point_id, &mut score)
                .total_cmp(&a.cached_score(target_point_id, &mut score))
        });

        self.links.clear();

        // The code below is similar to `fill_from_sorted_with_heuristic` with
        // two notable differences:
        //
        // (A) If both items have already been processed by the heuristic, the
        // score check is skipped as it is known to pass.
        //
        // (B) Instead of having separate input iterator (`candidates`) and
        // intermediate vector for the processed items (`self.links`), this
        // implementation reads and updates the same vector in-place:
        // - `items[read]` is the next candidate to be processed.
        // - `items[0..write]` are already processed items.
        // Since `read` ≤ `write`, there are no collisions, so this approach is
        // sound.
        let mut write = 0;
        'outer: for read in 0..items.0.len() {
            let candidate = items.0[read].clone();
            for existing in &items.0[0..write] {
                if candidate.order.is_some() && existing.order.is_some() {
                    continue; // See (A).
                }
                if score(candidate.idx, existing.idx)
                    > candidate.cached_score(target_point_id, &mut score)
                {
                    continue 'outer;
                }
            }

            self.links.push(candidate.idx);
            items.0[write] = candidate;
            write += 1;
            if write >= level_m {
                break;
            }
        }
        self.processed_by_heuristic = self.links.len() as u32;
    }
}

/// Internal buffer to avoid allocations.
#[derive(Default)]
pub struct ItemsBuffer(Vec<Item>);

#[derive(Debug, Clone)]
struct Item {
    idx: PointOffsetType,
    score: Cell<Option<ScoreType>>,
    /// Order is used to avoid recomputing the score for items known be sorted.
    order: Option<NonZeroU32>,
}

impl Item {
    /// Get the score. This value is lazy/cached: it's computed no more than once.
    fn cached_score<F>(&self, query: PointOffsetType, score: F) -> ScoreType
    where
        F: FnOnce(PointOffsetType, PointOffsetType) -> ScoreType,
    {
        if let Some(score) = self.score.get() {
            score
        } else {
            let score = score(query, self.idx);
            self.score.set(Some(score));
            score
        }
    }
}

#[cfg(test)]
mod tests {
    use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
    use itertools::Itertools as _;
    use rand::SeedableRng as _;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom as _;

    use super::*;
    use crate::data_types::vectors::DenseVector;
    use crate::fixtures::index_fixtures::{TestRawScorerProducer, random_vector};
    use crate::spaces::simple::EuclidMetric;

    #[test]
    #[ignore]
    fn test_candidate_selection_heuristics() {
        const NUM_VECTORS: usize = 100;
        const DIM: usize = 16;
        const M: usize = 16;

        let mut rng = StdRng::seed_from_u64(42);

        let vector_holder = TestRawScorerProducer::<EuclidMetric>::new(DIM, NUM_VECTORS, &mut rng);

        let mut candidates: FixedLengthPriorityQueue<ScoredPointOffset> =
            FixedLengthPriorityQueue::new(NUM_VECTORS);

        let new_vector_to_insert = random_vector(&mut rng, DIM);

        let scorer = vector_holder.get_scorer(new_vector_to_insert);

        for i in 0..NUM_VECTORS {
            candidates.push(ScoredPointOffset {
                idx: i as PointOffsetType,
                score: scorer.score_point(i as PointOffsetType),
            });
        }

        let sorted_candidates_vec = candidates.clone().into_sorted_vec();

        for x in sorted_candidates_vec.iter().take(M) {
            eprintln!("sorted_candidates = ({}, {})", x.idx, x.score);
        }

        let mut links_container = LinksContainer::with_capacity(M);
        links_container.fill_from_sorted_with_heuristic(
            candidates.into_iter_sorted(),
            M,
            |a, b| scorer.score_internal(a, b),
        );
        let selected_candidates = links_container.links().to_vec();

        for x in selected_candidates.iter() {
            eprintln!("selected_candidates = {x}");
        }
    }

    #[test]
    fn test_connect_new_point() {
        let m = 6;

        // ○ 10 K
        //
        //
        //
        //
        //
        //
        //
        //
        //
        //
        //                 ○ 9 H
        //
        //
        //
        //
        //
        //           ○ 7 I
        //                        ● 6 G
        //
        //
        //           ○ 8 J                                         ○ 4 E
        //
        //
        //                                                          ● 3 D
        //
        //
        //                                            ◉ Target             ○ 5 F
        //   Y
        //   ▲
        //   │                                  ● 1 B
        //   └──▶ X
        //                                         ○ 2 C
        let points: Vec<DenseVector> = vec![
            vec![21.79, 07.18], //   Target
            vec![20.58, 05.46], // + 1 B
            vec![21.19, 04.51], //   2 C   closer to B than to the target
            vec![24.73, 08.24], // + 3 D
            vec![24.55, 09.98], //   4 E   closer to D than to the target
            vec![26.11, 06.85], //   5 F   closer to D than to the target
            vec![17.64, 11.14], // + 6 G
            vec![14.97, 11.52], //   7 I   closer to G than to the target
            vec![14.97, 09.60], //   8 J   closer to B and G than to the target
            vec![16.23, 14.32], //   9 H   closer to G than to the target
            vec![12.69, 19.13], //  10 K   closer to G than to the target
        ];

        let scorer = |a: PointOffsetType, b: PointOffsetType| {
            -((points[a as usize][0] - points[b as usize][0]).powi(2)
                + (points[a as usize][1] - points[b as usize][1]).powi(2))
            .sqrt()
        };

        let mut insert_ids = (1..points.len() as PointOffsetType).collect_vec();

        let mut candidates = FixedLengthPriorityQueue::new(insert_ids.len());
        for &id in &insert_ids {
            candidates.push(ScoredPointOffset {
                idx: id,
                score: scorer(0, id),
            });
        }

        let mut res = LinksContainer::with_capacity(m);
        res.fill_from_sorted_with_heuristic(candidates.into_iter_sorted(), m, scorer);

        assert_eq!(&res.links(), &[1, 3, 6]);

        let mut rng = StdRng::seed_from_u64(42);

        let mut links_container = LinksContainer::with_capacity(m);
        insert_ids.shuffle(&mut rng);
        for &id in &insert_ids {
            links_container.connect(id, 0, m, scorer);
        }
        assert_eq!(links_container.links(), &vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_connect_new_point_with_heuristic() {
        let mut rng = StdRng::seed_from_u64(42);

        const NUM_VECTORS: usize = 20;
        const DIM: usize = 128;
        const M: usize = 5;

        for _ in 0..1000 {
            let vector_holder =
                TestRawScorerProducer::<EuclidMetric>::new(DIM, NUM_VECTORS, &mut rng);
            let scorer = vector_holder.get_scorer(random_vector(&mut rng, DIM));

            let mut candidate_indices: Vec<_> = (0..NUM_VECTORS as u32).collect();
            candidate_indices.shuffle(&mut rng);

            let query_idx = candidate_indices.pop().unwrap();
            let score = |a: u32, b: u32| scorer.score_internal(a, b);
            let scored_offfset = |idx: u32| ScoredPointOffset {
                idx,
                score: score(query_idx, idx),
            };

            let mut container = LinksContainer::with_capacity(M);
            container.fill_from_sorted_with_heuristic(
                candidate_indices
                    .iter()
                    .copied()
                    .map(scored_offfset)
                    .take(5)
                    .sorted_by(|a, b| b.score.total_cmp(&a.score)),
                M,
                score,
            );

            let mut reference_container = LinksContainer::with_capacity(M);
            reference_container.fill_from_sorted_with_heuristic(
                candidate_indices
                    .iter()
                    .copied()
                    .map(scored_offfset)
                    .take(5)
                    .sorted_by(|a, b| b.score.total_cmp(&a.score)),
                M,
                score,
            );

            let mut items = ItemsBuffer::default();
            for &candidate_idx in candidate_indices.iter().skip(5) {
                container.connect_with_heuristic(candidate_idx, query_idx, M, score, &mut items);
                reference_container.connect_with_heuristic_simple(
                    candidate_idx,
                    query_idx,
                    M,
                    score,
                );
                assert_eq!(container.links, reference_container.links);
            }
        }
    }
}
