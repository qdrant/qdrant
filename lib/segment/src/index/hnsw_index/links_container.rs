use std::collections::BinaryHeap;
use std::iter::Copied;

use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};

pub struct LinksContainer {
    links: Vec<PointOffsetType>,
}

impl LinksContainer {
    pub fn with_capacity(m: usize) -> Self {
        Self {
            links: Vec::with_capacity(m),
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

    pub fn fill_from(&mut self, points: impl Iterator<Item = PointOffsetType>) {
        self.links.clear();
        self.links.extend(points);
    }

    pub fn fill_from_sorted_with_heuristic(
        &mut self,
        candidates: impl Iterator<Item = ScoredPointOffset>,
        level_m: usize,
        score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    ) {
        self.links.clear();
        let selected = select_candidate_with_heuristic_from_sorted(candidates, level_m, score);
        self.links.clone_from(&selected);
    }

    /// Connect new point to links, so that links contains only closest points
    pub fn connect(
        &mut self,
        new_point_id: PointOffsetType,
        target_point_id: PointOffsetType,
        level_m: usize,
        mut score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    ) {
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

    pub fn connect_with_heuristic(
        &mut self,
        point_id: PointOffsetType,
        other_point: PointOffsetType,
        level_m: usize,
        mut score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    ) {
        if self.links.len() < level_m {
            // If linked point is lack of neighbours
            self.links.push(point_id);
        } else {
            let mut candidates = BinaryHeap::with_capacity(level_m + 1);
            candidates.push(ScoredPointOffset {
                idx: point_id,
                score: score(point_id, other_point),
            });
            for other_point_link in self.links.iter().take(level_m).copied() {
                candidates.push(ScoredPointOffset {
                    idx: other_point_link,
                    score: score(other_point_link, other_point),
                });
            }
            let selected_candidates = select_candidate_with_heuristic_from_sorted(
                candidates.into_sorted_vec().into_iter().rev(),
                level_m,
                score,
            );
            self.links.clear(); // this do not free memory, which is good
            for selected in selected_candidates.iter().copied() {
                self.links.push(selected);
            }
        }
    }
}

/// <https://github.com/nmslib/hnswlib/issues/99>
fn select_candidate_with_heuristic_from_sorted(
    candidates: impl Iterator<Item = ScoredPointOffset>,
    m: usize,
    mut score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
) -> Vec<PointOffsetType> {
    let mut result_list = Vec::with_capacity(m);
    for current_closest in candidates {
        if result_list.len() >= m {
            break;
        }
        let mut is_good = true;
        for &selected_point in &result_list {
            let dist_to_already_selected = score(current_closest.idx, selected_point);
            if dist_to_already_selected > current_closest.score {
                is_good = false;
                break;
            }
        }
        if is_good {
            result_list.push(current_closest.idx);
        }
    }

    result_list
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

        let mut container = LinksContainer::with_capacity(M);
        container.fill_from_sorted_with_heuristic(candidates.into_iter_sorted(), M, |a, b| {
            scorer.score_internal(a, b)
        });
        let selected_candidates = container.links().to_vec();

        for x in selected_candidates.iter() {
            eprintln!("selected_candidates = {x}");
        }
    }

    #[test]
    fn test_connect_new_point() {
        let m = 6;

        // See illustration in docs
        let points: Vec<DenseVector> = vec![
            vec![21.79, 7.18],  // Target
            vec![20.58, 5.46],  // 1  B - yes
            vec![21.19, 4.51],  // 2  C
            vec![24.73, 8.24],  // 3  D - yes
            vec![24.55, 9.98],  // 4  E
            vec![26.11, 6.85],  // 5  F
            vec![17.64, 11.14], // 6  G - yes
            vec![14.97, 11.52], // 7  I
            vec![14.97, 9.60],  // 8  J
            vec![16.23, 14.32], // 9  H
            vec![12.69, 19.13], // 10 K
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
}
