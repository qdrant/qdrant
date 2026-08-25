use crate::types::{ScoreType, ScoredPointOffset};

/// Avoid excessive memory allocation and allocation failures on huge limits
const LARGEST_REASONABLE_ALLOCATION_SIZE: usize = 1_048_576;

/// The threshold for switching from a sorted buffer to a median-based algorithm.
const SORTED_K: usize = 32;

#[derive(Default)]
pub struct TopK {
    k: usize,
    elements: Vec<ScoredPointOffset>,
    threshold: ScoreType,
}

impl TopK {
    /// Panics if `k` is 0.
    pub fn new(k: usize) -> Self {
        assert!(k > 0, "k must be greater than zero");
        let capacity = match k < SORTED_K {
            true => k,
            false => k.saturating_mul(2).min(LARGEST_REASONABLE_ALLOCATION_SIZE),
        };
        TopK {
            threshold: ScoreType::NEG_INFINITY,
            elements: Vec::with_capacity(capacity),
            k,
        }
    }

    #[inline]
    pub fn push(&mut self, point: ScoredPointOffset) {
        if point.score > self.threshold {
            self.cold_insert(point);
        }
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Score of the worst point that can still be kept.
    ///
    /// For `k` more than [`SORTED_K`] refreshed every `k` accepted points.
    pub fn threshold(&self) -> ScoreType {
        self.threshold
    }

    /// Descending by score.
    pub fn into_vec(mut self) -> Vec<ScoredPointOffset> {
        if self.k >= SORTED_K {
            self.elements.sort_unstable_by(descending);
            self.elements.truncate(self.k);
        }
        self.elements
    }

    #[inline(never)]
    fn cold_insert(&mut self, point: ScoredPointOffset) {
        if self.k < SORTED_K {
            if self.elements.len() < self.k {
                self.elements.push(point); // Placeholder, overwritten by the slide below.
            }

            let points = &mut self.elements;
            let mut position = points.len() - 1;
            while position > 0 && points[position - 1].score < point.score {
                points[position] = points[position - 1];
                position -= 1;
            }
            points[position] = point;

            if let Some(worst) = points.get(self.k - 1) {
                self.threshold = worst.score;
            }
        } else {
            // median algorithm described in https://quickwit.io/blog/top-k-complexity.
            self.elements.push(point);
            // check if full
            if self.elements.len() == self.k.saturating_mul(2) {
                let (_, kth, _) = self.elements.select_nth_unstable_by(self.k - 1, descending);
                self.threshold = kth.score;
                self.elements.truncate(self.k);
            }
        }
    }
}

#[inline(always)]
fn descending(a: &ScoredPointOffset, b: &ScoredPointOffset) -> std::cmp::Ordering {
    b.score.total_cmp(&a.score)
}

#[cfg(test)]
mod test {
    use rand::rngs::StdRng;
    use rand::{RngExt as _, SeedableRng as _};

    use super::*;
    use crate::fixed_length_priority_queue::FixedLengthPriorityQueue;

    /// Both strategies must keep the same scores as the reference queue, minus the NaNs.
    #[test]
    fn test_against_priority_queue() {
        let mut rng = StdRng::seed_from_u64(42);
        // Few distinct scores, to run into ties at the rejection threshold.
        let points = (0..1000)
            .map(|idx| ScoredPointOffset {
                idx,
                score: match idx % 37 {
                    0 => ScoreType::NAN,
                    _ => rng.random_range(0..50) as ScoreType,
                },
            })
            .collect::<Vec<_>>();

        for k in [1, 2, 10, 31, 32, 33, 999, 1000, 2000] {
            let mut reference = FixedLengthPriorityQueue::new(k);
            let mut top_k = TopK::new(k);
            for &point in &points {
                if !point.score.is_nan() {
                    reference.push(point);
                }
                top_k.push(point);
            }

            let scores = |points: Vec<ScoredPointOffset>| {
                points
                    .into_iter()
                    .map(|point| point.score)
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                scores(top_k.into_vec()),
                scores(reference.into_sorted_vec()),
                "k={k}",
            );
        }
    }

    #[test]
    fn empty_capacity() {
        let top_k = TopK::new(3);
        assert_eq!(top_k.len(), 0);
        assert_eq!(top_k.elements.capacity(), 3);
        assert_eq!(top_k.threshold(), ScoreType::NEG_INFINITY);

        // From `SORTED_K` on, the buffer holds twice as many points as it keeps.
        assert_eq!(TopK::new(SORTED_K).elements.capacity(), 2 * SORTED_K);
    }

    #[test]
    fn huge_k_does_not_panic() {
        // `k` is the client-supplied search limit. A huge value must not abort
        // on the initial reservation (capped below), and must not overflow the
        // `2 * k` push threshold once scoring starts.
        let mut top_k = TopK::new(usize::MAX);
        assert_eq!(top_k.len(), 0);
        assert_eq!(
            top_k.elements.capacity(),
            LARGEST_REASONABLE_ALLOCATION_SIZE
        );

        top_k.push(ScoredPointOffset { score: 1.0, idx: 1 });
        top_k.push(ScoredPointOffset { score: 2.0, idx: 2 });
        assert_eq!(top_k.len(), 2);

        let res = top_k.into_vec();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].score, 2.0);
        assert_eq!(res[1].score, 1.0);
    }

    #[test]
    fn test_top_k_under() {
        let mut top_k = TopK::new(3);
        top_k.push(ScoredPointOffset { score: 1.0, idx: 1 });
        assert_eq!(top_k.threshold(), ScoreType::NEG_INFINITY);
        assert_eq!(top_k.len(), 1);

        top_k.push(ScoredPointOffset { score: 2.0, idx: 2 });
        assert_eq!(top_k.threshold(), ScoreType::NEG_INFINITY);
        assert_eq!(top_k.len(), 2);

        let res = top_k.into_vec();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].score, 2.0);
        assert_eq!(res[1].score, 1.0);
    }

    #[test]
    fn test_top_k_over() {
        let mut top_k = TopK::new(3);
        top_k.push(ScoredPointOffset { score: 1.0, idx: 1 });
        assert_eq!(top_k.len(), 1);
        assert_eq!(top_k.threshold(), ScoreType::NEG_INFINITY);

        top_k.push(ScoredPointOffset { score: 3.0, idx: 3 });
        assert_eq!(top_k.len(), 2);
        assert_eq!(top_k.threshold(), ScoreType::NEG_INFINITY);

        // The third point fills the queue, from now on the worst one is the threshold.
        top_k.push(ScoredPointOffset { score: 2.0, idx: 2 });
        assert_eq!(top_k.len(), 3);
        assert_eq!(top_k.threshold(), 1.0);

        top_k.push(ScoredPointOffset { score: 4.0, idx: 4 });
        assert_eq!(top_k.len(), 3);
        assert_eq!(top_k.threshold(), 2.0);

        let res = top_k.into_vec();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0].score, 4.0);
        assert_eq!(res[1].score, 3.0);
        assert_eq!(res[2].score, 2.0);
    }

    #[test]
    fn test_top_k_pruned() {
        let mut top_k = TopK::new(3);
        for (score, idx) in [(1.0, 1), (4.0, 4), (2.0, 2)] {
            top_k.push(ScoredPointOffset { idx, score });
        }
        assert_eq!(top_k.len(), 3);
        assert_eq!(top_k.threshold(), 1.0);

        // Every further point evicts the worst one and raises the threshold.
        top_k.push(ScoredPointOffset { score: 5.0, idx: 5 });
        assert_eq!(top_k.threshold(), 2.0);

        top_k.push(ScoredPointOffset { score: 3.0, idx: 3 });
        assert_eq!(top_k.threshold(), 3.0);

        top_k.push(ScoredPointOffset { score: 6.0, idx: 6 });
        assert_eq!(top_k.threshold(), 4.0);
        assert_eq!(top_k.len(), 3);

        let res = top_k.into_vec();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0].score, 6.0);
        assert_eq!(res[1].score, 5.0);
        assert_eq!(res[2].score, 4.0);
    }

    #[test]
    fn test_top_same_scores() {
        let mut top_k = TopK::new(3);
        for (score, idx) in [(1.0, 1), (1.0, 4), (2.0, 2)] {
            top_k.push(ScoredPointOffset { idx, score });
        }
        assert_eq!(top_k.len(), 3);
        assert_eq!(top_k.threshold(), 1.0);

        // A score equal to the threshold does not displace the points already kept.
        for (score, idx) in [(1.0, 5), (1.0, 3), (1.0, 6)] {
            top_k.push(ScoredPointOffset { idx, score });
            assert_eq!(top_k.len(), 3);
            assert_eq!(top_k.threshold(), 1.0);
        }

        let res = top_k.into_vec();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0], ScoredPointOffset { score: 2.0, idx: 2 });
        assert_eq!(res[1], ScoredPointOffset { score: 1.0, idx: 1 });
        assert_eq!(res[2], ScoredPointOffset { score: 1.0, idx: 4 });
    }
}
