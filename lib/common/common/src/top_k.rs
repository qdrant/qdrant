use ordered_float::Float;

use crate::types::{ScoreType, ScoredPointOffset};

/// TopK implementation following the median algorithm described in
/// https://quickwit.io/blog/top-k-complexity
///
/// Keeps the largest `k` ScoredPointOffset.
#[derive(Default)]
pub struct TopK {
    k: usize,
    elements: Vec<ScoredPointOffset>,
    threshold: ScoreType,
}

impl TopK {
    pub fn new(k: usize) -> Self {
        TopK {
            k,
            elements: Vec::with_capacity(2 * k),
            threshold: ScoreType::min_value(),
        }
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Returns the minimum score of the top k elements.
    ///
    /// Updated every 2k elements
    /// Initially set to `ScoreType::MIN`.
    pub fn threshold(&self) -> ScoreType {
        self.threshold
    }

    pub fn prune_elements(&mut self) -> Vec<ScoredPointOffset> {
        let (_, median_el, _) = self
            .elements
            .select_nth_unstable_by(self.k - 1, |a, b| b.cmp(a));
        self.threshold = median_el.score;
        self.elements.split_off(self.k)
    }

    // Push an element to the top k.
    // It ejects (returns) low scoring elements when full
    pub fn push(&mut self, element: ScoredPointOffset) -> Option<Vec<ScoredPointOffset>> {
        if element.score > self.threshold {
            self.elements.push(element);
            // prune half the elements if full
            if self.elements.len() == self.k * 2 {
                return Some(self.prune_elements());
            }
            return None;
        }
        None
    }

    pub fn iter(&self) -> TopKIter<'_> {
        TopKIter {
            iter: self.elements.iter(),
        }
    }

    pub fn into_vec(mut self) -> Vec<ScoredPointOffset> {
        self.prune_elements();
        self.elements
    }
}

impl<'a> IntoIterator for &'a TopK {
    type Item = &'a ScoredPointOffset;
    type IntoIter = TopKIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for TopK {
    type Item = ScoredPointOffset;
    type IntoIter = std::vec::IntoIter<ScoredPointOffset>;

    fn into_iter(mut self) -> Self::IntoIter {
        self.prune_elements();
        self.elements.into_iter()
    }
}

pub struct TopKIter<'a> {
    iter: std::slice::Iter<'a, ScoredPointOffset>,
}

impl<'a> Iterator for TopKIter<'a> {
    type Item = &'a ScoredPointOffset;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn empty_with_double_capacity() {
        let top_k = TopK::new(3);
        assert_eq!(top_k.len(), 0);
        assert_eq!(top_k.elements.capacity(), 2 * 3);
        assert_eq!(top_k.threshold(), ScoreType::MIN);
    }

    #[test]
    fn test_top_k_under() {
        let mut top_k = TopK::new(3);
        top_k.push(ScoredPointOffset { score: 1.0, idx: 1 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
        assert_eq!(top_k.len(), 1);

        top_k.push(ScoredPointOffset { score: 2.0, idx: 2 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
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
        assert_eq!(top_k.threshold(), ScoreType::MIN);

        top_k.push(ScoredPointOffset { score: 3.0, idx: 3 });
        assert_eq!(top_k.len(), 2);
        assert_eq!(top_k.threshold(), ScoreType::MIN);

        top_k.push(ScoredPointOffset { score: 2.0, idx: 2 });
        assert_eq!(top_k.len(), 3);
        assert_eq!(top_k.threshold(), ScoreType::MIN);

        top_k.push(ScoredPointOffset { score: 4.0, idx: 4 });
        assert_eq!(top_k.len(), 4);
        assert_eq!(top_k.threshold(), ScoreType::MIN);

        let res = top_k.into_vec();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0].score, 4.0);
        assert_eq!(res[1].score, 3.0);
        assert_eq!(res[2].score, 2.0);
    }

    #[test]
    fn test_top_k_pruned() {
        let mut top_k = TopK::new(3);
        top_k.push(ScoredPointOffset { score: 1.0, idx: 1 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
        assert_eq!(top_k.len(), 1);

        top_k.push(ScoredPointOffset { score: 4.0, idx: 4 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
        assert_eq!(top_k.len(), 2);

        top_k.push(ScoredPointOffset { score: 2.0, idx: 2 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
        assert_eq!(top_k.len(), 3);

        top_k.push(ScoredPointOffset { score: 5.0, idx: 5 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
        assert_eq!(top_k.len(), 4);

        top_k.push(ScoredPointOffset { score: 3.0, idx: 3 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
        assert_eq!(top_k.len(), 5);

        top_k.push(ScoredPointOffset { score: 6.0, idx: 6 });
        assert_eq!(top_k.threshold(), 4.0);
        assert_eq!(top_k.len(), 3);
        assert_eq!(top_k.elements.capacity(), 6);

        let res = top_k.into_vec();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0].score, 6.0);
        assert_eq!(res[1].score, 5.0);
        assert_eq!(res[2].score, 4.0);
    }

    #[test]
    fn test_top_same_scores() {
        let mut top_k = TopK::new(3);
        top_k.push(ScoredPointOffset { score: 1.0, idx: 1 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
        assert_eq!(top_k.len(), 1);

        top_k.push(ScoredPointOffset { score: 1.0, idx: 4 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
        assert_eq!(top_k.len(), 2);

        top_k.push(ScoredPointOffset { score: 2.0, idx: 2 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
        assert_eq!(top_k.len(), 3);

        top_k.push(ScoredPointOffset { score: 1.0, idx: 5 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
        assert_eq!(top_k.len(), 4);

        top_k.push(ScoredPointOffset { score: 1.0, idx: 3 });
        assert_eq!(top_k.threshold(), ScoreType::MIN);
        assert_eq!(top_k.len(), 5);

        top_k.push(ScoredPointOffset { score: 1.0, idx: 6 });
        assert_eq!(top_k.threshold(), 1.0);
        assert_eq!(top_k.len(), 3);
        assert_eq!(top_k.elements.capacity(), 6);

        let res = top_k.into_vec();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0], ScoredPointOffset { score: 2.0, idx: 2 });
        assert_eq!(res[1], ScoredPointOffset { score: 1.0, idx: 1 });
        assert_eq!(res[2], ScoredPointOffset { score: 1.0, idx: 4 });
    }
}
