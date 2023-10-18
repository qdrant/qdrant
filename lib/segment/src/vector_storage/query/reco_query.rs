use common::types::ScoreType;

use super::{Query, TransformInto};
use crate::data_types::vectors::{QueryVector, VectorType};

#[derive(Debug, Clone)]
pub struct RecoQuery<T> {
    pub positives: Vec<T>,
    pub negatives: Vec<T>,
}

impl<T> RecoQuery<T> {
    pub fn new(positives: Vec<T>, negatives: Vec<T>) -> Self {
        Self {
            positives,
            negatives,
        }
    }

    pub fn flat_iter(&self) -> impl Iterator<Item = &T> {
        self.positives.iter().chain(self.negatives.iter())
    }
}

impl<T, U> TransformInto<RecoQuery<U>, T, U> for RecoQuery<T> {
    fn transform<F>(self, mut f: F) -> RecoQuery<U>
    where
        F: FnMut(T) -> U,
    {
        RecoQuery::new(
            self.positives.into_iter().map(&mut f).collect(),
            self.negatives.into_iter().map(&mut f).collect(),
        )
    }
}

impl<T> Query<T> for RecoQuery<T> {
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        // get similarities to all positives
        let positive_similarities = self.positives.iter().map(&similarity);

        // and all negatives
        let negative_similarities = self.negatives.iter().map(&similarity);

        merge_similarities(positive_similarities, negative_similarities)
    }
}

#[inline]
fn merge_similarities(
    positives: impl Iterator<Item = ScoreType>,
    negatives: impl Iterator<Item = ScoreType>,
) -> ScoreType {
    // get max similarity to positives and max to negatives
    let max_positive = positives
        .max_by(|a, b| a.total_cmp(b))
        .unwrap_or(ScoreType::NEG_INFINITY);

    let max_negative = negatives
        .max_by(|a, b| a.total_cmp(b))
        .unwrap_or(ScoreType::NEG_INFINITY);

    if max_positive > max_negative {
        max_positive
    } else {
        -(max_negative * max_negative)
    }
}

impl From<RecoQuery<VectorType>> for QueryVector {
    fn from(query: RecoQuery<VectorType>) -> Self {
        QueryVector::Recommend(query)
    }
}

#[cfg(test)]
mod test {
    use common::types::ScoreType;
    use rstest::rstest;

    use super::RecoQuery;
    use crate::vector_storage::query::Query;

    #[rstest]
    #[case::higher_positive(vec![42], vec![4], 42.0)]
    #[case::higher_negative(vec![4], vec![42], -(42.0 * 42.0))]
    #[case::negative_zero(vec![-1], vec![0], 0.0)]
    #[case::positive_zero(vec![0], vec![-1], 0.0)]
    #[case::both_under_zero(vec![-42], vec![-84], -42.0)]
    #[case::both_under_zero_but_negative_is_higher(vec![-84], vec![-42], -(42.0 * 42.0))]
    #[case::multiple_with_negative_best(vec![1, 2, 3], vec![4, 5, 6], -(6.0 * 6.0))]
    #[case::multiple_with_positive_best(vec![10, 2, 3], vec![4, 5, 6], 10.0)]
    #[case::no_input(vec![], vec![], ScoreType::NEG_INFINITY)]
    fn score_query(
        #[case] positives: Vec<isize>,
        #[case] negatives: Vec<isize>,
        #[case] expected: ScoreType,
    ) {
        let query = RecoQuery::new(positives, negatives);

        let dummy_similarity = |x: &isize| *x as ScoreType;

        let score = query.score_by(dummy_similarity);

        assert_eq!(score, expected);
    }
}
