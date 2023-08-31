use crate::types::ScoreType;

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

    pub fn new_preprocessed<F, B>(positives: Vec<B>, negatives: Vec<B>, preprocess: &F) -> Self
    where
        F: Fn(B) -> T,
    {
        Self {
            positives: positives.into_iter().map(preprocess).collect(),
            negatives: negatives.into_iter().map(preprocess).collect(),
        }
    }

    pub fn reprocess<F, U>(&self, preprocess: &F) -> RecoQuery<U>
    where
        F: Fn(&T) -> U,
    {
        RecoQuery::new(
            self.positives.iter().map(preprocess).collect(),
            self.negatives.iter().map(preprocess).collect(),
        )
    }

    /// Compares all vectors of the query against a single vector via a similarity function,
    /// then folds the similarites into a single score.
    pub fn score(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        // get similarities to all positives
        let positive_similarities = self.positives.iter().map(&similarity);

        // and all negatives
        let negative_similarities = self.negatives.iter().map(&similarity);

        merge_similarities(positive_similarities, negative_similarities)
    }
}

fn merge_similarities(
    positives: impl Iterator<Item = ScoreType>,
    negatives: impl Iterator<Item = ScoreType>,
) -> ScoreType {
    // get max similarity to positives and max to negatives
    let max_positive = positives.max_by(|a, b| a.total_cmp(b)).unwrap_or(0.0);

    let max_negative = negatives.max_by(|a, b| a.total_cmp(b)).unwrap_or(0.0);

    if max_positive > max_negative {
        max_positive
    } else {
        -(max_negative * max_negative)
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use super::RecoQuery;
    use crate::types::ScoreType;

    #[rstest]
    #[case::higher_positive(vec![42], vec![4], 42.0)]
    #[case::higher_negative(vec![4], vec![42], -(42.0 * 42.0))]
    #[case::negative_zero(vec![-1], vec![0], 0.0)]
    #[case::positive_zero(vec![0], vec![-1], 0.0)]
    #[case::both_under_zero(vec![-42], vec![-84], -42.0)]
    #[case::both_under_zero_but_negative_is_higher(vec![-84], vec![-42], -(42.0 * 42.0))]
    #[case::multiple_with_negative_best(vec![1, 2, 3], vec![4, 5, 6], -(6.0 * 6.0))]
    #[case::multiple_with_positive_best(vec![10, 2, 3], vec![4, 5, 6], 10.0)]
    fn score_query(
        #[case] positives: Vec<isize>,
        #[case] negatives: Vec<isize>,
        #[case] expected: ScoreType,
    ) {
        let query = RecoQuery::new(positives, negatives);

        let score = query.score(|x| *x as ScoreType);

        assert_eq!(score, expected);
    }
}
