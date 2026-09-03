use std::hash::Hash;
use std::iter::Peekable;

use common::math::scaled_fast_sigmoid;
use common::types::ScoreType;
use itertools::Itertools;
use serde::Serialize;
use sparse::common::sparse_vector::SparseVector;

use super::{Query, TransformInto};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::{
    DenseVector, QueryVector, TypedMultiDenseVector, VectorElementType, VectorInternal, VectorRef,
};

#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
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
    fn transform(self, f: &dyn Fn(T) -> OperationResult<U>) -> OperationResult<RecoQuery<U>> {
        let positives = self.positives.into_iter().map(f).try_collect()?;
        let negatives = self.negatives.into_iter().map(f).try_collect()?;
        Ok(RecoQuery::new(positives, negatives))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoBestScoreQuery<T>(RecoQuery<T>);

impl<T> From<RecoQuery<T>> for RecoBestScoreQuery<T> {
    fn from(query: RecoQuery<T>) -> Self {
        Self(query)
    }
}

impl<T, U> TransformInto<RecoBestScoreQuery<U>, T, U> for RecoBestScoreQuery<T> {
    fn transform(
        self,
        f: &dyn Fn(T) -> OperationResult<U>,
    ) -> OperationResult<RecoBestScoreQuery<U>> {
        Ok(RecoBestScoreQuery(self.0.transform(f)?))
    }
}

impl From<RecoBestScoreQuery<VectorInternal>> for QueryVector {
    fn from(query: RecoBestScoreQuery<VectorInternal>) -> Self {
        QueryVector::RecommendBestScore(query.0)
    }
}

impl<T> Query<T> for RecoBestScoreQuery<T> {
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        // get similarities to all positives
        let mut max_positive = ScoreType::NEG_INFINITY;
        for vector in &self.0.positives {
            let score = similarity(vector);
            if score.total_cmp(&max_positive).is_gt() {
                max_positive = score;
            }
        }

        // and all negatives
        let mut max_negative = ScoreType::NEG_INFINITY;
        for vector in &self.0.negatives {
            let score = similarity(vector);
            if score.total_cmp(&max_negative).is_gt() {
                max_negative = score;
            }
        }

        if max_positive > max_negative {
            scaled_fast_sigmoid(max_positive)
        } else {
            -scaled_fast_sigmoid(max_negative)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoSumScoresQuery<T>(RecoQuery<T>);

impl<T> From<RecoQuery<T>> for RecoSumScoresQuery<T> {
    fn from(query: RecoQuery<T>) -> Self {
        Self(query)
    }
}

impl<T, U> TransformInto<RecoSumScoresQuery<U>, T, U> for RecoSumScoresQuery<T> {
    fn transform(
        self,
        f: &dyn Fn(T) -> OperationResult<U>,
    ) -> OperationResult<RecoSumScoresQuery<U>> {
        Ok(RecoSumScoresQuery(self.0.transform(f)?))
    }
}

impl From<RecoSumScoresQuery<VectorInternal>> for QueryVector {
    fn from(query: RecoSumScoresQuery<VectorInternal>) -> Self {
        QueryVector::RecommendSumScores(query.0)
    }
}

impl<T> Query<T> for RecoSumScoresQuery<T> {
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        // Sum all positive vectors scores
        let mut positive_score: ScoreType = 0.0;
        for vector in &self.0.positives {
            positive_score += similarity(vector);
        }

        // Sum all negative vectors scores
        let mut negative_score: ScoreType = 0.0;
        for vector in &self.0.negatives {
            negative_score += similarity(vector);
        }

        // Subtract
        positive_score - negative_score
    }
}

/// The query vector of the `average_vector` recommend strategy:
/// `avg(positives)` when there are no negatives, else
/// `avg(positives) + (avg(positives) - avg(negatives))`.
///
/// All examples must be of one kind (all dense, all sparse or all multi); an
/// empty positive set or mixed kinds is a validation error.
pub fn avg_vector_for_recommendation<'a>(
    positive: impl IntoIterator<Item = VectorRef<'a>>,
    mut negative: Peekable<impl Iterator<Item = VectorRef<'a>>>,
) -> OperationResult<VectorInternal> {
    let avg_positive = avg_vectors(positive)?;

    let search_vector = if negative.peek().is_none() {
        avg_positive
    } else {
        let avg_negative = avg_vectors(negative)?;
        merge_positive_and_negative_avg(avg_positive, avg_negative)?
    };

    Ok(search_vector)
}

fn avg_vectors<'a>(
    vectors: impl IntoIterator<Item = VectorRef<'a>>,
) -> OperationResult<VectorInternal> {
    let mut avg_dense = DenseVector::default();
    let mut avg_sparse = SparseVector::default();
    let mut avg_multi: Option<TypedMultiDenseVector<VectorElementType>> = None;
    let mut dense_count = 0;
    let mut sparse_count = 0;
    let mut multi_count = 0;
    for vector in vectors {
        match vector {
            VectorRef::Dense(vector) => {
                dense_count += 1;
                for i in 0..vector.len() {
                    if i >= avg_dense.len() {
                        avg_dense.push(vector[i])
                    } else {
                        avg_dense[i] += vector[i];
                    }
                }
            }
            VectorRef::Sparse(vector) => {
                sparse_count += 1;
                avg_sparse = vector.combine_aggregate(&avg_sparse, |v1, v2| v1 + v2);
            }
            VectorRef::MultiDense(vector) => {
                multi_count += 1;
                avg_multi = Some(avg_multi.map_or_else(
                    || vector.to_owned(),
                    |mut avg_multi| {
                        avg_multi
                            .flattened_vectors
                            .extend_from_slice(vector.flattened_vectors);
                        avg_multi
                    },
                ));
            }
        }
    }

    match (dense_count, sparse_count, multi_count) {
        // TODO(sparse): what if vectors iterator is empty? We return a validation error,
        // but it's not clear if it's the best solution.
        // Currently it's hard to return an zeroed vector, because we don't know its type: dense or sparse.
        (0, 0, 0) => Err(OperationError::validation_error(
            "Positive vectors should not be empty with `average` strategy",
        )),
        (_, 0, 0) => {
            for item in &mut avg_dense {
                *item /= dense_count as VectorElementType;
            }
            Ok(VectorInternal::from(avg_dense))
        }
        (0, _, 0) => {
            for item in &mut avg_sparse.values {
                *item /= sparse_count as VectorElementType;
            }
            Ok(VectorInternal::from(avg_sparse))
        }
        (0, 0, _) => match avg_multi {
            Some(avg_multi) => Ok(VectorInternal::from(avg_multi)),
            None => Err(OperationError::validation_error(
                "Positive vectors should not be empty with `average` strategy",
            )),
        },
        (_, _, _) => Err(OperationError::validation_error(
            "Can't average vectors with different types",
        )),
    }
}

fn merge_positive_and_negative_avg(
    positive: VectorInternal,
    negative: VectorInternal,
) -> OperationResult<VectorInternal> {
    match (positive, negative) {
        (VectorInternal::Dense(positive), VectorInternal::Dense(negative)) => {
            // The `zip` below silently truncates to the shorter of the two
            // vectors. When the positive and negative averages have
            // different dimensions (e.g. positive from a local dim-2
            // collection, negative resolved from a dim-8 `lookup_from`
            // collection), the result loses the trailing negative
            // dimensions and the recommend score becomes magnitude-
            // dependent. Reject the mismatch explicitly (qdrant/qdrant#10369).
            if positive.len() != negative.len() {
                return Err(OperationError::validation_error(format!(
                    "Positive and negative vectors must have the same dimension \
                     for average-vector recommend: positive has dim {}, negative has dim {}",
                    positive.len(),
                    negative.len(),
                )));
            }
            let vector: DenseVector = positive
                .iter()
                .zip(negative.iter())
                .map(|(pos, neg)| pos + pos - neg)
                .collect();
            Ok(vector.into())
        }
        (VectorInternal::Sparse(positive), VectorInternal::Sparse(negative)) => {
            // `combine_aggregate` walks both vectors' index sets; if the
            // dimensions differ the result is a sparse vector with the
            // union of indices, not a length-matched merge. Reject the
            // mismatch explicitly.
            if positive.indices.len() != negative.indices.len() {
                return Err(OperationError::validation_error(format!(
                    "Positive and negative sparse vectors must have the same \
                     dimension for average-vector recommend: positive has dim {}, \
                     negative has dim {}",
                    positive.indices.len(),
                    negative.indices.len(),
                )));
            }
            Ok(positive
                .combine_aggregate(&negative, |pos, neg| pos + pos - neg)
                .into())
        }
        (VectorInternal::MultiDense(mut positive), VectorInternal::MultiDense(negative)) => {
            // merge positive and negative vectors as concatenated vectors with negative vectors negated
            positive
                .flattened_vectors
                .extend(negative.flattened_vectors.into_iter().map(|x| -x));
            Ok(VectorInternal::MultiDense(positive))
        }
        _ => Err(OperationError::validation_error(
            "Positive and negative vectors should be of the same type, either all dense or all sparse or all multi",
        )),
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use common::math::scaled_fast_sigmoid;
    use common::types::ScoreType;
    use proptest::prelude::*;
    use rstest::rstest;
    use sparse::common::sparse_vector::SparseVector;

    use super::{avg_vector_for_recommendation, avg_vectors, merge_positive_and_negative_avg};
    use crate::data_types::vectors::{VectorInternal, VectorRef};
    use crate::vector_storage::query::{Query, RecoBestScoreQuery, RecoQuery};

    enum Chosen {
        Positive,
        Negative,
    }

    #[rstest]
    #[case::higher_positive(vec![42], vec![4], Chosen::Positive, 42.0)]
    #[case::higher_negative(vec![4], vec![42], Chosen::Negative, 42.0)]
    #[case::negative_zero(vec![-1], vec![0], Chosen::Negative, 0.0)]
    #[case::positive_zero(vec![0], vec![-1], Chosen::Positive, 0.0)]
    #[case::both_under_zero(vec![-42], vec![-84], Chosen::Positive, -42.0)]
    #[case::both_under_zero_but_negative_is_higher(vec![-84], vec![-42], Chosen::Negative, -42.0)]
    #[case::multiple_with_negative_best(vec![1, 2, 3], vec![4, 5, 6], Chosen::Negative, 6.0)]
    #[case::multiple_with_positive_best(vec![10, 2, 3], vec![4, 5, 6], Chosen::Positive, 10.0)]
    fn score_query(
        #[case] positives: Vec<isize>,
        #[case] negatives: Vec<isize>,
        #[case] chosen: Chosen,
        #[case] expected: ScoreType,
    ) {
        use super::{RecoBestScoreQuery, RecoQuery};

        let query = RecoBestScoreQuery::from(RecoQuery::new(positives, negatives));

        let dummy_similarity = |x: &isize| *x as ScoreType;

        let positive_transformation = scaled_fast_sigmoid;
        let negative_transformation = |x| -scaled_fast_sigmoid(x);

        let score = query.score_by(dummy_similarity);

        match chosen {
            Chosen::Positive => {
                assert_eq!(score, positive_transformation(expected));
            }
            Chosen::Negative => {
                assert_eq!(score, negative_transformation(expected));
            }
        }
    }

    fn ulps_eq(a: f32, b: f32, ulps: u32) -> bool {
        if a.signum() != b.signum() {
            return false;
        }

        let a = a.to_bits();
        let b = b.to_bits();

        a.abs_diff(b) <= ulps
    }

    /// Relaxes the comparison of floats to allow for a some difference in units of least precision
    fn float_cmp(a: f32, b: f32) -> Ordering {
        if ulps_eq(a, b, 80) {
            Ordering::Equal
        } else {
            a.total_cmp(&b)
        }
    }

    proptest! {
        /// Checks that the negative-chosen scores invert the order of the candidates
        #[test]
        fn correct_negative_order(a in -100f32..=100f32, b in -100f32..=100f32) {
            let dummy_similarity = |x: &f32| *x as ScoreType;

            let ordering_before = float_cmp(dummy_similarity(&a), dummy_similarity(&b));

            let query_a = RecoBestScoreQuery::from(RecoQuery::new(vec![], vec![a]));
            let query_b = RecoBestScoreQuery::from(RecoQuery::new(vec![], vec![b]));

            let score_a = query_a.score_by(dummy_similarity);
            let score_b = query_b.score_by(dummy_similarity);

            let ordering_after = float_cmp(score_a, score_b);

            if ordering_before == std::cmp::Ordering::Equal {
                assert_eq!(ordering_before, ordering_after);
            } else {
                assert_ne!(ordering_before, ordering_after)
            }
        }

        /// Checks that the positive-chosen scores preserve the order of the candidates
        #[test]
        fn correct_positive_order(a in -100f32..=100f32, b in -100f32..=100f32) {
            let dummy_similarity = |x: &f32| *x as ScoreType;

            let ordering_before = float_cmp(dummy_similarity(&a), dummy_similarity(&b));

            // Too similar scores can get compressed to the same value by the sigmoid function.
            // This would make the test useless, so we skip those cases.
            prop_assume!(ordering_before != Ordering::Equal);

            let query_a = RecoBestScoreQuery::from(RecoQuery::new(vec![a], vec![]));
            let query_b = RecoBestScoreQuery::from(RecoQuery::new(vec![b], vec![]));

            let score_a = query_a.score_by(dummy_similarity);
            let score_b = query_b.score_by(dummy_similarity);

            let ordering_after = score_a.total_cmp(&score_b);

            assert_eq!(ordering_before, ordering_after);
        }

        /// Guarantees that the point that was chosen from positive is always preferred on
        /// the candidate list over a point that was chosen from negatives
        #[test]
        fn correct_positive_and_negative_order(p in -100f32..=100f32, n in -100f32..=100f32) {
            let dummy_similarity = |x: &f32| *x as ScoreType;

            let query_p = RecoBestScoreQuery::from(RecoQuery::new(vec![p], vec![]));
            let query_n = RecoBestScoreQuery::from(RecoQuery::new(vec![], vec![n]));

            let ordering = query_p.score_by(dummy_similarity).total_cmp(&query_n.score_by(dummy_similarity));

            assert_ne!(ordering, std::cmp::Ordering::Less);
        }
    }

    #[test]
    fn test_avg_vectors() {
        let vectors: Vec<VectorInternal> = vec![
            vec![1.0, 2.0, 3.0].into(),
            vec![1.0, 2.0, 3.0].into(),
            vec![1.0, 2.0, 3.0].into(),
        ];
        assert_eq!(
            avg_vectors(vectors.iter().map(VectorRef::from)).unwrap(),
            vec![1.0, 2.0, 3.0].into(),
        );

        let vectors: Vec<VectorInternal> = vec![
            SparseVector::new(vec![0, 1, 2], vec![0.0, 0.1, 0.2])
                .unwrap()
                .into(),
            SparseVector::new(vec![0, 1, 2], vec![0.0, 1.0, 2.0])
                .unwrap()
                .into(),
        ];
        assert_eq!(
            avg_vectors(vectors.iter().map(VectorRef::from)).unwrap(),
            SparseVector::new(vec![0, 1, 2], vec![0.0, 0.55, 1.1])
                .unwrap()
                .into(),
        );

        let vectors: Vec<VectorInternal> = vec![
            vec![1.0, 2.0, 3.0].into(),
            SparseVector::new(vec![0, 1, 2], vec![0.0, 0.1, 0.2])
                .unwrap()
                .into(),
        ];
        assert!(avg_vectors(vectors.iter().map(VectorRef::from)).is_err());
    }

    #[test]
    fn test_avg_vector_for_recommendation() {
        let positives: Vec<VectorInternal> = vec![vec![1.0, 0.0].into(), vec![0.0, 1.0].into()];
        let negatives: Vec<VectorInternal> = vec![vec![0.0, 1.0].into()];

        // No negatives: the plain average.
        let vector = avg_vector_for_recommendation(
            positives.iter().map(VectorRef::from),
            std::iter::empty().peekable(),
        )
        .unwrap();
        assert_eq!(vector, vec![0.5, 0.5].into());

        // With negatives: avg_pos + (avg_pos - avg_neg) = [0.5, 0.5] + ([0.5, 0.5] - [0, 1]).
        let vector = avg_vector_for_recommendation(
            positives.iter().map(VectorRef::from),
            negatives.iter().map(VectorRef::from).peekable(),
        )
        .unwrap();
        assert_eq!(vector, vec![1.0, 0.0].into());
    }

    /// Regression for qdrant/qdrant#10369: when a positive (e.g. bare
    /// vector in the main collection) and a negative (e.g. resolved
    /// from a `lookup_from` collection with a different dimension)
    /// have different dimensions, the `zip` inside
    /// `merge_positive_and_negative_avg` silently truncates the longer
    /// one. The score is then magnitude-dependent and the nearest
    /// neighbour can be wrong. Reject the mismatch explicitly so the
    /// caller sees the dimension error.
    #[test]
    fn test_merge_rejects_dimension_mismatch_dense() {
        let positive: VectorInternal = vec![1.0, 0.0].into();
        let negative: VectorInternal = vec![0.0, 1.0, 0.0, 0.0].into();
        let err = merge_positive_and_negative_avg(positive, negative).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("dimension") && msg.contains("positive") && msg.contains("negative"),
            "expected dimension-mismatch diagnostic, got: {msg}",
        );
    }

    /// Same for sparse vectors: `combine_aggregate` walks both index
    /// sets and produces a length-`union` sparse vector, which is
    /// also wrong when the source dimensions differ.
    #[test]
    fn test_merge_rejects_dimension_mismatch_sparse() {
        let positive: VectorInternal = SparseVector::new(vec![0], vec![1.0]).unwrap().into();
        let negative: VectorInternal = SparseVector::new(vec![0, 1, 2], vec![0.0, 1.0, 2.0])
            .unwrap()
            .into();
        let err = merge_positive_and_negative_avg(positive, negative).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("dimension") && msg.contains("sparse"),
            "expected dimension-mismatch diagnostic, got: {msg}",
        );
    }
}
