//! SEFR: Scalable, Efficient, and Fast classifieR.
//!
//! Keshavarz, Abadeh, Rawassizadeh (2020), "SEFR: A Fast Linear-Time Classifier
//! for Ultra-Low Power Devices", arXiv:2006.04620.
//!
//! The classifier is fit on the feedback items of a single query and produces a
//! linear decision function. Because that function is linear, and because the
//! `[0, 1]` feature scaling the algorithm expects is affine, the scaler folds
//! into the weights: scoring a candidate is one dot product against a single
//! vector. See [`SefrModel`].
//!
//! The min-max scaler is fit on the feedback items themselves, which is what
//! ties retrieval quality to how many there are. With two items every dimension
//! scales to exactly `0` or `1`, so the weights degenerate into a sign vector
//! and rank poorly; from roughly eight items on, the per-dimension weighting
//! becomes the method's advantage, especially when the embedding's dimensions
//! differ widely in scale. `lib/segment/tests/sefr_accuracy.rs` measures this.

use std::borrow::Cow;

use common::types::ScoreType;

use super::FeedbackItem;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::{DenseVector, VectorElementType, VectorInternal};
use crate::types::Distance;

/// Guard on the denominator of the weight formula (Eq. 5), keeping it defined
/// for dimensions that average to zero in both classes.
const WEIGHT_EPS: f64 = 1e-7;

/// Parameters of the `sefr` relevance feedback strategy.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct SefrParams {
    /// Feedback items scoring above this value form the positive class. Defaults
    /// to the mean of the feedback scores.
    pub threshold: Option<ScoreType>,

    /// Relative influence of the original query vector against the learned
    /// direction, which is matched in magnitude first so that a given weight
    /// means the same thing across queries. Zero uses the learned direction
    /// alone.
    ///
    /// Worth tuning: the useful value depends on how much the original query
    /// already separates the relevant results, so it ranges from around `0.01`
    /// to `1` in practice.
    pub target_weight: ScoreType,
}

impl Default for SefrParams {
    fn default() -> Self {
        Self {
            threshold: None,
            target_weight: 0.0,
        }
    }
}

/// A fitted binary SEFR model, with the min-max feature scaler folded into the
/// weights.
///
/// For a scaler with per-dimension minimum `m` and range `r`, folding rewrites
/// the decision function so that no candidate needs rescaling at scoring time:
///
/// ```text
/// w . scale(x) - bias  ==  w' . x - offset
/// w'_i   = w_i / r_i
/// offset = sum_i (w_i * m_i / r_i) + bias
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct SefrModel {
    /// Feature weights with the scaler folded in.
    pub weights: DenseVector,

    /// Constant term of the decision function. Shifting every candidate by the
    /// same amount, it does not affect ranking.
    pub offset: ScoreType,
}

impl SefrModel {
    /// Signed decision value; positive favors the positive class.
    pub fn score(&self, vector: &[VectorElementType]) -> ScoreType {
        let dot: ScoreType = self
            .weights
            .iter()
            .zip(vector)
            .map(|(weight, value)| weight * value)
            .sum();
        dot - self.offset
    }
}

/// Fit a binary SEFR classifier, scaling features to `[0, 1]` with a min-max
/// scaler fit on the training set.
pub fn fit_sefr(
    positives: &[&[VectorElementType]],
    negatives: &[&[VectorElementType]],
) -> OperationResult<SefrModel> {
    if positives.is_empty() || negatives.is_empty() {
        return Err(OperationError::validation_error(
            "SEFR requires at least one positive and one negative example",
        ));
    }

    let dim = positives[0].len();
    if dim == 0 {
        return Err(OperationError::validation_error(
            "SEFR requires non-empty vectors",
        ));
    }
    for vector in positives.iter().chain(negatives.iter()) {
        if vector.len() != dim {
            return Err(OperationError::WrongVectorDimension {
                expected_dim: dim,
                received_dim: vector.len(),
            });
        }
    }

    let mut min = vec![VectorElementType::INFINITY; dim];
    let mut max = vec![VectorElementType::NEG_INFINITY; dim];
    for vector in positives.iter().chain(negatives.iter()) {
        for (i, &value) in vector.iter().enumerate() {
            min[i] = min[i].min(value);
            max[i] = max[i].max(value);
        }
    }

    // A dimension with no spread scales to a constant 0, which in turn gives it
    // a zero weight, so a unit range is a safe stand-in for a zero one.
    let range: Vec<f64> = max
        .iter()
        .zip(&min)
        .map(|(max, min)| {
            let range = f64::from(*max) - f64::from(*min);
            if range > 0.0 { range } else { 1.0 }
        })
        .collect();

    let scaled_mean = |vectors: &[&[VectorElementType]]| -> Vec<f64> {
        let mut acc = vec![0.0f64; dim];
        for vector in vectors {
            for i in 0..dim {
                acc[i] += (f64::from(vector[i]) - f64::from(min[i])) / range[i];
            }
        }
        let count = vectors.len() as f64;
        for value in &mut acc {
            *value /= count;
        }
        acc
    };

    let avg_positive = scaled_mean(positives);
    let avg_negative = scaled_mean(negatives);

    // Eq. 5
    let weights: Vec<f64> = avg_positive
        .iter()
        .zip(&avg_negative)
        .map(|(positive, negative)| (positive - negative) / (positive + negative + WEIGHT_EPS))
        .collect();

    let scaled_score = |vector: &[VectorElementType]| -> f64 {
        (0..dim)
            .map(|i| (f64::from(vector[i]) - f64::from(min[i])) / range[i] * weights[i])
            .sum()
    };
    let mean_score = |vectors: &[&[VectorElementType]]| -> f64 {
        vectors.iter().map(|v| scaled_score(v)).sum::<f64>() / vectors.len() as f64
    };

    // Eq. 9: the threshold sits between the two class score means, each weighted
    // by the size of the opposite class.
    let positive_count = positives.len() as f64;
    let negative_count = negatives.len() as f64;
    let bias = (negative_count * mean_score(positives) + positive_count * mean_score(negatives))
        / (negative_count + positive_count);

    // Fold the scaler into the weights.
    let mut folded = DenseVector::with_capacity(dim);
    let mut offset = bias;
    for i in 0..dim {
        let weight = weights[i] / range[i];
        offset += weight * f64::from(min[i]);
        folded.push(weight as VectorElementType);
    }

    Ok(SefrModel {
        weights: folded,
        offset: offset as ScoreType,
    })
}

/// Build the query vector of the `sefr` relevance feedback strategy.
///
/// Fits SEFR on the feedback items, labelling those scoring above the threshold
/// as positive, then returns the weight vector optionally blended with the
/// original query. The result is a plain dense vector, so the search runs
/// through the regular nearest-neighbor path.
///
/// `distance` must be dot-product based, since the folded weight vector is
/// scored as a dot product against each candidate. The training vectors are
/// preprocessed the same way stored vectors are, so that the model is fit in the
/// space the candidates live in.
///
/// When the feedback lands entirely in one class there is nothing to separate,
/// and the original query vector is returned unchanged.
pub fn sefr_query_vector(
    target: &VectorInternal,
    feedback: &[FeedbackItem<VectorInternal>],
    params: &SefrParams,
    distance: Distance,
) -> OperationResult<VectorInternal> {
    match distance {
        // Cosine is dot product over normalized vectors.
        Distance::Dot | Distance::Cosine => {}
        Distance::Euclid | Distance::Manhattan => {
            return Err(OperationError::validation_error(format!(
                "`sefr` relevance feedback scores candidates with a dot product, \
                 which does not match the {distance:?} distance of this vector. \
                 Use a vector with Dot or Cosine distance."
            )));
        }
    }

    let target = as_dense(target)?;
    if feedback.is_empty() {
        return Ok(VectorInternal::from(target.to_vec()));
    }

    let threshold = match params.threshold {
        Some(threshold) => threshold,
        None => {
            let sum: f64 = feedback.iter().map(|item| f64::from(item.score.0)).sum();
            (sum / feedback.len() as f64) as ScoreType
        }
    };

    let mut training = Vec::with_capacity(feedback.len());
    for item in feedback {
        let vector = preprocess(as_dense(&item.vector)?, distance);
        training.push((vector, item.score.0 > threshold));
    }

    let positives: Vec<&[VectorElementType]> = training
        .iter()
        .filter(|(_, positive)| *positive)
        .map(|(vector, _)| vector.as_ref())
        .collect();
    let negatives: Vec<&[VectorElementType]> = training
        .iter()
        .filter(|(_, positive)| !*positive)
        .map(|(vector, _)| vector.as_ref())
        .collect();

    if positives.is_empty() || negatives.is_empty() {
        return Ok(VectorInternal::from(target.to_vec()));
    }

    let model = fit_sefr(&positives, &negatives)?;

    if model.weights.len() != target.len() {
        return Err(OperationError::WrongVectorDimension {
            expected_dim: target.len(),
            received_dim: model.weights.len(),
        });
    }

    let weights_norm = l2_norm(&model.weights);
    if params.target_weight == 0.0 || weights_norm == 0.0 {
        if weights_norm == 0.0 {
            // Nothing was learned, e.g. every dimension had no spread.
            return Ok(VectorInternal::from(target.to_vec()));
        }
        return Ok(VectorInternal::from(model.weights));
    }

    // Blending two linear scoring functions stays linear, so the result is still
    // a single vector.
    let target = preprocess(target, distance);
    let target_norm = l2_norm(target.as_ref());
    if target_norm == 0.0 {
        return Ok(VectorInternal::from(model.weights));
    }

    // Match the query's magnitude to the learned direction before mixing, so
    // that `target_weight` expresses relative influence and stays comparable
    // across queries rather than tracking the two vectors' norms.
    let scale = params.target_weight * weights_norm / target_norm;
    let blended: DenseVector = model
        .weights
        .iter()
        .zip(target.as_ref())
        .map(|(weight, target)| weight + scale * target)
        .collect();

    Ok(VectorInternal::from(blended))
}

fn l2_norm(vector: &[VectorElementType]) -> ScoreType {
    vector.iter().map(|x| x * x).sum::<ScoreType>().sqrt()
}

/// Apply the same preprocessing the vector storage does, so that the model is
/// fit over the representation the candidates are stored in.
fn preprocess(vector: &[VectorElementType], distance: Distance) -> Cow<'_, [VectorElementType]> {
    match distance {
        Distance::Cosine => {
            Cow::Owned(distance.preprocess_vector::<VectorElementType>(vector.to_vec()))
        }
        Distance::Dot | Distance::Euclid | Distance::Manhattan => Cow::Borrowed(vector),
    }
}

fn as_dense(vector: &VectorInternal) -> OperationResult<&[VectorElementType]> {
    match vector {
        VectorInternal::Dense(dense) => Ok(dense),
        VectorInternal::Sparse(_) => Err(OperationError::validation_error(
            "`sefr` relevance feedback is only supported for dense vectors, got a sparse vector",
        )),
        VectorInternal::MultiDense(_) => Err(OperationError::validation_error(
            "`sefr` relevance feedback is only supported for dense vectors, got a multi-dense vector",
        )),
    }
}

#[cfg(test)]
mod tests {
    use ordered_float::OrderedFloat;

    use super::*;

    /// Reference scoring path: scale the candidate explicitly, then apply the
    /// unfolded weights and bias.
    fn score_unfolded(
        vector: &[VectorElementType],
        weights: &[f64],
        min: &[f64],
        range: &[f64],
        bias: f64,
    ) -> f64 {
        (0..vector.len())
            .map(|i| (f64::from(vector[i]) - min[i]) / range[i] * weights[i])
            .sum::<f64>()
            - bias
    }

    #[test]
    fn separates_linearly_separable_data() {
        let positives: Vec<Vec<VectorElementType>> =
            vec![vec![0.9, 0.1], vec![0.8, 0.2], vec![1.0, 0.0]];
        let negatives: Vec<Vec<VectorElementType>> =
            vec![vec![0.1, 0.9], vec![0.2, 0.8], vec![0.0, 1.0]];

        let positive_refs: Vec<&[VectorElementType]> =
            positives.iter().map(|v| v.as_slice()).collect();
        let negative_refs: Vec<&[VectorElementType]> =
            negatives.iter().map(|v| v.as_slice()).collect();

        let model = fit_sefr(&positive_refs, &negative_refs).unwrap();

        for vector in &positives {
            assert!(
                model.score(vector) > 0.0,
                "positive {vector:?} scored {}",
                model.score(vector),
            );
        }
        for vector in &negatives {
            assert!(
                model.score(vector) <= 0.0,
                "negative {vector:?} scored {}",
                model.score(vector),
            );
        }
    }

    /// Folding the scaler into the weights must not change the decision value.
    #[test]
    fn folding_matches_explicit_scaling() {
        let positives: Vec<Vec<VectorElementType>> =
            vec![vec![4.0, -1.0, 7.5], vec![5.0, -2.0, 6.0]];
        let negatives: Vec<Vec<VectorElementType>> =
            vec![vec![-3.0, 2.0, 1.0], vec![-4.0, 3.5, 0.5]];

        let positive_refs: Vec<&[VectorElementType]> =
            positives.iter().map(|v| v.as_slice()).collect();
        let negative_refs: Vec<&[VectorElementType]> =
            negatives.iter().map(|v| v.as_slice()).collect();

        let model = fit_sefr(&positive_refs, &negative_refs).unwrap();

        // Recompute the scaler and the unfolded model from the same training set.
        let dim = 3;
        let all: Vec<&Vec<VectorElementType>> = positives.iter().chain(negatives.iter()).collect();
        let mut min = vec![f64::INFINITY; dim];
        let mut max = vec![f64::NEG_INFINITY; dim];
        for vector in &all {
            for i in 0..dim {
                min[i] = min[i].min(f64::from(vector[i]));
                max[i] = max[i].max(f64::from(vector[i]));
            }
        }
        let range: Vec<f64> = (0..dim)
            .map(|i| {
                let range = max[i] - min[i];
                if range > 0.0 { range } else { 1.0 }
            })
            .collect();

        let mean = |vectors: &[Vec<VectorElementType>]| -> Vec<f64> {
            let mut acc = vec![0.0; dim];
            for vector in vectors {
                for i in 0..dim {
                    acc[i] += (f64::from(vector[i]) - min[i]) / range[i];
                }
            }
            acc.iter().map(|v| v / vectors.len() as f64).collect()
        };
        let avg_positive = mean(&positives);
        let avg_negative = mean(&negatives);
        let weights: Vec<f64> = (0..dim)
            .map(|i| {
                (avg_positive[i] - avg_negative[i])
                    / (avg_positive[i] + avg_negative[i] + WEIGHT_EPS)
            })
            .collect();

        let scaled_score = |vector: &[VectorElementType]| -> f64 {
            (0..dim)
                .map(|i| (f64::from(vector[i]) - min[i]) / range[i] * weights[i])
                .sum()
        };
        let positive_mean =
            positives.iter().map(|v| scaled_score(v)).sum::<f64>() / positives.len() as f64;
        let negative_mean =
            negatives.iter().map(|v| scaled_score(v)).sum::<f64>() / negatives.len() as f64;
        let bias = (negatives.len() as f64 * positive_mean
            + positives.len() as f64 * negative_mean)
            / (positives.len() + negatives.len()) as f64;

        // Held-out candidates, deliberately outside the training range.
        for candidate in [
            vec![0.0, 0.0, 0.0],
            vec![10.0, -8.0, 20.0],
            vec![4.5, 1.25, 3.0],
            vec![-100.0, 50.0, -7.0],
        ] {
            let folded = f64::from(model.score(&candidate));
            let explicit = score_unfolded(&candidate, &weights, &min, &range, bias);
            assert!(
                (folded - explicit).abs() < 1e-3 * explicit.abs().max(1.0),
                "folded {folded} vs explicit {explicit} for {candidate:?}",
            );
        }
    }

    #[test]
    fn zero_range_dimension_gets_zero_weight() {
        let positives: Vec<Vec<VectorElementType>> = vec![vec![1.0, 0.5], vec![0.8, 0.5]];
        let negatives: Vec<Vec<VectorElementType>> = vec![vec![0.1, 0.5], vec![0.2, 0.5]];

        let positive_refs: Vec<&[VectorElementType]> =
            positives.iter().map(|v| v.as_slice()).collect();
        let negative_refs: Vec<&[VectorElementType]> =
            negatives.iter().map(|v| v.as_slice()).collect();

        let model = fit_sefr(&positive_refs, &negative_refs).unwrap();

        assert_eq!(model.weights[1], 0.0);
        assert!(model.weights[0] != 0.0);
        assert!(model.weights.iter().all(|w| w.is_finite()));
        assert!(model.offset.is_finite());
    }

    #[test]
    fn rejects_single_class_and_mismatched_dimensions() {
        let vectors = [vec![1.0, 2.0], vec![3.0, 4.0]];
        let refs: Vec<&[VectorElementType]> = vectors.iter().map(|v| v.as_slice()).collect();

        assert!(fit_sefr(&refs, &[]).is_err());
        assert!(fit_sefr(&[], &refs).is_err());

        let short: Vec<VectorElementType> = vec![1.0];
        assert!(fit_sefr(&refs, &[short.as_slice()]).is_err());
    }

    #[test]
    fn single_class_feedback_falls_back_to_target() {
        let target = VectorInternal::from(vec![0.25, 0.75]);
        let feedback = vec![
            FeedbackItem {
                vector: VectorInternal::from(vec![1.0, 0.0]),
                score: OrderedFloat(1.0),
            },
            FeedbackItem {
                vector: VectorInternal::from(vec![0.0, 1.0]),
                score: OrderedFloat(1.0),
            },
        ];

        let query =
            sefr_query_vector(&target, &feedback, &SefrParams::default(), Distance::Dot).unwrap();

        assert_eq!(query, target);
    }

    #[test]
    fn query_vector_ranks_positive_feedback_above_negative() {
        let target = VectorInternal::from(vec![0.5, 0.5]);
        let feedback = vec![
            FeedbackItem {
                vector: VectorInternal::from(vec![0.9, 0.1]),
                score: OrderedFloat(1.0),
            },
            FeedbackItem {
                vector: VectorInternal::from(vec![0.8, 0.2]),
                score: OrderedFloat(0.9),
            },
            FeedbackItem {
                vector: VectorInternal::from(vec![0.1, 0.9]),
                score: OrderedFloat(0.0),
            },
            FeedbackItem {
                vector: VectorInternal::from(vec![0.2, 0.8]),
                score: OrderedFloat(0.1),
            },
        ];

        let query =
            sefr_query_vector(&target, &feedback, &SefrParams::default(), Distance::Dot).unwrap();
        let VectorInternal::Dense(weights) = query else {
            panic!("expected a dense query vector");
        };

        let dot = |vector: &[VectorElementType]| -> f32 {
            weights.iter().zip(vector).map(|(w, x)| w * x).sum()
        };

        // The learned direction must score the well-rated examples higher.
        assert!(dot(&[0.9, 0.1]) > dot(&[0.1, 0.9]));
        assert!(dot(&[0.8, 0.2]) > dot(&[0.2, 0.8]));
    }

    #[test]
    fn target_weight_blends_the_original_query() {
        let target = VectorInternal::from(vec![0.0, 10.0]);
        let feedback = vec![
            FeedbackItem {
                vector: VectorInternal::from(vec![1.0, 0.0]),
                score: OrderedFloat(1.0),
            },
            FeedbackItem {
                vector: VectorInternal::from(vec![0.0, 1.0]),
                score: OrderedFloat(0.0),
            },
        ];

        let query = |target_weight| {
            let params = SefrParams {
                threshold: None,
                target_weight,
            };
            let VectorInternal::Dense(vector) =
                sefr_query_vector(&target, &feedback, &params, Distance::Dot).unwrap()
            else {
                panic!("expected a dense query vector");
            };
            vector
        };

        let pure = query(0.0);
        let blended = query(1.0);

        // The query only carries the second dimension, so only that one moves,
        // and it moves toward the query's (positive) direction.
        assert_eq!(blended[0], pure[0]);
        assert!(blended[1] > pure[1]);

        // At equal weight the query contributes as much magnitude as the
        // learned direction, independent of how the query vector is scaled.
        let l2 = |v: &[VectorElementType]| v.iter().map(|x| x * x).sum::<f32>().sqrt();
        let contribution = l2(&[blended[0] - pure[0], blended[1] - pure[1]]);
        assert!((contribution - l2(&pure)).abs() < 1e-5);
    }

    /// `target_weight` matches the query's magnitude to the learned direction,
    /// so rescaling the query must not change the blend.
    #[test]
    fn target_weight_is_insensitive_to_the_query_magnitude() {
        let feedback = vec![
            FeedbackItem {
                vector: VectorInternal::from(vec![0.9, 0.1]),
                score: OrderedFloat(1.0),
            },
            FeedbackItem {
                vector: VectorInternal::from(vec![0.1, 0.9]),
                score: OrderedFloat(0.0),
            },
        ];
        let params = SefrParams {
            threshold: None,
            target_weight: 0.5,
        };

        let blend_for = |scale: VectorElementType| {
            let target = VectorInternal::from(vec![0.25 * scale, 0.75 * scale]);
            let VectorInternal::Dense(vector) =
                sefr_query_vector(&target, &feedback, &params, Distance::Dot).unwrap()
            else {
                panic!("expected a dense query vector");
            };
            vector
        };

        let small = blend_for(1.0);
        let large = blend_for(1000.0);
        for (a, b) in small.iter().zip(&large) {
            assert!((a - b).abs() < 1e-4, "{small:?} vs {large:?}");
        }
    }

    #[test]
    fn rejects_sparse_feedback() {
        use sparse::common::sparse_vector::SparseVector;

        let target = VectorInternal::from(vec![0.5, 0.5]);
        let feedback = vec![
            FeedbackItem {
                vector: VectorInternal::Sparse(SparseVector::new(vec![0], vec![1.0]).unwrap()),
                score: OrderedFloat(1.0),
            },
            FeedbackItem {
                vector: VectorInternal::Sparse(SparseVector::new(vec![1], vec![1.0]).unwrap()),
                score: OrderedFloat(0.0),
            },
        ];

        assert!(
            sefr_query_vector(&target, &feedback, &SefrParams::default(), Distance::Dot).is_err()
        );
    }

    #[test]
    fn rejects_non_dot_product_distances() {
        let target = VectorInternal::from(vec![0.5, 0.5]);
        let feedback = vec![
            FeedbackItem {
                vector: VectorInternal::from(vec![0.9, 0.1]),
                score: OrderedFloat(1.0),
            },
            FeedbackItem {
                vector: VectorInternal::from(vec![0.1, 0.9]),
                score: OrderedFloat(0.0),
            },
        ];

        for distance in [Distance::Euclid, Distance::Manhattan] {
            assert!(
                sefr_query_vector(&target, &feedback, &SefrParams::default(), distance).is_err(),
                "{distance:?} should be rejected",
            );
        }
        for distance in [Distance::Dot, Distance::Cosine] {
            assert!(
                sefr_query_vector(&target, &feedback, &SefrParams::default(), distance).is_ok(),
                "{distance:?} should be accepted",
            );
        }
    }

    /// With cosine, candidates are stored as unit vectors, so the model has to
    /// be fit over normalized inputs too. Scaling an input must then not change
    /// the learned direction.
    #[test]
    fn cosine_fits_over_normalized_vectors() {
        let target = VectorInternal::from(vec![0.5, 0.5]);
        let scaled_feedback = |scale: VectorElementType| {
            vec![
                FeedbackItem {
                    vector: VectorInternal::from(vec![0.9 * scale, 0.1 * scale]),
                    score: OrderedFloat(1.0),
                },
                FeedbackItem {
                    vector: VectorInternal::from(vec![0.1, 0.9]),
                    score: OrderedFloat(0.0),
                },
            ]
        };

        let unscaled = sefr_query_vector(
            &target,
            &scaled_feedback(1.0),
            &SefrParams::default(),
            Distance::Cosine,
        )
        .unwrap();
        let scaled = sefr_query_vector(
            &target,
            &scaled_feedback(7.0),
            &SefrParams::default(),
            Distance::Cosine,
        )
        .unwrap();

        let (VectorInternal::Dense(unscaled), VectorInternal::Dense(scaled)) = (unscaled, scaled)
        else {
            panic!("expected dense query vectors");
        };
        for (a, b) in unscaled.iter().zip(&scaled) {
            assert!((a - b).abs() < 1e-5, "{unscaled:?} vs {scaled:?}");
        }
    }
}
