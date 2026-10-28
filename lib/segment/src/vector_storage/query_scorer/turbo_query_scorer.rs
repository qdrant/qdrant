use common::ambient::hw::HwScale;
use common::typelevel::True;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::DenseVector;
use crate::types::Distance;
use crate::vector_storage::TurboScoring;
use crate::vector_storage::query_scorer::QueryScorer;

/// Asymmetric raw scorer for a dense TurboQuant storage ([`TurboScoring`]).
///
/// Holds the query precomputed once (rotation + SIMD encoding) and scores it
/// against the stored TurboQuant bytes, delegating the actual arithmetic and
/// the metric sign convention to the storage.
pub struct TurboQueryScorer<'a, TStorage: TurboScoring> {
    hw: HwScale,
    query: EncodedQueryTQ,
    storage: &'a TStorage,
}

impl<'a, TStorage: TurboScoring> TurboQueryScorer<'a, TStorage> {
    pub fn new(query: DenseVector, storage: &'a TStorage) -> OperationResult<Self> {
        validate_query(&query, storage.distance())?;

        // Preprocess (per distance) and precompute the query once, so the
        // Hadamard rotation runs here rather than per scored point.
        let query = storage.preprocess_query(query);

        Ok(Self {
            hw: HwScale {
                cpu: storage.quantized_vector_size(),
                vector_io_read: if storage.is_cold() {
                    storage.quantized_vector_size()
                } else {
                    0
                },
            },
            query,
            storage,
        })
    }
}

fn validate_query(query: &[f32], distance: Distance) -> OperationResult<()> {
    if let Some(component) = query.iter().find(|component| !component.is_finite()) {
        return Err(OperationError::ValidationError {
            description: format!("TurboQuant query component must be finite, got {component}"),
        });
    }

    if distance != Distance::Euclid {
        return Ok(());
    }

    let squared_norm = query
        .iter()
        .map(|&component| f64::from(component).powi(2))
        .sum::<f64>();
    if !squared_norm.is_finite() || squared_norm > f64::from(f32::MAX) {
        return Err(OperationError::ValidationError {
            description: "TurboQuant Euclidean query squared norm must not exceed f32::MAX; reduce the query magnitude".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_query;
    use crate::common::operation_error::OperationError;
    use crate::types::Distance;

    #[test]
    fn euclidean_query_norm_must_fit_in_score_type() {
        let oversized = [1.31e19_f32, 1.31e19_f32];
        let error = validate_query(&oversized, Distance::Euclid).unwrap_err();

        assert!(matches!(
            error,
            OperationError::ValidationError { description }
                if description.contains("squared norm")
        ));
        assert!(validate_query(&[1.30e19_f32, 1.30e19_f32], Distance::Euclid).is_ok());
        assert!(validate_query(&oversized, Distance::Dot).is_ok());
    }

    #[test]
    fn non_finite_query_components_are_rejected_before_norm_validation() {
        for distance in [Distance::Euclid, Distance::Dot] {
            for component in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
                let error = validate_query(&[component], distance).unwrap_err();

                assert!(matches!(
                    error,
                    OperationError::ValidationError { description }
                        if description.starts_with("TurboQuant query component must be finite, got ")
                ));
            }
        }
    }
}

impl<TStorage: TurboScoring> QueryScorer for TurboQueryScorer<'_, TStorage> {
    #[inline]
    fn score_stored_batch(
        &self,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()> {
        debug_assert_eq!(ids.len(), scores.len());

        self.hw.vector_io_read(ids.len());
        self.hw.cpu(ids.len());

        self.storage.score_query_batch(&self.query, ids, scores)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.hw.cpu(1);
        self.storage.score_internal_encoded(point_a, point_b)
    }

    type SupportsBytes = True;
    fn score_bytes(&self, _: Self::SupportsBytes, bytes: &[u8]) -> ScoreType {
        // `bytes` are an already-fetched TQ-encoded vector: one vector of CPU
        // work, no IO (the caller owns the read).
        self.hw.cpu(1);
        self.storage.score_query_bytes(&self.query, bytes)
    }
}
