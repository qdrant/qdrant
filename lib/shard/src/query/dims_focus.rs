//! Re-scoring of nearest neighbor candidates on a subset of vector dimensions.

#[cfg(test)]
mod tests;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools as _;
use ordered_float::OrderedFloat;
use segment::common::dims_explained::DimsExplainedCalculator;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::vectors::{VectorInternal, VectorRef};
use segment::types::{Distance, Order, ScoredPoint};

use super::{DimsExplainedInternal, DimsFocusInternal};

/// Re-scores a set of points with vectors, considering only the given dimensions.
///
/// Assumes the points have the queried vector attached. Points without it are discarded.
///
/// Optionally attaches per-dimension explanations (restricted to the focused dimensions)
/// to each point.
///
/// Returns the top `limit` points, sorted by the focused score in the natural order
/// of the distance metric.
pub fn dims_focus_rescore(
    points_with_vector: impl IntoIterator<Item = ScoredPoint>,
    focus: DimsFocusInternal,
    distance: Distance,
    dims_explained: Option<DimsExplainedInternal>,
    limit: usize,
    hw_measurement_acc: HwMeasurementAcc,
) -> OperationResult<Vec<ScoredPoint>> {
    let DimsFocusInternal {
        vector,
        using,
        dims,
        candidates_limit: _,
    } = focus;

    let VectorInternal::Dense(query) = vector else {
        return Err(OperationError::validation_error(
            "Focused search on dimensions is only supported for dense vectors",
        ));
    };

    let calculator = DimsExplainedCalculator::new(query, distance);
    let hw_counter = hw_measurement_acc.get_counter_cell();

    let mut rescored: Vec<ScoredPoint> = points_with_vector
        .into_iter()
        .unique_by(|point| point.id)
        .filter_map(|mut point| {
            let Some(VectorRef::Dense(point_vector)) = point
                .vector
                .as_ref()
                .and_then(|vector| vector.get(&using))
            else {
                // Silently ignore points without this named dense vector
                return None;
            };

            point.score = calculator.score_for_dims(point_vector, &dims);
            if let Some(explain) = dims_explained {
                point.dims_explained = Some(calculator.top_contributions_for_dims(
                    point_vector,
                    &dims,
                    explain.top,
                ));
            }

            Some(point)
        })
        .collect();

    hw_counter
        .cpu_counter()
        .incr_delta(rescored.len().saturating_mul(dims.len()));

    match distance.distance_order() {
        Order::LargeBetter => {
            rescored.sort_unstable_by_key(|point| std::cmp::Reverse(OrderedFloat(point.score)));
        }
        Order::SmallBetter => {
            rescored.sort_unstable_by_key(|point| OrderedFloat(point.score));
        }
    }
    rescored.truncate(limit);

    Ok(rescored)
}
