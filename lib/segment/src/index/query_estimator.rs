//! Contains functions for estimating of how many points should be processed for a given filter query
//!
//! Filter query is used e.g. for determining how would be faster to process the query:
//! - use vector index or payload index first

use std::cmp::{max, min};

use itertools::Itertools;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PrimaryCondition};
use crate::types::{Condition, Filter, MinShould};

/// Re-estimate cardinality based on number of available vectors
/// Assuming that deleted vectors are not correlated with the filter
///
/// # Arguments
///
/// * `estimation` - cardinality estimations of number of points selected by payload filter
/// * `available_vectors` - number of available vectors for the named vector storage
/// * `total_vectors` - total number of points in the segment
///
/// # Result
///
/// * `CardinalityEstimation` - new cardinality estimation
pub fn adjust_to_available_vectors(
    estimation: CardinalityEstimation,
    available_vectors: usize,
    available_points: usize,
) -> CardinalityEstimation {
    if available_points == 0 || available_vectors == 0 {
        return CardinalityEstimation {
            primary_clauses: estimation.primary_clauses,
            min: 0,
            exp: 0,
            max: 0,
        };
    }

    let number_of_deleted_vectors = available_points.saturating_sub(available_vectors);

    // It is possible, all deleted vectors are selected in worst case
    let min = estimation.min.saturating_sub(number_of_deleted_vectors);
    // Another extreme case - all deleted vectors are not selected
    let max = estimation.max.min(available_vectors).min(available_points);

    let availability_prob = (available_vectors as f64 / available_points as f64).min(1.0);

    let exp = (estimation.exp as f64 * availability_prob).round() as usize;

    debug_assert!(
        min <= exp,
        "estimation: {estimation:?}, available_vectors: {available_vectors}, available_points: {available_points}, min: {min}, exp: {exp}"
    );
    debug_assert!(
        exp <= max,
        "estimation: {estimation:?}, available_vectors: {available_vectors}, available_points: {available_points}, exp: {exp}, max: {max}"
    );

    CardinalityEstimation {
        primary_clauses: estimation.primary_clauses,
        min,
        exp,
        max,
    }
}

/// Re-estimate cardinality based on deferred points. Assuming that deferred points are not correlated with the filter
pub fn adjust_for_deferred_points(
    estimation: CardinalityEstimation,
    visible_points: usize,
    total_points: usize,
) -> CardinalityEstimation {
    if visible_points == 0 || total_points == 0 {
        return CardinalityEstimation {
            primary_clauses: estimation.primary_clauses,
            min: 0,
            exp: 0,
            max: 0,
        };
    }

    let number_of_deferred_points = total_points.saturating_sub(visible_points);

    // It is possible, all deferred points are selected in worst case
    let min = estimation.min.saturating_sub(number_of_deferred_points);
    // Another extreme case - all deferred points are not selected
    let max = estimation.max.min(visible_points).min(total_points);

    let availability_prob = (visible_points as f64 / total_points as f64).min(1.0);

    let exp = (estimation.exp as f64 * availability_prob).round() as usize;

    debug_assert!(
        min <= exp,
        "estimation: {estimation:?}, visible_points: {visible_points}, total_points: {total_points}, min: {min}, exp: {exp}"
    );
    debug_assert!(
        exp <= max,
        "estimation: {estimation:?}, visible_points: {visible_points}, total_points: {total_points}, exp: {exp}, max: {max}"
    );

    CardinalityEstimation {
        primary_clauses: estimation.primary_clauses,
        min,
        exp,
        max,
    }
}

/// Combine cardinality of multiple estimations in an OR fashion by using the complement rule.
/// Assumes that the estimations are independent.
///
/// Formula is  `(1 - ∏(1-pᵢ)) * total`:
/// * For each condition, it calculates the probability that an item does not match it: `1 - (x / total)`.
/// * It multiplies these probabilities to get the probability that an item matches none of the conditions.
/// * Subtracts this from 1 to get the probability that an item matches at least one condition.
/// * Multiplies this probability by the total number of items and rounds to get the expected count.
pub fn expected_should_estimation(estimations: impl Iterator<Item = usize>, total: usize) -> usize {
    if total == 0 {
        return 0;
    }

    let element_not_hit_prob: f64 = estimations
        .map(|x| 1.0 - (x as f64 / total as f64))
        .product();

    let element_hit_prob = 1.0 - element_not_hit_prob;

    (element_hit_prob * (total as f64)).round() as usize
}

pub fn combine_should_estimations(
    estimations: &[CardinalityEstimation],
    total: usize,
) -> CardinalityEstimation {
    let mut clauses: Vec<PrimaryCondition> = vec![];
    for estimation in estimations {
        if estimation.primary_clauses.is_empty() {
            // If some branch is un-indexed - we can't make
            // any assumptions about the whole `should` clause
            clauses = vec![];
            break;
        }
        clauses.append(&mut estimation.primary_clauses.clone());
    }
    let expected_count = expected_should_estimation(estimations.iter().map(|x| x.exp), total);
    CardinalityEstimation {
        primary_clauses: clauses,
        min: estimations.iter().map(|x| x.min).max().unwrap_or(0),
        exp: expected_count,
        max: min(estimations.iter().map(|x| x.max).sum(), total),
    }
}

/// Estimate cardinality for `min_should` (at least `min_count` conditions).
///
/// Returns zero immediately when `min_count` exceeds the number of
/// estimations, which matches filter semantics and avoids generating
/// impossible combinations.
pub fn combine_min_should_estimations(
    estimations: &[CardinalityEstimation],
    min_count: usize,
    total: usize,
) -> CardinalityEstimation {
    // `min_count` larger than the number of conditions can never be satisfied.
    if min_count > estimations.len() {
        return CardinalityEstimation::exact(0);
    }

    // The exact estimate below enumerates every `C(N, min_count)` combination of
    // conditions. That count grows combinatorially, so a single `min_should` with
    // a few dozen conditions and a mid-range `min_count` would request a
    // pathologically large allocation (issue #7974). When there are too many
    // combinations, approximate instead of enumerating them.
    if !combinations_at_most(estimations.len(), min_count, MIN_SHOULD_MAX_COMBINATIONS) {
        return approximate_min_should_estimations(estimations, total);
    }

    /*
    | First estimate cardinality of intersections and then combine the estimations
    | ex) min_count : 2, # of estimations : 4
    | |(A ⋂ B) ∪ (A ⋂ C) ∪ (A ⋂ D) ∪ (B ⋂ C) ∪ (B ⋂ D) ∪ (C ⋂ D)|
     */
    let intersection_estimations = estimations
        .iter()
        .combinations(min_count)
        .map(|intersection| {
            combine_must_estimations(&intersection.into_iter().cloned().collect_vec(), total)
        })
        .collect_vec();

    combine_should_estimations(&intersection_estimations, total)
}

/// Maximum number of `min_should` condition combinations to enumerate exactly
/// before falling back to an approximation (see `combine_min_should_estimations`).
const MIN_SHOULD_MAX_COMBINATIONS: u128 = 100_000;

/// Returns `true` if `C(n, k) <= limit`, computed without overflowing.
///
/// The binomial coefficient is built up incrementally
/// (`C(n, i + 1) = C(n, i) * (n - i) / (i + 1)`), which stays exact at every
/// step, and short-circuits as soon as the running value exceeds `limit`.
fn combinations_at_most(n: usize, k: usize, limit: u128) -> bool {
    if k > n {
        // C(n, k) == 0, which is within any limit.
        return true;
    }
    // C(n, k) == C(n, n - k); iterate over the smaller exponent.
    let k = k.min(n - k);
    let mut combinations: u128 = 1;
    for i in 0..k as u128 {
        combinations = combinations.saturating_mul(n as u128 - i) / (i + 1);
        if combinations > limit {
            return false;
        }
    }
    true
}

/// Conservative cardinality approximation for `min_should`, used when enumerating
/// every combination would be too expensive (see `MIN_SHOULD_MAX_COMBINATIONS`).
///
/// "At least `min_count` of N conditions" is a subset of "at least one of N" (the
/// union), so the union is a valid upper bound for `exp`/`max`. No non-zero lower
/// bound is claimed: requiring several matches can yield fewer points than any
/// single condition, so `min` is `0`.
fn approximate_min_should_estimations(
    estimations: &[CardinalityEstimation],
    total: usize,
) -> CardinalityEstimation {
    let union = combine_should_estimations(estimations, total);
    CardinalityEstimation {
        primary_clauses: union.primary_clauses,
        min: 0,
        exp: union.exp,
        max: union.max,
    }
}

pub fn combine_must_estimations(
    estimations: &[CardinalityEstimation],
    total: usize,
) -> CardinalityEstimation {
    let min_estimation = estimations
        .iter()
        .map(|x| x.min)
        .fold(total as i64, |acc, x| {
            max(0, acc + (x as i64) - (total as i64))
        }) as usize;

    let max_estimation = estimations.iter().map(|x| x.max).min().unwrap_or(total);

    let exp_estimation_prob: f64 = estimations
        .iter()
        .map(|x| (x.exp as f64) / (total as f64))
        .product();

    let exp_estimation = (exp_estimation_prob * (total as f64)).round() as usize;

    let clauses = estimations
        .iter()
        .filter(|x| !x.primary_clauses.is_empty())
        .min_by_key(|x| x.exp)
        .map(|x| x.primary_clauses.clone())
        .unwrap_or_default();

    CardinalityEstimation {
        primary_clauses: clauses,
        min: min_estimation,
        exp: exp_estimation,
        max: max_estimation,
    }
}

fn estimate_condition<F>(
    estimator: &F,
    condition: &Condition,
    total: usize,
) -> OperationResult<CardinalityEstimation>
where
    F: Fn(&Condition) -> OperationResult<CardinalityEstimation>,
{
    match condition {
        Condition::Filter(filter) => estimate_filter(estimator, filter, total),
        Condition::Field(_)
        | Condition::IsEmpty(_)
        | Condition::IsNull(_)
        | Condition::HasId(_)
        | Condition::HasVector(_)
        | Condition::Nested(_)
        | Condition::CustomIdChecker(_) => estimator(condition),
    }
}

pub fn estimate_filter<F>(
    estimator: &F,
    filter: &Filter,
    total: usize,
) -> OperationResult<CardinalityEstimation>
where
    F: Fn(&Condition) -> OperationResult<CardinalityEstimation>,
{
    let mut filter_estimations: Vec<CardinalityEstimation> = vec![];

    match &filter.must {
        Some(conditions) if !conditions.is_empty() => {
            filter_estimations.push(estimate_must(estimator, conditions, total)?);
        }
        Some(_) | None => {}
    }
    match &filter.should {
        Some(conditions) if !conditions.is_empty() => {
            filter_estimations.push(estimate_should(estimator, conditions, total)?);
        }
        Some(_) | None => {}
    }
    if let Some(MinShould {
        conditions,
        min_count,
    }) = &filter.min_should
    {
        filter_estimations.push(estimate_min_should(
            estimator, conditions, *min_count, total,
        )?)
    }
    match &filter.must_not {
        Some(conditions) if !conditions.is_empty() => {
            filter_estimations.push(estimate_must_not(estimator, conditions, total)?)
        }
        Some(_) | None => {}
    }

    Ok(combine_must_estimations(&filter_estimations, total))
}

fn estimate_should<F>(
    estimator: &F,
    conditions: &[Condition],
    total: usize,
) -> OperationResult<CardinalityEstimation>
where
    F: Fn(&Condition) -> OperationResult<CardinalityEstimation>,
{
    let estimate = |x| estimate_condition(estimator, x, total);
    let should_estimations: OperationResult<Vec<_>> = conditions.iter().map(estimate).collect();
    Ok(combine_should_estimations(&should_estimations?, total))
}

fn estimate_min_should<F>(
    estimator: &F,
    conditions: &[Condition],
    min_count: usize,
    total: usize,
) -> OperationResult<CardinalityEstimation>
where
    F: Fn(&Condition) -> OperationResult<CardinalityEstimation>,
{
    let estimate = |x| estimate_condition(estimator, x, total);
    let min_should_estimations: OperationResult<Vec<_>> = conditions.iter().map(estimate).collect();
    Ok(combine_min_should_estimations(
        &min_should_estimations?,
        min_count,
        total,
    ))
}

fn estimate_must<F>(
    estimator: &F,
    conditions: &[Condition],
    total: usize,
) -> OperationResult<CardinalityEstimation>
where
    F: Fn(&Condition) -> OperationResult<CardinalityEstimation>,
{
    let estimate = |x| estimate_condition(estimator, x, total);
    let must_estimations: OperationResult<Vec<_>> = conditions.iter().map(estimate).collect();
    Ok(combine_must_estimations(&must_estimations?, total))
}

pub fn invert_estimation(
    estimation: &CardinalityEstimation,
    total: usize,
) -> CardinalityEstimation {
    CardinalityEstimation {
        primary_clauses: vec![],
        min: total.saturating_sub(estimation.max),
        exp: total.saturating_sub(estimation.exp),
        max: total.saturating_sub(estimation.min),
    }
}

fn estimate_must_not<F>(
    estimator: &F,
    conditions: &[Condition],
    total: usize,
) -> OperationResult<CardinalityEstimation>
where
    F: Fn(&Condition) -> OperationResult<CardinalityEstimation>,
{
    let estimate = |x| -> OperationResult<_> {
        let estimation = estimate_condition(estimator, x, total)?;
        Ok(invert_estimation(&estimation, total))
    };
    let must_not_estimations: OperationResult<Vec<_>> = conditions.iter().map(estimate).collect();
    Ok(combine_must_estimations(&must_not_estimations?, total))
}

#[cfg(test)]
mod tests {
    #![expect(clippy::wildcard_enum_match_arm, reason = "test code")]

    use super::*;
    use crate::index::field_index::ResolvedHasId;
    use crate::json_path::JsonPath;
    use crate::types::{FieldCondition, HasIdCondition};

    const TOTAL: usize = 1000;

    fn test_condition(key: &str) -> Condition {
        Condition::Field(FieldCondition {
            key: JsonPath::new(key),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            values_count: None,
            is_empty: None,
            geo_polygon: None,
            is_null: None,
        })
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "estimate_filter expects an OperationResult"
    )]
    fn test_estimator(condition: &Condition) -> OperationResult<CardinalityEstimation> {
        Ok(match condition {
            Condition::Filter(_) => panic!("unexpected Filter"),
            Condition::Nested(_) => panic!("unexpected Nested"),
            Condition::CustomIdChecker(_) => panic!("unexpected CustomIdChecker"),
            Condition::Field(field) => match field.key.to_string().as_str() {
                "color" => CardinalityEstimation {
                    primary_clauses: vec![PrimaryCondition::Condition(Box::new(field.clone()))],
                    min: 100,
                    exp: 200,
                    max: 300,
                },
                "size" => CardinalityEstimation {
                    primary_clauses: vec![PrimaryCondition::Condition(Box::new(field.clone()))],
                    min: 100,
                    exp: 100,
                    max: 100,
                },
                "price" => CardinalityEstimation {
                    primary_clauses: vec![PrimaryCondition::Condition(Box::new(field.clone()))],
                    min: 10,
                    exp: 15,
                    max: 20,
                },
                _ => CardinalityEstimation::unknown(TOTAL),
            },
            Condition::HasId(has_id) => CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Ids(ResolvedHasId {
                    point_ids: has_id.has_id.clone(),
                    resolved_point_offsets: has_id
                        .has_id
                        .iter()
                        .map(|id| id.to_string().parse().unwrap())
                        .collect(),
                })],
                min: has_id.has_id.len(),
                exp: has_id.has_id.len(),
                max: has_id.has_id.len(),
            },
            Condition::IsEmpty(condition) => CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(Box::new(
                    FieldCondition::new_is_empty(condition.is_empty.key.clone(), true),
                ))],
                min: 0,
                exp: TOTAL / 2,
                max: TOTAL,
            },
            Condition::IsNull(condition) => CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(Box::new(
                    FieldCondition::new_is_null(condition.is_null.key.clone(), true),
                ))],
                min: 0,
                exp: TOTAL / 2,
                max: TOTAL,
            },
            Condition::HasVector(condition) => CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::HasVector(condition.has_vector.clone())],
                min: 0,
                exp: TOTAL / 2,
                max: TOTAL,
            },
        })
    }

    #[test]
    fn simple_query_estimation_test() {
        let query = Filter::new_must(test_condition("color"));
        let estimation = estimate_filter(&test_estimator, &query, TOTAL).unwrap();
        assert_eq!(estimation.exp, 200);
        assert!(!estimation.primary_clauses.is_empty());
    }

    #[test]
    fn must_estimation_query_test() {
        let query = Filter {
            should: None,
            min_should: None,
            must: Some(vec![
                test_condition("color"),
                test_condition("size"),
                test_condition("un-indexed"),
            ]),
            must_not: None,
        };

        let estimation = estimate_filter(&test_estimator, &query, TOTAL).unwrap();
        assert_eq!(estimation.primary_clauses.len(), 1);
        match &estimation.primary_clauses[0] {
            PrimaryCondition::Condition(field) => assert_eq!(&field.key.to_string(), "size"),
            _ => panic!(),
        }
        assert!(estimation.max <= TOTAL);
        assert!(estimation.exp <= estimation.max);
        assert!(estimation.min <= estimation.exp);
    }

    #[test]
    fn should_estimation_query_test() {
        let query = Filter {
            should: Some(vec![test_condition("color"), test_condition("size")]),
            min_should: None,
            must: None,
            must_not: None,
        };

        let estimation = estimate_filter(&test_estimator, &query, TOTAL).unwrap();
        assert_eq!(estimation.primary_clauses.len(), 2);
        assert!(estimation.max <= TOTAL);
        assert!(estimation.exp <= estimation.max);
        assert!(estimation.min <= estimation.exp);
    }

    #[test]
    fn another_should_estimation_query_test() {
        let query = Filter {
            should: Some(vec![
                test_condition("color"),
                test_condition("size"),
                test_condition("un-indexed"),
            ]),
            min_should: None,
            must: None,
            must_not: None,
        };

        let estimation = estimate_filter(&test_estimator, &query, TOTAL).unwrap();
        assert_eq!(estimation.primary_clauses.len(), 0);
        eprintln!("estimation = {estimation:#?}");
        assert!(estimation.max <= TOTAL);
        assert!(estimation.exp <= estimation.max);
        assert!(estimation.min <= estimation.exp);
    }

    #[test]
    fn min_should_estimation_query_test() {
        let query = Filter::new_min_should(MinShould {
            conditions: vec![test_condition("color"), test_condition("size")],
            min_count: 1,
        });
        let estimation = estimate_filter(&test_estimator, &query, TOTAL).unwrap();
        assert_eq!(estimation.primary_clauses.len(), 2);
        assert!(estimation.max <= TOTAL);
        assert!(estimation.exp <= estimation.max);
        assert!(estimation.min <= estimation.exp);
    }

    #[test]
    fn another_min_should_estimation_query_test() {
        let query = Filter::new_min_should(MinShould {
            conditions: vec![
                test_condition("color"),
                test_condition("size"),
                test_condition("price"),
            ],
            min_count: 2,
        });

        let estimation = estimate_filter(&test_estimator, &query, TOTAL).unwrap();
        assert_eq!(estimation.primary_clauses.len(), 3);
        assert!(estimation.max <= TOTAL);
        assert!(estimation.exp <= estimation.max);
        assert!(estimation.min <= estimation.exp);
    }

    #[test]
    fn combine_min_should_min_count_above_len_returns_exact_zero() {
        let total = 1_000usize;
        let estimations = vec![
            CardinalityEstimation::exact(10),
            CardinalityEstimation::exact(20),
        ];

        let estimation = combine_min_should_estimations(&estimations, estimations.len() + 1, total);
        assert_eq!(estimation, CardinalityEstimation::exact(0));
    }

    #[test]
    fn min_should_large_combination_count_is_approximated() {
        // Regression for #7974: C(64, 32) is astronomically large. Enumerating it
        // used to overflow `Vec`'s capacity and panic; it must now be approximated.
        let total = 1_000usize;
        let estimations: Vec<CardinalityEstimation> =
            (0..64).map(|_| CardinalityEstimation::exact(1)).collect();

        let estimation = combine_min_should_estimations(&estimations, 32, total);

        // Conservative approximation: no non-zero lower bound, union as upper bound.
        assert_eq!(estimation.min, 0);
        assert_eq!(estimation.max, estimations.len()); // min(sum of maxes, total)
        assert!(estimation.exp <= estimation.max);
    }

    #[test]
    fn combinations_at_most_is_overflow_safe_and_exact() {
        // Small, exact cases.
        assert!(combinations_at_most(5, 2, 100)); // C(5, 2) = 10
        assert!(combinations_at_most(10, 5, 252)); // C(10, 5) = 252
        assert!(!combinations_at_most(10, 5, 251)); // just below
        // Degenerate exponents.
        assert!(combinations_at_most(64, 0, 1)); // C(n, 0) = 1
        assert!(combinations_at_most(64, 64, 1)); // C(n, n) = 1
        assert!(combinations_at_most(5, 6, 0)); // C(n, k) = 0 when k > n
        assert!(combinations_at_most(0, 1, 0)); // also safe for n = 0
        // Astronomically large counts must not overflow and must report "too big".
        assert!(!combinations_at_most(60, 20, MIN_SHOULD_MAX_COMBINATIONS)); // ~4.2e15
        assert!(!combinations_at_most(64, 32, MIN_SHOULD_MAX_COMBINATIONS)); // ~1.8e18
    }

    #[test]
    fn min_should_large_combination_count_approximation_keeps_primary_clauses_consistent() {
        // Same explosive count as the regression above, but with INDEXED conditions
        // (non-empty primary_clauses). This locks the approximation onto the path
        // the exact estimator would otherwise take: it must return the union's
        // primary clauses (covering every condition, exactly as the exact path does
        // in `another_min_should_estimation_query_test`) and a zero lower bound, so
        // downstream post-filtering behaves the same as before this change.
        let total = 1_000usize;
        let estimations: Vec<CardinalityEstimation> = (0..64)
            .map(|i| {
                let Condition::Field(field) = test_condition(&format!("field_{i}")) else {
                    unreachable!()
                };
                CardinalityEstimation::exact(1)
                    .with_primary_clause(PrimaryCondition::Condition(Box::new(field)))
            })
            .collect();

        let estimation = combine_min_should_estimations(&estimations, 32, total);

        assert_eq!(estimation.min, 0);
        assert!(estimation.max <= total);
        assert!(estimation.exp <= estimation.max);
        // Union clauses cover every condition, consistent with the exact path.
        assert_eq!(estimation.primary_clauses.len(), estimations.len());
    }

    /// Exact `min_should` estimation by brute-force enumeration (mirrors the
    /// path used below the threshold), to differentially check the approximation.
    fn combine_min_should_estimations_exact(
        estimations: &[CardinalityEstimation],
        min_count: usize,
        total: usize,
    ) -> CardinalityEstimation {
        if min_count > estimations.len() {
            return CardinalityEstimation::exact(0);
        }
        let intersection_estimations = estimations
            .iter()
            .combinations(min_count)
            .map(|intersection| {
                combine_must_estimations(&intersection.into_iter().cloned().collect_vec(), total)
            })
            .collect_vec();
        combine_should_estimations(&intersection_estimations, total)
    }

    fn assert_envelope(approx: &CardinalityEstimation, exact: &CardinalityEstimation) {
        // Note: `approx.max >= exact.max` does NOT hold in general (the union is not
        // an upper bound on the exact min_should `max`, e.g. n=4/k=2: 3800 > 3200),
        // so it is intentionally not asserted. The properties that do hold:
        assert!(
            approx.exp >= exact.exp,
            "exp {} < {}",
            approx.exp,
            exact.exp
        );
        assert!(
            approx.min <= exact.min,
            "min {} > {}",
            approx.min,
            exact.min
        );
        assert!(
            exact.min <= exact.exp && exact.exp <= exact.max,
            "exact inconsistent"
        );
        assert!(
            approx.min <= approx.exp && approx.exp <= approx.max,
            "approx inconsistent"
        );
    }

    fn est(min: usize, exp: usize, max: usize) -> CardinalityEstimation {
        CardinalityEstimation {
            primary_clauses: vec![],
            min,
            exp,
            max,
        }
    }

    #[test]
    fn min_should_approx_envelopes_exact_across_shapes() {
        let total = 5_000;
        let cases: Vec<(Vec<CardinalityEstimation>, usize)> = vec![
            (
                vec![est(0, 100, 100), est(0, 100, 100), est(0, 100, 100)],
                2,
            ), // disjoint
            (
                vec![est(0, 400, 500), est(0, 350, 500), est(0, 300, 500)],
                2,
            ), // overlapping
            (
                vec![est(50, 100, 150), est(50, 100, 150), est(50, 100, 150)],
                2,
            ), // identical
            (
                vec![
                    CardinalityEstimation::exact(0),
                    CardinalityEstimation::exact(100),
                    CardinalityEstimation::exact(50),
                ],
                2,
            ),
            (vec![est(10, 100, 200), est(20, 150, 300)], 1), // min_count=1
            (vec![est(10, 100, 200), est(20, 150, 300)], 2), // min_count=N
            (vec![est(50, 200, 300)], 1),                    // single
            (
                vec![
                    est(100, 500, 1000),
                    est(50, 200, 5000),
                    est(0, 100, 5000),
                    est(200, 1000, 2000),
                ],
                3,
            ),
            (
                vec![est(0, 900, 1000), est(0, 950, 1000), est(0, 800, 1000)],
                2,
            ), // near total
            (
                vec![
                    CardinalityEstimation::exact(0),
                    CardinalityEstimation::exact(0),
                    est(100, 500, 700),
                ],
                2,
            ),
        ];
        for (estimations, min_count) in cases {
            let exact = combine_min_should_estimations_exact(&estimations, min_count, total);
            let approx = approximate_min_should_estimations(&estimations, total);
            assert_envelope(&approx, &exact);
            assert!(approx.max <= total);
            assert_eq!(approx.min, 0);
        }
    }

    #[test]
    fn min_should_approx_envelopes_exact_grid() {
        let total = 5_000;
        for (n, min_count) in [(4usize, 2usize), (6, 3), (8, 4), (10, 5), (12, 6), (15, 7)] {
            let estimations: Vec<CardinalityEstimation> = (0..n)
                .map(|i| {
                    est(
                        (i * 50).min(200),
                        (i * 100 + 200).min(total),
                        (i * 200 + 500).min(total),
                    )
                })
                .collect();
            let exact = combine_min_should_estimations_exact(&estimations, min_count, total);
            let approx = approximate_min_should_estimations(&estimations, total);
            assert_envelope(&approx, &exact);
        }
    }

    #[test]
    fn combinations_threshold_boundary_matches_design() {
        assert!(!combinations_at_most(25, 12, MIN_SHOULD_MAX_COMBINATIONS)); // ~5.2M
        assert!(!combinations_at_most(20, 10, MIN_SHOULD_MAX_COMBINATIONS)); // ~184k
        assert!(combinations_at_most(15, 7, MIN_SHOULD_MAX_COMBINATIONS)); // ~6.4k
        assert!(combinations_at_most(10, 5, MIN_SHOULD_MAX_COMBINATIONS)); // 252
    }

    #[test]
    fn min_should_with_min_count_same_as_condition_count_is_equivalent_to_must() {
        let conditions = vec![
            test_condition("color"),
            test_condition("size"),
            test_condition("price"),
        ];
        let min_should_query = Filter::new_min_should(MinShould {
            conditions: conditions.clone(),
            min_count: 3,
        });

        let estimation = estimate_filter(&test_estimator, &min_should_query, TOTAL).unwrap();

        let must_query = Filter {
            should: None,
            min_should: None,
            must: Some(conditions),
            must_not: None,
        };

        let expected_estimation = estimate_filter(&test_estimator, &must_query, TOTAL).unwrap();

        assert_eq!(
            estimation.primary_clauses,
            expected_estimation.primary_clauses
        );
        assert_eq!(estimation.max, expected_estimation.max);
        assert_eq!(estimation.exp, expected_estimation.exp);
        assert_eq!(estimation.min, expected_estimation.min);
    }

    #[test]
    fn complex_estimation_query_test() {
        let query = Filter {
            should: Some(vec![
                Condition::Filter(Filter {
                    should: None,
                    min_should: None,
                    must: Some(vec![test_condition("color"), test_condition("size")]),
                    must_not: None,
                }),
                Condition::Filter(Filter {
                    should: None,
                    min_should: None,
                    must: Some(vec![test_condition("price"), test_condition("size")]),
                    must_not: None,
                }),
            ]),
            min_should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(HasIdCondition {
                has_id: [1, 2, 3, 4, 5].into_iter().map(u64::into).collect(),
            })]),
        };

        let estimation = estimate_filter(&test_estimator, &query, TOTAL).unwrap();
        assert_eq!(estimation.primary_clauses.len(), 2);
        assert!(estimation.max <= TOTAL);
        assert!(estimation.exp <= estimation.max);
        assert!(estimation.min <= estimation.exp);
    }

    #[test]
    fn another_complex_estimation_query_test() {
        let query = Filter {
            should: None,
            min_should: None,
            must: Some(vec![
                Condition::Filter(Filter {
                    must: None,
                    should: Some(vec![test_condition("color"), test_condition("size")]),
                    min_should: None,
                    must_not: None,
                }),
                Condition::Filter(Filter {
                    must: None,
                    should: Some(vec![test_condition("price"), test_condition("size")]),
                    min_should: None,
                    must_not: None,
                }),
            ]),
            must_not: Some(vec![Condition::HasId(HasIdCondition {
                has_id: [1, 2, 3, 4, 5].into_iter().map(u64::into).collect(),
            })]),
        };

        let estimation = estimate_filter(&test_estimator, &query, TOTAL).unwrap();
        assert_eq!(estimation.primary_clauses.len(), 2);
        estimation.primary_clauses.iter().for_each(|x| match x {
            PrimaryCondition::Condition(field) => {
                assert!(["price", "size"].contains(&field.key.to_string().as_str()))
            }
            _ => panic!("Should not go here"),
        });
        assert!(estimation.max <= TOTAL);
        assert!(estimation.exp <= estimation.max);
        assert!(estimation.min <= estimation.exp);
    }

    #[test]
    fn test_combine_must_estimations() {
        let estimations = vec![CardinalityEstimation {
            primary_clauses: vec![],
            min: 12,
            exp: 12,
            max: 12,
        }];

        let res = combine_must_estimations(&estimations, 10_000);
        eprintln!("res = {res:#?}");
    }

    #[test]
    fn test_adjust_to_available_vectors() {
        let estimation = CardinalityEstimation {
            primary_clauses: vec![],
            min: 0,
            exp: 64,
            max: 100,
        };

        let new_estimation = adjust_to_available_vectors(estimation, 50, 200);

        assert_eq!(new_estimation.min, 0);
        assert_eq!(new_estimation.exp, 16);
        assert_eq!(new_estimation.max, 50);
    }
}
