//! Contains functions for estimating of how many points should be processed for a given filter query
//!
//! Filter query is used e.g. for determining how would be faster to process the query:
//! - use vector index or payload index first

use std::cmp::{max, min};

use itertools::Itertools;

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
        "estimation: {:?}, available_vectors: {}, available_points: {}, min: {}, exp: {}",
        estimation,
        available_vectors,
        available_points,
        min,
        exp
    );
    debug_assert!(
        exp <= max,
        "estimation: {:?}, available_vectors: {}, available_points: {}, exp: {}, max: {}",
        estimation,
        available_vectors,
        available_points,
        exp,
        max
    );

    CardinalityEstimation {
        primary_clauses: estimation.primary_clauses,
        min,
        exp,
        max,
    }
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
    let element_not_hit_prob: f64 = estimations
        .iter()
        .map(|x| (total - x.exp) as f64 / (total as f64))
        .product();
    let element_hit_prob = 1.0 - element_not_hit_prob;
    let expected_count = (element_hit_prob * (total as f64)).round() as usize;
    CardinalityEstimation {
        primary_clauses: clauses,
        min: estimations.iter().map(|x| x.min).max().unwrap_or(0),
        exp: expected_count,
        max: min(estimations.iter().map(|x| x.max).sum(), total),
    }
}

pub fn combine_min_should_estimations(
    estimations: &[CardinalityEstimation],
    min_count: usize,
    total: usize,
) -> CardinalityEstimation {
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
) -> CardinalityEstimation
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    match condition {
        Condition::Filter(filter) => estimate_filter(estimator, filter, total),
        _ => estimator(condition),
    }
}

pub fn estimate_filter<F>(estimator: &F, filter: &Filter, total: usize) -> CardinalityEstimation
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let mut filter_estimations: Vec<CardinalityEstimation> = vec![];

    match &filter.must {
        None => {}
        Some(conditions) => {
            if !conditions.is_empty() {
                filter_estimations.push(estimate_must(estimator, conditions, total));
            }
        }
    }
    match &filter.should {
        None => {}
        Some(conditions) => {
            if !conditions.is_empty() {
                filter_estimations.push(estimate_should(estimator, conditions, total));
            }
        }
    }
    match &filter.min_should {
        None => {}
        Some(MinShould {
            conditions,
            min_count,
        }) => filter_estimations.push(estimate_min_should(
            estimator, conditions, *min_count, total,
        )),
    }
    match &filter.must_not {
        None => {}
        Some(conditions) => {
            if !conditions.is_empty() {
                filter_estimations.push(estimate_must_not(estimator, conditions, total))
            }
        }
    }

    combine_must_estimations(&filter_estimations, total)
}

fn estimate_should<F>(
    estimator: &F,
    conditions: &[Condition],
    total: usize,
) -> CardinalityEstimation
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let estimate = |x| estimate_condition(estimator, x, total);
    let should_estimations = conditions.iter().map(estimate).collect_vec();
    combine_should_estimations(&should_estimations, total)
}

fn estimate_min_should<F>(
    estimator: &F,
    conditions: &[Condition],
    min_count: usize,
    total: usize,
) -> CardinalityEstimation
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let estimate = |x| estimate_condition(estimator, x, total);
    let min_should_estimations = conditions.iter().map(estimate).collect_vec();
    combine_min_should_estimations(&min_should_estimations, min_count, total)
}

fn estimate_must<F>(estimator: &F, conditions: &[Condition], total: usize) -> CardinalityEstimation
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let estimate = |x| estimate_condition(estimator, x, total);
    let must_estimations = conditions.iter().map(estimate).collect_vec();

    combine_must_estimations(&must_estimations, total)
}

pub fn invert_estimation(
    estimation: &CardinalityEstimation,
    total: usize,
) -> CardinalityEstimation {
    CardinalityEstimation {
        primary_clauses: vec![],
        min: total - estimation.max,
        exp: total - estimation.exp,
        max: total - estimation.min,
    }
}

fn estimate_must_not<F>(
    estimator: &F,
    conditions: &[Condition],
    total: usize,
) -> CardinalityEstimation
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let estimate = |x| invert_estimation(&estimate_condition(estimator, x, total), total);
    let must_not_estimations = conditions.iter().map(estimate).collect_vec();
    combine_must_estimations(&must_not_estimations, total)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use super::*;
    use crate::json_path::path;
    use crate::types::{FieldCondition, HasIdCondition};

    const TOTAL: usize = 1000;

    fn test_condition(key: &str) -> Condition {
        Condition::Field(FieldCondition {
            key: path(key),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            values_count: None,
            geo_polygon: None,
        })
    }

    fn test_estimator(condition: &Condition) -> CardinalityEstimation {
        match condition {
            Condition::Filter(_) => panic!("unexpected Filter"),
            Condition::Nested(_) => panic!("unexpected Nested"),
            Condition::Field(field) => match field.key.to_string().as_str() {
                "color" => CardinalityEstimation {
                    primary_clauses: vec![PrimaryCondition::Condition(field.clone())],
                    min: 100,
                    exp: 200,
                    max: 300,
                },
                "size" => CardinalityEstimation {
                    primary_clauses: vec![PrimaryCondition::Condition(field.clone())],
                    min: 100,
                    exp: 100,
                    max: 100,
                },
                "price" => CardinalityEstimation {
                    primary_clauses: vec![PrimaryCondition::Condition(field.clone())],
                    min: 10,
                    exp: 15,
                    max: 20,
                },
                _ => CardinalityEstimation::unknown(TOTAL),
            },
            Condition::HasId(has_id) => CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Ids(
                    has_id
                        .has_id
                        .iter()
                        .map(|&x| format!("{x}").parse().unwrap()) // hack to convert ID as "number"
                        .collect(),
                )],
                min: has_id.has_id.len(),
                exp: has_id.has_id.len(),
                max: has_id.has_id.len(),
            },
            Condition::IsEmpty(condition) => CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::IsEmpty(condition.to_owned())],
                min: 0,
                exp: TOTAL / 2,
                max: TOTAL,
            },
            Condition::IsNull(condition) => CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::IsNull(condition.to_owned())],
                min: 0,
                exp: TOTAL / 2,
                max: TOTAL,
            },
            Condition::HasIdInternal(hash_id_internal) => CardinalityEstimation {
                primary_clauses: vec![],
                min: hash_id_internal.number_of_ids(),
                exp: hash_id_internal.number_of_ids(),
                max: hash_id_internal.number_of_ids(),
            },
        }
    }

    #[test]
    fn simple_query_estimation_test() {
        let query = Filter::new_must(test_condition("color"));
        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
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

        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
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

        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
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

        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
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
        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
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

        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
        assert_eq!(estimation.primary_clauses.len(), 3);
        assert!(estimation.max <= TOTAL);
        assert!(estimation.exp <= estimation.max);
        assert!(estimation.min <= estimation.exp);
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

        let estimation = estimate_filter(&test_estimator, &min_should_query, TOTAL);

        let must_query = Filter {
            should: None,
            min_should: None,
            must: Some(conditions),
            must_not: None,
        };

        let expected_estimation = estimate_filter(&test_estimator, &must_query, TOTAL);

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
                has_id: HashSet::from_iter([1, 2, 3, 4, 5].into_iter().map(|x| x.into())),
            })]),
        };

        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
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
                has_id: HashSet::from_iter([1, 2, 3, 4, 5].into_iter().map(|x| x.into())),
            })]),
        };

        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
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
