//! Contains functions for estimating of how many points should be processed for a given filter query
//!
//! Filter query is used e.g. for determining how would be faster to process the query:
//! - use vector index or payload index first

use crate::index::field_index::{CardinalityEstimation, PrimaryCondition};
use crate::types::{Condition, Filter};
use itertools::Itertools;
use std::cmp::{max, min};

fn combine_must_estimations(
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

    let exp_estimation = (exp_estimation_prob * (total as f64)) as usize;

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
    let mut clauses: Vec<PrimaryCondition> = vec![];
    for estimation in &should_estimations {
        if estimation.primary_clauses.is_empty() {
            // If some branch is un-indexed - we can't make
            // any assumptions about the whole `should` clause
            clauses = vec![];
            break;
        }
        clauses.append(&mut estimation.primary_clauses.clone());
    }
    let element_not_hit_prob: f64 = should_estimations
        .iter()
        .map(|x| (total - x.exp) as f64 / (total as f64))
        .product();
    let element_hit_prob = 1.0 - element_not_hit_prob;
    let expected_count = (element_hit_prob * (total as f64)) as usize;
    CardinalityEstimation {
        primary_clauses: clauses,
        min: should_estimations.iter().map(|x| x.min).max().unwrap_or(0),
        exp: expected_count,
        max: min(should_estimations.iter().map(|x| x.max).sum(), total),
    }
}

fn estimate_must<F>(estimator: &F, conditions: &[Condition], total: usize) -> CardinalityEstimation
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let estimate = |x| estimate_condition(estimator, x, total);
    let must_estimations = conditions.iter().map(estimate).collect_vec();

    combine_must_estimations(&must_estimations, total)
}

fn invert_estimation(estimation: &CardinalityEstimation, total: usize) -> CardinalityEstimation {
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
    use super::*;
    use crate::types::{FieldCondition, HasIdCondition, PointOffsetType};
    use std::collections::HashSet;
    use std::iter::FromIterator;

    const TOTAL: usize = 1000;

    fn test_condition(key: String) -> Condition {
        Condition::Field(FieldCondition {
            key,
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
        })
    }

    fn test_estimator(condition: &Condition) -> CardinalityEstimation {
        match condition {
            Condition::Filter(_) => panic!("unexpected Filter"),
            Condition::Field(field) => match field.key.as_str() {
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
                        .map(|&x| x as PointOffsetType)
                        .collect(),
                )],
                min: has_id.has_id.len(),
                exp: has_id.has_id.len(),
                max: has_id.has_id.len(),
            },
        }
    }

    #[test]
    fn simple_query_estimation_test() {
        let query = Filter::new_must(test_condition("color".to_owned()));
        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
        assert_eq!(estimation.exp, 200);
        assert!(!estimation.primary_clauses.is_empty());
    }

    #[test]
    fn must_estimation_query_test() {
        let query = Filter {
            should: None,
            must: Some(vec![
                test_condition("color".to_owned()),
                test_condition("size".to_owned()),
                test_condition("un-indexed".to_owned()),
            ]),
            must_not: None,
        };

        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
        assert_eq!(estimation.primary_clauses.len(), 1);
        match &estimation.primary_clauses[0] {
            PrimaryCondition::Condition(field) => assert_eq!(&field.key, "size"),
            PrimaryCondition::Ids(_) => assert!(false),
        }
        assert!(estimation.max <= TOTAL);
        assert!(estimation.exp <= estimation.max);
        assert!(estimation.min <= estimation.exp);
    }

    #[test]
    fn should_estimation_query_test() {
        let query = Filter {
            should: Some(vec![
                test_condition("color".to_owned()),
                test_condition("size".to_owned()),
            ]),
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
                test_condition("color".to_owned()),
                test_condition("size".to_owned()),
                test_condition("un-indexed".to_owned()),
            ]),
            must: None,
            must_not: None,
        };

        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
        assert_eq!(estimation.primary_clauses.len(), 0);
        eprintln!("estimation = {:#?}", estimation);
        assert!(estimation.max <= TOTAL);
        assert!(estimation.exp <= estimation.max);
        assert!(estimation.min <= estimation.exp);
    }

    #[test]
    fn complex_estimation_query_test() {
        let query = Filter {
            should: Some(vec![
                Condition::Filter(Filter {
                    should: None,
                    must: Some(vec![
                        test_condition("color".to_owned()),
                        test_condition("size".to_owned()),
                    ]),
                    must_not: None,
                }),
                Condition::Filter(Filter {
                    should: None,
                    must: Some(vec![
                        test_condition("price".to_owned()),
                        test_condition("size".to_owned()),
                    ]),
                    must_not: None,
                }),
            ]),
            must: None,
            must_not: Some(vec![Condition::HasId(HasIdCondition {
                has_id: HashSet::from_iter([1, 2, 3, 4, 5]),
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
            must: Some(vec![
                Condition::Filter(Filter {
                    must: None,
                    should: Some(vec![
                        test_condition("color".to_owned()),
                        test_condition("size".to_owned()),
                    ]),
                    must_not: None,
                }),
                Condition::Filter(Filter {
                    must: None,
                    should: Some(vec![
                        test_condition("price".to_owned()),
                        test_condition("size".to_owned()),
                    ]),
                    must_not: None,
                }),
            ]),
            must_not: Some(vec![Condition::HasId(HasIdCondition {
                has_id: HashSet::from_iter([1, 2, 3, 4, 5]),
            })]),
        };

        let estimation = estimate_filter(&test_estimator, &query, TOTAL);
        assert_eq!(estimation.primary_clauses.len(), 2);
        estimation.primary_clauses.iter().for_each(|x| match x {
            PrimaryCondition::Condition(field) => {
                assert!(vec!["price".to_owned(), "size".to_owned(),].contains(&field.key))
            }
            PrimaryCondition::Ids(_) => assert!(false, "Should not go here"),
        });
        assert!(estimation.max <= TOTAL);
        assert!(estimation.exp <= estimation.max);
        assert!(estimation.min <= estimation.exp);
    }
}
