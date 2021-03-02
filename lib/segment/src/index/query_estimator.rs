use crate::index::index::{QueryEstimator, PayloadIndex};
use crate::types::{Filter, Condition};
use crate::index::field_index::{CardinalityEstimation, PrimaryCondition};
use crate::index::struct_payload_index::StructPayloadIndex;
use itertools::Itertools;
use std::cmp::max;

fn combine_must_estimations(estimations: &Vec<CardinalityEstimation>, total: usize) -> CardinalityEstimation {
    let min_estimation = estimations.iter()
        .map(|x| x.min)
        .fold(total as i64, |acc, x| max(0, acc + (x as i64) - (total as i64))) as usize;

    let max_estimation = estimations
        .iter()
        .map(|x| x.max).min()
        .unwrap_or(total);

    let exp_estimation_prob: f64 = estimations.iter()
        .map(|x| (x.exp as f64) / (total as f64))
        .product();

    let exp_estimation = (exp_estimation_prob * (total as f64)) as usize;

    let clauses = estimations.iter()
        .filter(|x| !x.primary_clauses.is_empty())
        .min_by_key(|x| x.exp)
        .map(|x| x.primary_clauses.clone())
        .unwrap_or(vec![]);

    CardinalityEstimation {
        primary_clauses: clauses,
        min: min_estimation,
        exp: exp_estimation,
        max: max_estimation,
    }
}

fn estimate_condition<F>(estimator: &F, condition: &Condition, total: usize) -> CardinalityEstimation
    where F: Fn(&Condition) -> CardinalityEstimation {
    match condition {
        Condition::Filter(filter) => estimate_filter(estimator, filter, total),
        _ => estimator(condition)
    }
}

pub fn estimate_filter<F>(estimator: &F, filter: &Filter, total: usize) -> CardinalityEstimation
    where F: Fn(&Condition) -> CardinalityEstimation {
    let mut filter_estimations: Vec<CardinalityEstimation> = vec![];

    match &filter.must {
        None => {}
        Some(conditions) =>
            filter_estimations.push(estimate_must(estimator, conditions, total))
    }
    match &filter.should {
        None => {}
        Some(conditions) =>
            filter_estimations.push(estimate_should(estimator, conditions, total))
    }
    match &filter.must_not {
        None => {}
        Some(conditions) =>
            filter_estimations.push(estimate_must_not(estimator, conditions, total))
    }

    combine_must_estimations(&filter_estimations, total)
}


fn estimate_should<F>(estimator: &F, conditions: &Vec<Condition>, total: usize) -> CardinalityEstimation
    where F: Fn(&Condition) -> CardinalityEstimation {
    let estimate = |x| estimate_condition(estimator, x, total);

    let should_estimations = conditions.iter().map(estimate).collect_vec();
    let mut clauses: Vec<PrimaryCondition> = vec![];
    for estimation in should_estimations.iter() {
        if estimation.primary_clauses.is_empty() {
            // If some branch is un-indexed - we can't make
            // any assumptions about the whole `should` clause
            clauses = vec![];
            break;
        } else {
            clauses.append(&mut estimation.primary_clauses.clone());
        }
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
        max: should_estimations.iter().map(|x| x.max).sum(),
    }
}

fn estimate_must<F>(estimator: &F, conditions: &Vec<Condition>, total: usize) -> CardinalityEstimation
    where F: Fn(&Condition) -> CardinalityEstimation {
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

fn estimate_must_not<F>(estimator: &F, conditions: &Vec<Condition>, total: usize) -> CardinalityEstimation
    where F: Fn(&Condition) -> CardinalityEstimation {
    let estimate = |x| invert_estimation(&estimate_condition(estimator, x, total), total);
    let must_not_estimations = conditions.iter().map(estimate).collect_vec();
    combine_must_estimations(&must_not_estimations, total)
}


impl QueryEstimator for StructPayloadIndex {
    fn estimate_cardinality(&self, query: &Filter) -> CardinalityEstimation {
        let total = self.total_points();

        let estimator = |condition: &Condition| {
            match condition {
                Condition::Filter(_) => panic!("Unexpected branching"),
                Condition::HasId(ids) => CardinalityEstimation {
                    primary_clauses: vec![PrimaryCondition::Ids(ids.clone())],
                    min: 0,
                    exp: ids.len(),
                    max: ids.len(),
                },
                Condition::Field(field_condition) => self
                    .estimate_field_condition(field_condition)
                    .unwrap_or(CardinalityEstimation {
                        primary_clauses: vec![],
                        min: 0,
                        exp: self.total_points() / 2,
                        max: self.total_points(),
                    }),
            }
        };

        estimate_filter(&estimator, query, total)
    }
}
