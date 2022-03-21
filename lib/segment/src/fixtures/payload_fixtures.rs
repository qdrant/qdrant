use crate::types::{Condition, FieldCondition, Filter, Range as RangeCondition, VectorElementType};
use itertools::Itertools;
use rand::prelude::ThreadRng;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::{json, Value};
use std::ops::Range;

const ADJECTIVE: &[&str] = &[
    "jobless",
    "rightful",
    "breakable",
    "impartial",
    "shocking",
    "faded",
    "phobic",
    "overt",
    "like",
    "wide-eyed",
    "broad",
];

const NOUN: &[&str] = &[
    "territory",
    "jam",
    "neck",
    "chicken",
    "cap",
    "kiss",
    "veil",
    "trail",
    "size",
    "digestion",
    "rod",
    "seed",
];

const INT_RANGE: Range<i64> = 0..500;
pub const LON_RANGE: Range<f64> = -180.0..180.0;
pub const LAT_RANGE: Range<f64> = -90.0..90.0;

pub fn random_keyword(rnd_gen: &mut ThreadRng) -> String {
    let random_adj = ADJECTIVE.choose(rnd_gen).unwrap();
    let random_noun = NOUN.choose(rnd_gen).unwrap();
    format!("{} {}", random_adj, random_noun)
}

pub fn random_keyword_payload(rnd_gen: &mut ThreadRng) -> String {
    random_keyword(rnd_gen)
}

pub fn random_int_payload(rnd_gen: &mut ThreadRng, num_values: usize) -> Vec<i64> {
    (0..num_values)
        .map(|_| rnd_gen.gen_range(INT_RANGE))
        .collect_vec()
}

pub fn random_geo_payload<R: Rng + ?Sized>(rnd_gen: &mut R, num_values: usize) -> Vec<Value> {
    (0..num_values)
        .map(|_| {
            json!( {
                "lon": rnd_gen.gen_range(LON_RANGE),
                "lat": rnd_gen.gen_range(LAT_RANGE),
            })
        })
        .collect_vec()
}

pub fn random_vector(rnd_gen: &mut ThreadRng, size: usize) -> Vec<VectorElementType> {
    (0..size).map(|_| rnd_gen.gen()).collect()
}

pub fn random_field_condition(rnd_gen: &mut ThreadRng) -> Condition {
    let kv_or_int: bool = rnd_gen.gen();
    if kv_or_int {
        Condition::Field(FieldCondition {
            key: "kvd".to_string(),
            r#match: Some(random_keyword(rnd_gen).into()),
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
        })
    } else {
        Condition::Field(FieldCondition {
            key: "int".to_string(),
            r#match: None,
            range: Some(RangeCondition {
                lt: None,
                gt: None,
                gte: Some(rnd_gen.gen_range(INT_RANGE) as f64),
                lte: Some(rnd_gen.gen_range(INT_RANGE) as f64),
            }),
            geo_bounding_box: None,
            geo_radius: None,
        })
    }
}

pub fn random_filter(rnd_gen: &mut ThreadRng) -> Filter {
    let mut rnd1 = rand::thread_rng();

    let should_conditions = (0..=2)
        .take_while(|_| rnd1.gen::<f64>() > 0.6)
        .map(|_| random_field_condition(rnd_gen))
        .collect_vec();

    let should_conditions_opt = if !should_conditions.is_empty() {
        Some(should_conditions)
    } else {
        None
    };

    let must_conditions = (0..=2)
        .take_while(|_| rnd1.gen::<f64>() > 0.6)
        .map(|_| random_field_condition(rnd_gen))
        .collect_vec();

    let must_conditions_opt = if !must_conditions.is_empty() {
        Some(must_conditions)
    } else {
        None
    };

    Filter {
        should: should_conditions_opt,
        must: must_conditions_opt,
        must_not: None,
    }
}
