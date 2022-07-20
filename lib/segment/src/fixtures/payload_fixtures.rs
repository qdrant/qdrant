use std::ops::{Range, RangeInclusive};

use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::{json, Value};

use crate::types::{
    Condition, ExtendedPointId, FieldCondition, Filter, HasIdCondition, IsEmptyCondition, Payload,
    PayloadField, Range as RangeCondition, ValuesCount, VectorElementType,
};

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

pub const STR_KEY: &str = "kvd";
pub const INT_KEY: &str = "int";
pub const INT_KEY_2: &str = "int2";
pub const FLT_KEY: &str = "flt";
pub const FLICKING_KEY: &str = "flicking";
pub const GEO_KEY: &str = "geo";

pub fn random_keyword<R: Rng + ?Sized>(rnd_gen: &mut R) -> String {
    let random_adj = ADJECTIVE.choose(rnd_gen).unwrap();
    let random_noun = NOUN.choose(rnd_gen).unwrap();
    format!("{} {}", random_adj, random_noun)
}

pub fn random_keyword_payload<R: Rng + ?Sized>(
    rnd_gen: &mut R,
    num_values: RangeInclusive<usize>,
) -> Value {
    let sample_num_values = rnd_gen.gen_range(num_values);
    if sample_num_values > 1 {
        Value::Array(
            (0..sample_num_values)
                .map(|_| Value::String(random_keyword(rnd_gen)))
                .collect(),
        )
    } else {
        Value::String(random_keyword(rnd_gen))
    }
}

pub fn random_int_payload<R: Rng + ?Sized>(
    rnd_gen: &mut R,
    num_values: RangeInclusive<usize>,
) -> Vec<i64> {
    (0..rnd_gen.gen_range(num_values))
        .map(|_| rnd_gen.gen_range(INT_RANGE))
        .collect_vec()
}

pub fn random_geo_payload<R: Rng + ?Sized>(
    rnd_gen: &mut R,
    num_values: RangeInclusive<usize>,
) -> Vec<Value> {
    (0..rnd_gen.gen_range(num_values))
        .map(|_| {
            json!( {
                "lon": rnd_gen.gen_range(LON_RANGE),
                "lat": rnd_gen.gen_range(LAT_RANGE),
            })
        })
        .collect_vec()
}

pub fn random_vector<R: Rng + ?Sized>(rnd_gen: &mut R, size: usize) -> Vec<VectorElementType> {
    (0..size).map(|_| rnd_gen.gen()).collect()
}

pub fn random_uncommon_condition<R: Rng + ?Sized>(rnd_gen: &mut R) -> Condition {
    let switch = rnd_gen.gen_range(0..=3);
    match switch {
        0 => Condition::Field(FieldCondition::new_values_count(
            STR_KEY.to_string(),
            ValuesCount {
                lt: None,
                gt: None,
                gte: Some(3),
                lte: None,
            },
        )),
        1 => Condition::Field(FieldCondition::new_values_count(
            STR_KEY.to_string(),
            ValuesCount {
                lt: None,
                gt: None,
                gte: None,
                lte: Some(2),
            },
        )),
        2 => Condition::HasId(HasIdCondition {
            has_id: (0..rnd_gen.gen_range(10..50))
                .map(|_| ExtendedPointId::NumId(rnd_gen.gen_range(0..1000)))
                .collect(),
        }),
        3 => Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: FLICKING_KEY.to_string(),
            },
        }),
        _ => unreachable!(),
    }
}

pub fn random_simple_condition<R: Rng + ?Sized>(rnd_gen: &mut R) -> Condition {
    let kv_or_int: bool = rnd_gen.gen();
    if kv_or_int {
        Condition::Field(FieldCondition::new_match(
            STR_KEY.to_string(),
            random_keyword(rnd_gen).into(),
        ))
    } else {
        Condition::Field(FieldCondition::new_range(
            INT_KEY.to_string(),
            RangeCondition {
                lt: None,
                gt: None,
                gte: Some(rnd_gen.gen_range(INT_RANGE) as f64),
                lte: Some(rnd_gen.gen_range(INT_RANGE) as f64),
            },
        ))
    }
}

pub fn random_condition<R: Rng + ?Sized>(rnd_gen: &mut R) -> Condition {
    let is_simple: bool = rnd_gen.gen_range(0..100) < 80;
    if is_simple {
        random_simple_condition(rnd_gen)
    } else {
        random_uncommon_condition(rnd_gen)
    }
}

pub fn random_must_filter<R: Rng + ?Sized>(rnd_gen: &mut R, num_conditions: usize) -> Filter {
    let must_conditions = (0..num_conditions)
        .map(|_| random_simple_condition(rnd_gen))
        .collect_vec();

    Filter {
        should: None,
        must: Some(must_conditions),
        must_not: None,
    }
}

pub fn random_filter<R: Rng + ?Sized>(rnd_gen: &mut R, total_conditions: usize) -> Filter {
    let num_should = rnd_gen.gen_range(0..=total_conditions);
    let num_must = total_conditions - num_should;

    let should_conditions = (0..num_should)
        .map(|_| random_condition(rnd_gen))
        .collect_vec();

    let should_conditions_opt = if !should_conditions.is_empty() {
        Some(should_conditions)
    } else {
        None
    };

    let must_conditions = (0..num_must)
        .map(|_| random_condition(rnd_gen))
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

pub fn generate_diverse_payload<R: Rng + ?Sized>(rnd_gen: &mut R) -> Payload {
    let payload: Payload = if rnd_gen.gen_range(0.0..1.0) < 0.5 {
        json!({
            STR_KEY: random_keyword_payload(rnd_gen, 1..=3),
            INT_KEY: random_int_payload(rnd_gen, 1..=3),
            INT_KEY_2: random_int_payload(rnd_gen, 1..=2),
            FLT_KEY: rnd_gen.gen_range(0.0..10.0),
            GEO_KEY: random_geo_payload(rnd_gen, 1..=3)
        })
        .into()
    } else {
        json!({
            STR_KEY: random_keyword_payload(rnd_gen, 1..=2),
            INT_KEY: random_int_payload(rnd_gen, 1..=3),
            INT_KEY_2: random_int_payload(rnd_gen, 1..=2),
            FLT_KEY: rnd_gen.gen_range(0.0..10.0),
            GEO_KEY: random_geo_payload(rnd_gen, 1..=3),
            FLICKING_KEY: random_int_payload(rnd_gen, 1..=3)
        })
        .into()
    };

    payload
}
