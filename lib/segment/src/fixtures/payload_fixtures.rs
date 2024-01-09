use std::ops::{Range, RangeInclusive};

use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::{json, Value};

use crate::data_types::vectors::VectorElementType;
use crate::types::{
    Condition, ExtendedPointId, FieldCondition, Filter, HasIdCondition, IsEmptyCondition, Match,
    Payload, PayloadField, Range as RangeCondition, ValuesCount,
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
pub const STR_PROJ_KEY: &str = "kvd_proj";
pub const STR_ROOT_PROJ_KEY: &str = "kvd_root_proj";
pub const INT_KEY: &str = "int";
pub const INT_KEY_2: &str = "int2";
pub const FLT_KEY: &str = "flt";
pub const FLICKING_KEY: &str = "flicking";
pub const GEO_KEY: &str = "geo";
pub const TEXT_KEY: &str = "text";
pub const BOOL_KEY: &str = "bool";

pub fn random_adj<R: Rng + ?Sized>(rnd_gen: &mut R) -> String {
    ADJECTIVE.choose(rnd_gen).unwrap().to_string()
}

pub fn random_keyword<R: Rng + ?Sized>(rnd_gen: &mut R) -> String {
    let random_adj = ADJECTIVE.choose(rnd_gen).unwrap();
    let random_noun = NOUN.choose(rnd_gen).unwrap();
    format!("{random_adj} {random_noun}")
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

pub fn random_bool_payload<R: Rng + ?Sized>(
    rnd_gen: &mut R,
    num_values: RangeInclusive<usize>,
) -> Vec<Value> {
    (0..rnd_gen.gen_range(num_values))
        .map(|_| Value::Bool(rnd_gen.gen()))
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
    let str_or_int: bool = rnd_gen.gen();
    if str_or_int {
        let kv_or_txt: bool = rnd_gen.gen();
        if kv_or_txt {
            Condition::Field(FieldCondition::new_match(
                STR_KEY.to_string(),
                random_keyword(rnd_gen).into(),
            ))
        } else {
            Condition::Field(FieldCondition::new_match(
                TEXT_KEY.to_string(),
                Match::Text(random_adj(rnd_gen).into()),
            ))
        }
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
        min_should: None,
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
        min_should: None,
        must: must_conditions_opt,
        must_not: None,
    }
}

pub fn random_nested_filter<R: Rng + ?Sized>(rnd_gen: &mut R) -> Filter {
    let nested_or_proj: bool = rnd_gen.gen();
    let nested_str_key = if nested_or_proj {
        format!("{}.{}.{}", STR_KEY, "nested_1", "nested_2")
    } else {
        format!("{}.{}[].{}", STR_PROJ_KEY, "nested_1", "nested_2")
    };
    let condition = Condition::Field(FieldCondition::new_match(
        nested_str_key,
        random_keyword(rnd_gen).into(),
    ));
    Filter {
        should: Some(vec![condition]),
        min_should: None,
        must: None,
        must_not: None,
    }
}

fn random_json<R: Rng + ?Sized>(rnd_gen: &mut R) -> Value {
    if rnd_gen.gen_range(0.0..1.0) < 0.5 {
        json!({
            STR_KEY: random_keyword_payload(rnd_gen, 1..=3),
            INT_KEY: random_int_payload(rnd_gen, 1..=3),
            INT_KEY_2: random_int_payload(rnd_gen, 1..=2),
            FLT_KEY: rnd_gen.gen_range(0.0..10.0),
            GEO_KEY: random_geo_payload(rnd_gen, 1..=3),
            TEXT_KEY: random_keyword_payload(rnd_gen, 1..=1),
            BOOL_KEY: random_bool_payload(rnd_gen, 1..=1),
        })
    } else {
        json!({
            STR_KEY: random_keyword_payload(rnd_gen, 1..=2),
            INT_KEY: random_int_payload(rnd_gen, 1..=3),
            INT_KEY_2: random_int_payload(rnd_gen, 1..=2),
            FLT_KEY: rnd_gen.gen_range(0.0..10.0),
            GEO_KEY: random_geo_payload(rnd_gen, 1..=3),
            TEXT_KEY: random_keyword_payload(rnd_gen, 1..=1),
            BOOL_KEY: random_bool_payload(rnd_gen, 1..=2),
            FLICKING_KEY: random_int_payload(rnd_gen, 1..=3)
        })
    }
}

pub fn generate_diverse_payload<R: Rng + ?Sized>(rnd_gen: &mut R) -> Payload {
    random_json(rnd_gen).into()
}

pub fn generate_diverse_nested_payload<R: Rng + ?Sized>(rnd_gen: &mut R) -> Payload {
    json!({
        STR_KEY: {
            "nested_1": {
                "nested_2": random_keyword_payload(rnd_gen, 1..=3)
            }
        },
        STR_PROJ_KEY: {
            "nested_1": [
                { "nested_2": random_keyword_payload(rnd_gen, 1..=3) }
            ]
        },
        STR_ROOT_PROJ_KEY: [
            {
                "nested_1": [
                    { "nested_2":  random_keyword_payload(rnd_gen, 1..=3) }
                ]
            }
        ],
    })
    .into()
}

pub const NESTED_ARRAY_1: &str = "arr1";
pub const NESTED_ARRAY_2: &str = "arr2";
pub const NESTED_ARRAY_3: &str = "arr3";

pub fn generate_nested_array_payload<R: Rng + ?Sized>(rnd_gen: &mut R) -> Payload {
    json!({
        NESTED_ARRAY_1: [
            random_json(rnd_gen),
            random_json(rnd_gen),
            random_json(rnd_gen),
            random_json(rnd_gen),
        ],
        NESTED_ARRAY_2: [
            {
                   NESTED_ARRAY_3: [
                       random_json(rnd_gen),
                       random_json(rnd_gen),
                       random_json(rnd_gen),
                       random_json(rnd_gen),
                   ]
            }
        ]
    })
    .into()
}
