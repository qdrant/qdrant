use std::ops::Deref;

use bitvec::bitvec;
use bitvec::prelude::BitVec;
use serde_json::Value;

use crate::common::utils::{IndexesMap, JsonPathPayload, MultiValue};
use crate::payload_storage::condition_checker::ValueChecker;
use crate::types::{
    Condition, FieldCondition, Filter, IsEmptyCondition, IsNullCondition, OwnedPayloadRef, Payload,
};

/// Executes condition checks for all `must` conditions of the nester objects.
/// If there is at least one nested object that matches all `must` conditions, returns `true`.
/// If there are no conditions, returns `true`.
/// If there are no nested objects, returns `false`.
fn check_nested_must_conditions<F>(checker: &F, must: &Option<Vec<Condition>>) -> bool
where
    F: Fn(&Condition) -> BitVec,
{
    match must {
        None => true,
        Some(conditions) => conditions
            .iter()
            .map(checker)
            .reduce(|acc, matches| acc & matches)
            .map(|matches: BitVec| matches.count_ones() > 0)
            .unwrap_or(false), // At least one sub-object must match all conditions
    }
}

fn check_nested_must_not_conditions<F>(checker: &F, must_not: &Option<Vec<Condition>>) -> bool
where
    F: Fn(&Condition) -> BitVec,
{
    match must_not {
        None => true,
        Some(conditions) => {
            conditions
                .iter()
                .map(checker)
                .reduce(|acc, matches| acc | matches)
                // There is at least one sub-object that does not match any of the conditions
                .map(|matches: BitVec| matches.count_zeros() > 0)
                .unwrap_or(true) // If there are no sub-objects, then must_not conditions are satisfied
        }
    }
}

pub fn check_nested_filter<'a, F>(
    nested_path: &JsonPathPayload,
    nested_filter: &Filter,
    get_payload: F,
) -> bool
where
    F: Fn() -> OwnedPayloadRef<'a>,
{
    let nested_checker = |condition: &Condition| match condition {
        Condition::Field(field_condition) => nested_check_field_condition(
            field_condition,
            get_payload().deref(),
            nested_path,
            &Default::default(),
        ),
        Condition::IsEmpty(is_empty) => {
            check_nested_is_empty_condition(nested_path, is_empty, get_payload().deref())
        }
        Condition::IsNull(is_null) => {
            check_nested_is_null_condition(nested_path, is_null, get_payload().deref())
        }
        Condition::HasId(_) => unreachable!(), // Is there a use case for nested HasId?
        Condition::Nested(_) => unreachable!(), // Several layers of nesting are not supported here
        Condition::Filter(_) => unreachable!(),
    };

    nested_filter_checker(&nested_checker, nested_filter)
}

pub fn nested_filter_checker<F>(matching_paths: &F, nested_filter: &Filter) -> bool
where
    F: Fn(&Condition) -> BitVec,
{
    check_nested_must_conditions(matching_paths, &nested_filter.must)
        && check_nested_must_not_conditions(matching_paths, &nested_filter.must_not)
}

/// Return element indices matching the condition in the payload
pub fn check_nested_is_empty_condition(
    nested_path: &JsonPathPayload,
    is_empty: &IsEmptyCondition,
    payload: &Payload,
) -> BitVec {
    let full_path = nested_path.extend(&is_empty.is_empty.key);
    let field_values = payload.get_value(&full_path.path).values();
    let mut result = BitVec::with_capacity(field_values.len());
    for p in field_values {
        match p {
            Value::Null => result.push(true),
            Value::Array(vec) if vec.is_empty() => result.push(true),
            _ => result.push(false),
        }
    }
    result
}

/// Return element indices matching the condition in the payload
pub fn check_nested_is_null_condition(
    nested_path: &JsonPathPayload,
    is_null: &IsNullCondition,
    payload: &Payload,
) -> BitVec {
    let full_path = nested_path.extend(&is_null.is_null.key);
    let field_values = payload.get_value(&full_path.path);
    match field_values {
        MultiValue::Single(None) => bitvec![1; 1],
        MultiValue::Single(Some(v)) => {
            if v.is_null() {
                bitvec![1; 1]
            } else {
                bitvec![0; 1]
            }
        }
        MultiValue::Multiple(multiple_values) => {
            let mut paths = BitVec::with_capacity(multiple_values.len());
            for p in multiple_values {
                let check_res = match p {
                    Value::Null => true,
                    Value::Array(vec) => vec.iter().any(|val| val.is_null()),
                    _ => false,
                };
                paths.push(check_res);
            }
            paths
        }
    }
}

/// Return indexes of the elements matching the condition in the payload values
pub fn nested_check_field_condition(
    field_condition: &FieldCondition,
    payload: &Payload,
    nested_path: &JsonPathPayload,
    field_indexes: &IndexesMap,
) -> BitVec {
    let full_path = nested_path.extend(&field_condition.key);
    let field_values = payload.get_value(&full_path.path).values();
    let mut result = BitVec::with_capacity(field_values.len());

    let field_indexes = field_indexes.get(&full_path.path);

    for p in field_values {
        // This covers a case, when a field index affects the result of the condition.
        // Only required in the nested case,
        // because non-nested payload is checked by the index directly.
        let mut index_check_res = None;
        if let Some(field_indexes) = field_indexes {
            for index in field_indexes {
                index_check_res = index.check_condition(field_condition, p);
                if index_check_res.is_some() {
                    break;
                }
            }
        }
        // Fallback to regular condition check if index-aware check did not return a result
        let res = index_check_res.unwrap_or_else(|| field_condition.check(p));
        result.push(res);
    }
    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use atomic_refcell::AtomicRefCell;
    use serde_json::json;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::id_tracker::simple_id_tracker::SimpleIdTracker;
    use crate::id_tracker::IdTracker;
    use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
    use crate::payload_storage::query_checker::SimpleConditionChecker;
    use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
    use crate::payload_storage::{ConditionChecker, PayloadStorage};
    use crate::types::{
        FieldCondition, GeoBoundingBox, GeoPoint, GeoRadius, PayloadField, Range, ValuesCount,
    };

    #[test]
    fn test_nested_condition_checker() {
        let dir = Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let germany_id: u32 = 0;
        let payload_germany: Payload = json!(
        {
            "country": {
                "name": "Germany",
                "capital": "Berlin",
                "cities": [
                    {
                        "name": "Berlin",
                        "population": 3.7,
                        "location": {
                            "lon": 13.76116,
                            "lat": 52.33826,
                        },
                        "sightseeing": ["Brandenburg Gate", "Reichstag"]
                    },
                    {
                        "name": "Munich",
                        "population": 1.5,
                        "location": {
                            "lon": 11.57549,
                            "lat": 48.13743,
                        },
                        "sightseeing": ["Marienplatz", "Olympiapark"]
                    },
                    {
                        "name": "Hamburg",
                        "population": 1.8,
                        "location": {
                            "lon": 9.99368,
                            "lat": 53.55108,
                        },
                        "sightseeing": ["Reeperbahn", "Elbphilharmonie"]
                    }
                ],
            }
        })
        .into();

        let japan_id: u32 = 1;
        let payload_japan: Payload = json!(
        {
            "country": {
                "name": "Japan",
                "capital": "Tokyo",
                "cities": [
                    {
                        "name": "Tokyo",
                        "population": 13.5,
                        "location": {
                            "lon": 139.69171,
                            "lat": 35.6895,
                        },
                        "sightseeing": ["Tokyo Tower", "Tokyo Skytree", "Tokyo Disneyland"]
                    },
                    {
                        "name": "Osaka",
                        "population": 2.7,
                        "location": {
                            "lon": 135.50217,
                            "lat": 34.69374,
                        },
                        "sightseeing": ["Osaka Castle", "Universal Studios Japan"]
                    },
                    {
                        "name": "Kyoto",
                        "population": 1.5,
                        "location": {
                            "lon": 135.76803,
                            "lat": 35.01163,
                        },
                        "sightseeing": ["Kiyomizu-dera", "Fushimi Inari-taisha"]
                    }
                ],
            }
        })
        .into();

        let boring_id: u32 = 2;
        let payload_boring: Payload = json!(
        {
            "country": {
                "name": "Boring",
                "cities": [
                    {
                        "name": "Boring-ville",
                        "population": 0,
                        "sightseeing": [],
                    },
                ],
            }
        })
        .into();

        let mut payload_storage: PayloadStorageEnum =
            SimplePayloadStorage::open(db.clone()).unwrap().into();
        let mut id_tracker = SimpleIdTracker::open(db).unwrap();

        // point 0 - Germany
        id_tracker.set_link((germany_id as u64).into(), 0).unwrap();
        payload_storage.assign(0, &payload_germany).unwrap();

        // point 1 - Japan
        id_tracker.set_link((japan_id as u64).into(), 1).unwrap();
        payload_storage.assign(1, &payload_japan).unwrap();

        // point 2 - Boring
        id_tracker.set_link((boring_id as u64).into(), 2).unwrap();
        payload_storage.assign(2, &payload_boring).unwrap();

        let payload_checker = SimpleConditionChecker::new(
            Arc::new(AtomicRefCell::new(payload_storage)),
            Arc::new(AtomicRefCell::new(id_tracker)),
        );

        // single must range condition nested field in array
        let population_range_condition = Filter::new_must(Condition::new_nested(
            "country.cities".to_string(),
            Filter::new_must(Condition::Field(FieldCondition::new_range(
                "population".to_string(),
                Range {
                    lt: None,
                    gt: Some(8.0),
                    gte: None,
                    lte: None,
                },
            ))),
        ));

        assert!(!payload_checker.check(germany_id, &population_range_condition));
        assert!(payload_checker.check(japan_id, &population_range_condition));
        assert!(!payload_checker.check(boring_id, &population_range_condition));

        // single must_not range condition nested field in array
        let population_range_condition = Filter::new_must(Condition::new_nested(
            "country.cities".to_string(),
            Filter::new_must_not(Condition::Field(FieldCondition::new_range(
                "population".to_string(),
                Range {
                    lt: None,
                    gt: Some(8.0),
                    gte: None,
                    lte: None,
                },
            ))),
        ));

        assert!(payload_checker.check(germany_id, &population_range_condition)); // all cities less than 8.0
        assert!(payload_checker.check(japan_id, &population_range_condition)); // tokyo is 13.5, but other cities are less than 8.0
        assert!(payload_checker.check(boring_id, &population_range_condition)); // has no cities

        // single must values_count condition nested field in array
        let sightseeing_value_count_condition = Filter::new_must(Condition::new_nested(
            "country.cities".to_string(),
            Filter::new_must(Condition::Field(FieldCondition::new_values_count(
                "sightseeing".to_string(),
                ValuesCount {
                    lt: None,
                    gt: None,
                    gte: Some(3),
                    lte: None,
                },
            ))),
        ));

        assert!(!payload_checker.check(germany_id, &sightseeing_value_count_condition));
        assert!(payload_checker.check(japan_id, &sightseeing_value_count_condition));
        assert!(!payload_checker.check(boring_id, &sightseeing_value_count_condition));

        // single must_not values_count condition nested field in array
        let sightseeing_value_count_condition = Filter::new_must(Condition::new_nested(
            "country.cities".to_string(),
            Filter::new_must_not(Condition::Field(FieldCondition::new_values_count(
                "sightseeing".to_string(),
                ValuesCount {
                    lt: None,
                    gt: None,
                    gte: Some(3),
                    lte: None,
                },
            ))),
        ));

        assert!(payload_checker.check(germany_id, &sightseeing_value_count_condition));
        assert!(payload_checker.check(japan_id, &sightseeing_value_count_condition));
        assert!(payload_checker.check(boring_id, &sightseeing_value_count_condition));

        // single IsEmpty condition nested field in array
        let is_empty_condition = Filter::new_must(Condition::new_nested(
            "country.cities".to_string(),
            Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
                is_empty: PayloadField {
                    key: "sightseeing".to_string(),
                },
            })),
        ));

        assert!(!payload_checker.check(germany_id, &is_empty_condition));
        assert!(!payload_checker.check(japan_id, &is_empty_condition));
        assert!(payload_checker.check(boring_id, &is_empty_condition));

        // single IsNull condition nested field in array
        let is_empty_condition = Filter::new_must(Condition::new_nested(
            "country.cities".to_string(),
            Filter::new_must(Condition::IsNull(IsNullCondition {
                is_null: PayloadField {
                    key: "location".to_string(),
                },
            })),
        ));

        assert!(!payload_checker.check(germany_id, &is_empty_condition));
        assert!(!payload_checker.check(japan_id, &is_empty_condition));
        assert!(payload_checker.check(boring_id, &is_empty_condition));

        // single geo-bounding box in nested field in array
        let location_close_to_berlin_box_condition = Filter::new_must(Condition::new_nested(
            "country.cities".to_string(),
            Filter::new_must(Condition::Field(FieldCondition::new_geo_bounding_box(
                "location".to_string(),
                GeoBoundingBox {
                    top_left: GeoPoint {
                        lon: 13.08835,
                        lat: 52.67551,
                    },
                    bottom_right: GeoPoint {
                        lon: 13.76117,
                        lat: 52.33825,
                    },
                },
            ))),
        ));

        // Germany has a city whose location is within the box
        assert!(payload_checker.check(germany_id, &location_close_to_berlin_box_condition));
        assert!(!payload_checker.check(japan_id, &location_close_to_berlin_box_condition));
        assert!(!payload_checker.check(boring_id, &location_close_to_berlin_box_condition));

        // single geo-bounding box in nested field in array
        let location_close_to_berlin_radius_condition = Filter::new_must(Condition::new_nested(
            "country.cities".to_string(),
            Filter::new_must(Condition::Field(FieldCondition::new_geo_radius(
                "location".to_string(),
                GeoRadius {
                    center: GeoPoint {
                        lon: 13.76117,
                        lat: 52.33825,
                    },
                    radius: 1000.0,
                },
            ))),
        ));

        // Germany has a city whose location is within the radius
        assert!(payload_checker.check(germany_id, &location_close_to_berlin_radius_condition));
        assert!(!payload_checker.check(japan_id, &location_close_to_berlin_radius_condition));
        assert!(!payload_checker.check(boring_id, &location_close_to_berlin_radius_condition));
    }
}
