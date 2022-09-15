use std::collections::HashMap;

use serde_json::Value;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::VectorElementType;

pub fn rev_range(a: usize, b: usize) -> impl Iterator<Item = usize> {
    (b + 1..=a).rev()
}

pub fn get_value_from_json_map<'a>(
    path: &str,
    value: &'a serde_json::Map<String, Value>,
) -> Option<&'a Value> {
    match path.split_once('.') {
        Some((element, path)) => match value.get(element) {
            Some(Value::Object(map)) => get_value_from_json_map(path, map),
            Some(value) => match path.is_empty() {
                true => Some(value),
                false => None,
            },
            None => None,
        },
        None => value.get(path),
    }
}

pub fn remove_value_from_json_map(
    path: &str,
    value: &mut serde_json::Map<String, Value>,
) -> Option<Value> {
    match path.split_once('.') {
        Some((element, new_path)) => {
            if new_path.is_empty() {
                value.remove(element)
            } else {
                match value.get_mut(element) {
                    None => None,
                    Some(Value::Object(map)) => remove_value_from_json_map(new_path, map),
                    Some(_value) => None,
                }
            }
        }
        None => value.remove(path),
    }
}

pub fn transpose_map_into_named_vector(
    map: HashMap<String, Vec<Vec<VectorElementType>>>,
) -> Vec<NamedVectors<'static>> {
    let mut result = Vec::new();
    for (key, values) in map {
        result.resize_with(values.len(), NamedVectors::default);
        for (i, value) in values.into_iter().enumerate() {
            result[i].insert(key.clone(), value);
        }
    }
    result
}
