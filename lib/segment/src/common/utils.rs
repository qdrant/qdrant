use std::collections::HashMap;

use serde_json::Value;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::VectorElementType;

pub fn rev_range(a: usize, b: usize) -> impl Iterator<Item = usize> {
    (b + 1..=a).rev()
}

/// Parse array path and index from path
///
/// return Some((path, Some(index))) if path is an array path with index
fn parse_array_path(path: &str) -> Option<(&str, Option<u32>)> {
    // shortcut no array path
    if !path.contains('[') || !path.ends_with(']') {
        return None;
    }
    let mut path = path.split('[');
    let element = path.next();
    let index = path.next();
    match (element, index) {
        (Some(element), None) => Some((element, None)), // no index info
        (Some(element), Some("]")) => Some((element, None)), // full array
        (Some(element), Some(index)) => {
            let trimmed_index = index.trim_matches(']');
            // get numeric index
            match trimmed_index.parse::<u32>() {
                Ok(num_index) => Some((element, Some(num_index))),
                Err(_) => None, // not a well formed path array
            }
        }
        _ => None,
    }
}

/// Focus on array values references according to array path
///
/// Expects to be called with a path that is a path to an Array
fn focus_array_path<'a>(
    array_path: &str,
    array_index: Option<u32>,
    rest_path: Option<&str>,
    value: &'a serde_json::Map<String, Value>,
) -> Vec<&'a Value> {
    match value.get(array_path) {
        Some(Value::Array(array)) => {
            let mut values = Vec::new();
            for (i, value) in array.iter().enumerate() {
                if let Value::Object(map) = value {
                    if let Some(array_index) = array_index {
                        if i == array_index as usize {
                            match rest_path {
                                Some(rest_path) => {
                                    values.extend(get_value_from_json_map(rest_path, map))
                                }
                                None => values.push(value),
                            }
                        }
                    } else {
                        match rest_path {
                            Some(rest_path) => {
                                values.extend(get_value_from_json_map(rest_path, map))
                            }
                            None => values.push(value),
                        }
                    }
                }
            }
            values
        }
        _ => vec![],
    }
}

// TODO make TinyVec
pub fn get_value_from_json_map<'a>(
    path: &str,
    value: &'a serde_json::Map<String, Value>,
) -> Vec<&'a Value> {
    // check if leaf path element
    match path.split_once('.') {
        Some((element, rest_path)) => {
            // check if targeting array
            match parse_array_path(element) {
                Some((array_element_path, array_index)) => {
                    focus_array_path(array_element_path, array_index, Some(rest_path), value)
                }
                None => {
                    // targeting object
                    match value.get(element) {
                        Some(Value::Object(map)) => get_value_from_json_map(rest_path, map),
                        Some(value) => match rest_path.is_empty() {
                            true => vec![value],
                            false => vec![],
                        },
                        None => vec![],
                    }
                }
            }
        }
        None => match parse_array_path(path) {
            Some((array_element_path, array_index)) => {
                focus_array_path(array_element_path, array_index, None, value)
            }
            None => match value.get(path) {
                Some(value) => vec![value],
                None => vec![],
            },
        },
    }
}

/// Delete array values according to array path
///
/// Expects to be called with a path that is a path to an Array
fn delete_array_path(
    array_path: &str,
    array_index: Option<u32>,
    rest_path: Option<&str>,
    value: &mut serde_json::Map<String, Value>,
) -> Vec<Value> {
    if let Some(Value::Array(array)) = value.get_mut(array_path) {
        match rest_path {
            None => {
                // end of path - delete and collect
                if let Some(array_index) = array_index {
                    if array.len() > array_index as usize {
                        return vec![array.remove(array_index as usize)];
                    }
                } else {
                    return vec![Value::Array(array.drain(..).collect())];
                }
            }
            Some(rest_path) => {
                // dig deeper
                let mut values = Vec::new();
                for (i, value) in array.iter_mut().enumerate() {
                    if let Value::Object(map) = value {
                        if let Some(array_index) = array_index {
                            if i == array_index as usize {
                                values.extend(remove_value_from_json_map(rest_path, map));
                            }
                        } else {
                            values.extend(remove_value_from_json_map(rest_path, map));
                        }
                    }
                }
                return values;
            }
        }
    }
    vec![]
}

pub fn remove_value_from_json_map(
    path: &str,
    value: &mut serde_json::Map<String, Value>,
) -> Vec<Value> {
    // check if leaf path element
    match path.split_once('.') {
        Some((element, rest_path)) => {
            // check if targeting array
            match parse_array_path(element) {
                Some((array_element_path, array_index)) => {
                    delete_array_path(array_element_path, array_index, Some(rest_path), value)
                }
                None => {
                    // targeting object
                    if rest_path.is_empty() {
                        value.remove(element).into_iter().collect()
                    } else {
                        match value.get_mut(element) {
                            None => vec![],
                            Some(Value::Object(map)) => remove_value_from_json_map(rest_path, map),
                            Some(_value) => vec![],
                        }
                    }
                }
            }
        }
        None => match parse_array_path(path) {
            Some((array_element_path, array_index)) => {
                delete_array_path(array_element_path, array_index, None, value)
            }
            None => value.remove(path).into_iter().collect(),
        },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_nested_value_from_json_map() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "a": {
                    "b": {
                        "c": 1
                    }
                },
                "d": 2
            }
            "#,
        )
        .unwrap();

        assert_eq!(
            get_value_from_json_map("a.b", &map),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // going deeper
        assert_eq!(
            get_value_from_json_map("a.b.c", &map),
            vec![&Value::Number(1.into())]
        );

        // missing path
        assert!(get_value_from_json_map("a.b.c.d", &map).is_empty());
    }

    #[test]
    fn test_get_nested_array_value_from_json_map() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "a": {
                    "b": [
                        { "c": 1 },
                        { "c": 2 },
                        { "d": { "e": 3 } }
                    ]
                },
                "f": 3
            }
            "#,
        )
        .unwrap();

        // get JSON array
        assert_eq!(
            get_value_from_json_map("a.b", &map),
            vec![&Value::Array(vec![
                Value::Object(serde_json::Map::from_iter(vec![(
                    "c".to_string(),
                    Value::Number(1.into())
                )])),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "c".to_string(),
                    Value::Number(2.into())
                )])),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "d".to_string(),
                    Value::Object(serde_json::Map::from_iter(vec![(
                        "e".to_string(),
                        Value::Number(3.into())
                    )]))
                )]))
            ])]
        );

        // a.b[] extract all elements from array
        assert_eq!(
            get_value_from_json_map("a.b[]", &map),
            vec![
                &Value::Object(serde_json::Map::from_iter(vec![(
                    "c".to_string(),
                    Value::Number(1.into())
                )])),
                &Value::Object(serde_json::Map::from_iter(vec![(
                    "c".to_string(),
                    Value::Number(2.into())
                )])),
                &Value::Object(serde_json::Map::from_iter(vec![(
                    "d".to_string(),
                    Value::Object(serde_json::Map::from_iter(vec![(
                        "e".to_string(),
                        Value::Number(3.into())
                    )]))
                )]))
            ]
        );

        // project scalar field through array
        assert_eq!(
            get_value_from_json_map("a.b[].c", &map),
            vec![&Value::Number(1.into()), &Value::Number(2.into())]
        );

        // project object field through array
        assert_eq!(
            get_value_from_json_map("a.b[].d", &map),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "e".to_string(),
                Value::Number(3.into())
            )]))]
        );

        // select scalar element from array
        assert_eq!(
            get_value_from_json_map("a.b[0]", &map),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // select scalar element from array different index
        assert_eq!(
            get_value_from_json_map("a.b[1]", &map),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(2.into())
            )]))]
        );

        // select object element from array
        assert_eq!(
            get_value_from_json_map("a.b[2]", &map),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "d".to_string(),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "e".to_string(),
                    Value::Number(3.into())
                )]))
            )]))]
        );

        // select out of bound index from array
        assert!(get_value_from_json_map("a.b[3]", &map).is_empty());

        // select bad index from array
        assert!(get_value_from_json_map("a.b[z]", &map).is_empty());
    }
}
