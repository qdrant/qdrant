use std::collections::HashMap;
use std::ops::Bound;

use serde_json::Value;
use smallvec::{smallvec, SmallVec};

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::Vector;
use crate::index::field_index::FieldIndex;
use crate::json_path::JsonPathString;
use crate::types::PayloadKeyType;

pub type IndexesMap = HashMap<PayloadKeyType, Vec<FieldIndex>>;

/// A container for JSON values, optimized for the common case of a single value.
pub type MultiValue<T> = SmallVec<[T; 1]>;

pub fn check_is_empty<'a>(values: impl IntoIterator<Item = &'a Value>) -> bool {
    values.into_iter().all(|x| match x {
        serde_json::Value::Null => true,
        serde_json::Value::Array(arr) => arr.is_empty(),
        _ => false,
    })
}

pub fn check_is_null<'a>(values: impl IntoIterator<Item = &'a Value>) -> bool {
    values.into_iter().any(|x| x.is_null())
    // { "a": [ { "b": null }, { "b": 1 } ] } => true
    // { "a": [ { "b": 1 }, { "b": null } ] } => true
    // { "a": [ { "b": 1 }, { "b": 2 } ] } => false
}

pub fn rev_range(a: usize, b: usize) -> impl Iterator<Item = usize> {
    (b + 1..=a).rev()
}

/// Alternative to [core::ops::Bound::map](https://doc.rust-lang.org/std/ops/enum.Bound.html#method.map)
// TODO(luis): replace with the stabilized function. It is already merged, seems like it will be available in 1.76
pub fn bound_map<T, U, F: FnOnce(T) -> U>(bound: Bound<T>, f: F) -> Bound<U> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(x) => Bound::Included(f(x)),
        Bound::Excluded(x) => Bound::Excluded(f(x)),
    }
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
    rest_of_path: Option<&str>,
    json_map: &'a serde_json::Map<String, Value>,
) -> Option<MultiValue<&'a Value>> {
    match json_map.get(array_path) {
        Some(Value::Array(array)) => {
            let mut values = SmallVec::new();
            for (i, value) in array.iter().enumerate() {
                if let Some(array_index) = array_index {
                    if i == array_index as usize {
                        match rest_of_path {
                            Some(rest_path) => {
                                // expect an Object if there is a rest path
                                if let Value::Object(map) = value {
                                    #[allow(deprecated)]
                                    values.extend(get_value_from_json_map(rest_path, map))
                                }
                            }
                            None => values.push(value),
                        }
                    }
                } else {
                    match rest_of_path {
                        Some(rest_path) => {
                            // expect an Object if there is a rest path
                            if let Value::Object(map) = value {
                                #[allow(deprecated)]
                                values.extend(get_value_from_json_map(rest_path, map))
                            }
                        }
                        None => values.push(value),
                    }
                }
            }
            Some(values)
        }
        _ => None,
    }
}

fn get_value_from_json_map_opt<'a>(
    path: &str,
    json_map: &'a serde_json::Map<String, Value>,
) -> Option<MultiValue<&'a Value>> {
    // check if leaf path element
    match path.split_once('.') {
        Some((element, rest_path)) => {
            // check if targeting array
            match parse_array_path(element) {
                Some((array_element_path, array_index)) => {
                    focus_array_path(array_element_path, array_index, Some(rest_path), json_map)
                }
                None => {
                    // no array notation
                    match json_map.get(element) {
                        Some(Value::Object(map)) => get_value_from_json_map_opt(rest_path, map),
                        Some(value) => rest_path.is_empty().then_some(smallvec![value]),
                        None => None,
                    }
                }
            }
        }
        None => match parse_array_path(path) {
            Some((array_element_path, array_index)) => {
                focus_array_path(array_element_path, array_index, None, json_map)
            }
            None => json_map.get(path).map(|v| smallvec![v]),
        },
    }
}

#[deprecated(note = "Use JsonPathInterface::value_set instead")]
pub fn set_value_to_json_map<'a>(
    path: &str,
    dest: &'a mut serde_json::Map<String, Value>,
    src: &'a serde_json::Map<String, Value>,
) {
    // check if leaf path element
    match path.split_once('.') {
        Some((element, rest_path)) => {
            // check if targeting array
            match parse_array_path(element) {
                Some((array_element_path, array_index)) => {
                    set_by_array_path(array_element_path, array_index, Some(rest_path), dest, src)
                }
                None => {
                    // no array notation
                    if let Some(Value::Object(map)) = dest.get_mut(element) {
                        #[allow(deprecated)]
                        set_value_to_json_map(rest_path, map, src);
                    } else {
                        // insert new one
                        if !rest_path.is_empty() {
                            dest.insert(element.to_owned(), Value::Object(Default::default()));
                            #[allow(deprecated)]
                            set_value_to_json_map(
                                rest_path,
                                dest.get_mut(element).unwrap().as_object_mut().unwrap(),
                                src,
                            );
                        } else {
                            dest.insert(element.to_owned(), Value::Object(src.clone()));
                        }
                    }
                }
            }
        }
        None => match parse_array_path(path) {
            Some((array_element_path, array_index)) => {
                set_by_array_path(array_element_path, array_index, None, dest, src)
            }
            None => {
                if path.is_empty() {
                    merge_map(dest, src);
                } else if let Some(Value::Object(map)) = dest.get_mut(path) {
                    merge_map(map, src);
                } else {
                    // insert new one
                    dest.insert(path.to_owned(), Value::Object(src.clone()));
                }
            }
        },
    }
}

// Merge source map into destination map
pub fn merge_map(
    dest: &mut serde_json::Map<String, Value>,
    source: &serde_json::Map<String, Value>,
) {
    for (key, value) in source {
        match value {
            Value::Null => dest.remove(key),
            _ => dest.insert(key.to_owned(), value.to_owned()),
        };
    }
}

fn set_by_array_path<'a>(
    array_path: &str,
    array_index: Option<u32>,
    rest_path: Option<&str>,
    dest: &'a mut serde_json::Map<String, Value>,
    src: &'a serde_json::Map<String, Value>,
) {
    if let Some(Value::Array(array)) = dest.get_mut(array_path) {
        for (i, value) in array.iter_mut().enumerate() {
            if let Some(array_index) = array_index {
                if i == array_index as usize {
                    let map = match value {
                        Value::Object(map) => map,
                        _ => {
                            *value = Value::Object(Default::default());
                            value.as_object_mut().unwrap()
                        }
                    };
                    if let Some(rest_path) = rest_path {
                        #[allow(deprecated)]
                        set_value_to_json_map(rest_path, map, src);
                    } else {
                        merge_map(map, src);
                    }
                }
            } else {
                let map = match value {
                    Value::Object(map) => map,
                    _ => {
                        *value = Value::Object(Default::default());
                        value.as_object_mut().unwrap()
                    }
                };
                if let Some(rest_path) = rest_path {
                    #[allow(deprecated)]
                    set_value_to_json_map(rest_path, map, src);
                } else {
                    merge_map(map, src);
                }
            }
        }
    } else if dest.is_empty() {
        // insert new one
        let array = vec![];
        dest.insert(array_path.to_owned(), Value::Array(array));
    }
}

/// Focus on value references according to path
/// Flatten intermediate arrays but keep leaf array values on demand.
/// E.g
/// {
///   "arr": [
///       { "a": [1, 2, 3] },
///       { "a": 4 },
///       { "b": 5 }
///   ]
/// }
///
/// path: "arr[].a"   => Vec![Value::Array[ 1, 2, 3], 4]
/// path: "arr[].a[]" => Vec![ 1, 2, 3, 4]
///
/// performance: the function could be improved by using the Entry API instead of BTreeMap.get
#[deprecated(note = "Use JsonPathInterface::value_get instead")]
pub fn get_value_from_json_map<'a>(
    path: &str,
    json_map: &'a serde_json::Map<String, Value>,
) -> MultiValue<&'a Value> {
    get_value_from_json_map_opt(path, json_map).unwrap_or_default()
}

/// Delete array values according to array path
///
/// Expects to be called with a path that is a path to an Array
fn delete_array_path(
    array_path: &str,
    array_index: Option<u32>,
    rest_of_path: Option<&str>,
    json_map: &mut serde_json::Map<String, Value>,
) -> MultiValue<Value> {
    if let Some(Value::Array(array)) = json_map.get_mut(array_path) {
        match rest_of_path {
            None => {
                // end of path - delete and collect
                if let Some(array_index) = array_index {
                    if array.len() > array_index as usize {
                        return smallvec![array.remove(array_index as usize)];
                    }
                } else {
                    return smallvec![Value::Array(std::mem::take(array))];
                }
            }
            Some(rest_path) => {
                // dig deeper
                let mut values = SmallVec::new();
                for (i, value) in array.iter_mut().enumerate() {
                    if let Value::Object(map) = value {
                        if let Some(array_index) = array_index {
                            if i == array_index as usize {
                                #[allow(deprecated)]
                                values.extend(remove_value_from_json_map(rest_path, map));
                            }
                        } else {
                            #[allow(deprecated)]
                            values.extend(remove_value_from_json_map(rest_path, map));
                        }
                    }
                }
                return values;
            }
        }
    }
    // no array found
    SmallVec::new()
}

/// Remove value at a given JSON path from JSON map
///
/// performance: the function could be improved by using the Entry API instead of BTreeMap.get_mut
#[deprecated(note = "Use JsonPathInterface::value_remove instead")]
pub fn remove_value_from_json_map(
    path: &str,
    json_map: &mut serde_json::Map<String, Value>,
) -> MultiValue<Value> {
    // check if leaf path element
    match path.split_once('.') {
        Some((element, rest_path)) => {
            // check if targeting array
            match parse_array_path(element) {
                Some((array_element_path, array_index)) => {
                    delete_array_path(array_element_path, array_index, Some(rest_path), json_map)
                }
                None => {
                    // no array notation
                    if rest_path.is_empty() {
                        json_map.remove(element).into_iter().collect()
                    } else {
                        match json_map.get_mut(element) {
                            None => SmallVec::new(),
                            Some(Value::Object(map)) => remove_value_from_json_map(rest_path, map),
                            Some(_value) => SmallVec::new(),
                        }
                    }
                }
            }
        }
        None => match parse_array_path(path) {
            Some((array_element_path, array_index)) => {
                delete_array_path(array_element_path, array_index, None, json_map)
            }
            None => json_map.remove(path).into_iter().collect(),
        },
    }
}

/// Check if a path is included in a list of patterns
///
/// Basically, it checks if either the pattern or path is a prefix of the other.
pub fn check_include_pattern(pattern: &str, path: &str) -> bool {
    pattern
        .split(['.', '['])
        .zip(path.split(['.', '[']))
        .all(|(p, v)| p == v)
}

/// Check if a path should be excluded by a pattern
///
/// Basically, it checks if pattern is a prefix of path, but not the other way around.
pub fn check_exclude_pattern(pattern: &str, path: &str) -> bool {
    if pattern.len() > path.len() {
        return false;
    }
    pattern
        .split(['.', '['])
        .zip(path.split(['.', '[']))
        .all(|(p, v)| p == v)
}

fn _filter_json_values<'a>(
    path: &mut JsonPathString,
    value: &'a Value,
    filter: &dyn Fn(&JsonPathString, &Value) -> bool,
) -> Value {
    match &value {
        Value::Null => value.clone(),
        Value::Bool(_) => value.clone(),
        Value::Number(_) => value.clone(),
        Value::String(_) => value.clone(),
        Value::Array(array) => {
            let mut new_array = Vec::new();
            path.0.push_str("[]");
            for value in array.iter() {
                if filter(path, value) {
                    let value = _filter_json_values(path, value, filter);
                    new_array.push(value);
                }
            }
            path.0.truncate(path.0.len() - 2);
            Value::Array(new_array)
        }
        Value::Object(object) => {
            let mut new_object = serde_json::Map::new();
            for (key, value) in object.iter() {
                if !path.0.is_empty() {
                    path.0.push('.');
                }
                path.0.push_str(key);
                if filter(path, value) {
                    let value = _filter_json_values(path, value, filter);
                    new_object.insert(key.clone(), value);
                }
                path.0.truncate(path.0.len() - key.len());
                if !path.0.is_empty() {
                    path.0.pop();
                }
            }
            Value::Object(new_object)
        }
    }
}

/// Filter json map based on external filter function
///
/// Filter function takes path and value as input and returns true if the value should be kept
#[deprecated(note = "Use JsonPathInterface::value_filter instead")]
pub fn filter_json_values(
    json_map: &serde_json::Map<String, Value>,
    filter: impl Fn(&JsonPathString, &Value) -> bool,
) -> serde_json::Map<String, Value> {
    let mut path = JsonPathString("".to_string());
    let res = _filter_json_values(&mut path, &Value::Object(json_map.clone()), &filter);

    if let Value::Object(map) = res {
        map
    } else {
        // This should never happen, because _filter_json_values always returns same
        // type as input
        unreachable!("Unexpected value type")
    }
}

pub fn transpose_map_into_named_vector<TVector: Into<Vector>>(
    map: HashMap<String, Vec<TVector>>,
) -> Vec<NamedVectors<'static>> {
    let mut result = Vec::new();
    for (key, values) in map {
        result.resize_with(values.len(), NamedVectors::default);
        for (i, value) in values.into_iter().enumerate() {
            result[i].insert(key.clone(), value.into());
        }
    }
    result
}

#[cfg(test)]
#[generic_tests::define]
mod tests {
    use super::*;
    use crate::json_path::{path, JsonPathInterface, JsonPathString};

    fn json(str: &str) -> serde_json::Map<String, Value> {
        serde_json::from_str(str).unwrap()
    }

    #[test]
    fn test_get_nested_value_from_json_map<P: JsonPathInterface>() {
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
            path::<P>("a.b").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // going deeper
        assert_eq!(
            path::<P>("a.b.c").value_get(&map).into_vec(),
            vec![&Value::Number(1.into())]
        );

        // missing path
        assert!(check_is_empty(
            path::<P>("a.b.c.d").value_get(&map).iter().copied()
        ));
    }

    #[test]
    fn test_is_empty<P: JsonPathInterface>() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
                {
                   "a": [
                     { "b": 1 },
                     { "b": 2 },
                     { "b": null },
                     { "d": [] },
                     { "d": [] },
                     { "f": null }
                   ]
                }
            "#,
        )
        .unwrap();
        let multivalue = path::<P>("a[].b").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());

        assert!(!is_empty, "a[].b is not empty");

        let multivalue = path::<P>("a[].c").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());

        assert!(is_empty, "a[].c is empty");

        let multivalue = path::<P>("a[].d").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());
        assert!(is_empty, "a[].d is empty");

        let multivalue = path::<P>("a[].f").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());
        assert!(is_empty, "a[].f is empty");
    }

    #[test]
    fn test_get_nested_array_value_from_json_map<P: JsonPathInterface>() {
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
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        )
        .unwrap();

        // get JSON array
        assert_eq!(
            path::<P>("a.b").value_get(&map).into_vec(),
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
            path::<P>("a.b[]").value_get(&map).into_vec(),
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
            path::<P>("a.b[].c").value_get(&map).into_vec(),
            vec![&Value::Number(1.into()), &Value::Number(2.into())]
        );

        // project object field through array
        assert_eq!(
            path::<P>("a.b[].d").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "e".to_string(),
                Value::Number(3.into())
            )]))]
        );

        // select scalar element from array
        assert_eq!(
            path::<P>("a.b[0]").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // select scalar object from array different index
        assert_eq!(
            path::<P>("a.b[1]").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(2.into())
            )]))]
        );

        // select field element from array different index
        assert_eq!(
            path::<P>("a.b[1].c").value_get(&map).into_vec(),
            vec![&Value::Number(2.into())]
        );

        // select scalar element from array different index
        assert_eq!(
            path::<P>("g[2]").value_get(&map).into_vec(),
            vec![&Value::String("g2".to_string())]
        );

        // select object element from array
        assert_eq!(
            path::<P>("a.b[2]").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "d".to_string(),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "e".to_string(),
                    Value::Number(3.into())
                )]))
            )]))]
        );

        // select out of bound index from array
        assert!(check_is_empty(
            path::<P>("a.b[3]").value_get(&map).iter().copied()
        ));

        // select bad index from array
        // NOTE: we will restrict invalid json path in the future
        assert!(check_is_empty(
            path::<P>("a.b[z]").value_get(&map).iter().copied()
        ));
    }

    #[test]
    fn test_get_deeply_nested_array_value_from_json_map<P: JsonPathInterface>() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "arr1": [
                    {
                        "arr2": [
                            {"a": 1, "b": 2}
                        ]
                    },
                    {
                        "arr2": [
                            {"a": 3, "b": 4},
                            {"a": 5, "b": 6}
                        ]
                    }
                ]
            }
            "#,
        )
        .unwrap();

        // extract and flatten all elements from arrays
        assert_eq!(
            path::<P>("arr1[].arr2[].a").value_get(&map).into_vec(),
            vec![
                &Value::Number(1.into()),
                &Value::Number(3.into()),
                &Value::Number(5.into()),
            ]
        );
    }

    #[test]
    fn test_no_flatten_array_value_from_json_map<P: JsonPathInterface>() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "arr": [
                    { "a": [1, 2, 3] },
                    { "a": 4 },
                    { "b": 5 }
                ]
            }
            "#,
        )
        .unwrap();

        // extract and retain structure for arrays arrays
        assert_eq!(
            path::<P>("arr[].a").value_get(&map).into_vec(),
            vec![
                &Value::Array(vec![
                    Value::Number(1.into()),
                    Value::Number(2.into()),
                    Value::Number(3.into()),
                ]),
                &Value::Number(4.into()),
            ]
        );

        // expect an array as leaf, ignore non arrays
        assert_eq!(
            path::<P>("arr[].a[]").value_get(&map).into_vec(),
            vec![
                &Value::Number(1.into()),
                &Value::Number(2.into()),
                &Value::Number(3.into()),
            ]
        );
    }

    #[test]
    fn test_get_null_and_absent_values<P: JsonPathInterface>() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "a": null,
                "b": [null, null],
                "c": []
            }
            "#,
        )
        .unwrap();

        assert_eq!(path::<P>("a").value_get(&map).as_slice(), &[&Value::Null],);

        assert!(path::<P>("a[]").value_get(&map).is_empty());

        assert_eq!(
            path::<P>("b").value_get(&map).as_slice(),
            &[&Value::Array(vec![Value::Null, Value::Null])],
        );

        assert_eq!(
            path::<P>("b[]").value_get(&map).as_slice(),
            &[&Value::Null, &Value::Null],
        );

        assert_eq!(
            path::<P>("c").value_get(&map).as_slice(),
            &[&Value::Array(vec![])],
        );

        assert!(path::<P>("c[]").value_get(&map).is_empty());

        assert!(path::<P>("d").value_get(&map).is_empty());

        assert!(path::<P>("d[]").value_get(&map).is_empty());
    }

    #[test]
    fn test_filter_json<P: JsonPathInterface>() {
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
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        )
        .unwrap();

        let res = P::value_filter(&map, |path, _value| {
            let path = path.to_string();
            path.starts_with("a.b[].c") || "a.b[].c".starts_with(&path)
        });

        assert_eq!(
            res,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "a": {
                        "b": [
                            { "c": 1 },
                            { "c": 2 },
                            {}
                        ]
                    }
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_check_include_pattern<P: JsonPathInterface>() {
        assert!(path::<P>("a.b.c").check_include_pattern(&path::<P>("a.b.c")));
        assert!(path::<P>("a.b.c").check_include_pattern(&path::<P>("a.b")));
        assert!(!path::<P>("a.b.c").check_include_pattern(&path::<P>("a.b.d")));
        assert!(path::<P>("a.b.c").check_include_pattern(&path::<P>("a")));
        assert!(path::<P>("a").check_include_pattern(&path::<P>("a.d")));
    }

    #[test]
    fn test_check_exclude_pattern<P: JsonPathInterface>() {
        assert!(path::<P>("a.b.c").check_exclude_pattern(&path::<P>("a.b.c")));
        assert!(!path::<P>("a.b.c").check_exclude_pattern(&path::<P>("a.b")));
        assert!(!path::<P>("a.b.c").check_exclude_pattern(&path::<P>("a.b.d")));
        assert!(!path::<P>("a.b.c").check_exclude_pattern(&path::<P>("a")));
        assert!(path::<P>("a").check_exclude_pattern(&path::<P>("a.d")));
    }

    #[test]
    fn test_set_value_to_json_with_empty_key<P: JsonPathInterface>() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "a": {
                    "b": [
                        { "c": 1 },
                        { "c": 2 },
                        { "d": { "e": 3 } }
                    ]
                },
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            { "c": 5 }
            "#,
        )
        .unwrap();

        P::value_set(None, &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "a": {
                        "b": [
                            { "c": 1 },
                            { "c": 2 },
                            { "d": { "e": 3 } }
                        ]
                    },
                    "f": 3,
                    "g": ["g0", "g1", "g2"],
                    "c": 5
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_one_level_key<P: JsonPathInterface>() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "a": {
                    "b": [
                        { "c": 1 },
                        { "c": 2 },
                        { "d": { "e": 3 } }
                    ]
                },
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            { "b": 5 }
            "#,
        )
        .unwrap();

        JsonPathInterface::value_set(Some(&path::<P>("a")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "a": {
                        "b": 5
                    },
                    "f": 3,
                    "g": ["g0", "g1", "g2"]
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_array_index<P: JsonPathInterface>() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "a": {
                    "b": [
                        { "c": 1 },
                        { "c": 2 },
                        { "d": { "e": 3 } }
                    ]
                },
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            { "c": 5 }
            "#,
        )
        .unwrap();

        JsonPathInterface::value_set(Some(&path::<P>("a.b[1]")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "a": {
                        "b": [
                            { "c": 1 },
                            { "c": 5 },
                            { "d": { "e": 3 } }
                        ]
                    },
                    "f": 3,
                    "g": ["g0", "g1", "g2"]
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_empty_src<P: JsonPathInterface>() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "a": {
                    "b": [
                        { "c": 1 },
                        { "c": 2 },
                        { "d": { "e": 3 } }
                    ]
                },
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {}
            "#,
        )
        .unwrap();

        JsonPathInterface::value_set(Some(&path::<P>("a.b[1]")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "a": {
                        "b": [
                            { "c": 1 },
                            { "c": 2 },
                            { "d": { "e": 3 } }
                        ]
                    },
                    "f": 3,
                    "g": ["g0", "g1", "g2"]
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_empty_dest<P: JsonPathInterface>() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
            }
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {"c": 1}
            "#,
        )
        .unwrap();

        P::value_set(None, &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "c": 1
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_empty_dest_nested_key<P: JsonPathInterface>() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
            }
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {"c": 1}
            "#,
        )
        .unwrap();

        JsonPathInterface::value_set(Some(&path::<P>("key1.key2")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "key1": {"key2": { "c": 1 } }
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_empty_dest_nested_array_index_key<P: JsonPathInterface>() {
        let mut map = json("{}");
        let src = json(r#" {"c": 1} "#);
        JsonPathInterface::value_set(Some(&path::<P>("key1.key2[3]")), &mut map, &src);
        assert_eq!(map, json(r#" {"key1": {"key2": []}} "#));

        let mut map = json("{}");
        let src = json(r#" {"c": 1} "#);
        JsonPathInterface::value_set(Some(&path::<P>("key1.key2[0]")), &mut map, &src);
        assert_eq!(map, json(r#" {"key1": {"key2": []}} "#));
    }

    #[test]
    fn test_expand_payload_with_non_existing_array<P: JsonPathInterface>() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
            }
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {"c": 1}
            "#,
        )
        .unwrap();

        JsonPathInterface::value_set(Some(&path::<P>("key1.key2[].key3")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "key1": { "key2": [] }
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_replace_scalar_key_with_object<P: JsonPathInterface>() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {"a": 10}
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {"x": 1}
            "#,
        )
        .unwrap();

        JsonPathInterface::value_set(Some(&path::<P>("a.b.c")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {"a": {"b": {"c": {"x": 1}}}}
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_get_path_head<P: JsonPathInterface>() {
        assert_eq!(path::<P>("a.b.c").head(), "a");
        assert_eq!(path::<P>("a[0].b").head(), "a");
        assert_eq!(path::<P>("a").head(), "a");
        assert_eq!(path::<P>("").head(), "");
    }

    #[instantiate_tests(<JsonPathString>)]
    mod string {}

    // TODO: #[instantiate_tests(<JsonPathV2>)] mod v2 {}
}
