use std::collections::HashMap;
use std::ops::Bound;

use serde_json::Value;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::Vector;
use crate::index::field_index::FieldIndex;
use crate::types::PayloadKeyType;

pub type IndexesMap = HashMap<PayloadKeyType, Vec<FieldIndex>>;

/// Avoids allocating Vec with a single element
#[derive(Debug)]
pub enum MultiValue<T> {
    Single(Option<T>),
    Multiple(Vec<T>),
}

impl<T> Default for MultiValue<T> {
    fn default() -> Self {
        Self::Single(None)
    }
}

impl<T> MultiValue<T> {
    pub(crate) fn one(value: T) -> Self {
        Self::Single(Some(value))
    }

    fn option(value: Option<T>) -> Self {
        Self::Single(value)
    }

    fn push(&mut self, value: T) {
        match self {
            Self::Single(opt) => match opt.take() {
                Some(v) => {
                    *self = Self::Multiple(vec![v, value]);
                }
                None => {
                    *self = Self::Single(Some(value));
                }
            },
            Self::Multiple(vec) => {
                vec.push(value);
            }
        }
    }

    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for value in iter {
            self.push(value);
        }
    }

    pub fn values(self) -> Vec<T> {
        match self {
            Self::Single(opt) => opt.into_iter().collect(),
            Self::Multiple(vec) => vec,
        }
    }

    #[cfg(test)]
    pub(crate) fn as_ref(&self) -> MultiValue<&T> {
        match self {
            Self::Single(opt) => MultiValue::option(opt.as_ref()),
            Self::Multiple(vec) => MultiValue::Multiple(vec.iter().collect()),
        }
    }
}

impl MultiValue<&Value> {
    pub(crate) fn check_is_empty(&self) -> bool {
        match self {
            Self::Multiple(vec) => vec.iter().all(|x| match x {
                Value::Array(vec) => vec.is_empty(),
                Value::Null => true,
                _ => false,
            }),
            Self::Single(val) => match val {
                None => true,
                Some(Value::Array(vec)) => vec.is_empty(),
                Some(Value::Null) => true,
                _ => false,
            },
        }
    }

    pub(crate) fn check_is_null(&self) -> bool {
        match self {
            MultiValue::Single(val) => {
                if let Some(val) = val {
                    return val.is_null();
                }
                false
            }
            // { "a": [ { "b": null }, { "b": 1 } ] } => true
            // { "a": [ { "b": 1 }, { "b": null } ] } => true
            // { "a": [ { "b": 1 }, { "b": 2 } ] } => false
            MultiValue::Multiple(vals) => vals.iter().any(|val| val.is_null()),
        }
    }
}

impl<T> IntoIterator for MultiValue<T> {
    type Item = T;
    // propagate to Vec internal iterator
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Single(None) => vec![].into_iter(),
            Self::Single(Some(a)) => vec![a].into_iter(),
            Self::Multiple(vec) => vec.into_iter(),
        }
    }
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
            let mut values: MultiValue<_> = MultiValue::default();
            for (i, value) in array.iter().enumerate() {
                if let Some(array_index) = array_index {
                    if i == array_index as usize {
                        match rest_of_path {
                            Some(rest_path) => {
                                // expect an Object if there is a rest path
                                if let Value::Object(map) = value {
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

pub fn get_value_from_json_map_opt<'a>(
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
                        Some(value) => rest_path.is_empty().then_some(MultiValue::one(value)),
                        None => None,
                    }
                }
            }
        }
        None => match parse_array_path(path) {
            Some((array_element_path, array_index)) => {
                focus_array_path(array_element_path, array_index, None, json_map)
            }
            None => json_map.get(path).map(MultiValue::one),
        },
    }
}

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
                    if let Some(v) = dest.get_mut(element) {
                        if let Value::Object(map) = v {
                            set_value_to_json_map(rest_path, map, src);
                        }
                    } else {
                        // insert new one
                        if !rest_path.is_empty() {
                            dest.insert(element.to_owned(), Value::Object(Default::default()));
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
                } else if let Some(v) = dest.get_mut(path) {
                    if let Value::Object(map) = v {
                        merge_map(map, src);
                    }
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
                    if let Some(rest_path) = rest_path {
                        if let Value::Object(map) = value {
                            set_value_to_json_map(rest_path, map, src);
                        }
                    } else if let Value::Object(map) = value {
                        merge_map(map, src);
                    }
                }
            } else if let Some(rest_path) = rest_path {
                if let Value::Object(map) = value {
                    set_value_to_json_map(rest_path, map, src);
                }
            } else if let Value::Object(map) = value {
                merge_map(map, src);
            }
        }
    } else if dest.is_empty() {
        // insert new one
        let array_len = array_index.map(|i| i + 1).unwrap_or(1);
        let mut array = vec![Value::Null; (array_len - 1) as usize];
        if let Some(rest_path) = rest_path {
            array.push(Value::Object(Default::default()));
            dest.insert(array_path.to_owned(), Value::Array(array));
            set_by_array_path(array_path, array_index, Some(rest_path), dest, src);
        } else {
            array.push(Value::Object(src.clone()));
            dest.insert(array_path.to_owned(), Value::Array(array));
            set_by_array_path(array_path, array_index, None, dest, src);
        }
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
                        return MultiValue::one(array.remove(array_index as usize));
                    }
                } else {
                    return MultiValue::one(Value::Array(std::mem::take(array)));
                }
            }
            Some(rest_path) => {
                // dig deeper
                let mut values = MultiValue::default();
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
    // no array found
    MultiValue::default()
}

/// Remove value at a given JSON path from JSON map
///
/// performance: the function could be improved by using the Entry API instead of BTreeMap.get_mut
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
                        MultiValue::option(json_map.remove(element))
                    } else {
                        match json_map.get_mut(element) {
                            None => MultiValue::default(),
                            Some(Value::Object(map)) => remove_value_from_json_map(rest_path, map),
                            Some(_value) => MultiValue::default(),
                        }
                    }
                }
            }
        }
        None => match parse_array_path(path) {
            Some((array_element_path, array_index)) => {
                delete_array_path(array_element_path, array_index, None, json_map)
            }
            None => MultiValue::option(json_map.remove(path)),
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
    mut path: String,
    value: &'a Value,
    filter: &dyn Fn(&str, &Value) -> bool,
) -> (String, Value) {
    let value = match &value {
        Value::Null => value.clone(),
        Value::Bool(_) => value.clone(),
        Value::Number(_) => value.clone(),
        Value::String(_) => value.clone(),
        Value::Array(array) => {
            let mut new_array = Vec::new();
            path.push_str("[]");
            for value in array.iter() {
                if filter(&path, value) {
                    let (path_, value) = _filter_json_values(path, value, filter);
                    path = path_;
                    new_array.push(value);
                }
            }
            path.truncate(path.len() - 2);
            Value::Array(new_array)
        }
        Value::Object(object) => {
            let mut new_object = serde_json::Map::new();
            for (key, value) in object.iter() {
                if !path.is_empty() {
                    path.push('.');
                }
                path.push_str(key);
                if filter(&path, value) {
                    let (path_, value) = _filter_json_values(path, value, filter);
                    path = path_;
                    new_object.insert(key.clone(), value);
                }
                path.truncate(path.len() - key.len());
                if !path.is_empty() {
                    path.pop();
                }
            }
            Value::Object(new_object)
        }
    };
    (path, value)
}

/// Filter json map based on external filter function
///
/// Filter function takes path and value as input and returns true if the value should be kept
pub fn filter_json_values(
    json_map: &serde_json::Map<String, Value>,
    filter: impl Fn(&str, &Value) -> bool,
) -> serde_json::Map<String, Value> {
    let path = "".to_string();
    let (_, res) = _filter_json_values(path, &Value::Object(json_map.clone()), &filter);

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

/// Light abstraction over a JSON path to avoid concatenating strings
#[derive(Debug, Clone)]
pub struct JsonPathPayload {
    pub path: String,
}

impl JsonPathPayload {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    pub fn extend(&self, segment: &str) -> Self {
        let full_path = format!("{}.{}", self.path, segment);
        JsonPathPayload::new(full_path)
    }

    pub fn extend_or_new(base: Option<&Self>, segment: &str) -> Self {
        match base {
            Some(path) => path.extend(segment),
            None => JsonPathPayload::new(segment.to_string()),
        }
    }
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
            get_value_from_json_map("a.b", &map).values(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // going deeper
        assert_eq!(
            get_value_from_json_map("a.b.c", &map).values(),
            vec![&Value::Number(1.into())]
        );

        // missing path
        assert!(get_value_from_json_map("a.b.c.d", &map).check_is_empty());
    }

    #[test]
    fn test_is_empty() {
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
        let multivalue = get_value_from_json_map("a[].b", &map);
        let is_empty = multivalue.check_is_empty();

        assert!(!is_empty, "a[].b is not empty");

        let multivalue = get_value_from_json_map("a[].c", &map);
        let is_empty = multivalue.check_is_empty();

        assert!(is_empty, "a[].c is empty");

        let multivalue = get_value_from_json_map("a[].d", &map);
        let is_empty = multivalue.check_is_empty();
        assert!(is_empty, "a[].d is empty");

        let multivalue = get_value_from_json_map("a[].f", &map);
        let is_empty = multivalue.check_is_empty();
        assert!(is_empty, "a[].f is empty");
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
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        )
        .unwrap();

        // get JSON array
        assert_eq!(
            get_value_from_json_map("a.b", &map).values(),
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
            get_value_from_json_map("a.b[]", &map).values(),
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
            get_value_from_json_map("a.b[].c", &map).values(),
            vec![&Value::Number(1.into()), &Value::Number(2.into())]
        );

        // project object field through array
        assert_eq!(
            get_value_from_json_map("a.b[].d", &map).values(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "e".to_string(),
                Value::Number(3.into())
            )]))]
        );

        // select scalar element from array
        assert_eq!(
            get_value_from_json_map("a.b[0]", &map).values(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // select scalar object from array different index
        assert_eq!(
            get_value_from_json_map("a.b[1]", &map).values(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(2.into())
            )]))]
        );

        // select field element from array different index
        assert_eq!(
            get_value_from_json_map("a.b[1].c", &map).values(),
            vec![&Value::Number(2.into())]
        );

        // select scalar element from array different index
        assert_eq!(
            get_value_from_json_map("g[2]", &map).values(),
            vec![&Value::String("g2".to_string())]
        );

        // select object element from array
        assert_eq!(
            get_value_from_json_map("a.b[2]", &map).values(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "d".to_string(),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "e".to_string(),
                    Value::Number(3.into())
                )]))
            )]))]
        );

        // select out of bound index from array
        assert!(get_value_from_json_map("a.b[3]", &map).check_is_empty());

        // select bad index from array
        assert!(get_value_from_json_map("a.b[z]", &map).check_is_empty());
    }

    #[test]
    fn test_get_deeply_nested_array_value_from_json_map() {
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
            get_value_from_json_map("arr1[].arr2[].a", &map).values(),
            vec![
                &Value::Number(1.into()),
                &Value::Number(3.into()),
                &Value::Number(5.into()),
            ]
        );
    }

    #[test]
    fn test_no_flatten_array_value_from_json_map() {
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
            get_value_from_json_map("arr[].a", &map).values(),
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
            get_value_from_json_map("arr[].a[]", &map).values(),
            vec![
                &Value::Number(1.into()),
                &Value::Number(2.into()),
                &Value::Number(3.into()),
            ]
        );
    }

    #[test]
    fn test_filter_json() {
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

        let res = filter_json_values(&map, |path, _value| {
            path.starts_with("a.b[].c") || "a.b[].c".starts_with(path)
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
    fn test_check_include_pattern() {
        assert!(check_include_pattern("a.b.c", "a.b.c"));
        assert!(check_include_pattern("a.b.c", "a.b"));
        assert!(!check_include_pattern("a.b.c", "a.b.d"));
        assert!(check_include_pattern("a.b.c", "a"));
        assert!(check_include_pattern("a", "a.d"));
    }

    #[test]
    fn test_check_exclude_pattern() {
        assert!(check_exclude_pattern("a.b.c", "a.b.c"));
        assert!(!check_exclude_pattern("a.b.c", "a.b"));
        assert!(!check_exclude_pattern("a.b.c", "a.b.d"));
        assert!(!check_exclude_pattern("a.b.c", "a"));
        assert!(check_exclude_pattern("a", "a.d"));
    }

    #[test]
    fn test_set_value_to_json_with_empty_key() {
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

        set_value_to_json_map("", &mut map, &src);

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
    fn test_set_value_to_json_with_one_level_key() {
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

        set_value_to_json_map("a", &mut map, &src);

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
    fn test_set_value_to_json_with_array_index() {
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

        set_value_to_json_map("a.b[1]", &mut map, &src);

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
    fn test_set_value_to_json_with_empty_src() {
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

        set_value_to_json_map("a.b[1]", &mut map, &src);

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
    fn test_set_value_to_json_with_empty_dest() {
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

        set_value_to_json_map("", &mut map, &src);

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
    fn test_set_value_to_json_with_empty_dest_nested_key() {
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

        set_value_to_json_map("key1.key2", &mut map, &src);

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
    fn test_set_value_to_json_with_empty_dest_nested_array_index_key() {
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

        set_value_to_json_map("key1.key2[3]", &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "key1": {"key2": [null, null, null, { "c": 1 }] }
                }
                "#,
            )
            .unwrap()
        );
    }
}
