use std::collections::HashMap;

use serde_json::Value;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::VectorElementType;

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

    pub(crate) fn values(self) -> Vec<T> {
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

impl<T> Iterator for MultiValue<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(opt) => opt.take(),
            Self::Multiple(vec) => vec.pop(),
        }
    }
}

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
) -> MultiValue<&'a Value> {
    match value.get(array_path) {
        Some(Value::Array(array)) => {
            let mut values: MultiValue<_> = MultiValue::default();
            for (i, value) in array.iter().enumerate() {
                if let Some(array_index) = array_index {
                    if i == array_index as usize {
                        match rest_path {
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
                    match rest_path {
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
            values
        }
        _ => MultiValue::default(),
    }
}

pub fn get_value_from_json_map<'a>(
    path: &str,
    value: &'a serde_json::Map<String, Value>,
) -> MultiValue<&'a Value> {
    // check if leaf path element
    match path.split_once('.') {
        Some((element, rest_path)) => {
            // check if targeting array
            match parse_array_path(element) {
                Some((array_element_path, array_index)) => {
                    focus_array_path(array_element_path, array_index, Some(rest_path), value)
                }
                None => {
                    // no array notation
                    match value.get(element) {
                        Some(Value::Object(map)) => get_value_from_json_map(rest_path, map),
                        Some(value) => match rest_path.is_empty() {
                            true => MultiValue::one(value),
                            false => MultiValue::default(),
                        },
                        None => MultiValue::default(),
                    }
                }
            }
        }
        None => match parse_array_path(path) {
            Some((array_element_path, array_index)) => {
                focus_array_path(array_element_path, array_index, None, value)
            }
            None => match value.get(path) {
                Some(value) => MultiValue::one(value),
                None => MultiValue::default(),
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
) -> MultiValue<Value> {
    if let Some(Value::Array(array)) = value.get_mut(array_path) {
        match rest_path {
            None => {
                // end of path - delete and collect
                if let Some(array_index) = array_index {
                    if array.len() > array_index as usize {
                        return MultiValue::one(array.remove(array_index as usize));
                    }
                } else {
                    return MultiValue::one(Value::Array(array.drain(..).collect()));
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

pub fn remove_value_from_json_map(
    path: &str,
    value: &mut serde_json::Map<String, Value>,
) -> MultiValue<Value> {
    // check if leaf path element
    match path.split_once('.') {
        Some((element, rest_path)) => {
            // check if targeting array
            match parse_array_path(element) {
                Some((array_element_path, array_index)) => {
                    delete_array_path(array_element_path, array_index, Some(rest_path), value)
                }
                None => {
                    // no array notation
                    if rest_path.is_empty() {
                        MultiValue::option(value.remove(element))
                    } else {
                        match value.get_mut(element) {
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
                delete_array_path(array_element_path, array_index, None, value)
            }
            None => MultiValue::option(value.remove(path)),
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
}
