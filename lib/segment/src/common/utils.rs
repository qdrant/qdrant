use std::collections::HashMap;
use std::ops::Bound;

use serde_json::Value;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::Vector;
use crate::index::field_index::FieldIndex;
use crate::types::PayloadKeyType;

pub type IndexesMap = HashMap<PayloadKeyType, Vec<FieldIndex>>;

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
mod tests {
    use super::*;
    use crate::json_path::{get_value_from_json_map, path};

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
        let multivalue = get_value_from_json_map(&path("a[].b"), &map);
        let is_empty = check_is_empty(multivalue.iter().copied());

        assert!(!is_empty, "a[].b is not empty");

        let multivalue = get_value_from_json_map(&path("a[].c"), &map);
        let is_empty = check_is_empty(multivalue.iter().copied());

        assert!(is_empty, "a[].c is empty");

        let multivalue = get_value_from_json_map(&path("a[].d"), &map);
        let is_empty = check_is_empty(multivalue.iter().copied());
        assert!(is_empty, "a[].d is empty");

        let multivalue = get_value_from_json_map(&path("a[].f"), &map);
        let is_empty = check_is_empty(multivalue.iter().copied());
        assert!(is_empty, "a[].f is empty");
    }
}
