use serde_json::Value;

use super::{JsonPath, JsonPathItem};

/// Filter json map based on external filter function
///
/// Filter function takes path and value as input and returns true if the value should be kept
pub fn filter_json_values(
    json_map: &serde_json::Map<String, Value>,
    filter: impl Fn(&JsonPath, &Value) -> bool,
) -> serde_json::Map<String, Value> {
    let mut new_map = serde_json::Map::new();
    let mut path = JsonPath {
        first_key: "".to_string(),
        rest: Vec::new(),
    };
    for (key, value) in json_map.iter() {
        path.first_key = key.clone();
        if filter(&path, value) {
            let value = run_filter(&mut path, value, &filter);
            new_map.insert(key.clone(), value);
        }
    }
    new_map
}

fn run_filter<'a>(
    path: &mut JsonPath,
    value: &'a Value,
    filter: &dyn Fn(&JsonPath, &Value) -> bool,
) -> Value {
    match &value {
        Value::Null => value.clone(),
        Value::Bool(_) => value.clone(),
        Value::Number(_) => value.clone(),
        Value::String(_) => value.clone(),
        Value::Array(array) => {
            let mut new_array = Vec::new();
            path.rest.push(JsonPathItem::WildcardIndex);
            for value in array.iter() {
                if filter(path, value) {
                    let value = run_filter(path, value, filter);
                    new_array.push(value);
                }
            }
            path.rest.pop();
            Value::Array(new_array)
        }
        Value::Object(object) => {
            let mut new_object = serde_json::Map::new();
            for (key, value) in object.iter() {
                path.rest.push(JsonPathItem::Key(key.clone()));
                if filter(path, value) {
                    let value = run_filter(path, value, filter);
                    new_object.insert(key.clone(), value);
                }
                path.rest.pop();
            }
            Value::Object(new_object)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
