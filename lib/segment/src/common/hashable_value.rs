use std::hash::{Hash, Hasher};

use serde_json::Value;

/// Wrapper around `serde_json::Value` that implements `std::hash::Hash`.
/// Reference: https://github.com/serde-rs/json/issues/747
pub struct HashableValue(pub Value);

impl Hash for HashableValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_value_to(&self.0, state)
    }
}

fn hash_value_to<H: Hasher>(val: &Value, hash: &mut H) {
    match val {
        Value::Null => (),
        Value::Bool(b) => b.hash(hash),
        Value::Number(n) => n.hash(hash),
        Value::String(s) => s.hash(hash),
        Value::Array(a) => {
            for i in a {
                hash_value_to(i, hash);
            }
        }
        Value::Object(o) => {
            // The 'preserve_order' feature for serde_json is enabled, so iterating
            // this map is always in the same order.
            for (k, v) in o {
                k.hash(hash);
                hash_value_to(v, hash);
            }
        }
    }
}
