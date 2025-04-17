use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};
pub use macros::Anonymize;
use uuid::Uuid;

/// This trait provides a derive macro.
///
/// # Usage example
///
/// ```ignore
/// #[derive(Anonymize)]
/// struct Test {
///    foo: Foo,
///    bar: Bar,
///    baz: Baz,
/// }
/// ```
///
/// This will generate code that calls `anonymize()` recursively on each field:
/// ```ignore
/// impl Anonymize for Test {
///     fn anonymize(&self) -> Self {
///         Self {
///             foo: Anonymize::anonymize(&self.foo),
///             bar: Anonymize::anonymize(&self.bar),
///             baz: Anonymize::anonymize(&self.baz),
///         }
///     }
/// }
/// ```
///
/// # Attributes
///
/// The following attributes can be used to customize the behavior:
/// - `#[anonymize(true)]` to enable anonymization for a field (default).
/// - `#[anonymize(false)]` to disable anonymization for a field.
///   An equivalent of `#[anonymize(with = Clone::clone)]`.
/// - `#[anonymize(value = None)]` to specify a value to replace the field with.
/// - `#[anonymize(with = path:to:function)]` to specify a custom function.
pub trait Anonymize {
    fn anonymize(&self) -> Self;
}

impl<T: Anonymize> Anonymize for Option<T> {
    fn anonymize(&self) -> Self {
        self.as_ref().map(|t| t.anonymize())
    }
}

impl<T: Anonymize> Anonymize for Vec<T> {
    fn anonymize(&self) -> Self {
        self.iter().map(|e| e.anonymize()).collect()
    }
}

impl<T: Anonymize> Anonymize for Box<T> {
    fn anonymize(&self) -> Self {
        Box::new(self.as_ref().anonymize())
    }
}

impl<K: Anonymize + Hash + Eq, V: Anonymize> Anonymize for HashMap<K, V> {
    fn anonymize(&self) -> Self {
        self.iter()
            .map(|(k, v)| (k.anonymize(), v.anonymize()))
            .collect()
    }
}

impl<K: Anonymize + Eq + Ord, V: Anonymize> Anonymize for BTreeMap<K, V> {
    fn anonymize(&self) -> Self {
        self.iter()
            .map(|(k, v)| (k.anonymize(), v.anonymize()))
            .collect()
    }
}

/// Anonymize the values of a collection, but keeps the keys intact.
pub fn anonymize_collection_values<C, K, V>(collection: &C) -> C
where
    for<'a> &'a C: IntoIterator<Item = (&'a K, &'a V)>,
    C: FromIterator<(K, V)>,
    K: Clone,
    V: Anonymize,
{
    collection
        .into_iter()
        .map(|(k, v)| (k.clone(), v.anonymize()))
        .collect()
}

pub fn hash_u64(value: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

pub fn anonymize_collection_with_u64_hashable_key<C, V>(collection: &C) -> C
where
    for<'a> &'a C: IntoIterator<Item = (&'a u64, &'a V)>,
    C: FromIterator<(u64, V)>,
    V: Anonymize,
{
    collection
        .into_iter()
        .map(|(k, v)| (hash_u64(*k), v.anonymize()))
        .collect()
}

impl Anonymize for String {
    fn anonymize(&self) -> Self {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish().to_string()
    }
}

impl Anonymize for Uuid {
    fn anonymize(&self) -> Self {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        Uuid::from_u128(u128::from(hasher.finish()))
    }
}

impl Anonymize for usize {
    fn anonymize(&self) -> Self {
        let log10 = (*self as f32).log10().round() as u32;
        if log10 > 4 {
            let skip_digits = log10 - 4;
            let coeff = 10usize.pow(skip_digits);
            (*self / coeff) * coeff
        } else {
            *self
        }
    }
}

impl Anonymize for bool {
    fn anonymize(&self) -> Self {
        *self
    }
}

impl Anonymize for DateTime<Utc> {
    fn anonymize(&self) -> Self {
        let coeff: f32 = rand::random();

        *self + chrono::Duration::try_seconds(((coeff * 20.0) - 10.0) as i64).unwrap_or_default()
    }
}

impl Anonymize for serde_json::Value {
    fn anonymize(&self) -> Self {
        match self {
            serde_json::Value::Null => serde_json::Value::Null,
            serde_json::Value::Bool(b) => serde_json::Value::Bool(b.anonymize()),
            serde_json::Value::Number(n) => serde_json::Value::Number(n.clone()),
            serde_json::Value::String(s) => serde_json::Value::String(s.anonymize()),
            serde_json::Value::Array(a) => {
                serde_json::Value::Array(a.iter().map(|v| v.anonymize()).collect())
            }
            serde_json::Value::Object(o) => serde_json::Value::Object(
                o.iter()
                    .map(|(k, v)| (k.anonymize(), v.anonymize()))
                    .collect(),
            ),
        }
    }
}
