use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};

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

impl Anonymize for String {
    fn anonymize(&self) -> Self {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish().to_string()
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

impl Anonymize for DateTime<Utc> {
    fn anonymize(&self) -> Self {
        let coeff: f32 = rand::random();

        *self
            + chrono::Duration::try_seconds(((coeff * 20.0) - 10.0) as i64)
                .expect("Failed to create TimeDelta")
    }
}
