use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

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

impl<K: Anonymize + Hash + Eq, V: Anonymize> Anonymize for HashMap<K, V> {
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
        let leading_zeros = self.leading_zeros();
        let skip_bytes_count = if leading_zeros > 4 {
            leading_zeros - 4
        } else {
            0
        };
        (self >> skip_bytes_count) << skip_bytes_count
    }
}
