//! Pending payload index changes buffered by a proxy segment.

use ahash::AHashMap;
use itertools::Itertools as _;

use super::change::ProxyIndexChange;
use crate::types::PayloadKeyType;

#[derive(Debug, Default)]
pub struct ProxyIndexChanges {
    changes: AHashMap<PayloadKeyType, ProxyIndexChange>,
}

impl ProxyIndexChanges {
    pub fn insert(&mut self, key: PayloadKeyType, change: ProxyIndexChange) {
        self.changes.insert(key, change);
    }

    pub fn remove(&mut self, key: &PayloadKeyType) {
        self.changes.remove(key);
    }

    pub fn len(&self) -> usize {
        self.changes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn clear(&mut self) {
        self.changes.clear();
    }

    /// Iterate over proxy index changes in order of version.
    ///
    /// Index changes must be applied in order because changes with an old version will silently be
    /// rejected.
    pub fn iter_ordered(&self) -> impl Iterator<Item = (&PayloadKeyType, &ProxyIndexChange)> {
        self.changes
            .iter()
            .sorted_by_key(|(_, change)| change.version())
    }

    /// Iterate over proxy index changes in arbitrary order.
    pub fn iter_unordered(&self) -> impl Iterator<Item = (&PayloadKeyType, &ProxyIndexChange)> {
        self.changes.iter()
    }

    pub fn merge(&mut self, other: &Self) {
        for (key, change) in &other.changes {
            self.changes.insert(key.clone(), change.clone());
        }
    }
}
