//! Pending payload index changes buffered by a proxy segment.

use ahash::AHashMap;
use itertools::Itertools as _;

use super::change::ProxyIndexChange;
use crate::types::{PayloadFieldSchema, PayloadKeyType};

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

    /// Whether the wrapped segment's index on `key`, of `wrapped_schema`, is
    /// no longer the one the proxy presents: a pending `Delete`, a
    /// `DeleteIfIncompatible` it does not match, or a `Create` of another
    /// schema. The same reading as `ProxySegment::get_indexed_fields`.
    ///
    /// Only read paths for which the index is the data need this. A filter
    /// gives the same answer through an old index, so it does not; BM25 ranks
    /// by the index's tokenizer, vocabulary and lengths, so it does.
    pub fn is_wrapped_index_stale(
        &self,
        key: &PayloadKeyType,
        wrapped_schema: Option<&PayloadFieldSchema>,
    ) -> bool {
        match self.changes.get(key) {
            None => false,
            Some(ProxyIndexChange::Delete(_)) => true,
            Some(ProxyIndexChange::DeleteIfIncompatible(_, schema)) => {
                wrapped_schema.is_some_and(|wrapped| wrapped != schema)
            }
            Some(ProxyIndexChange::Create(schema, _)) => wrapped_schema != Some(schema),
        }
    }

    pub fn merge(&mut self, other: &Self) {
        for (key, change) in &other.changes {
            self.changes.insert(key.clone(), change.clone());
        }
    }
}
