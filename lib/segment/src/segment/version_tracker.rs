use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use crate::json_path::JsonPath;
use crate::types::{SeqNumberType, VectorNameBuf};

/// Tracks versions of different sub-structures of segment to optimize partial snapshots.
#[derive(Clone, Debug, Default)]
pub struct VersionTracker {
    /// Tracks version of *mutable* files inside vector storage.
    /// Should be updated when vector storage is modified.
    vector_storage: HashMap<VectorNameBuf, SeqNumberType>,

    /// Tracks version of *mutable* files inside payload storage.
    /// Should be updated when payload storage is modified.
    payload_storage: Option<SeqNumberType>,

    /// Tracks version of *immutable* files inside payload index.
    /// Should be updated when payload index *schema* of the field is modified.
    ///
    /// Generally, we can rely on `Segment::initial_version` to filter immutable files.
    /// E.g., HNSW index is created when creating immutable segment, and so immutable files
    /// of HNSW index stays immutable for the whole lifetime of the segment.
    ///
    /// However, payload indices can be updated at any moment even for immutable segments.
    /// E.g., payload index can be created *after* immutable segment is created, and so we have to
    /// track payload index schema version separately from `Segment::initial_version`.
    payload_index_schema: HashMap<JsonPath, SeqNumberType>,
}

impl VersionTracker {
    pub fn get_vector(&self, vector: &str) -> Option<SeqNumberType> {
        self.vector_storage.get(vector).copied()
    }

    pub fn set_vector(&mut self, vector: &str, version: Option<SeqNumberType>) {
        bump_key(&mut self.vector_storage, vector, version)
    }

    pub fn get_payload(&self) -> Option<SeqNumberType> {
        self.payload_storage
    }

    pub fn set_payload(&mut self, version: Option<SeqNumberType>) {
        self.payload_storage = bump(self.payload_storage, version);
    }

    pub fn get_payload_index_schema(&self, field: &JsonPath) -> Option<SeqNumberType> {
        self.payload_index_schema.get(field).copied()
    }

    pub fn set_payload_index_schema(&mut self, field: &JsonPath, version: Option<SeqNumberType>) {
        bump_key(&mut self.payload_index_schema, field, version)
    }
}

fn bump(current: Option<SeqNumberType>, new: Option<SeqNumberType>) -> Option<SeqNumberType> {
    match (current, new) {
        (Some(current), Some(new)) => {
            if current < new {
                Some(new)
            } else {
                None
            }
        }

        (None, Some(new)) => Some(new),
        (_, None) => None,
    }
}

fn bump_key<K, Q>(map: &mut HashMap<K, SeqNumberType>, key: &Q, version: Option<SeqNumberType>)
where
    K: Hash + Eq + Borrow<Q>,
    Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
{
    let Some(new) = version else {
        map.remove(key);
        return;
    };

    let Some(current) = map.get_mut(key) else {
        map.insert(key.to_owned(), new);
        return;
    };

    if *current < new {
        *current = new;
    } else {
        map.remove(key);
    }
}
