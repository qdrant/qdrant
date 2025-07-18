use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use crate::json_path::JsonPath;
use crate::types::{SeqNumberType, VectorNameBuf};

#[derive(Clone, Debug, Default)]
pub struct VersionTracker {
    vector_storage: HashMap<VectorNameBuf, SeqNumberType>,
    payload_storage: Option<SeqNumberType>,
    payload_index: HashMap<JsonPath, SeqNumberType>,
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

    pub fn get_payload_index(&self, field: &JsonPath) -> Option<SeqNumberType> {
        self.payload_index.get(field).copied()
    }

    pub fn set_payload_index(&mut self, field: &JsonPath, version: Option<SeqNumberType>) {
        bump_key(&mut self.payload_index, field, version)
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
