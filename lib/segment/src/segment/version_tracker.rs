use std::collections::HashMap;

use crate::types::{SeqNumberType, VectorNameBuf};

#[derive(Clone, Debug, Default)]
pub struct VersionTracker {
    vector_storage: HashMap<VectorNameBuf, SeqNumberType>,
    payload_storage: Option<SeqNumberType>,
}

impl VersionTracker {
    pub fn get_vector(&self, vector: &str) -> Option<SeqNumberType> {
        self.vector_storage.get(vector).copied()
    }

    pub fn set_vector(&mut self, vector: &str, version: Option<SeqNumberType>) {
        let Some(new) = version else {
            self.vector_storage.remove(vector);
            return;
        };

        let Some(current) = self.vector_storage.get_mut(vector) else {
            self.vector_storage.insert(vector.into(), new);
            return;
        };

        if *current < new {
            *current = new;
        } else {
            self.vector_storage.remove(vector);
        }
    }

    pub fn get_payload(&self) -> Option<SeqNumberType> {
        self.payload_storage
    }

    pub fn set_payload(&mut self, version: Option<SeqNumberType>) {
        self.payload_storage = bump(self.payload_storage, version);
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
