use std::borrow::Borrow;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::str::FromStr;

use common::persisted_hashmap::Key;
use ecow::EcoString;

use super::BLOCK_SIZE_KEYWORD;
use crate::index::field_index::stored_point_to_values::StoredValue;
use crate::types::{IntPayloadType, UuidIntType};

pub trait MapIndexKey: Key + StoredValue + Eq + Display + Debug {
    type Owned: Borrow<Self> + Hash + Eq + Clone + FromStr + Default + 'static;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned;

    fn gridstore_block_size() -> usize {
        size_of::<<Self as MapIndexKey>::Owned>()
    }

    /// Extra heap bytes for an owned value beyond `size_of::<Owned>()`.
    /// Override for types with heap allocations (e.g., strings).
    fn owned_heap_bytes(_value: &<Self as MapIndexKey>::Owned) -> usize {
        0
    }
}

impl MapIndexKey for str {
    type Owned = EcoString;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned {
        EcoString::from(self)
    }

    fn gridstore_block_size() -> usize {
        BLOCK_SIZE_KEYWORD
    }

    fn owned_heap_bytes(value: &<Self as MapIndexKey>::Owned) -> usize {
        if value.len() > EcoString::INLINE_LIMIT {
            value.len()
        } else {
            0
        }
    }
}

impl MapIndexKey for IntPayloadType {
    type Owned = IntPayloadType;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned {
        *self
    }
}

impl MapIndexKey for UuidIntType {
    type Owned = UuidIntType;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned {
        *self
    }
}
