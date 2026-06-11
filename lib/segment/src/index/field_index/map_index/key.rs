use std::borrow::Borrow;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::str::FromStr;

use common::persisted_hashmap::Key;
use ecow::EcoString;

use super::BLOCK_SIZE_KEYWORD;
use crate::data_types::facets::FacetValue;
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::types::{IntPayloadType, UuidIntType};

pub trait MapIndexKey: Key + StoredValue + Eq + Display + Debug {
    type Owned: Borrow<Self> + Hash + Eq + Clone + FromStr + Default + 'static;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned;

    /// Borrow this key type out of a [`FacetValue`], or `None` if the variant
    /// doesn't match (e.g. a keyword value against an integer index).
    fn from_facet_value(value: &FacetValue) -> Option<&Self>;

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

    fn from_facet_value(value: &FacetValue) -> Option<&Self> {
        match value {
            FacetValue::Keyword(keyword) => Some(keyword.as_str()),
            _ => None,
        }
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

    fn from_facet_value(value: &FacetValue) -> Option<&Self> {
        match value {
            FacetValue::Int(int) => Some(int),
            _ => None,
        }
    }
}

impl MapIndexKey for UuidIntType {
    type Owned = UuidIntType;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned {
        *self
    }

    fn from_facet_value(value: &FacetValue) -> Option<&Self> {
        match value {
            FacetValue::Uuid(uuid) => Some(uuid),
            _ => None,
        }
    }
}
