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

pub trait MapIndexKey: Key + StoredValue + Eq + Display + Debug + 'static {
    type Owned: Borrow<Self> + Hash + Eq + Ord + Clone + FromStr + Default + 'static;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned;

    /// Whether this key type supports the prefix index; gates writing its
    /// file at build time. Only string keys support it.
    const SUPPORTS_PREFIX_INDEX: bool = false;

    /// Natural byte representation used by the prefix index
    /// ([`super::prefix_index`]). `None` for key types without prefix
    /// matching semantics; only string keys support it.
    ///
    /// Byte-wise ordering of these representations must match `Ord` on
    /// [`Self::Owned`] so that sorted in-memory structures and the on-disk
    /// prefix index agree on key order.
    fn prefix_index_bytes(&self) -> Option<&[u8]> {
        None
    }

    /// Whether a key of this type starts with the given prefix key. `false`
    /// for key types without prefix matching semantics.
    fn starts_with(&self, _prefix: &Self) -> bool {
        false
    }

    /// Convert a [`FacetValue`] into this key's owned type, or `None` if the
    /// variant doesn't match (e.g. a keyword value against an integer index).
    fn from_facet_value(value: FacetValue) -> Option<<Self as MapIndexKey>::Owned>;

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

    const SUPPORTS_PREFIX_INDEX: bool = true;

    fn prefix_index_bytes(&self) -> Option<&[u8]> {
        Some(self.as_bytes())
    }

    fn starts_with(&self, prefix: &Self) -> bool {
        str::starts_with(self, prefix)
    }

    fn from_facet_value(value: FacetValue) -> Option<<Self as MapIndexKey>::Owned> {
        match value {
            FacetValue::Keyword(keyword) => Some(EcoString::from(keyword)),
            FacetValue::Uuid(_) | FacetValue::Int(_) | FacetValue::Bool(_) => None,
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

    fn from_facet_value(value: FacetValue) -> Option<<Self as MapIndexKey>::Owned> {
        match value {
            FacetValue::Int(int) => Some(int),
            FacetValue::Keyword(_) | FacetValue::Uuid(_) | FacetValue::Bool(_) => None,
        }
    }
}

impl MapIndexKey for UuidIntType {
    type Owned = UuidIntType;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned {
        *self
    }

    fn from_facet_value(value: FacetValue) -> Option<<Self as MapIndexKey>::Owned> {
        match value {
            FacetValue::Uuid(uuid) => Some(uuid),
            FacetValue::Keyword(_) | FacetValue::Int(_) | FacetValue::Bool(_) => None,
        }
    }
}
