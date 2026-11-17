use std::marker::PhantomData;

use blobstore::Blob;
use serde_json::Value;

use super::super::{MapIndex, MapIndexKey};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{UpdateOnlyIndexKind, ValueIndexer};

/// Writes what [`MutableMapIndex`] persists: the point's keys, owned. The
/// prefix index the keyword variant can also keep is rebuilt from them on open.
///
/// [`MutableMapIndex`]: super::MutableMapIndex
pub struct UpdateOnlyMapKind<N: MapIndexKey + ?Sized>(PhantomData<&'static N>);

impl<N: MapIndexKey + ?Sized> Default for UpdateOnlyMapKind<N> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<N> UpdateOnlyIndexKind for UpdateOnlyMapKind<N>
where
    N: MapIndexKey + ?Sized,
    MapIndex<N>: ValueIndexer,
    // The keyword index reads `String` out of the payload and stores the compact `EcoString`
    <MapIndex<N> as ValueIndexer>::ValueType: Into<<N as MapIndexKey>::Owned>,
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    type Stored = Vec<<N as MapIndexKey>::Owned>;

    fn extract(&self, values: &[&Value]) -> OperationResult<Option<Self::Stored>> {
        let stored: Self::Stored = <MapIndex<N> as ValueIndexer>::flatten_values(values)
            .into_iter()
            .map(Into::into)
            .collect();

        Ok((!stored.is_empty()).then_some(stored))
    }
}
