use std::marker::PhantomData;

use blobstore::Blob;
use serde_json::Value;

use super::super::{Encodable, NumericIndex, NumericIndexIntoInnerValue};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::field_index::{UpdateOnlyIndexKind, ValueIndexer};

/// Writes what [`MutableNumericIndex`] persists: the point's values, encoded to
/// the index's own numeric type.
///
/// `P` is the type read out of the payload and `T` the type stored, which are
/// the same for the integer and float indexes and differ for the datetime and
/// UUID ones. Both the extraction and the encoding are taken from
/// [`NumericIndex`] itself, so a new numeric index type is writable here as
/// soon as it implements them.
///
/// [`MutableNumericIndex`]: super::MutableNumericIndex
pub struct UpdateOnlyNumericKind<T, P>(PhantomData<(T, P)>);

impl<T, P> Default for UpdateOnlyNumericKind<T, P> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T, P> UpdateOnlyIndexKind for UpdateOnlyNumericKind<T, P>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    NumericIndex<T, P>: ValueIndexer<ValueType = P> + NumericIndexIntoInnerValue<T, P>,
    Vec<T>: Blob,
{
    type Stored = Vec<T>;

    fn extract(&self, values: &[&Value]) -> OperationResult<Option<Vec<T>>> {
        let stored: Vec<T> = <NumericIndex<T, P> as ValueIndexer>::flatten_values(values)
            .into_iter()
            .map(<NumericIndex<T, P>>::into_inner_value)
            .collect();

        Ok((!stored.is_empty()).then_some(stored))
    }
}
