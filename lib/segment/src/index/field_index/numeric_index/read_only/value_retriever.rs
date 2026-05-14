//! JSON value retrieval for [`ReadOnlyNumericIndex`].
//!
//! Every numeric variant builds the same retriever closure — iterate a
//! point's stored values and convert each to JSON. Only the per-value
//! `T -> Value` conversion is `(T, P)`-specific; it lives in the
//! [`NumericValueToJson`] trait, and the single generic `value_retriever`
//! below plugs it in. Mirror of the writable side in
//! `numeric_index/value_indexer.rs`.

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::Blob;
use serde_json::{Number, Value};

use super::super::Encodable;
use super::super::numeric_index_read::NumericIndexRead;
use super::ReadOnlyNumericIndex;
use crate::common::utils::MultiValue;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::stored_point_to_values::StoredValue;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, UuidIntType, UuidPayloadType,
};

/// Converts one stored numeric value into the JSON representation
/// dictated by the index's payload type `P`.
///
/// Keyed on the concrete `ReadOnlyNumericIndex<T, P, S>` so the same
/// stored type `T` can decode differently per `P` — e.g. an `i64` as a
/// plain number vs. a datetime string.
pub trait NumericValueToJson<T> {
    /// Convert one stored value. `None` drops it (e.g. a float with no
    /// finite JSON representation).
    fn value_to_json(value: T) -> Option<Value>;
}

impl<S: UniversalRead> NumericValueToJson<IntPayloadType>
    for ReadOnlyNumericIndex<IntPayloadType, IntPayloadType, S>
{
    fn value_to_json(value: IntPayloadType) -> Option<Value> {
        Some(Value::Number(Number::from(value)))
    }
}

impl<S: UniversalRead> NumericValueToJson<IntPayloadType>
    for ReadOnlyNumericIndex<IntPayloadType, DateTimePayloadType, S>
{
    fn value_to_json(value: IntPayloadType) -> Option<Value> {
        serde_json::to_value(DateTimePayloadType::from_timestamp(value)?).ok()
    }
}

impl<S: UniversalRead> NumericValueToJson<FloatPayloadType>
    for ReadOnlyNumericIndex<FloatPayloadType, FloatPayloadType, S>
{
    fn value_to_json(value: FloatPayloadType) -> Option<Value> {
        Some(Value::Number(Number::from_f64(value)?))
    }
}

impl<S: UniversalRead> NumericValueToJson<UuidIntType>
    for ReadOnlyNumericIndex<UuidIntType, UuidPayloadType, S>
{
    fn value_to_json(value: UuidIntType) -> Option<Value> {
        Some(Value::String(UuidPayloadType::from_u128(value).to_string()))
    }
}

impl<T, P, S> ReadOnlyNumericIndex<T, P, S>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    S: UniversalRead,
    Vec<T>: Blob,
    Self: NumericValueToJson<T>,
{
    /// Build a closure mapping a point id to its indexed values as JSON.
    ///
    /// Used by rescore-formula value lookup; mirrors
    /// `NumericIndex::value_retriever` on the writable side.
    pub fn value_retriever<'a>(
        &'a self,
        _hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id)
                .into_iter()
                .flatten()
                .filter_map(Self::value_to_json)
                .collect()
        })
    }
}
