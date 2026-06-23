mod builders;
mod encodable;
pub mod immutable_numeric_index;
mod lifecycle;
pub mod mutable_numeric_index;
mod numeric_field_index;
pub mod numeric_index_read;
pub mod on_disk_numeric_index;
mod query;
mod read_only;
mod read_ops;
mod storage;
mod value_indexer;

use std::marker::PhantomData;

pub use builders::{NumericIndexBuilder, NumericIndexGridstoreBuilder, NumericIndexMmapBuilder};
pub use encodable::Encodable;
use gridstore::Blob;
pub use numeric_field_index::{
    NumericFieldIndex, NumericFieldIndexRead, NumericFieldIndexView, ReadOnlyNumericFieldIndex,
};
pub use numeric_index_read::NumericIndexRead;
pub use query::NumericIndexValue;
pub use read_only::{NumericValueToJson, ReadOnlyNumericIndex};
pub use read_ops::StreamRange;
pub use storage::NumericIndexInner;
pub use storage::read_only::ReadOnlyNumericIndexInner;

use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;

#[cfg(test)]
mod tests;

pub struct NumericIndex<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P>
where
    Vec<T>: Blob,
{
    inner: NumericIndexInner<T>,
    _phantom: PhantomData<P>,
}

pub trait NumericIndexIntoInnerValue<T, P> {
    fn into_inner_value(value: P) -> T;
}
