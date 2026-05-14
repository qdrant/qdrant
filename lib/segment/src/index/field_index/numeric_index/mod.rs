mod builders;
pub mod immutable_numeric_index;
mod index;
mod lifecycle;
pub mod mmap_numeric_index;
pub mod mutable_numeric_index;
mod numeric_field_index;
pub mod numeric_index_read;
mod read_ops;
mod storage;
mod value_indexer;

pub use builders::{NumericIndexBuilder, NumericIndexGridstoreBuilder, NumericIndexMmapBuilder};
pub use index::{NumericIndex, NumericIndexIntoInnerValue};
pub use lifecycle::Encodable;
pub use numeric_field_index::{NumericFieldIndex, NumericFieldIndexRead};
pub use numeric_index_read::NumericIndexRead;
pub use read_ops::StreamRange;
pub use storage::NumericIndexInner;
pub use storage::read_only::ReadOnlyNumericIndexInner;

#[cfg(test)]
mod tests;
