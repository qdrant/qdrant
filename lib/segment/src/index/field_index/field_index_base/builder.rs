use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::field_index::FieldIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::bool_index::BoolIndex;
use crate::index::field_index::bool_index::immutable_bool_index::ImmutableBoolIndexBuilder;
use crate::index::field_index::bool_index::mutable_bool_index::MutableBoolIndexBuilder;
use crate::index::field_index::full_text_index::mmap_text_index::FullTextMmapIndexBuilder;
use crate::index::field_index::full_text_index::text_index::FullTextGridstoreIndexBuilder;
use crate::index::field_index::geo_index::{GeoMapIndexGridstoreBuilder, GeoMapIndexMmapBuilder};
use crate::index::field_index::map_index::{MapIndexGridstoreBuilder, MapIndexMmapBuilder};
use crate::index::field_index::null_index::NullIndex;
use crate::index::field_index::null_index::immutable_null_index::ImmutableNullIndexBuilder;
use crate::index::field_index::null_index::mutable_null_index::MutableNullIndexBuilder;
use crate::index::field_index::numeric_index::{
    NumericIndexGridstoreBuilder, NumericIndexMmapBuilder,
};
use crate::types::{DateTimePayloadType, FloatPayloadType, IntPayloadType, UuidIntType};

/// Common interface for all index builders.
pub trait FieldIndexBuilderTrait {
    /// The resulting type of the index.
    type FieldIndexType;

    /// Start building the index, e.g. create a database column or a directory.
    /// Expected to be called exactly once before any other method.
    fn init(&mut self) -> OperationResult<()>;

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    fn finalize(self) -> OperationResult<Self::FieldIndexType>;

    /// Create an empty index for testing purposes.
    #[cfg(test)]
    fn make_empty(mut self) -> OperationResult<Self::FieldIndexType>
    where
        Self: Sized,
    {
        self.init()?;
        self.finalize()
    }
}

/// Builders for all index types
pub enum FieldIndexBuilder {
    IntMmapIndex(NumericIndexMmapBuilder<IntPayloadType, IntPayloadType>),
    IntGridstoreIndex(NumericIndexGridstoreBuilder<IntPayloadType, IntPayloadType>),
    DatetimeMmapIndex(NumericIndexMmapBuilder<IntPayloadType, DateTimePayloadType>),
    DatetimeGridstoreIndex(NumericIndexGridstoreBuilder<IntPayloadType, DateTimePayloadType>),
    IntMapMmapIndex(MapIndexMmapBuilder<IntPayloadType>),
    IntMapGridstoreIndex(MapIndexGridstoreBuilder<IntPayloadType>),
    KeywordMmapIndex(MapIndexMmapBuilder<str>),
    KeywordGridstoreIndex(MapIndexGridstoreBuilder<str>),
    FloatMmapIndex(NumericIndexMmapBuilder<FloatPayloadType, FloatPayloadType>),
    FloatGridstoreIndex(NumericIndexGridstoreBuilder<FloatPayloadType, FloatPayloadType>),
    GeoMmapIndex(GeoMapIndexMmapBuilder),
    GeoGridstoreIndex(GeoMapIndexGridstoreBuilder),
    FullTextMmapIndex(FullTextMmapIndexBuilder),
    FullTextGridstoreIndex(FullTextGridstoreIndexBuilder),
    BoolMmapIndex(ImmutableBoolIndexBuilder),
    BoolGridstoreIndex(MutableBoolIndexBuilder),
    UuidMmapIndex(MapIndexMmapBuilder<UuidIntType>),
    UuidGridstoreIndex(MapIndexGridstoreBuilder<UuidIntType>),
    MutableNullIndex(MutableNullIndexBuilder),
    ImmutableNullIndex(ImmutableNullIndexBuilder),
}

impl FieldIndexBuilderTrait for FieldIndexBuilder {
    type FieldIndexType = FieldIndex;

    fn init(&mut self) -> OperationResult<()> {
        match self {
            Self::IntMmapIndex(index) => index.init(),
            Self::IntGridstoreIndex(index) => index.init(),
            Self::DatetimeMmapIndex(index) => index.init(),
            Self::DatetimeGridstoreIndex(index) => index.init(),
            Self::IntMapMmapIndex(index) => index.init(),
            Self::IntMapGridstoreIndex(index) => index.init(),
            Self::KeywordMmapIndex(index) => index.init(),
            Self::KeywordGridstoreIndex(index) => index.init(),
            Self::FloatMmapIndex(index) => index.init(),
            Self::FloatGridstoreIndex(index) => index.init(),
            Self::GeoMmapIndex(index) => index.init(),
            Self::GeoGridstoreIndex(index) => index.init(),
            Self::BoolMmapIndex(index) => index.init(),
            Self::BoolGridstoreIndex(index) => index.init(),
            Self::FullTextMmapIndex(builder) => builder.init(),
            Self::FullTextGridstoreIndex(builder) => builder.init(),
            Self::UuidMmapIndex(index) => index.init(),
            Self::UuidGridstoreIndex(index) => index.init(),
            Self::MutableNullIndex(index) => index.init(),
            Self::ImmutableNullIndex(index) => index.init(),
        }
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            Self::IntMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::IntGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            Self::DatetimeMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::DatetimeGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            Self::IntMapMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::IntMapGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            Self::KeywordMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::KeywordGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            Self::FloatMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::FloatGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            Self::GeoMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::GeoGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            Self::BoolGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            Self::BoolMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::FullTextMmapIndex(builder) => {
                FieldIndexBuilderTrait::add_point(builder, id, payload, hw_counter)
            }
            Self::FullTextGridstoreIndex(builder) => {
                FieldIndexBuilderTrait::add_point(builder, id, payload, hw_counter)
            }
            Self::UuidMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::UuidGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            Self::MutableNullIndex(index) => index.add_point(id, payload, hw_counter),
            Self::ImmutableNullIndex(index) => index.add_point(id, payload, hw_counter),
        }
    }

    fn finalize(self) -> OperationResult<FieldIndex> {
        Ok(match self {
            Self::IntMmapIndex(index) => FieldIndex::IntIndex(index.finalize()?),
            Self::IntGridstoreIndex(index) => FieldIndex::IntIndex(index.finalize()?),
            Self::DatetimeMmapIndex(index) => FieldIndex::DatetimeIndex(index.finalize()?),
            Self::DatetimeGridstoreIndex(index) => FieldIndex::DatetimeIndex(index.finalize()?),
            Self::IntMapMmapIndex(index) => FieldIndex::IntMapIndex(index.finalize()?),
            Self::IntMapGridstoreIndex(index) => FieldIndex::IntMapIndex(index.finalize()?),
            Self::KeywordMmapIndex(index) => FieldIndex::KeywordIndex(index.finalize()?),
            Self::KeywordGridstoreIndex(index) => FieldIndex::KeywordIndex(index.finalize()?),
            Self::FloatMmapIndex(index) => FieldIndex::FloatIndex(index.finalize()?),
            Self::FloatGridstoreIndex(index) => FieldIndex::FloatIndex(index.finalize()?),
            Self::GeoMmapIndex(index) => FieldIndex::GeoIndex(index.finalize()?),
            Self::GeoGridstoreIndex(index) => FieldIndex::GeoIndex(index.finalize()?),
            Self::BoolGridstoreIndex(index) => {
                FieldIndex::BoolIndex(BoolIndex::from(index.finalize()?))
            }
            Self::BoolMmapIndex(index) => FieldIndex::BoolIndex(BoolIndex::from(index.finalize()?)),
            Self::FullTextMmapIndex(builder) => FieldIndex::FullTextIndex(builder.finalize()?),
            Self::FullTextGridstoreIndex(builder) => FieldIndex::FullTextIndex(builder.finalize()?),
            Self::UuidMmapIndex(index) => FieldIndex::UuidMapIndex(index.finalize()?),
            Self::UuidGridstoreIndex(index) => FieldIndex::UuidMapIndex(index.finalize()?),
            Self::MutableNullIndex(index) => {
                FieldIndex::NullIndex(NullIndex::from(index.finalize()?))
            }
            Self::ImmutableNullIndex(index) => {
                FieldIndex::NullIndex(NullIndex::from(index.finalize()?))
            }
        })
    }
}
