use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::field_index::FieldIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::bool_index::BoolIndex;
use crate::index::field_index::bool_index::immutable_bool_index::ImmutableBoolIndexBuilder;
use crate::index::field_index::bool_index::mutable_bool_index::MutableBoolIndexBuilder;
use crate::index::field_index::full_text_index::FullTextGridstoreIndexBuilder;
use crate::index::field_index::full_text_index::on_disk_text_index::FullTextMmapIndexBuilder;
use crate::index::field_index::geo_index::{GeoIndexGridstoreBuilder, GeoIndexMmapBuilder};
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
    fn init(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()>;

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    fn finalize(self, hw_counter: &HardwareCounterCell) -> OperationResult<Self::FieldIndexType>;

    /// Create an empty index for testing purposes.
    #[cfg(test)]
    fn make_empty(mut self) -> OperationResult<Self::FieldIndexType>
    where
        Self: Sized,
    {
        self.init(&HardwareCounterCell::new())?;
        self.finalize(&HardwareCounterCell::new())
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
    GeoMmapIndex(GeoIndexMmapBuilder),
    GeoGridstoreIndex(GeoIndexGridstoreBuilder),
    FullTextMmapIndex(FullTextMmapIndexBuilder),
    // Boxed: the mutable index it builds in place is the largest variant.
    FullTextGridstoreIndex(Box<FullTextGridstoreIndexBuilder>),
    BoolMmapIndex(ImmutableBoolIndexBuilder),
    BoolGridstoreIndex(MutableBoolIndexBuilder),
    UuidMmapIndex(MapIndexMmapBuilder<UuidIntType>),
    UuidGridstoreIndex(MapIndexGridstoreBuilder<UuidIntType>),
    MutableNullIndex(MutableNullIndexBuilder),
    ImmutableNullIndex(ImmutableNullIndexBuilder),
}

impl FieldIndexBuilder {
    /// Don't journal the value mappings of an index stored in a Gridstore, see
    /// [`Blobstore::disable_journal`](blobstore::Blobstore::disable_journal). Call after `init`.
    pub fn disable_journal(&mut self) {
        match self {
            Self::IntGridstoreIndex(builder) => builder.disable_journal(),
            Self::DatetimeGridstoreIndex(builder) => builder.disable_journal(),
            Self::IntMapGridstoreIndex(builder) => builder.disable_journal(),
            Self::KeywordGridstoreIndex(builder) => builder.disable_journal(),
            Self::FloatGridstoreIndex(builder) => builder.disable_journal(),
            Self::GeoGridstoreIndex(builder) => builder.disable_journal(),
            Self::FullTextGridstoreIndex(builder) => builder.disable_journal(),
            Self::UuidGridstoreIndex(builder) => builder.disable_journal(),
            // Not stored in a Gridstore, the mutable bool index uses flags despite its name
            Self::IntMmapIndex(_)
            | Self::DatetimeMmapIndex(_)
            | Self::IntMapMmapIndex(_)
            | Self::KeywordMmapIndex(_)
            | Self::FloatMmapIndex(_)
            | Self::GeoMmapIndex(_)
            | Self::FullTextMmapIndex(_)
            | Self::BoolMmapIndex(_)
            | Self::BoolGridstoreIndex(_)
            | Self::UuidMmapIndex(_)
            | Self::MutableNullIndex(_)
            | Self::ImmutableNullIndex(_) => {}
        }
    }
}

impl FieldIndexBuilderTrait for FieldIndexBuilder {
    type FieldIndexType = FieldIndex;

    fn init(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        match self {
            Self::IntMmapIndex(index) => index.init(hw_counter),
            Self::IntGridstoreIndex(index) => index.init(hw_counter),
            Self::DatetimeMmapIndex(index) => index.init(hw_counter),
            Self::DatetimeGridstoreIndex(index) => index.init(hw_counter),
            Self::IntMapMmapIndex(index) => index.init(hw_counter),
            Self::IntMapGridstoreIndex(index) => index.init(hw_counter),
            Self::KeywordMmapIndex(index) => index.init(hw_counter),
            Self::KeywordGridstoreIndex(index) => index.init(hw_counter),
            Self::FloatMmapIndex(index) => index.init(hw_counter),
            Self::FloatGridstoreIndex(index) => index.init(hw_counter),
            Self::GeoMmapIndex(index) => index.init(hw_counter),
            Self::GeoGridstoreIndex(index) => index.init(hw_counter),
            Self::BoolMmapIndex(index) => index.init(hw_counter),
            Self::BoolGridstoreIndex(index) => index.init(hw_counter),
            Self::FullTextMmapIndex(builder) => builder.init(hw_counter),
            Self::FullTextGridstoreIndex(builder) => builder.init(hw_counter),
            Self::UuidMmapIndex(index) => index.init(hw_counter),
            Self::UuidGridstoreIndex(index) => index.init(hw_counter),
            Self::MutableNullIndex(index) => index.init(hw_counter),
            Self::ImmutableNullIndex(index) => index.init(hw_counter),
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
                FieldIndexBuilderTrait::add_point(builder.as_mut(), id, payload, hw_counter)
            }
            Self::UuidMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::UuidGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            Self::MutableNullIndex(index) => index.add_point(id, payload, hw_counter),
            Self::ImmutableNullIndex(index) => index.add_point(id, payload, hw_counter),
        }
    }

    fn finalize(self, hw_counter: &HardwareCounterCell) -> OperationResult<FieldIndex> {
        Ok(match self {
            Self::IntMmapIndex(index) => FieldIndex::IntIndex(index.finalize(hw_counter)?),
            Self::IntGridstoreIndex(index) => FieldIndex::IntIndex(index.finalize(hw_counter)?),
            Self::DatetimeMmapIndex(index) => {
                FieldIndex::DatetimeIndex(index.finalize(hw_counter)?)
            }
            Self::DatetimeGridstoreIndex(index) => {
                FieldIndex::DatetimeIndex(index.finalize(hw_counter)?)
            }
            Self::IntMapMmapIndex(index) => FieldIndex::IntMapIndex(index.finalize(hw_counter)?),
            Self::IntMapGridstoreIndex(index) => {
                FieldIndex::IntMapIndex(index.finalize(hw_counter)?)
            }
            Self::KeywordMmapIndex(index) => FieldIndex::KeywordIndex(index.finalize(hw_counter)?),
            Self::KeywordGridstoreIndex(index) => {
                FieldIndex::KeywordIndex(index.finalize(hw_counter)?)
            }
            Self::FloatMmapIndex(index) => FieldIndex::FloatIndex(index.finalize(hw_counter)?),
            Self::FloatGridstoreIndex(index) => FieldIndex::FloatIndex(index.finalize(hw_counter)?),
            Self::GeoMmapIndex(index) => FieldIndex::GeoIndex(index.finalize(hw_counter)?),
            Self::GeoGridstoreIndex(index) => FieldIndex::GeoIndex(index.finalize(hw_counter)?),
            Self::BoolGridstoreIndex(index) => {
                FieldIndex::BoolIndex(BoolIndex::from(index.finalize(hw_counter)?))
            }
            Self::BoolMmapIndex(index) => {
                FieldIndex::BoolIndex(BoolIndex::from(index.finalize(hw_counter)?))
            }
            Self::FullTextMmapIndex(builder) => {
                FieldIndex::FullTextIndex(builder.finalize(hw_counter)?)
            }
            Self::FullTextGridstoreIndex(builder) => {
                FieldIndex::FullTextIndex((*builder).finalize(hw_counter)?)
            }
            Self::UuidMmapIndex(index) => FieldIndex::UuidMapIndex(index.finalize(hw_counter)?),
            Self::UuidGridstoreIndex(index) => {
                FieldIndex::UuidMapIndex(index.finalize(hw_counter)?)
            }
            Self::MutableNullIndex(index) => {
                FieldIndex::NullIndex(NullIndex::from(index.finalize(hw_counter)?))
            }
            Self::ImmutableNullIndex(index) => {
                FieldIndex::NullIndex(NullIndex::from(index.finalize(hw_counter)?))
            }
        })
    }
}
