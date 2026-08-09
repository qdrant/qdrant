//! The write half of the appendable payload indexes, for update-only segments.
//!
//! Only the values an index persists per point are state: the structure it
//! answers queries from is rebuilt from them on every open. So a writer that
//! never answers a query holds nothing — it turns a point's payload into the
//! values its index would persist and appends them at the point's slot.
//!
//! That translation is all the index types differ by, which is what
//! [`UpdateOnlyIndexKind`] captures; [`UpdateOnlyValueIndex`] is the storage
//! around it, the same for all of them. Each kind lives under the appendable
//! index it writes for, next to that index's read-only counterpart.

#[cfg(test)]
mod tests;
mod writer;

use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;
use serde_json::Value;

pub use self::writer::{UpdateOnlyIndexKind, UpdateOnlyValueIndex};
use crate::common::operation_error::OperationResult;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::bool_index::mutable_bool_index::update_only::UpdateOnlyBoolIndex;
use crate::index::field_index::full_text_index::UpdateOnlyTextKind;
use crate::index::field_index::geo_index::mutable_geo_index::update_only::UpdateOnlyGeoKind;
use crate::index::field_index::map_index::mutable_map_index::update_only::UpdateOnlyMapKind;
use crate::index::field_index::null_index::mutable_null_index::update_only::UpdateOnlyNullIndex;
use crate::index::field_index::numeric_index::mutable_numeric_index::update_only::UpdateOnlyNumericKind;
use crate::index::payload_config::{FullPayloadIndexType, PayloadIndexType};
use crate::json_path::JsonPath;
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, PayloadFieldSchema, UuidIntType,
};

/// The write half of one appendable field index, over the backend `S` — the
/// update-only counterpart of [`ReadOnlyFieldIndex`][1].
///
/// Most store values per point and append them. The boolean and null indexes
/// instead keep a bitmask over all points, which cannot be appended to — they
/// rewrite it whole, see [`UpdateOnlyStoredFlags`][2].
///
/// [1]: crate::index::field_index::ReadOnlyFieldIndex
/// [2]: crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags
pub enum UpdateOnlyFieldIndex<S: UniversalAppend + 'static> {
    IntIndex(UpdateOnlyValueIndex<UpdateOnlyNumericKind<IntPayloadType, IntPayloadType>, S>),
    DatetimeIndex(
        UpdateOnlyValueIndex<UpdateOnlyNumericKind<IntPayloadType, DateTimePayloadType>, S>,
    ),
    FloatIndex(UpdateOnlyValueIndex<UpdateOnlyNumericKind<FloatPayloadType, FloatPayloadType>, S>),
    IntMapIndex(UpdateOnlyValueIndex<UpdateOnlyMapKind<IntPayloadType>, S>),
    KeywordIndex(UpdateOnlyValueIndex<UpdateOnlyMapKind<str>, S>),
    UuidMapIndex(UpdateOnlyValueIndex<UpdateOnlyMapKind<UuidIntType>, S>),
    GeoIndex(UpdateOnlyValueIndex<UpdateOnlyGeoKind, S>),
    FullTextIndex(UpdateOnlyValueIndex<UpdateOnlyTextKind, S>),
    BoolIndex(UpdateOnlyBoolIndex<S>),
    NullIndex(UpdateOnlyNullIndex<S>),
}

impl<S: UniversalAppend + 'static> UpdateOnlyFieldIndex<S> {
    /// Open the writer for `field`'s index of type `index_type`, under the
    /// payload index root `dir`, creating its storage if it is not there yet.
    pub fn open(
        fs: S::Fs,
        dir: &Path,
        field: &JsonPath,
        schema: &PayloadFieldSchema,
        index_type: &FullPayloadIndexType,
    ) -> OperationResult<Self> {
        let index_type = &index_type.index_type;
        let dir = &index_type.storage_dir(dir, field);

        let index = match index_type {
            PayloadIndexType::IntIndex => {
                Self::IntIndex(UpdateOnlyValueIndex::open(fs, dir, Default::default())?)
            }
            PayloadIndexType::DatetimeIndex => {
                Self::DatetimeIndex(UpdateOnlyValueIndex::open(fs, dir, Default::default())?)
            }
            PayloadIndexType::FloatIndex => {
                Self::FloatIndex(UpdateOnlyValueIndex::open(fs, dir, Default::default())?)
            }
            PayloadIndexType::IntMapIndex => {
                Self::IntMapIndex(UpdateOnlyValueIndex::open(fs, dir, Default::default())?)
            }
            PayloadIndexType::KeywordIndex => {
                Self::KeywordIndex(UpdateOnlyValueIndex::open(fs, dir, Default::default())?)
            }
            // The `UuidIndex` discriminant is historically map-backed
            PayloadIndexType::UuidIndex | PayloadIndexType::UuidMapIndex => {
                Self::UuidMapIndex(UpdateOnlyValueIndex::open(fs, dir, Default::default())?)
            }
            PayloadIndexType::GeoIndex => {
                Self::GeoIndex(UpdateOnlyValueIndex::open(fs, dir, UpdateOnlyGeoKind)?)
            }
            PayloadIndexType::FullTextIndex => {
                let config = TextIndexParams::try_from(schema)?;
                Self::FullTextIndex(UpdateOnlyValueIndex::open(
                    fs,
                    dir,
                    UpdateOnlyTextKind::new(&config),
                )?)
            }
            PayloadIndexType::BoolIndex => Self::BoolIndex(UpdateOnlyBoolIndex::open(fs, dir)?),
            PayloadIndexType::NullIndex => Self::NullIndex(UpdateOnlyNullIndex::open(fs, dir)?),
        };

        Ok(index)
    }

    /// Index `values`, the point's values for this index's field, at the slot
    /// the ID tracker claimed for it. Buffers only, see
    /// [`flush`](Self::flush).
    pub fn add_point(
        &mut self,
        slot: PointOffsetType,
        values: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            Self::IntIndex(index) => index.add_point(slot, values, hw_counter),
            Self::DatetimeIndex(index) => index.add_point(slot, values, hw_counter),
            Self::FloatIndex(index) => index.add_point(slot, values, hw_counter),
            Self::IntMapIndex(index) => index.add_point(slot, values, hw_counter),
            Self::KeywordIndex(index) => index.add_point(slot, values, hw_counter),
            Self::UuidMapIndex(index) => index.add_point(slot, values, hw_counter),
            Self::GeoIndex(index) => index.add_point(slot, values, hw_counter),
            Self::FullTextIndex(index) => index.add_point(slot, values, hw_counter),
            Self::BoolIndex(index) => index.add_point(slot, values),
            Self::NullIndex(index) => index.add_point(slot, values),
        }
    }

    /// Persist everything buffered since the last flush.
    ///
    /// `hw_counter` is charged only by the bitmask-backed indexes, which do
    /// their writing here; the rest charge each value as it is put.
    pub fn flush(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        match self {
            Self::IntIndex(index) => index.flush(),
            Self::DatetimeIndex(index) => index.flush(),
            Self::FloatIndex(index) => index.flush(),
            Self::IntMapIndex(index) => index.flush(),
            Self::KeywordIndex(index) => index.flush(),
            Self::UuidMapIndex(index) => index.flush(),
            Self::GeoIndex(index) => index.flush(),
            Self::FullTextIndex(index) => index.flush(),
            Self::BoolIndex(index) => index.flush(hw_counter),
            Self::NullIndex(index) => index.flush(hw_counter),
        }
    }
}
