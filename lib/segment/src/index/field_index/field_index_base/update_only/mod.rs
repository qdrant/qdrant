#[cfg(test)]
mod tests;

use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;
use serde_json::Value;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::full_text_index::update_only::UpdateOnlyTextKind;
use crate::index::field_index::geo_index::update_only::UpdateOnlyGeoKind;
use crate::index::field_index::map_index::update_only::UpdateOnlyMapKind;
use crate::index::field_index::numeric_index::update_only::UpdateOnlyNumericKind;
use crate::index::field_index::update_only::UpdateOnlyValueIndex;
use crate::index::payload_config::{FullPayloadIndexType, PayloadIndexType};
use crate::json_path::JsonPath;
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, PayloadFieldSchema, PayloadSchemaParams,
    UuidIntType, UuidPayloadType,
};

/// The write half of one appendable field index, over the backend `S`.
///
/// The update-only counterpart of [`ReadOnlyFieldIndex`][1], and it covers the
/// same index types minus the two whose state is a bitmask rather than a value
/// per point — see [`open`](Self::open).
///
/// [1]: crate::index::field_index::ReadOnlyFieldIndex
pub enum UpdateOnlyFieldIndex<S: UniversalAppend + 'static> {
    IntIndex(UpdateOnlyValueIndex<UpdateOnlyNumericKind<IntPayloadType, IntPayloadType>, S>),
    DatetimeIndex(
        UpdateOnlyValueIndex<UpdateOnlyNumericKind<IntPayloadType, DateTimePayloadType>, S>,
    ),
    FloatIndex(UpdateOnlyValueIndex<UpdateOnlyNumericKind<FloatPayloadType, FloatPayloadType>, S>),
    UuidIndex(UpdateOnlyValueIndex<UpdateOnlyNumericKind<UuidIntType, UuidPayloadType>, S>),
    IntMapIndex(UpdateOnlyValueIndex<UpdateOnlyMapKind<IntPayloadType>, S>),
    KeywordIndex(UpdateOnlyValueIndex<UpdateOnlyMapKind<str>, S>),
    UuidMapIndex(UpdateOnlyValueIndex<UpdateOnlyMapKind<UuidIntType>, S>),
    GeoIndex(UpdateOnlyValueIndex<UpdateOnlyGeoKind, S>),
    FullTextIndex(UpdateOnlyValueIndex<UpdateOnlyTextKind, S>),
}

impl<S: UniversalAppend + 'static> UpdateOnlyFieldIndex<S> {
    /// Open the writer for `field`'s index of type `index_type`, under the
    /// payload index root `dir`, creating its storage if it is not there yet.
    ///
    /// Fails for the boolean and null indexes. Those keep a bitmask over all
    /// points rather than values per point, and persist it through
    /// random-offset writes, which an append-only backend does not offer. It is
    /// an error rather than a skip because a skipped index goes stale, and a
    /// stale index answers queries wrongly — and because the null index
    /// complements *every* other index of every indexed field, so a caller that
    /// swallowed this would leave every field it touched wrong.
    ///
    /// `Ok(None)` when the schema and the stored index type disagree, which is
    /// what [`IndexSelector::new_index_with_type`][1] reports as a corrupt
    /// config; opening for writes is not where that gets diagnosed.
    ///
    /// [1]: crate::index::field_index::index_selector::IndexSelector::new_index_with_type
    pub fn open(
        fs: S::Fs,
        dir: &Path,
        field: &JsonPath,
        schema: &PayloadFieldSchema,
        index_type: &FullPayloadIndexType,
    ) -> OperationResult<Option<Self>> {
        let index_dir = index_type.index_type.storage_dir(dir, field);

        let index = match (&index_type.index_type, schema.expand().as_ref()) {
            (PayloadIndexType::IntIndex, PayloadSchemaParams::Integer(_)) => Self::IntIndex(
                UpdateOnlyValueIndex::open(fs, &index_dir, Default::default())?,
            ),
            (PayloadIndexType::DatetimeIndex, PayloadSchemaParams::Datetime(_)) => {
                Self::DatetimeIndex(UpdateOnlyValueIndex::open(
                    fs,
                    &index_dir,
                    Default::default(),
                )?)
            }
            (PayloadIndexType::FloatIndex, PayloadSchemaParams::Float(_)) => Self::FloatIndex(
                UpdateOnlyValueIndex::open(fs, &index_dir, Default::default())?,
            ),
            (PayloadIndexType::UuidIndex, PayloadSchemaParams::Uuid(_)) => Self::UuidIndex(
                UpdateOnlyValueIndex::open(fs, &index_dir, Default::default())?,
            ),
            (PayloadIndexType::IntMapIndex, PayloadSchemaParams::Integer(_)) => Self::IntMapIndex(
                UpdateOnlyValueIndex::open(fs, &index_dir, Default::default())?,
            ),
            (PayloadIndexType::KeywordIndex, PayloadSchemaParams::Keyword(_)) => {
                Self::KeywordIndex(UpdateOnlyValueIndex::open(
                    fs,
                    &index_dir,
                    Default::default(),
                )?)
            }
            (PayloadIndexType::UuidMapIndex, PayloadSchemaParams::Uuid(_)) => Self::UuidMapIndex(
                UpdateOnlyValueIndex::open(fs, &index_dir, Default::default())?,
            ),
            (PayloadIndexType::GeoIndex, PayloadSchemaParams::Geo(_)) => Self::GeoIndex(
                UpdateOnlyValueIndex::open(fs, &index_dir, UpdateOnlyGeoKind)?,
            ),
            (PayloadIndexType::FullTextIndex, PayloadSchemaParams::Text(params)) => {
                Self::FullTextIndex(UpdateOnlyValueIndex::open(
                    fs,
                    &index_dir,
                    UpdateOnlyTextKind::new(params),
                )?)
            }

            // Bitmask-backed, see the doc comment above.
            (index_type @ (PayloadIndexType::BoolIndex | PayloadIndexType::NullIndex), _) => {
                return Err(OperationError::service_error(format!(
                    "Cannot open {index_type:?} of field {field} for appending: it keeps a \
                     bitmask over all points, which an append-only backend cannot rewrite",
                )));
            }

            // Schema and stored index type disagree.
            (PayloadIndexType::IntIndex, _)
            | (PayloadIndexType::DatetimeIndex, _)
            | (PayloadIndexType::FloatIndex, _)
            | (PayloadIndexType::UuidIndex, _)
            | (PayloadIndexType::IntMapIndex, _)
            | (PayloadIndexType::KeywordIndex, _)
            | (PayloadIndexType::UuidMapIndex, _)
            | (PayloadIndexType::GeoIndex, _)
            | (PayloadIndexType::FullTextIndex, _) => return Ok(None),
        };

        Ok(Some(index))
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
            Self::UuidIndex(index) => index.add_point(slot, values, hw_counter),
            Self::IntMapIndex(index) => index.add_point(slot, values, hw_counter),
            Self::KeywordIndex(index) => index.add_point(slot, values, hw_counter),
            Self::UuidMapIndex(index) => index.add_point(slot, values, hw_counter),
            Self::GeoIndex(index) => index.add_point(slot, values, hw_counter),
            Self::FullTextIndex(index) => index.add_point(slot, values, hw_counter),
        }
    }

    /// Persist everything buffered since the last flush.
    pub fn flush(&self) -> OperationResult<()> {
        match self {
            Self::IntIndex(index) => index.flush(),
            Self::DatetimeIndex(index) => index.flush(),
            Self::FloatIndex(index) => index.flush(),
            Self::UuidIndex(index) => index.flush(),
            Self::IntMapIndex(index) => index.flush(),
            Self::KeywordIndex(index) => index.flush(),
            Self::UuidMapIndex(index) => index.flush(),
            Self::GeoIndex(index) => index.flush(),
            Self::FullTextIndex(index) => index.flush(),
        }
    }
}
