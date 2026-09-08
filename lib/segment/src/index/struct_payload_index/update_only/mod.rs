//! The write half of a segment's payload indexes, for update-only segments.

#[cfg(test)]
mod tests;

use std::path::Path;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{UniversalAppend, UniversalAppendFs};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::UpdateOnlyFieldIndex;
use crate::index::payload_config::PayloadConfig;
use crate::segment_constructor::get_payload_index_path;
use crate::types::{Payload, PayloadContainer as _, PayloadKeyType};

/// Every field index of one segment, open for one batch of updates and dropped
/// with it — the update-only counterpart of [`ReadOnlyStructPayloadIndex`][1].
///
/// Holds no payload storage, no id tracker and no vector storages, unlike both
/// the writable index and the read-only one: by the time a batch reaches here
/// every point arrives with the payload it will be stored with.
///
/// [1]: crate::index::struct_payload_index::read_only::ReadOnlyStructPayloadIndex
pub struct UpdateOnlyStructPayloadIndex<S: UniversalAppend + 'static> {
    /// One writer per index of each indexed field, as the payload config
    /// declares them.
    field_indexes: AHashMap<PayloadKeyType, Vec<UpdateOnlyFieldIndex<S>>>,
}

impl<S: UniversalAppend + 'static> UpdateOnlyStructPayloadIndex<S> {
    /// Open a writer for every index of every field the segment at
    /// `segment_path` has indexed, creating storages the segment does not have
    /// yet. A segment with no payload config has no indexed fields, and opens
    /// with nothing.
    ///
    /// A field whose index types the config does not spell out is refused: that
    /// is a config from before those types were recorded, which only the
    /// writable index can repair, by deriving them from the schema on its next
    /// open.
    pub fn open(
        fs: &impl UniversalAppendFs<AppendFile = S>,
        segment_path: &Path,
    ) -> OperationResult<Self> {
        let path = get_payload_index_path(segment_path);
        let config = PayloadConfig::load_universal(fs, &PayloadConfig::get_config_path(&path))?
            .unwrap_or_default();

        let mut field_indexes = AHashMap::with_capacity(config.indices.len());
        for (field, indexed) in config.indices.iter() {
            if indexed.types.is_empty() {
                return Err(OperationError::service_error(format!(
                    "Payload index of field {field} does not record which indexes it has; \
                     open the segment for writing once to have them derived and stored",
                )));
            }

            let indexes = indexed
                .types
                .iter()
                .map(|index_type| {
                    UpdateOnlyFieldIndex::open(fs, &path, field, &indexed.schema, index_type)
                })
                .collect::<OperationResult<Vec<_>>>()?;

            field_indexes.insert(field.clone(), indexes);
        }

        Ok(Self { field_indexes })
    }

    /// Index a batch of points, each at the slot the ID tracker claimed for it,
    /// and persist every index that gained anything.
    ///
    /// The slots must come in increasing order and must all be above every slot
    /// these indexes already cover: they are append-only, so a slot that was
    /// indexed cannot be indexed again.
    ///
    /// Every field is offered every point, including the points whose payload
    /// has nothing under that field: an index that stores values per point
    /// leaves the slot empty, the null index records that the point has no
    /// value there.
    pub fn append_many<'a>(
        &mut self,
        fs: &impl UniversalAppendFs<AppendFile = S>,
        points: impl IntoIterator<Item = (PointOffsetType, &'a Payload)>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        for (slot, payload) in points {
            for (field, indexes) in &mut self.field_indexes {
                let values = payload.get_value(field);
                for index in indexes.iter_mut() {
                    index.add_point(fs, slot, &values, hw_counter)?;
                }
            }
        }

        // Once per index per batch: for the bitmask-backed indexes a flush
        // rewrites a whole mask
        for index in self.field_indexes.values_mut().flatten() {
            index.flush(fs, hw_counter)?;
        }

        Ok(())
    }
}
