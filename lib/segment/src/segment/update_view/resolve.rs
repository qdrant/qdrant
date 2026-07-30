//! Resolving stored points: the read half of applying a batch of updates.
//!
//! Everything here is batched: each component is handed the whole set of ids
//! at once, one pass per component rather than one round-trip per point.

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::types::{DeferredBehavior, PointOffsetType};

use super::SegmentUpdateView;
use crate::common::operation_error::OperationResult;
use crate::data_types::fully_qualified_point::StoredPoint;
use crate::data_types::segment_record::NamedVectorBytesOwned;
use crate::id_tracker::IdTrackerRead;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::vector_data_storage::VectorDataStorageRead;
use crate::types::{Payload, PointIdType, SeqNumberType};
use crate::vector_storage::VectorStorageRead;

impl<TIdT, TPS, TVD> SegmentUpdateView<'_, TIdT, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataStorageRead,
{
    /// Locate `point_ids` in this segment, streaming each `(point_id,
    /// internal_id)` pair that resolves; ids the segment does not hold are
    /// skipped. Deferred heads are included, so a point shadowed by an
    /// optimization in progress resolves to its latest slot.
    pub fn locate_points(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        callback: impl FnMut(PointIdType, PointOffsetType),
    ) -> OperationResult<()> {
        self.id_tracker
            .resolve_external_ids(point_ids, DeferredBehavior::WithDeferred, callback)
    }

    /// Versions of the points occupying `internal_ids`, in the same order.
    ///
    /// A slot the tracker has no version for reads back as `0`, the version an
    /// unwritten point compares as.
    pub fn point_versions(
        &self,
        internal_ids: &[PointOffsetType],
    ) -> OperationResult<Vec<SeqNumberType>> {
        let mut versions = vec![0; internal_ids.len()];

        // The callback is keyed by internal id rather than by input position,
        // so pair the results back through a position lookup.
        let position_of = positions_of(internal_ids);
        self.id_tracker.internal_versions_batch(
            internal_ids.iter().copied(),
            |internal_id, version| {
                if let Some(&position) = position_of.get(&internal_id) {
                    versions[position] = version;
                }
            },
        )?;

        Ok(versions)
    }

    /// Read the stored form of the points occupying `internal_ids`, returned
    /// in the same order — one batched pass per component, vectors as
    /// storage-native bytes.
    ///
    /// A slot with no value in a given component contributes nothing: a point
    /// without payload gets an empty [`Payload`], and a vector name the point
    /// does not have (or has deleted) is absent from [`StoredPoint::vectors`].
    ///
    /// `internal_ids` must be free of duplicates.
    pub fn read_stored_points(
        &self,
        internal_ids: &[PointOffsetType],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<StoredPoint>> {
        let mut stored: Vec<StoredPoint> = internal_ids
            .iter()
            .map(|&internal_id| StoredPoint {
                internal_id,
                vectors: NamedVectorBytesOwned::new(),
                payload: Payload::default(),
            })
            .collect();

        self.payload_storage.read_payloads::<Random, usize>(
            internal_ids.iter().copied().enumerate(),
            |position, payload| {
                stored[position].payload = payload;
                Ok(())
            },
            hw_counter,
        )?;

        for (vector_name, vector_data) in self.vector_data {
            let vector_storage = vector_data.vector_storage();
            vector_storage.read_vector_bytes::<Random, usize>(
                internal_ids.iter().copied().enumerate(),
                |position, internal_id, bytes| {
                    // A vector deleted on its own (`delete_vectors`) still has
                    // bytes in the storage; carrying them over would resurrect
                    // it in the rewritten point.
                    if vector_storage.is_deleted_vector(internal_id) {
                        return;
                    }
                    stored[position].vectors.push((vector_name.clone(), bytes));
                },
            )?;
        }

        Ok(stored)
    }
}

/// Reverse lookup from internal id back to its position in the input, for
/// pairing results of the id-tracker's batch reads (which are keyed by internal
/// id) with the caller's ordering.
fn positions_of(internal_ids: &[PointOffsetType]) -> AHashMap<PointOffsetType, usize> {
    let positions: AHashMap<PointOffsetType, usize> = internal_ids
        .iter()
        .enumerate()
        .map(|(position, &internal_id)| (internal_id, position))
        .collect();
    debug_assert_eq!(
        positions.len(),
        internal_ids.len(),
        "distinct points occupy distinct slots",
    );
    positions
}
