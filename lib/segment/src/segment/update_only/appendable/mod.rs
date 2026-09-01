//! The write phase for the appendable segment: the one segment of a shard a
//! batch appends its points to.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;

use super::AppendableIdTrackerState;
use crate::common::operation_error::OperationResult;
use crate::data_types::fully_qualified_point::FullyQualifiedPoint;
use crate::id_tracker::mutable_id_tracker::update_only::{
    MappingOperation, UpdateOnlyAppendableIdTracker,
};
use crate::index::struct_payload_index::update_only::UpdateOnlyStructPayloadIndex;
use crate::payload_storage::update_only::UpdateOnlyPayloadStorage;
use crate::segment_constructor::get_vector_storage_path;
use crate::types::{PointIdType, SegmentConfig, SeqNumberType, VectorNameBuf};
use crate::vector_storage::quantized::update_only::UpdateOnlyQuantizedVectors;
use crate::vector_storage::update_only::{UpdateOnlyVectorStorage, VectorToStore};

/// A segment open for appends: the write target. Every point a batch stores
/// lands here, in a fresh slot — nothing is ever rewritten in place.
pub struct AppendableSegment<S: UniversalAppend + 'static> {
    id_tracker: UpdateOnlyAppendableIdTracker<S>,
    /// What [`store_components`](Self::store_components) opens with.
    fs: S::Fs,
    segment_path: PathBuf,
    config: SegmentConfig,
    /// The components a stored point's data goes into, opened on the first
    /// [`store_points`](Self::store_points). A batch that only deletes writes
    /// nothing but the mappings log, so it never pays for these opens.
    store: Option<StoreComponents<S>>,
}

/// Everywhere a stored point's data goes. The mappings log that publishes the
/// point is not here: it belongs to the segment itself, since deletes need it
/// too.
struct StoreComponents<S: UniversalAppend + 'static> {
    payload_storage: UpdateOnlyPayloadStorage<S>,
    payload_indexes: UpdateOnlyStructPayloadIndex<S>,
    /// One writer per named vector, dense and sparse alike.
    vector_storages: Vec<(VectorNameBuf, UpdateOnlyVectorStorage<S>)>,
    /// Quantized overlays, keyed by vector name — only for dense, non-multivector vectors
    /// whose quantization method supports incremental appends (Binary/Turbo — see
    /// `QuantizationConfig::supports_appendable`). A vector name with no entry here simply
    /// has no live quantization (never configured, an unsupported method, or a multivector —
    /// that support is a follow-up, needing its own append-only offsets storage): it stays
    /// searchable exactly, through the raw storage alone, same as before this existed.
    quantized_vectors: HashMap<VectorNameBuf, UpdateOnlyQuantizedVectors<S>>,
}

impl<S: UniversalAppend + 'static> StoreComponents<S> {
    fn open(fs: &S::Fs, segment_path: &Path, config: &SegmentConfig) -> OperationResult<Self> {
        let payload_storage = UpdateOnlyPayloadStorage::open(fs.clone(), segment_path)?;
        let payload_indexes = UpdateOnlyStructPayloadIndex::open(fs.clone(), segment_path)?;

        let mut vector_storages =
            Vec::with_capacity(config.vector_data.len() + config.sparse_vector_data.len());
        let mut quantized_vectors = HashMap::new();
        for (vector_name, vector_config) in &config.vector_data {
            let path = get_vector_storage_path(segment_path, vector_name);
            let storage = UpdateOnlyVectorStorage::open(fs.clone(), &path, vector_config)?;
            vector_storages.push((vector_name.clone(), storage));

            if let Some(quantized) =
                UpdateOnlyQuantizedVectors::open(fs.clone(), &path, vector_config)?
            {
                quantized_vectors.insert(vector_name.clone(), quantized);
            }
        }
        for vector_name in config.sparse_vector_data.keys() {
            let path = get_vector_storage_path(segment_path, vector_name);
            let storage = UpdateOnlyVectorStorage::open_sparse(fs.clone(), &path)?;
            vector_storages.push((vector_name.clone(), storage));
        }

        Ok(Self {
            payload_storage,
            payload_indexes,
            vector_storages,
            quantized_vectors,
        })
    }
}

impl<S: UniversalAppend + 'static> AppendableSegment<S> {
    /// Resume the segment directory at `segment_path` from the mappings-log
    /// state the read phase observed.
    ///
    /// Opening is not free of side effects: points left on slots whose
    /// versions were never committed are retired here, since which components
    /// got to write their data is unknowable.
    pub fn open(
        fs: S::Fs,
        segment_path: &Path,
        config: &SegmentConfig,
        state: AppendableIdTrackerState,
    ) -> OperationResult<Self> {
        let AppendableIdTrackerState {
            max_claimed_internal_id,
            pending_inserts,
            mappings_end,
        } = state;

        let id_tracker = UpdateOnlyAppendableIdTracker::new(
            fs.clone(),
            segment_path,
            max_claimed_internal_id,
            pending_inserts,
            mappings_end,
        )?;

        Ok(Self {
            id_tracker,
            fs,
            segment_path: segment_path.to_path_buf(),
            config: config.clone(),
            store: None,
        })
    }

    fn store_components(&mut self) -> OperationResult<&mut StoreComponents<S>> {
        if self.store.is_none() {
            self.store = Some(StoreComponents::open(
                &self.fs,
                &self.segment_path,
                &self.config,
            )?);
        }
        Ok(self.store.as_mut().expect("just opened"))
    }

    /// Append `points` to this segment, each into a fresh slot, and repoint
    /// the id tracker at those slots. A point that already exists here is
    /// never rewritten in place: it is written anew, and the mappings log
    /// retires its previous slot on its own.
    ///
    /// The order of writes is what makes a crash safe anywhere in between:
    /// the slots are claimed first, every component then writes its data at
    /// them, and only after all of that do the versions cover them — the step
    /// that makes the points visible to readers. A batch cut short leaves
    /// claimed, unpublished slots, which the next writer to open this segment
    /// retires.
    pub fn store_points(
        &mut self,
        points: &[FullyQualifiedPoint],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if points.is_empty() {
            return Ok(());
        }

        let operations: Vec<MappingOperation> = points
            .iter()
            .map(|point| MappingOperation::Insert(point.id))
            .collect();
        let inserted = self.id_tracker.insert_operations(&operations)?;
        let (_, start_slot) = *inserted.first().expect("one slot per insert");

        let store = self.store_components()?;

        // Each named vector, from whichever half of the point holds it: the
        // batch's own decoded vectors win over the bytes carried from the
        // point's previous slot, and a name in neither still takes its slot —
        // the storage records it as a vector the point does not have.
        for (vector_name, storage) in &mut store.vector_storages {
            let vectors: Vec<VectorToStore> = points
                .iter()
                .map(|point| {
                    if let Some(vector) = point.updated_vectors.get(vector_name) {
                        VectorToStore::Decoded(vector)
                    } else if let Some((_, bytes)) = point
                        .stored_vectors
                        .iter()
                        .find(|(name, _)| name == vector_name)
                    {
                        VectorToStore::Raw(bytes)
                    } else {
                        VectorToStore::Missing
                    }
                })
                .collect();
            storage.append_many(start_slot, vectors.iter().copied(), hw_counter)?;

            // The quantized storage takes the same run, row-for-row with the raw storage.
            if let Some(quantized) = store.quantized_vectors.get_mut(vector_name) {
                quantized.append_many(start_slot, vectors.iter().copied(), hw_counter)?;
            }
        }

        let slot_payloads = || {
            inserted
                .iter()
                .zip(points)
                .map(|((_, slot), point)| (*slot, &point.payload))
        };
        store
            .payload_storage
            .append_many(slot_payloads(), hw_counter)?;
        store
            .payload_indexes
            .append_many(slot_payloads(), hw_counter)?;

        // Publish: covering the slots with their versions is what makes the
        // points visible, so everything above must already be durable.
        let slots: Vec<PointOffsetType> = inserted.iter().map(|(_, slot)| *slot).collect();
        let versions: Vec<SeqNumberType> = points.iter().map(|point| point.version).collect();
        self.id_tracker.set_internal_versions(&slots, &versions)?;

        Ok(())
    }

    /// Retire the given points, addressed by their external ids — the slots
    /// they occupy play no part here, since a retired mapping is what makes a
    /// point unreachable. The data on those slots is left where it is.
    ///
    /// Only call this for points the batch *deletes*. A point the batch stores
    /// again needs no retirement — its new mapping supersedes the old slot —
    /// and asking for one here retires the point outright: a delete addresses
    /// the external id, so it takes the fresh slot along with the stale one.
    pub fn tombstone_points(
        &mut self,
        points: &[(PointIdType, PointOffsetType)],
    ) -> OperationResult<()> {
        self.id_tracker
            .delete_points(points.iter().map(|(point_id, _internal_id)| *point_id))
    }
}
