//! The write phase for the appendable segment: the one segment of a shard a
//! batch appends its points to.

use std::path::{Path, PathBuf};

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{CachedFs, CachedReadFs, UniversalAppendFs};
use rayon::ThreadPool;
use rayon::iter::{IntoParallelRefMutIterator as _, ParallelIterator as _};

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
pub struct AppendableSegment<Fs: UniversalAppendFs> {
    id_tracker: UpdateOnlyAppendableIdTracker,
    /// What the id tracker appends through and
    /// [`store_components`](Self::store_components) opens with.
    fs: CachedFs<Fs>,
    segment_path: PathBuf,
    config: SegmentConfig,
    /// The components a stored point's data goes into, opened on the first
    /// [`store_points`](Self::store_points). A batch that only deletes writes
    /// nothing but the mappings log, so it never pays for these opens.
    store: Option<StoreComponents<CachedFs<Fs>>>,
}

/// Everywhere a stored point's data goes. The mappings log that publishes the
/// point is not here: it belongs to the segment itself, since deletes need it
/// too.
struct StoreComponents<Fs: UniversalAppendFs> {
    payload_storage: UpdateOnlyPayloadStorage<Fs::File>,
    payload_indexes: UpdateOnlyStructPayloadIndex<Fs::File>,
    /// The writers of each named vector, dense and sparse alike.
    vectors: Vec<(VectorNameBuf, VectorComponents<Fs>)>,
}

/// The writers of one named vector.
struct VectorComponents<Fs: UniversalAppendFs> {
    storage: UpdateOnlyVectorStorage<Fs::File>,
    /// Quantized overlay — only for dense, non-multivector vectors whose
    /// quantization method supports incremental appends.
    quantized: Option<UpdateOnlyQuantizedVectors<Fs>>,
}

impl<Fs: UniversalAppendFs> VectorComponents<Fs> {
    /// Appends many points to the original and quantized (if any) storages in parallel
    fn append_many(
        &mut self,
        pool: &rayon::ThreadPool,
        points: &[FullyQualifiedPoint],
        start_slot: u32,
        fs: &Fs,
        hw_acc: &HwMeasurementAcc,
        vector_name: &String,
    ) -> OperationResult<()> {
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

        let mut original_res = Ok(());
        let mut quantized_res = Ok(());
        pool.scope(|s| {
            s.spawn(|_| {
                let hw_counter = hw_acc.get_counter_cell();
                original_res =
                    self.storage
                        .append_many(fs, start_slot, vectors.iter().copied(), &hw_counter);
            });
            if let Some(quantized) = &mut self.quantized {
                s.spawn(|_| {
                    let hw_counter = hw_acc.get_counter_cell();
                    quantized_res =
                        quantized.append_many(start_slot, vectors.iter().copied(), &hw_counter);
                });
            }
        });
        original_res.and(quantized_res)
    }
}

impl<Fs: UniversalAppendFs> StoreComponents<Fs> {
    fn open(fs: &Fs, segment_path: &Path, config: &SegmentConfig) -> OperationResult<Self> {
        let payload_storage = UpdateOnlyPayloadStorage::open(fs, segment_path)?;
        let payload_indexes = UpdateOnlyStructPayloadIndex::open(fs, segment_path)?;

        let mut vectors =
            Vec::with_capacity(config.vector_data.len() + config.sparse_vector_data.len());
        for (vector_name, vector_config) in &config.vector_data {
            let path = get_vector_storage_path(segment_path, vector_name);
            let components = VectorComponents {
                storage: UpdateOnlyVectorStorage::open(fs, &path, vector_config)?,
                quantized: UpdateOnlyQuantizedVectors::open(fs.clone(), &path, vector_config)?,
            };
            vectors.push((vector_name.clone(), components));
        }
        for vector_name in config.sparse_vector_data.keys() {
            let path = get_vector_storage_path(segment_path, vector_name);
            let components = VectorComponents {
                storage: UpdateOnlyVectorStorage::open_sparse(fs, &path)?,
                quantized: None,
            };
            vectors.push((vector_name.clone(), components));
        }

        Ok(Self {
            payload_storage,
            payload_indexes,
            vectors,
        })
    }
}

impl<Fs: UniversalAppendFs> AppendableSegment<Fs> {
    /// Resume the segment directory at `segment_path` from the mappings-log
    /// state the read phase observed.
    ///
    /// Opening is not free of side effects: points left on slots whose
    /// versions were never committed are retired here, since which components
    /// got to write their data is unknowable.
    pub fn open(
        fs: Fs,
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
            &fs,
            segment_path,
            max_claimed_internal_id,
            pending_inserts,
            mappings_end,
        )?;

        let fs = CachedFs::new(fs, segment_path)?;

        Ok(Self {
            id_tracker,
            fs,
            segment_path: segment_path.to_path_buf(),
            config: config.clone(),
            store: None,
        })
    }

    /// Lends the fs alongside the components, so a caller holding the mutable
    /// borrow can still pass it down per call.
    fn store_components(
        &mut self,
    ) -> OperationResult<(&CachedFs<Fs>, &mut StoreComponents<CachedFs<Fs>>)> {
        if self.store.is_none() {
            self.store = Some(StoreComponents::open(
                &self.fs,
                &self.segment_path,
                &self.config,
            )?);
        }
        Ok((&self.fs, self.store.as_mut().expect("just opened")))
    }

    /// Append `points` to fresh slots in this segment and update the id tracker.
    /// Writes component data in parallel before making points visible by publishing versions.
    pub fn store_points(
        &mut self,
        pool: &ThreadPool,
        points: &[FullyQualifiedPoint],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if points.is_empty() {
            return Ok(());
        }

        // Ensure fresh new view of the files
        self.fs.rotate_cache_file_info();
        self.fs.cache_file_info()?;

        let operations: Vec<MappingOperation> = points
            .iter()
            .map(|point| MappingOperation::Insert(point.id))
            .collect();
        let inserted = self.id_tracker.insert_operations(&self.fs, &operations)?;
        let (_, start_slot) = *inserted.first().expect("one slot per insert");

        let (fs, store) = self.store_components()?;

        // One cell per task, off the accumulator: the cell itself is not Sync
        let hw_acc = hw_counter.new_accumulator();

        let slot_payloads = || {
            inserted
                .iter()
                .zip(points)
                .map(|((_, slot), point)| (*slot, &point.payload))
        };

        // Each component owns its files, so none of them waits on another
        let mut vectors_res = Ok(());
        let mut payload_res = Ok(());
        let mut indexes_res = Ok(());
        pool.scope(|s| {
            s.spawn(|_| {
                vectors_res =
                    store
                        .vectors
                        .par_iter_mut()
                        .try_for_each(|(vector_name, components)| {
                            components.append_many(
                                pool,
                                points,
                                start_slot,
                                fs,
                                &hw_acc,
                                vector_name,
                            )
                        });
            });

            s.spawn(|_| {
                let hw_counter = hw_acc.get_counter_cell();
                payload_res = store
                    .payload_storage
                    .append_many(fs, slot_payloads(), &hw_counter);
            });

            s.spawn(|_| {
                let hw_counter = hw_acc.get_counter_cell();
                indexes_res =
                    store
                        .payload_indexes
                        .par_append_many(fs, slot_payloads(), &hw_counter);
            });
        });
        vectors_res.and(payload_res).and(indexes_res)?;

        // Publish: covering the slots with their versions is what makes the
        // points visible, so everything above must already be durable.
        let slots: Vec<PointOffsetType> = inserted.iter().map(|(_, slot)| *slot).collect();
        let versions: Vec<SeqNumberType> = points.iter().map(|point| point.version).collect();
        self.id_tracker
            .set_internal_versions(&self.fs, &slots, &versions)?;

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
        self.id_tracker.delete_points(
            &self.fs,
            points.iter().map(|(point_id, _internal_id)| *point_id),
        )
    }
}
