use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::iterator_ext::IteratorExt;
use common::types::{DeferredBehavior, ScoredPointOffset};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::{check_query_vectors, check_stopped};
use crate::data_types::query_context::{IdfStatsKey, QueryContext, SegmentQueryContext};
use crate::data_types::segment_record::{NamedVectorsOwned, SegmentRecord};
use crate::data_types::vectors::{QueryVector, VectorStructInternal};
use crate::id_tracker::IdTrackerRead;
use crate::index::{PayloadIndexRead, VectorIndexRead};
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{
    ExtendedPointId, Filter, PointIdType, ScoredPoint, SearchParams, VectorName, VectorNameBuf,
    WithPayload, WithVector,
};

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    /// Reads records from the segment for the given external point IDs,
    /// optionally enriched with vectors and payload.
    pub fn retrieve(
        &self,
        point_ids: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<AHashMap<ExtendedPointId, SegmentRecord>> {
        // Stage 1: resolve external → internal once, into two parallel vectors.
        // The id tracker owns this: deferred filtering happens inline (no
        // `point_is_deferred` lookup). The parallel-vector shape lets
        // a future batched payload / vector fetcher consume `&offsets`
        // straight without unzipping first.
        let (resolved_ids, resolved_offsets) = self
            .id_tracker
            .resolve_external_ids(point_ids, deferred_behavior);
        debug_assert_eq!(resolved_ids.len(), resolved_offsets.len());

        // Stage 2: pre-allocate one record per resolved point. The `vectors`
        // slot is initialised here according to `with_vector`, so the
        // `WithVector::Bool(false)` path needs no separate clearing pass.
        let needs_vectors = match with_vector {
            WithVector::Bool(true) | WithVector::Selector(_) => true,
            WithVector::Bool(false) => false,
        };
        let mut records: AHashMap<ExtendedPointId, SegmentRecord> = resolved_ids
            .iter()
            .map(|&id| {
                let record = SegmentRecord {
                    id,
                    vectors: needs_vectors.then(NamedVectorsOwned::default),
                    payload: None,
                };
                (id, record)
            })
            .collect();

        // Stage 3: vectors. The external id rides along as the read's user
        // data — it comes back unchanged in the callback, so no extra
        // `offset → id` lookup is needed.
        if needs_vectors {
            let mut process_vectors = |vector_name: &VectorNameBuf| -> OperationResult<()> {
                let keys = resolved_ids
                    .iter()
                    .zip(&resolved_offsets)
                    .map(|(&id, &offset)| (id, offset))
                    .stop_if(is_stopped);
                self.vectors_by_offsets(vector_name, keys, hw_counter, |id, _offset, vec| {
                    if let Some(record) = records.get_mut(&id) {
                        record
                            .vectors
                            .as_mut()
                            .expect("needs_vectors path keeps vectors as Some")
                            .push((vector_name.clone(), vec));
                    }
                })
            };

            match with_vector {
                WithVector::Bool(true) => {
                    for vector_name in self.vector_data.keys() {
                        process_vectors(vector_name)?;
                    }
                }
                WithVector::Selector(names) => {
                    for vector_name in names {
                        process_vectors(vector_name)?;
                    }
                }
                WithVector::Bool(false) => unreachable!("guarded by needs_vectors"),
            }
        }

        // Stage 4: payload. Use the already-resolved offsets to skip another
        // external→internal lookup per point. The per-iteration shape here
        // mirrors what a future batched payload fetcher would consume —
        // `&resolved_offsets` becomes its input directly.
        if with_payload.enable {
            let point_offsets = resolved_ids.into_iter().zip(resolved_offsets);

            self.read_payloads::<Random, _>(
                point_offsets,
                |point_id, payload| {
                    check_stopped(is_stopped)?;

                    let payload = match &with_payload.payload_selector {
                        Some(selector) => selector.process(payload),
                        None => payload,
                    };

                    if let Some(record) = records.get_mut(&point_id) {
                        record.payload = Some(payload);
                    }

                    Ok(())
                },
                hw_counter,
            )?;
        }

        Ok(records)
    }

    /// Converts raw `ScoredPointOffset` search results into user-facing
    /// `ScoredPoint`s. Deferred points are filtered out.
    pub fn process_search_result(
        &self,
        internal_result: Vec<ScoredPointOffset>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let (point_ids, scored_offsets): (Vec<_>, Vec<_>) = internal_result
            .into_iter()
            .filter_map(|scored_point_offset| {
                let point_offset = scored_point_offset.idx;
                let point_id = self.id_tracker.external_id(point_offset);
                // This can happen if a point was modified between retrieving and post-processing,
                // but this function locks the segment so it can't be modified during execution.
                debug_assert!(
                    point_id.is_some(),
                    "Point with internal ID {point_offset} not found in id tracker"
                );
                point_id.map(|id| (id, scored_point_offset))
            })
            .unzip();

        let mut segment_records = self.retrieve(
            &point_ids,
            with_payload,
            with_vector,
            hw_counter,
            is_stopped,
            DeferredBehavior::VisibleOnly,
        )?;

        let mut results = Vec::with_capacity(point_ids.len());

        for (point_id, scored_offset) in point_ids.into_iter().zip(scored_offsets) {
            let ScoredPointOffset {
                idx: point_offset,
                score: point_score,
            } = scored_offset;

            let record = segment_records.remove(&point_id);

            // It is still possible for scored points to have duplicates for some reason, so we
            // probably don't want to return error in release mode. We also don't want to copy all
            // data just to handle this unexpected case.
            let Some(record) = record else {
                debug_assert!(
                    false,
                    "Record for point ID {point_id} not found during search result processing"
                );
                continue;
            };

            let point_version =
                self.id_tracker
                    .internal_version(point_offset)
                    .ok_or_else(|| {
                        OperationError::service_error(format!(
                            "Corrupter id_tracker, no version for point {point_id}"
                        ))
                    })?;

            let SegmentRecord {
                id,
                vectors,
                payload,
            } = record;

            results.push(ScoredPoint {
                id,
                version: point_version,
                score: point_score,
                payload,
                vector: vectors.map(VectorStructInternal::from),
                shard_key: None,
                order_value: None,
            });
        }

        Ok(results)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn search_batch(
        &self,
        vector_name: &VectorName,
        query_vectors: &[&QueryVector],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &SegmentQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        check_query_vectors(vector_name, query_vectors, self.segment_config)?;
        let vector_data = self
            .vector_data
            .get(vector_name)
            .ok_or_else(|| OperationError::vector_name_not_exists(vector_name))?;
        let idf_stats_key = IdfStatsKey::for_search(vector_name, filter, params);
        let vector_query_context = query_context.get_vector_context_for_key(&idf_stats_key);
        let internal_results = vector_data.vector_index().search(
            query_vectors,
            filter,
            top,
            params,
            &vector_query_context,
        )?;

        check_stopped(&vector_query_context.is_stopped())?;

        let hw_counter = vector_query_context.hardware_counter();

        internal_results
            .into_iter()
            .map(|internal_result| {
                self.process_search_result(
                    internal_result,
                    with_payload,
                    with_vector,
                    &hw_counter,
                    &vector_query_context.is_stopped(),
                )
            })
            .collect()
    }

    pub fn fill_query_context(&self, query_context: &mut QueryContext) -> OperationResult<()> {
        query_context.add_available_point_count(self.available_point_count_without_deferred());
        let hw_acc = query_context.hardware_usage_accumulator();
        let hw_counter = hw_acc.get_counter_cell();
        let is_stopped = query_context.is_stopped_flag();

        for (idf_key, stats) in query_context.mut_idf_stats().stats.iter_mut() {
            if let Some(vector_data) = self.vector_data.get(idf_key.vector_name()) {
                match idf_key.filter() {
                    None => {
                        let vector_index = vector_data.vector_index();

                        stats.indexed_vectors += vector_index.indexed_vector_count();
                        vector_index.fill_idf_statistics(&mut stats.idf, &hw_counter)?;
                    }
                    Some(filter) => {
                        let vector_index = vector_data.vector_index();
                        let filtered_points =
                            self.payload_index
                                .query_points(filter, &hw_counter, &is_stopped)?;

                        stats.indexed_vectors += vector_index.fill_idf_statistics_filtered(
                            &mut stats.idf,
                            &filtered_points,
                            &hw_counter,
                            is_stopped.as_ref(),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}
