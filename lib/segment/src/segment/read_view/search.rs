use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{DeferredBehavior, ScoredPointOffset};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::{check_query_vectors, check_stopped};
use crate::data_types::query_context::{QueryContext, QueryIdfStats, SegmentQueryContext};
use crate::data_types::segment_record::{NamedVectorsOwned, SegmentRecord};
use crate::data_types::vectors::{QueryVector, VectorInternal, VectorStructInternal};
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
        let mut records = AHashMap::with_capacity(point_ids.len());

        // Filter out deferred points. This is done in two stages to prevent cloning `point_ids`
        // and iterating more than needed but still satisfy Rust's ownership constraints.
        let behavior_allows_filtering = !deferred_behavior.include_all_points();
        let filter_deferred = self.has_deferred_points() && behavior_allows_filtering;
        let filtered_point_ids = filter_deferred.then(|| {
            point_ids
                .iter()
                .filter(|&&point_id| !self.point_is_deferred(point_id))
                .copied()
                .collect::<Vec<_>>()
        });

        // Stage two: select the correct slice and shadow `point_ids`.
        let point_ids = filtered_point_ids.as_deref().unwrap_or(point_ids);

        let mut update_record_vector =
            |vector_name: &VectorNameBuf,
             point_id: PointIdType,
             vector_internal: VectorInternal| {
                let point_record = records
                    .entry(point_id)
                    .or_insert_with(|| SegmentRecord::empty(point_id));

                point_record
                    .vectors
                    .get_or_insert_with(NamedVectorsOwned::default)
                    .push((vector_name.clone(), vector_internal));
            };

        match with_vector {
            WithVector::Bool(true) => {
                for vector_name in self.vector_data.keys() {
                    self.read_vectors(
                        vector_name,
                        point_ids,
                        hw_counter,
                        is_stopped,
                        |point_id, vec| {
                            update_record_vector(vector_name, point_id, vec);
                        },
                    )?;
                }
            }
            WithVector::Bool(false) => {
                // Do not display empty `vectors: {}` if disabled.
                for &point_id in point_ids {
                    let point_record = records
                        .entry(point_id)
                        .or_insert_with(|| SegmentRecord::empty(point_id));
                    point_record.vectors = None;
                }
            }
            WithVector::Selector(selector) => {
                for vector_name in selector {
                    self.read_vectors(
                        vector_name,
                        point_ids,
                        hw_counter,
                        is_stopped,
                        |point_id, vec| {
                            update_record_vector(vector_name, point_id, vec);
                        },
                    )?;
                }
            }
        }

        for &point_id in point_ids {
            let payload = if with_payload.enable {
                if let Some(selector) = &with_payload.payload_selector {
                    Some(selector.process(self.payload(point_id, hw_counter)?))
                } else {
                    Some(self.payload(point_id, hw_counter)?)
                }
            } else {
                None
            };
            let point_record = records
                .entry(point_id)
                .or_insert_with(|| SegmentRecord::empty(point_id));
            point_record.payload = payload;
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
            DeferredBehavior::Exclude,
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
        let vector_query_context =
            query_context.get_vector_context(vector_name, self.deferred_internal_id());
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

    pub fn fill_query_context(&self, query_context: &mut QueryContext) {
        query_context.add_available_point_count(self.available_point_count_without_deferred());
        let hw_acc = query_context.hardware_usage_accumulator();
        let hw_counter = hw_acc.get_counter_cell();

        let QueryIdfStats {
            idf,
            indexed_vectors,
        } = query_context.mut_idf_stats();

        for (vector_name, idf) in idf.iter_mut() {
            if let Some(vector_data) = self.vector_data.get(vector_name) {
                let vector_index = vector_data.vector_index();

                let indexed_vector_count = vector_index.indexed_vector_count();

                if let Some(count) = indexed_vectors.get_mut(vector_name) {
                    *count += indexed_vector_count;
                } else {
                    indexed_vectors.insert(vector_name.clone(), indexed_vector_count);
                }

                vector_index.fill_idf_statistics(idf, &hw_counter);
            }
        }
    }
}
