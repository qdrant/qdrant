use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::iterator_ext::IteratorExt;
use common::types::{DeferredBehavior, PointOffsetType, ScoredPointOffset};
use smallvec::SmallVec;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::{check_query_vectors, check_stopped};
use crate::data_types::query_context::{
    IdfScopeStats, QueryContext, QueryIdfStats, SegmentQueryContext,
};
use crate::data_types::segment_record::{SegmentRecord, SegmentRecordRaw};
use crate::data_types::vectors::{QueryVector, VectorStructInternal};
use crate::id_tracker::IdTrackerRead;
use crate::index::{PayloadIndexRead, VectorIndexRead};
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{
    ExtendedPointId, Filter, Payload, PointIdType, ScoredPoint, SearchParams, VectorName,
    VectorNameBuf, WithPayload, WithVector,
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
        let mut resolved_ids = Vec::with_capacity(point_ids.len());
        let mut resolved_offsets = Vec::with_capacity(point_ids.len());
        self.id_tracker
            .resolve_external_ids(point_ids, deferred_behavior, |point_id, offset| {
                resolved_ids.push(point_id);
                resolved_offsets.push(offset);
            });

        // One blank record per resolved point; `vectors` is `Some` only when
        // vectors were requested, so the `WithVector::Bool(false)` path needs
        // no cleanup.
        let mut records: AHashMap<ExtendedPointId, SegmentRecord> = resolved_ids
            .iter()
            .map(|&id| {
                let record = SegmentRecord {
                    id,
                    vectors: needs_vectors(with_vector).then(SmallVec::new),
                    payload: None,
                };
                (id, record)
            })
            .collect();

        for vector_name in self.requested_vector_names(with_vector) {
            let keys = resolved_keys(&resolved_ids, &resolved_offsets, is_stopped);
            self.vectors_by_offsets(vector_name, keys, hw_counter, |id, _offset, vector| {
                if let Some(record) = records.get_mut(&id) {
                    record
                        .vectors
                        .as_mut()
                        .expect("vectors is Some whenever vector names are requested")
                        .push((vector_name.clone(), vector));
                }
            })?;
        }

        let payloads = self.requested_payloads(
            resolved_ids,
            resolved_offsets,
            with_payload,
            is_stopped,
            hw_counter,
        )?;
        for (id, payload) in payloads {
            if let Some(record) = records.get_mut(&id) {
                record.payload = Some(payload);
            }
        }

        Ok(records)
    }

    /// Byte-blob analogue of [`Self::retrieve`]: vectors are read as
    /// storage-native bytes (`Vec<u8>`) to avoid a lossy round-trip.
    /// The body mirrors [`Self::retrieve`] — keep the two in sync.
    pub fn retrieve_raw(
        &self,
        point_ids: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<AHashMap<ExtendedPointId, SegmentRecordRaw>> {
        let mut resolved_ids = Vec::with_capacity(point_ids.len());
        let mut resolved_offsets = Vec::with_capacity(point_ids.len());
        self.id_tracker.resolve_external_ids(
            point_ids.iter().copied(),
            deferred_behavior,
            |point_id, offset| {
                resolved_ids.push(point_id);
                resolved_offsets.push(offset);
            },
        );

        // One blank record per resolved point; `vectors` is `Some` only when
        // vectors were requested, so the `WithVector::Bool(false)` path needs
        // no cleanup.
        let mut records: AHashMap<ExtendedPointId, SegmentRecordRaw> = resolved_ids
            .iter()
            .map(|&id| {
                let record = SegmentRecordRaw {
                    id,
                    vectors: needs_vectors(with_vector).then(SmallVec::new),
                    payload: None,
                };
                (id, record)
            })
            .collect();

        for vector_name in self.requested_vector_names(with_vector) {
            let keys = resolved_keys(&resolved_ids, &resolved_offsets, is_stopped);
            self.vector_bytes_by_offsets(vector_name, keys, hw_counter, |id, _offset, bytes| {
                if let Some(record) = records.get_mut(&id) {
                    record
                        .vectors
                        .as_mut()
                        .expect("vectors is Some whenever vector names are requested")
                        .push((vector_name.clone(), bytes));
                }
            })?;
        }

        let payloads = self.requested_payloads(
            resolved_ids,
            resolved_offsets,
            with_payload,
            is_stopped,
            hw_counter,
        )?;
        for (id, payload) in payloads {
            if let Some(record) = records.get_mut(&id) {
                record.payload = Some(payload);
            }
        }

        Ok(records)
    }

    /// The vector names retrieval must read for the given `with_vector`:
    /// all of them, a selection, or none.
    fn requested_vector_names<'a>(
        &'a self,
        with_vector: &'a WithVector,
    ) -> impl Iterator<Item = &'a VectorNameBuf> {
        let (all, selected) = match with_vector {
            WithVector::Bool(true) => (Some(self.vector_data.keys()), None),
            WithVector::Bool(false) => (None, None),
            WithVector::Selector(names) => (None, Some(names.iter())),
        };
        all.into_iter()
            .flatten()
            .chain(selected.into_iter().flatten())
    }

    /// Reads each resolved point's (selected) payload, reusing the
    /// already-resolved offsets. Empty when payload wasn't requested.
    fn requested_payloads(
        &self,
        resolved_ids: Vec<ExtendedPointId>,
        resolved_offsets: Vec<PointOffsetType>,
        with_payload: &WithPayload,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<(ExtendedPointId, Payload)>> {
        if !with_payload.enable {
            return Ok(Vec::new());
        }

        let mut payloads = Vec::with_capacity(resolved_ids.len());
        let point_offsets = resolved_ids.into_iter().zip(resolved_offsets);

        self.read_payloads::<Random, _>(
            point_offsets,
            |point_id, payload| {
                check_stopped(is_stopped)?;

                let payload = match &with_payload.payload_selector {
                    Some(selector) => selector.process(payload),
                    None => payload,
                };

                payloads.push((point_id, payload));

                Ok(())
            },
            hw_counter,
        )?;

        Ok(payloads)
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
        let external_ids = self
            .id_tracker
            .external_ids_batch(internal_result.iter().map(|scored| scored.idx));

        let (point_ids, scored_offsets): (Vec<_>, Vec<_>) = internal_result
            .into_iter()
            .zip(external_ids)
            .filter_map(|(scored_point_offset, point_id)| {
                // This can happen if a point was modified between retrieving and post-processing,
                // but this function locks the segment so it can't be modified during execution.
                debug_assert!(
                    point_id.is_some(),
                    "Point with internal ID {} not found in id tracker",
                    scored_point_offset.idx,
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

        let versions = self
            .id_tracker
            .internal_versions_batch(scored_offsets.iter().map(|scored| scored.idx));

        let mut results = Vec::with_capacity(point_ids.len());

        for ((point_id, scored_offset), point_version) in
            point_ids.into_iter().zip(scored_offsets).zip(versions)
        {
            let ScoredPointOffset {
                idx: _,
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

            let point_version = point_version.ok_or_else(|| {
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
        let idf_corpus = params
            .and_then(|params| params.idf.as_ref())
            .and_then(|idf| idf.corpus());
        let vector_query_context = query_context.get_vector_context(vector_name, idf_corpus);
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
        let hw_counter = query_context
            .hardware_usage_accumulator()
            .get_counter_cell();
        let is_stopped = query_context.is_stopped_handle();

        let QueryIdfStats { scopes } = query_context.mut_idf_stats();

        for scope in scopes.iter_mut() {
            let IdfScopeStats {
                corpus,
                idf,
                indexed_vectors,
            } = scope;

            for (vector_name, idf) in idf.iter_mut() {
                if let Some(vector_data) = self.vector_data.get(vector_name) {
                    let document_count = vector_data.vector_index().fill_idf_statistics(
                        idf,
                        corpus.as_ref(),
                        &is_stopped,
                        &hw_counter,
                    )?;

                    if let Some(count) = indexed_vectors.get_mut(vector_name) {
                        *count += document_count;
                    } else {
                        indexed_vectors.insert(vector_name.clone(), document_count);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Whether records must carry a `vectors` container for the given request.
fn needs_vectors(with_vector: &WithVector) -> bool {
    match with_vector {
        WithVector::Bool(true) | WithVector::Selector(_) => true,
        WithVector::Bool(false) => false,
    }
}

/// Resolved `(external id, offset)` keys for a vector reader, wired to the
/// stop signal.
fn resolved_keys<'a>(
    resolved_ids: &'a [ExtendedPointId],
    resolved_offsets: &'a [PointOffsetType],
    is_stopped: &'a AtomicBool,
) -> impl Iterator<Item = (ExtendedPointId, PointOffsetType)> + 'a {
    resolved_ids
        .iter()
        .copied()
        .zip(resolved_offsets.iter().copied())
        .stop_if(is_stopped)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use common::types::DeferredBehavior;
    use rstest::rstest;
    use sparse::common::sparse_vector::SparseVector;
    use tempfile::Builder;

    use crate::common::operation_error::OperationError;
    use crate::data_types::named_vectors::NamedVectors;
    use crate::data_types::vectors::VectorInternal;
    use crate::entry::entry_point::{ReadSegmentEntry as _, SegmentEntry as _};
    use crate::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
    use crate::segment::Segment;
    use crate::segment_constructor::build_segment;
    use crate::types::{
        Distance, Indexes, Payload, SegmentConfig, SparseVectorDataConfig, SparseVectorStorageType,
        VectorDataConfig, VectorStorageType, WithPayload, WithVector,
    };
    use crate::vector_storage::sparse::StoredSparseVector;

    const DENSE_NAME: &str = "dense";
    const SPARSE_NAME: &str = "sparse";
    const DIM: usize = 4;

    /// Two-point segment for the retrieve equivalence tests: point 1 has the
    /// dense + sparse vectors and a payload, point 2 only the dense vector and
    /// no payload. Returns the segment and point 1's payload.
    fn build_two_point_segment(path: &Path) -> (Segment, Payload) {
        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DENSE_NAME.to_owned(),
                VectorDataConfig {
                    size: DIM,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::default(),
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: HashMap::from([(
                SPARSE_NAME.to_owned(),
                SparseVectorDataConfig {
                    index: SparseIndexConfig::new(Some(1), SparseIndexType::MutableRam, None, None),
                    storage_type: SparseVectorStorageType::Mmap,
                    modifier: None,
                },
            )]),
            payload_storage_type: Default::default(),
        };
        let (mut segment, _) = build_segment(path, &config, None, true).unwrap();
        let hw_counter = HardwareCounterCell::new();

        // Point 1: dense + sparse vectors, with payload.
        let sparse = SparseVector::new(vec![1, 5, 42], vec![0.5, 1.5, 2.5]).unwrap();
        let mut vectors = NamedVectors::default();
        vectors.insert(
            DENSE_NAME.to_owned(),
            VectorInternal::Dense(vec![0.1, 0.2, 0.3, 0.4]),
        );
        vectors.insert(SPARSE_NAME.to_owned(), VectorInternal::Sparse(sparse));
        segment
            .upsert_point(100, 1.into(), vectors, &hw_counter)
            .unwrap();
        let payload: Payload = serde_json::from_str(r#"{"city": "Berlin", "count": 3}"#).unwrap();
        segment
            .set_full_payload(101, 1.into(), &payload, &hw_counter)
            .unwrap();

        // Point 2: dense vector only, no payload.
        let mut vectors = NamedVectors::default();
        vectors.insert(
            DENSE_NAME.to_owned(),
            VectorInternal::Dense(vec![1.0, 2.0, 3.0, 4.0]),
        );
        segment
            .upsert_point(102, 2.into(), vectors, &hw_counter)
            .unwrap();

        (segment, payload)
    }

    /// [`SegmentReadView::retrieve`] and [`SegmentReadView::retrieve_raw`]
    /// have hand-duplicated bodies — this guards them against drifting apart:
    /// both must return the same records (ids, payloads, vector names), and
    /// the raw storage-native bytes must decode to exactly the vectors
    /// `retrieve` returns. Covers every `with_vector` shape crossed with
    /// payload on/off; `expected_point1_vectors` pins how many vectors the
    /// fully-vectored point must come back with.
    #[rstest]
    #[case::all_vectors(WithVector::Bool(true), 2)]
    #[case::no_vectors(WithVector::Bool(false), 0)]
    #[case::empty_selector(WithVector::Selector(vec![]), 0)]
    #[case::dense_selector(WithVector::Selector(vec![DENSE_NAME.to_owned()]), 1)]
    #[case::sparse_then_dense_selector(
        WithVector::Selector(vec![SPARSE_NAME.to_owned(), DENSE_NAME.to_owned()]),
        2
    )]
    #[case::duplicate_name_selector(
        WithVector::Selector(vec![DENSE_NAME.to_owned(), DENSE_NAME.to_owned()]),
        2
    )]
    fn test_retrieve_and_retrieve_raw_equivalence(
        #[case] with_vector: WithVector,
        #[case] expected_point1_vectors: usize,
        #[values(true, false)] payload_enabled: bool,
    ) {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let (segment, payload) = build_two_point_segment(dir.path());
        let hw_counter = HardwareCounterCell::new();

        // Id 3 does not exist; both functions must skip it the same way.
        let point_ids = [1.into(), 2.into(), 3.into()];
        let with_payload = WithPayload {
            enable: payload_enabled,
            payload_selector: None,
        };
        // Expectation derived independently of the `needs_vectors` helper the
        // functions under test share.
        let vectors_requested = match &with_vector {
            WithVector::Bool(true) | WithVector::Selector(_) => true,
            WithVector::Bool(false) => false,
        };
        let is_stopped = AtomicBool::new(false);

        let records = segment
            .retrieve(
                &point_ids,
                &with_payload,
                &with_vector,
                &hw_counter,
                &is_stopped,
                DeferredBehavior::VisibleOnly,
            )
            .unwrap();
        let raw_records = segment
            .retrieve_raw(
                &point_ids,
                &with_payload,
                &with_vector,
                &hw_counter,
                &is_stopped,
                DeferredBehavior::VisibleOnly,
            )
            .unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records.len(), raw_records.len());

        // Sanity beyond equivalence, so the loop below can't pass vacuously:
        // the fully-populated point must carry the requested vectors/payload.
        let point1 = records.get(&1.into()).expect("point 1 must be retrieved");
        if vectors_requested {
            let point1_vectors = point1.vectors.as_ref().expect("vectors requested");
            assert_eq!(point1_vectors.len(), expected_point1_vectors);
        }
        if payload_enabled {
            assert_eq!(point1.payload.as_ref(), Some(&payload));
        }

        for (id, record) in &records {
            let raw_record = raw_records.get(id).expect("raw record for the same id");
            assert_eq!(record.id, raw_record.id);
            assert_eq!(record.payload, raw_record.payload);
            if !payload_enabled {
                assert!(record.payload.is_none(), "payload was not requested");
            }

            if !vectors_requested {
                assert!(record.vectors.is_none(), "vectors were not requested");
                assert!(raw_record.vectors.is_none(), "vectors were not requested");
                continue;
            }

            let vectors = record.vectors.as_ref().expect("vectors requested");
            let raw_vectors = raw_record.vectors.as_ref().expect("vectors requested");
            assert_eq!(
                vectors.len(),
                raw_vectors.len(),
                "point {id} must have the same vector names in both results",
            );

            // Both functions read vector names in the same order.
            for ((name, vector), (raw_name, bytes)) in vectors.iter().zip(raw_vectors) {
                assert_eq!(name, raw_name);
                let decoded = match name.as_str() {
                    DENSE_NAME => VectorInternal::Dense(
                        bytes
                            .chunks_exact(size_of::<f32>())
                            .map(|chunk| f32::from_ne_bytes(chunk.try_into().unwrap()))
                            .collect(),
                    ),
                    SPARSE_NAME => VectorInternal::Sparse(
                        SparseVector::try_from(StoredSparseVector::try_from_bytes(bytes).unwrap())
                            .unwrap(),
                    ),
                    other => panic!("unexpected vector name {other}"),
                };
                assert_eq!(vector, &decoded, "vector {name} of point {id} must match");
            }
        }
    }

    /// A selector naming a vector the segment doesn't have must fail with the
    /// exact same error from both functions — the two bodies hit different
    /// storage readers, so one erroring while the other silently skips is the
    /// kind of drift this guards against.
    #[test]
    fn test_retrieve_and_retrieve_raw_unknown_vector_name_error() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let (segment, _payload) = build_two_point_segment(dir.path());
        let hw_counter = HardwareCounterCell::new();
        let is_stopped = AtomicBool::new(false);

        let point_ids = [1.into()];
        let with_payload = WithPayload {
            enable: false,
            payload_selector: None,
        };
        let with_vector = WithVector::Selector(vec!["unknown".to_owned()]);

        let err = segment
            .retrieve(
                &point_ids,
                &with_payload,
                &with_vector,
                &hw_counter,
                &is_stopped,
                DeferredBehavior::VisibleOnly,
            )
            .err()
            .expect("retrieve of an unknown vector name must fail");
        let raw_err = segment
            .retrieve_raw(
                &point_ids,
                &with_payload,
                &with_vector,
                &hw_counter,
                &is_stopped,
                DeferredBehavior::VisibleOnly,
            )
            .err()
            .expect("retrieve_raw of an unknown vector name must fail");

        assert_eq!(err, raw_err);
        assert_eq!(err, OperationError::vector_name_not_exists("unknown"));
    }

    /// A deleted named vector must be skipped by both functions. This is the
    /// one behavior the two vector readers (`vectors_by_offsets` vs
    /// `vector_bytes_by_offsets`) implement independently at the storage
    /// layer, so it gets its own equivalence check.
    #[test]
    fn test_retrieve_and_retrieve_raw_deleted_vector_equivalence() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let (mut segment, _payload) = build_two_point_segment(dir.path());
        let hw_counter = HardwareCounterCell::new();
        let is_stopped = AtomicBool::new(false);

        assert!(segment.delete_vector(103, 1.into(), DENSE_NAME).unwrap());

        let point_ids = [1.into(), 2.into()];
        let with_payload = WithPayload {
            enable: false,
            payload_selector: None,
        };
        let with_vector = WithVector::Bool(true);

        let records = segment
            .retrieve(
                &point_ids,
                &with_payload,
                &with_vector,
                &hw_counter,
                &is_stopped,
                DeferredBehavior::VisibleOnly,
            )
            .unwrap();
        let raw_records = segment
            .retrieve_raw(
                &point_ids,
                &with_payload,
                &with_vector,
                &hw_counter,
                &is_stopped,
                DeferredBehavior::VisibleOnly,
            )
            .unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records.len(), raw_records.len());

        // Point 1's dense vector is deleted: only the sparse one remains, and
        // its raw bytes still decode to the vector `retrieve` returns.
        let point1_vectors = records
            .get(&1.into())
            .and_then(|record| record.vectors.as_deref())
            .expect("vectors requested");
        let raw_point1_vectors = raw_records
            .get(&1.into())
            .and_then(|record| record.vectors.as_deref())
            .expect("vectors requested");
        let [(name, vector)] = point1_vectors else {
            panic!("expected exactly the sparse vector, got {point1_vectors:?}");
        };
        let [(raw_name, bytes)] = raw_point1_vectors else {
            panic!("expected exactly the sparse vector in the raw result");
        };
        assert_eq!(name, SPARSE_NAME);
        assert_eq!(raw_name, SPARSE_NAME);
        let decoded = VectorInternal::Sparse(
            SparseVector::try_from(StoredSparseVector::try_from_bytes(bytes).unwrap()).unwrap(),
        );
        assert_eq!(vector, &decoded);

        // Point 2 is untouched: same vector names in both results.
        let point2_names: Vec<_> = records
            .get(&2.into())
            .and_then(|record| record.vectors.as_deref())
            .expect("vectors requested")
            .iter()
            .map(|(name, _)| name)
            .collect();
        let raw_point2_names: Vec<_> = raw_records
            .get(&2.into())
            .and_then(|record| record.vectors.as_deref())
            .expect("vectors requested")
            .iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(point2_names, raw_point2_names);
        assert!(point2_names.contains(&&DENSE_NAME.to_owned()));
    }
}
