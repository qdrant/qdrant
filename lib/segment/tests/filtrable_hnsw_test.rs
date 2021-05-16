
#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use segment::types::{StorageType, Distance, PayloadIndexType, Indexes, SegmentConfig, TheMap, PayloadKeyType, PayloadType};
    use segment::segment_constructor::segment_constructor::build_segment;
    use segment::fixtures::payload_fixtures::{random_vector, random_int_payload};
    use segment::entry::entry_point::SegmentEntry;
    use segment::index::struct_payload_index::StructPayloadIndex;
    use segment::index::index::{PayloadIndex, Index};
    use segment::index::hnsw_index::hnsw::HNSWIndex;
    use std::sync::Arc;
    use atomic_refcell::AtomicRefCell;

    #[test]
    fn test_filterable_hnsw() {
        let dim = 8;
        let m = 8;
        let num_vectors = 10_000;
        let ef = 32;
        let ef_construct = 16;
        let distance = Distance::Cosine;
        let indexing_threshold = 1000;

        let dir = TempDir::new("segment_dir").unwrap();
        let payload_index_dir = TempDir::new("payload_index_dir").unwrap();
        let hnsw_dir = TempDir::new("hnsw_dir").unwrap();

        let mut config = SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
            distance,
            indexing_threshold,
        };

        let int_key = "int".to_string();

        let mut segment = build_segment(dir.path(), &config).unwrap();
        for idx in 0..num_vectors {
            let vector = random_vector(&mut rnd, dim);
            let mut payload: TheMap<PayloadKeyType, PayloadType> = Default::default();
            payload.insert(int_key.clone(), random_int_payload(&mut rnd));

            segment.upsert_point(idx, idx, &vector).unwrap();
            segment.set_full_payload(idx, idx, payload.clone()).unwrap();
        }
        let mut opnum = num_vectors + 1;

        let mut payload_index = StructPayloadIndex::open(
            segment.condition_checker.clone(),
            segment.vector_storage.clone(),
            segment.payload_storage.clone(),
            segment.id_mapper.clone(),
            payload_index_dir.path()
        ).unwrap();

        payload_index.set_indexed(&int_key).unwrap();

        let payload_index_ptr = Arc::new(AtomicRefCell::new(payload_index));

        let index_config = Some(Indexes::Hnsw {
            m,
            ef_construct
        });

        let mut hnsw_index = HNSWIndex::open(
            hnsw_dir.path(),
            distance,
            segment.condition_checker.clone(),
            segment.vector_storage.clone(),
            payload_index_ptr.clone(),
            index_config,
            indexing_threshold
        ).unwrap();

        hnsw_index.build_index();

    }
}