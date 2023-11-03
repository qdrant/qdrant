use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use segment::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use segment::data_types::vectors::QueryVector;
use segment::fixtures::payload_context_fixture::FixtureIdTracker;
use segment::fixtures::payload_fixtures::STR_KEY;
use segment::index::sparse_index::sparse_vector_index::SparseVectorIndex;
use segment::index::struct_payload_index::StructPayloadIndex;
use segment::index::{PayloadIndex, VectorIndex};
use segment::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use segment::types::PayloadFieldSchema::FieldType;
use segment::types::PayloadSchemaType::Keyword;
use segment::types::{Condition, Distance, FieldCondition, Filter, Payload};
use segment::vector_storage::simple_sparse_vector_storage::open_simple_sparse_vector_storage;
use segment::vector_storage::VectorStorage;
use serde_json::json;
use sparse::common::sparse_vector::SparseVector;
use sparse::common::sparse_vector_fixture::{random_full_sparse_vector, random_sparse_vector};
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use sparse::index::inverted_index::InvertedIndex;
use tempfile::Builder;

/// Max dimension of sparse vectors used in tests
const MAX_SPARSE_DIM: usize = 1024;

/// Prepares a sparse vector index with random sparse vectors
fn fixture_sparse_index<I: InvertedIndex, R: Rng + ?Sized>(
    rnd: &mut R,
    max_dim: usize,
    stopped: &AtomicBool,
) -> SparseVectorIndex<I> {
    // test params
    let num_vectors = 1000;

    // temp dirs
    let payload_dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
    let index_dir = Builder::new().prefix("index_dir").tempdir().unwrap();
    let storage_dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    // setup
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(num_vectors)));
    let payload_storage = InMemoryPayloadStorage::default();
    let wrapped_payload_storage = Arc::new(AtomicRefCell::new(payload_storage.into()));
    let payload_index = StructPayloadIndex::open(
        wrapped_payload_storage,
        id_tracker.clone(),
        payload_dir.path(),
        true,
    )
    .unwrap();
    let wrapped_payload_index = Arc::new(AtomicRefCell::new(payload_index));

    let db = open_db(storage_dir.path(), &[DB_VECTOR_CF]).unwrap();
    let vector_storage =
        open_simple_sparse_vector_storage(db, DB_VECTOR_CF, Distance::Dot).unwrap();

    let mut sparse_vector_index: SparseVectorIndex<I> = SparseVectorIndex::open(
        id_tracker,
        vector_storage.clone(),
        wrapped_payload_index,
        index_dir.path(),
    )
    .unwrap();

    // add points to storage
    for idx in 0..num_vectors {
        let vec = &random_sparse_vector(rnd, max_dim);
        vector_storage
            .borrow_mut()
            .insert_vector(idx as PointOffsetType, vec.into())
            .unwrap();
    }
    assert_eq!(
        vector_storage.borrow().available_vector_count(),
        num_vectors
    );

    // build index
    sparse_vector_index.build_index(stopped).unwrap();
    assert_eq!(sparse_vector_index.indexed_vector_count(), num_vectors);
    sparse_vector_index
}

#[test]
fn sparse_vector_index_ram_no_filter_search() {
    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);

    let sparse_vector_index: SparseVectorIndex<InvertedIndexRam> =
        fixture_sparse_index(&mut rnd, MAX_SPARSE_DIM, &stopped);

    // random query vectors
    let attempts = 100;
    let query_vectors = (0..attempts)
        .map(|_| random_sparse_vector(&mut rnd, MAX_SPARSE_DIM))
        .collect::<Vec<_>>();

    // filter matches everything
    let filter = Filter::new_must_not(Condition::Field(FieldCondition::new_match(
        STR_KEY,
        STR_KEY.to_owned().into(),
    )));

    // compares results with and without filters
    // expects the filter to have no effect on the results because the filter matches everything
    for query in query_vectors.into_iter() {
        // top to get all results
        let top = sparse_vector_index.max_result_count(&query);
        assert!(top > 0);
        let query_vector: QueryVector = query.into();
        // with filter
        let index_results_filter = sparse_vector_index
            .search(&[&query_vector], Some(&filter), top, None, &stopped)
            .unwrap();

        // without filter
        let index_results_no_filter = sparse_vector_index
            .search(&[&query_vector], None, top, None, &stopped)
            .unwrap();

        assert_eq!(index_results_filter.len(), index_results_no_filter.len());

        for (filter_result, no_filter_result) in index_results_filter
            .iter()
            .zip(index_results_no_filter.iter())
        {
            assert_eq!(filter_result.len(), top);
            assert_eq!(filter_result.len(), no_filter_result.len());
            for (filter_result, no_filter_result) in
                filter_result.iter().zip(no_filter_result.iter())
            {
                assert_eq!(filter_result, no_filter_result);
            }
        }
    }
}

#[test]
fn sparse_vector_index_ram_consistent_with_storage() {
    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);

    let sparse_vector_index: SparseVectorIndex<InvertedIndexRam> =
        fixture_sparse_index(&mut rnd, MAX_SPARSE_DIM, &stopped);
    let borrowed_vector_storage = sparse_vector_index.vector_storage.borrow();
    let point_count = borrowed_vector_storage.available_vector_count();
    for id in 0..point_count as PointOffsetType {
        // assuming no deleted points
        let vector: &SparseVector = borrowed_vector_storage.get_vector(id).try_into().unwrap();
        // check posting lists are consistent with storage
        for (dim_id, dim_value) in vector.indices.iter().zip(vector.values.iter()) {
            let posting_list = sparse_vector_index.inverted_index.get(dim_id).unwrap();
            // assert posting list sorted  by record id
            assert!(posting_list
                .elements
                .windows(2)
                .all(|w| w[0].record_id < w[1].record_id));
            // assert posted list contains record id
            assert!(posting_list
                .elements
                .iter()
                .any(|e| e.record_id == id && e.weight == *dim_value));
        }
        // check the vector can be found via search using large top
        let top = sparse_vector_index.max_result_count(vector);
        let query_vector: QueryVector = vector.to_owned().into();
        let results = sparse_vector_index
            .search(&[&query_vector], None, top, None, &stopped)
            .unwrap();
        assert!(results[0].iter().any(|s| s.idx == id));
    }
}

#[test]
fn sparse_vector_index_ram_deleted_points_search() {
    let stopped = AtomicBool::new(false);
    let top = 10;
    let mut rnd = StdRng::seed_from_u64(42);

    let mut sparse_vector_index: SparseVectorIndex<InvertedIndexRam> =
        fixture_sparse_index(&mut rnd, MAX_SPARSE_DIM, &stopped);

    // sanity check (all indexed, no deleted points)
    assert_eq!(
        sparse_vector_index
            .id_tracker
            .borrow()
            .available_point_count(),
        sparse_vector_index.indexed_vector_count()
    );
    assert_eq!(
        sparse_vector_index
            .id_tracker
            .borrow()
            .deleted_point_count(),
        0
    );

    // query index
    let query_vector: QueryVector = random_sparse_vector(&mut rnd, MAX_SPARSE_DIM).into();
    let before_deletion_results: Vec<_> = sparse_vector_index
        .search(&[&query_vector], None, top, None, &stopped)
        .unwrap();

    // pick a point to delete
    let deleted_idx = before_deletion_results[0][0].idx;

    // delete a point
    let deleted_external = sparse_vector_index
        .id_tracker
        .borrow_mut()
        .external_id(deleted_idx)
        .unwrap();
    sparse_vector_index
        .id_tracker
        .borrow_mut()
        .drop(deleted_external)
        .unwrap();

    assert!(sparse_vector_index
        .id_tracker
        .borrow()
        .is_deleted_point(deleted_idx));
    assert_eq!(
        sparse_vector_index
            .id_tracker
            .borrow()
            .deleted_point_count(),
        1
    );
    // still need to update index
    assert_eq!(
        sparse_vector_index
            .id_tracker
            .borrow()
            .available_point_count(),
        sparse_vector_index.indexed_vector_count() - 1
    );

    // refresh index to remove point
    sparse_vector_index.build_index(&stopped).unwrap();
    assert_eq!(
        sparse_vector_index
            .id_tracker
            .borrow()
            .available_point_count(),
        sparse_vector_index.indexed_vector_count()
    );

    // assert that the deleted point is no longer in the index
    let after_deletion_results: Vec<_> = sparse_vector_index
        .search(&[&query_vector], None, top, None, &stopped)
        .unwrap();
    assert_ne!(before_deletion_results, after_deletion_results);
    assert!(after_deletion_results
        .iter()
        .all(|x| x.iter().all(|y| y.idx != deleted_idx)));
}

#[test]
fn sparse_vector_index_ram_filtered_search() {
    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);
    let field_name = "field";
    let field_value = "important value";

    // setup index
    let sparse_vector_index: SparseVectorIndex<InvertedIndexRam> =
        fixture_sparse_index(&mut rnd, MAX_SPARSE_DIM, &stopped);

    // query index by payload
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        field_name,
        field_value.to_owned().into(),
    )));

    // query all sparse dimension to get all points
    let query_vector: QueryVector = random_full_sparse_vector(&mut rnd, MAX_SPARSE_DIM).into();
    let before_result = sparse_vector_index
        .search(&[&query_vector], Some(&filter), 10, None, &stopped)
        .unwrap();
    assert_eq!(before_result.len(), 1);
    assert_eq!(before_result[0].len(), 0);

    // create payload field index
    let mut payload_index = sparse_vector_index.payload_index.borrow_mut();
    payload_index
        .set_indexed(field_name, Keyword.into())
        .unwrap();
    drop(payload_index);

    // assert payload field index created and empty
    let payload_index = sparse_vector_index.payload_index.borrow();
    let indexed_fields = payload_index.indexed_fields();
    assert_eq!(*indexed_fields.get(field_name).unwrap(), FieldType(Keyword));

    let field_indexes = &payload_index.field_indexes;
    let field_index = field_indexes.get(field_name).unwrap();
    assert_eq!(field_index[0].count_indexed_points(), 0);
    drop(payload_index);

    // add payload on the first half of the points
    let half_indexed_count = sparse_vector_index.indexed_vector_count() / 2;
    let payload: Payload = json!({
        field_name: field_value,
    })
    .into();
    let mut payload_index = sparse_vector_index.payload_index.borrow_mut();
    for idx in 0..half_indexed_count {
        payload_index
            .assign(idx as PointOffsetType, &payload)
            .unwrap();
    }
    drop(payload_index);

    // assert payload index updated
    let payload_index = sparse_vector_index.payload_index.borrow();
    let field_indexes = &payload_index.field_indexes;
    let field_index = field_indexes.get(field_name).unwrap();
    assert_eq!(field_index[0].count_indexed_points(), half_indexed_count);
    drop(payload_index);

    // request all points with payload
    let after_result = sparse_vector_index
        .search(
            &[&query_vector],
            Some(&filter),
            half_indexed_count * 2, // original top
            None,
            &stopped,
        )
        .unwrap();
    assert_eq!(after_result.len(), 1);
    assert_eq!(after_result[0].len(), half_indexed_count); // expect half of the points
}
