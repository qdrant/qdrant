use std::sync::atomic::AtomicBool;

use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::SeedableRng;
use segment::common::operation_error::OperationResult;
use segment::data_types::vectors::QueryVector;
use segment::fixtures::payload_fixtures::STR_KEY;
use segment::fixtures::sparse_fixtures::{fixture_open_sparse_index, fixture_sparse_index_ram};
use segment::index::sparse_index::sparse_vector_index::SparseVectorIndex;
use segment::index::{PayloadIndex, VectorIndex};
use segment::types::PayloadFieldSchema::FieldType;
use segment::types::PayloadSchemaType::Keyword;
use segment::types::{Condition, FieldCondition, Filter, Payload};
use segment::vector_storage::VectorStorage;
use serde_json::json;
use sparse::common::sparse_vector::SparseVector;
use sparse::common::sparse_vector_fixture::{random_full_sparse_vector, random_sparse_vector};
use sparse::index::inverted_index::inverted_index_mmap::InvertedIndexMmap;
use sparse::index::inverted_index::InvertedIndex;
use tempfile::Builder;

/// Max dimension of sparse vectors used in tests
const MAX_SPARSE_DIM: usize = 2048;

/// Number of vectors to index in tests
const NUM_VECTORS: usize = 1000;

/// Default full scan threshold in tests
/// very low value to force usage of index
const LOW_FULL_SCAN_THRESHOLD: usize = 1;

/// Expects the filter to match ALL points in order to compare the results with/without filter
fn compare_sparse_vectors_search_with_without_filter(full_scan_threshold: usize) {
    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();

    let sparse_vector_index = fixture_sparse_index_ram(
        &mut rnd,
        NUM_VECTORS,
        MAX_SPARSE_DIM,
        full_scan_threshold,
        data_dir.path(),
        &stopped,
    );

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
fn sparse_vector_index_ram_filter_search() {
    // very low full scan threshold to force usage of inverted index
    compare_sparse_vectors_search_with_without_filter(LOW_FULL_SCAN_THRESHOLD);
}

#[test]
fn sparse_vector_index_fallback_plain_search() {
    // very high full scan threshold to force fallback to plain search
    compare_sparse_vectors_search_with_without_filter(NUM_VECTORS + 1);
}

/// Checks that the sparse vector index is consistent with the underlying storage
fn check_index_storage_consistency<T: InvertedIndex>(sparse_vector_index: &SparseVectorIndex<T>) {
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
            .search(&[&query_vector], None, top, None, &false.into())
            .unwrap();
        assert!(results[0].iter().any(|s| s.idx == id));
    }
}

#[test]
fn sparse_vector_index_consistent_with_storage() {
    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();
    let sparse_vector_ram_index = fixture_sparse_index_ram(
        &mut rnd,
        NUM_VECTORS,
        MAX_SPARSE_DIM,
        LOW_FULL_SCAN_THRESHOLD,
        data_dir.path(),
        &stopped,
    );

    // check consistency with underlying RAM inverted index
    check_index_storage_consistency(&sparse_vector_ram_index);

    let mmap_index_dir = Builder::new().prefix("mmap_index_dir").tempdir().unwrap();
    // copy index mmap and save to disk
    let mmap_inverted_index = InvertedIndexMmap::convert_and_save(
        &sparse_vector_ram_index.inverted_index,
        &mmap_index_dir,
    )
    .unwrap();
    drop(mmap_inverted_index);

    // load index from memmap file
    let sparse_index_config = sparse_vector_ram_index.config;
    let sparse_vector_mmap_index: SparseVectorIndex<InvertedIndexMmap> = SparseVectorIndex::open(
        sparse_index_config,
        sparse_vector_ram_index.id_tracker.clone(),
        sparse_vector_ram_index.vector_storage.clone(),
        sparse_vector_ram_index.payload_index.clone(),
        mmap_index_dir.path(),
    )
    .unwrap();

    assert_eq!(
        sparse_vector_mmap_index.indexed_vector_count(),
        sparse_vector_ram_index.indexed_vector_count()
    );

    // check consistency with underlying mmap inverted index
    check_index_storage_consistency(&sparse_vector_mmap_index);
}

#[test]
fn sparse_vector_index_load_missing_mmap() {
    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();
    let sparse_vector_index: OperationResult<SparseVectorIndex<InvertedIndexMmap>> =
        fixture_open_sparse_index(data_dir.path(), 0, 10_000);
    // fails to open index if mmap file is missing
    assert!(sparse_vector_index.is_err())
}

#[test]
fn sparse_vector_index_ram_deleted_points_search() {
    let stopped = AtomicBool::new(false);
    let top = 10;
    let mut rnd = StdRng::seed_from_u64(42);

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();

    let mut sparse_vector_index = fixture_sparse_index_ram(
        &mut rnd,
        NUM_VECTORS,
        MAX_SPARSE_DIM,
        LOW_FULL_SCAN_THRESHOLD,
        data_dir.path(),
        &stopped,
    );

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

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();

    // setup index
    let sparse_vector_index = fixture_sparse_index_ram(
        &mut rnd,
        NUM_VECTORS,
        MAX_SPARSE_DIM,
        LOW_FULL_SCAN_THRESHOLD,
        data_dir.path(),
        &stopped,
    );

    // query index by payload
    let field_name = "field";
    let field_value = "important value";
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

#[test]
fn sparse_vector_index_plain_search() {
    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();
    // setup index
    let sparse_vector_index = fixture_sparse_index_ram(
        &mut rnd,
        NUM_VECTORS,
        MAX_SPARSE_DIM,
        LOW_FULL_SCAN_THRESHOLD,
        data_dir.path(),
        &stopped,
    );

    // query index by payload
    let field_name = "field";
    let field_value = "important value";
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        field_name,
        field_value.to_owned().into(),
    )));

    // query all sparse dimension to get all points
    let query_vector: QueryVector = random_full_sparse_vector(&mut rnd, MAX_SPARSE_DIM).into();

    // empty when searching payload index directly
    let before_plain_results = sparse_vector_index
        .search_plain(&[&query_vector], &filter, 10, &stopped)
        .unwrap();

    assert_eq!(before_plain_results.len(), 1);
    assert_eq!(before_plain_results[0].len(), 0);

    let payload: Payload = json!({
        field_name: field_value,
    })
    .into();

    // add payload to all points
    let mut payload_index = sparse_vector_index.payload_index.borrow_mut();
    for idx in 0..NUM_VECTORS {
        payload_index
            .assign(idx as PointOffsetType, &payload)
            .unwrap();
    }
    drop(payload_index);

    // same results when searching payload index directly
    let after_plain_results = sparse_vector_index
        .search_plain(&[&query_vector], &filter, NUM_VECTORS, &stopped)
        .unwrap();

    assert_eq!(after_plain_results.len(), 1);
    assert_eq!(after_plain_results[0].len(), NUM_VECTORS);
}
