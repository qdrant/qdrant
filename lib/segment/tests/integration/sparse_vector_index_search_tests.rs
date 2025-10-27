use std::cmp::max;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, TelemetryDetail};
use fs_err as fs;
use io::storage_version::VERSION_FILE;
use itertools::Itertools;
use rand::SeedableRng;
use rand::rngs::StdRng;
use segment::common::operation_error::OperationResult;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{QueryVector, VectorInternal};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::STR_KEY;
use segment::fixtures::sparse_fixtures::{fixture_sparse_index, fixture_sparse_index_from_iter};
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::index::sparse_index::sparse_vector_index::{
    SparseVectorIndex, SparseVectorIndexOpenArgs,
};
use segment::index::{PayloadIndex, VectorIndex, VectorIndexEnum};
use segment::json_path::JsonPath;
use segment::segment::Segment;
use segment::segment_constructor::{build_segment, load_segment};
use segment::types::PayloadFieldSchema::FieldType;
use segment::types::PayloadSchemaType::Keyword;
use segment::types::{
    Condition, DEFAULT_SPARSE_FULL_SCAN_THRESHOLD, FieldCondition, Filter, ScoredPoint,
    SegmentConfig, SeqNumberType, SparseVectorDataConfig, SparseVectorStorageType, VectorName,
    VectorStorageDatatype,
};
use segment::vector_storage::{Random, VectorStorage};
use segment::{fixture_for_all_indices, payload_json};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::sparse_vector_fixture::{random_full_sparse_vector, random_sparse_vector};
use sparse::common::types::DimId;
use sparse::index::inverted_index::InvertedIndex;
use sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use sparse::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use sparse::index::posting_list_common::PostingListIter as _;
use tempfile::Builder;

/// Max dimension of sparse vectors used in tests
const MAX_SPARSE_DIM: usize = 4096;

/// Number of vectors to index in tests
const NUM_VECTORS: usize = 2000;

/// Default full scan threshold in tests
/// very low value to force usage of index
const LOW_FULL_SCAN_THRESHOLD: usize = 1;

/// Full scan threshold to force plain search
const LARGE_FULL_SCAN_THRESHOLD: usize = 10 * NUM_VECTORS;

const SPARSE_VECTOR_NAME: &VectorName = "sparse_vector";

/// Expects the filter to match ALL points in order to compare the results with/without filter
fn compare_sparse_vectors_search_with_without_filter(full_scan_threshold: usize) {
    let mut rnd = StdRng::seed_from_u64(43);

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();

    let sparse_vector_index = fixture_sparse_index::<InvertedIndexCompressedImmutableRam<f32>, _>(
        &mut rnd,
        NUM_VECTORS,
        MAX_SPARSE_DIM,
        full_scan_threshold,
        data_dir.path(),
    );

    // random query vectors
    let attempts = 1000;
    let query_vectors = (0..attempts)
        .map(|_| random_sparse_vector(&mut rnd, MAX_SPARSE_DIM))
        .collect::<Vec<_>>();

    // filter matches everything
    let filter = Filter::new_must_not(Condition::Field(FieldCondition::new_match(
        JsonPath::new(STR_KEY),
        STR_KEY.to_owned().into(),
    )));

    // compares results with and without filters
    // expects the filter to have no effect on the results because the filter matches everything
    for query in query_vectors {
        let maximum_number_of_results = sparse_vector_index.max_result_count(&query);
        // get all results minus 10 to force a bit of pruning
        let top = max(1, maximum_number_of_results.saturating_sub(10));
        let query_vector: QueryVector = query.clone().into();
        // with filter
        let index_results_filter = sparse_vector_index
            .search(
                &[&query_vector],
                Some(&filter),
                top,
                None,
                &Default::default(),
            )
            .unwrap();

        // without filter
        let index_results_no_filter = sparse_vector_index
            .search(&[&query_vector], None, top, None, &Default::default())
            .unwrap();

        assert_eq!(index_results_filter.len(), index_results_no_filter.len());

        for (filter_result, no_filter_result) in index_results_filter
            .iter()
            .zip(index_results_no_filter.iter())
        {
            assert_eq!(
                filter_result.len(),
                no_filter_result.len(),
                "query = {query:#?}, filter_result = {filter_result:#?} no_filter_result = {no_filter_result:#?}",
            );
            // skip zero scores because index skips non-overlapping points, but plain search does not
            for (filter_result, no_filter_result) in filter_result
                .iter()
                .filter(|s| s.score != 0.0)
                .zip(no_filter_result.iter().filter(|s| s.score != 0.0))
            {
                if filter_result.idx != no_filter_result.idx {
                    // we do not break ties when identical scores
                    assert_eq!(filter_result.score, no_filter_result.score);
                } else {
                    assert_eq!(filter_result, no_filter_result);
                }
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
    let borrowed_vector_storage = sparse_vector_index.vector_storage().borrow();
    let point_count = borrowed_vector_storage.available_vector_count();
    let hw_counter = HardwareCounterCell::disposable();
    for id in 0..point_count as PointOffsetType {
        // assuming no deleted points
        let vector = borrowed_vector_storage.get_vector::<Random>(id);
        let vector: &SparseVector = vector.as_vec_ref().try_into().unwrap();
        let remapped_vector = sparse_vector_index
            .indices_tracker()
            .remap_vector(vector.to_owned());
        // check posting lists are consistent with storage
        for (dim_id, dim_value) in remapped_vector
            .indices
            .iter()
            .zip(remapped_vector.values.iter())
        {
            let posting_list = sparse_vector_index
                .inverted_index()
                .get(*dim_id, &hw_counter)
                .unwrap();
            // assert posting list sorted by record id
            assert!(
                posting_list
                    .clone()
                    .into_std_iter()
                    .tuple_windows()
                    .all(|(w0, w1)| w0.record_id < w1.record_id),
            );
            // assert posted list contains record id
            assert!(
                posting_list
                    .into_std_iter()
                    .any(|e| e.record_id == id && e.weight == *dim_value),
            );
        }
        // check the vector can be found via search using large top
        let top = sparse_vector_index.max_result_count(vector);
        let query_vector: QueryVector = vector.to_owned().into();
        let results = sparse_vector_index
            .search(&[&query_vector], None, top, None, &Default::default())
            .unwrap();
        assert!(results[0].iter().any(|s| s.idx == id));
    }
}

#[test]
fn sparse_vector_index_consistent_with_storage() {
    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();
    let sparse_vector_ram_index = fixture_sparse_index::<InvertedIndexCompressedImmutableRam<f32>, _>(
        &mut rnd,
        NUM_VECTORS,
        MAX_SPARSE_DIM,
        LOW_FULL_SCAN_THRESHOLD,
        data_dir.path(),
    );

    // check consistency with underlying RAM inverted index
    check_index_storage_consistency(&sparse_vector_ram_index);

    let mmap_index_dir = Builder::new().prefix("mmap_index_dir").tempdir().unwrap();

    // create mmap sparse vector index
    let mut sparse_index_config = sparse_vector_ram_index.config();
    sparse_index_config.index_type = SparseIndexType::Mmap;
    let sparse_vector_mmap_index: SparseVectorIndex<InvertedIndexCompressedMmap<f32>> =
        SparseVectorIndex::open(SparseVectorIndexOpenArgs {
            config: sparse_index_config,
            id_tracker: sparse_vector_ram_index.id_tracker().clone(),
            vector_storage: sparse_vector_ram_index.vector_storage().clone(),
            payload_index: sparse_vector_ram_index.payload_index().clone(),
            path: mmap_index_dir.path(),
            stopped: &stopped,
            tick_progress: || (),
        })
        .unwrap();

    assert_eq!(
        sparse_vector_mmap_index.indexed_vector_count(),
        sparse_vector_ram_index.indexed_vector_count()
    );

    // check consistency with underlying mmap inverted index
    check_index_storage_consistency(&sparse_vector_mmap_index);

    // drop and reload index
    drop(sparse_vector_mmap_index);

    // load index from memmap file
    let mut sparse_index_config = sparse_vector_ram_index.config();
    sparse_index_config.index_type = SparseIndexType::Mmap;
    let sparse_vector_mmap_index: SparseVectorIndex<InvertedIndexCompressedMmap<f32>> =
        SparseVectorIndex::open(SparseVectorIndexOpenArgs {
            config: sparse_index_config,
            id_tracker: sparse_vector_ram_index.id_tracker().clone(),
            vector_storage: sparse_vector_ram_index.vector_storage().clone(),
            payload_index: sparse_vector_ram_index.payload_index().clone(),
            path: mmap_index_dir.path(),
            stopped: &stopped,
            tick_progress: || (),
        })
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
    let sparse_vector_index: OperationResult<SparseVectorIndex<InvertedIndexCompressedMmap<f32>>> =
        fixture_sparse_index_from_iter(
            data_dir.path(),
            [].iter().cloned(),
            10_000,
            SparseIndexType::Mmap,
        );
    // absent configuration file for mmap are ignored
    // a new index is created
    assert!(sparse_vector_index.is_ok())
}

#[test]
fn sparse_vector_index_ram_deleted_points_search() {
    let top = 10;
    let mut rnd = StdRng::seed_from_u64(42);

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();

    let sparse_vector_index = fixture_sparse_index_from_iter::<InvertedIndexRam>(
        data_dir.path(),
        (0..NUM_VECTORS).map(|_| random_sparse_vector(&mut rnd, MAX_SPARSE_DIM)),
        LOW_FULL_SCAN_THRESHOLD,
        SparseIndexType::MutableRam,
    )
    .unwrap();

    // sanity check (all indexed, no deleted points)
    assert_eq!(
        sparse_vector_index
            .id_tracker()
            .borrow()
            .available_point_count(),
        sparse_vector_index.indexed_vector_count()
    );
    assert_eq!(
        sparse_vector_index
            .id_tracker()
            .borrow()
            .deleted_point_count(),
        0
    );

    // query index
    let query_vector: QueryVector = random_sparse_vector(&mut rnd, MAX_SPARSE_DIM).into();
    let before_deletion_results: Vec<_> = sparse_vector_index
        .search(&[&query_vector], None, top, None, &Default::default())
        .unwrap();

    // pick a point to delete
    let deleted_idx = before_deletion_results[0][0].idx;

    // delete a point
    let deleted_external = sparse_vector_index
        .id_tracker()
        .borrow_mut()
        .external_id(deleted_idx)
        .unwrap();
    sparse_vector_index
        .id_tracker()
        .borrow_mut()
        .drop(deleted_external)
        .unwrap();

    assert!(
        sparse_vector_index
            .id_tracker()
            .borrow()
            .is_deleted_point(deleted_idx),
    );
    assert_eq!(
        sparse_vector_index
            .id_tracker()
            .borrow()
            .deleted_point_count(),
        1
    );

    // assert that the deleted point is no longer in the index
    let after_deletion_results: Vec<_> = sparse_vector_index
        .search(&[&query_vector], None, top, None, &Default::default())
        .unwrap();
    assert_ne!(before_deletion_results, after_deletion_results);
    assert!(
        after_deletion_results
            .iter()
            .all(|x| x.iter().all(|y| y.idx != deleted_idx)),
    );
}

#[test]
fn sparse_vector_index_ram_filtered_search() {
    let mut rnd = StdRng::seed_from_u64(42);

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();

    // setup index
    let sparse_vector_index = fixture_sparse_index::<InvertedIndexCompressedImmutableRam<f32>, _>(
        &mut rnd,
        NUM_VECTORS,
        MAX_SPARSE_DIM,
        LOW_FULL_SCAN_THRESHOLD,
        data_dir.path(),
    );

    // query index by payload
    let field_name = "field";
    let field_value = "important value";
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new(field_name),
        field_value.to_owned().into(),
    )));

    // query all sparse dimension to get all points
    let query_vector: QueryVector = random_full_sparse_vector(&mut rnd, MAX_SPARSE_DIM).into();
    let before_result = sparse_vector_index
        .search(
            &[&query_vector],
            Some(&filter),
            10,
            None,
            &Default::default(),
        )
        .unwrap();
    assert_eq!(before_result.len(), 1);
    assert_eq!(before_result[0].len(), 0);

    let hw_counter = HardwareCounterCell::new();

    // create payload field index
    let mut payload_index = sparse_vector_index.payload_index().borrow_mut();
    payload_index
        .set_indexed(&JsonPath::new(field_name), Keyword, &hw_counter)
        .unwrap();
    drop(payload_index);

    // assert payload field index created and empty
    let payload_index = sparse_vector_index.payload_index().borrow();
    let indexed_fields = payload_index.indexed_fields();
    assert_eq!(
        *indexed_fields.get(&JsonPath::new(field_name)).unwrap(),
        FieldType(Keyword)
    );

    let field_indexes = &payload_index.field_indexes;
    let field_index = field_indexes.get(&JsonPath::new(field_name)).unwrap();
    assert_eq!(field_index[0].count_indexed_points(), 0);
    drop(payload_index);

    // add payload on the first half of the points
    let half_indexed_count = sparse_vector_index.indexed_vector_count() / 2;
    let payload = payload_json! {field_name: field_value};
    let hw_counter = HardwareCounterCell::new();
    let mut payload_index = sparse_vector_index.payload_index().borrow_mut();
    for idx in 0..half_indexed_count {
        payload_index
            .set_payload(idx as PointOffsetType, &payload, &None, &hw_counter)
            .unwrap();
    }
    drop(payload_index);

    // assert payload index updated
    let payload_index = sparse_vector_index.payload_index().borrow();
    let field_indexes = &payload_index.field_indexes;
    let field_index = field_indexes.get(&JsonPath::new(field_name)).unwrap();
    assert_eq!(field_index[0].count_indexed_points(), half_indexed_count);
    drop(payload_index);

    // request all points with payload
    let after_result = sparse_vector_index
        .search(
            &[&query_vector],
            Some(&filter),
            half_indexed_count * 2, // original top
            None,
            &Default::default(),
        )
        .unwrap();
    assert_eq!(after_result.len(), 1);
    assert_eq!(after_result[0].len(), half_indexed_count); // expect half of the points
}

#[test]
fn sparse_vector_index_plain_search() {
    let mut rnd = StdRng::seed_from_u64(42);

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();
    // setup index
    let sparse_vector_index = fixture_sparse_index::<InvertedIndexCompressedImmutableRam<f32>, _>(
        &mut rnd,
        NUM_VECTORS,
        MAX_SPARSE_DIM,
        LARGE_FULL_SCAN_THRESHOLD,
        data_dir.path(),
    );

    // query index by payload
    let field_name = "field";
    let field_value = "important value";
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new(field_name),
        field_value.to_owned().into(),
    )));

    // query all sparse dimension to get all points
    let query_vector: QueryVector = random_full_sparse_vector(&mut rnd, MAX_SPARSE_DIM).into();

    // empty when searching payload index directly
    let before_plain_results = sparse_vector_index
        .search(
            &[&query_vector],
            Some(&filter),
            10,
            None,
            &Default::default(),
        )
        .unwrap();

    assert_eq!(before_plain_results.len(), 1);
    assert_eq!(before_plain_results[0].len(), 0);

    let payload = payload_json! {field_name: field_value};

    let hw_counter = HardwareCounterCell::new();

    // add payload to all points
    let mut payload_index = sparse_vector_index.payload_index().borrow_mut();
    for idx in 0..NUM_VECTORS {
        payload_index
            .set_payload(idx as PointOffsetType, &payload, &None, &hw_counter)
            .unwrap();
    }
    drop(payload_index);

    // same results when searching payload index directly
    let after_plain_results = sparse_vector_index
        .search(
            &[&query_vector],
            Some(&filter),
            NUM_VECTORS,
            None,
            &Default::default(),
        )
        .unwrap();

    assert_eq!(after_plain_results.len(), 1);
    assert_eq!(after_plain_results[0].len(), NUM_VECTORS);

    // check that plain searchers were used
    assert_eq!(
        sparse_vector_index
            .get_telemetry_data(TelemetryDetail::default())
            .filtered_small_cardinality
            .count,
        2
    );
}

#[test]
fn handling_empty_sparse_vectors() {
    let mut rnd = StdRng::seed_from_u64(42);

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();
    let sparse_vector_index: SparseVectorIndex<InvertedIndexCompressedImmutableRam<f32>> =
        fixture_sparse_index_from_iter(
            data_dir.path(),
            (0..NUM_VECTORS).map(|_| SparseVector::default()),
            DEFAULT_SPARSE_FULL_SCAN_THRESHOLD,
            SparseIndexType::ImmutableRam,
        )
        .unwrap();
    let mut borrowed_storage = sparse_vector_index.vector_storage().borrow_mut();

    let hw_counter = HardwareCounterCell::new();
    // add empty points to storage
    for idx in 0..NUM_VECTORS {
        let vec = &SparseVector::new(vec![], vec![]).unwrap();
        borrowed_storage
            .insert_vector(idx as PointOffsetType, vec.into(), &hw_counter)
            .unwrap();
    }
    drop(borrowed_storage);

    // assert all empty points are in storage
    assert_eq!(
        sparse_vector_index
            .vector_storage()
            .borrow()
            .available_vector_count(),
        NUM_VECTORS,
    );

    // empty vectors are not indexed
    assert_eq!(sparse_vector_index.indexed_vector_count(), 0);

    let query_vector: QueryVector = random_sparse_vector(&mut rnd, MAX_SPARSE_DIM).into();

    // empty vectors are not searchable (recommend using scroll API to retrieve those)
    let results = sparse_vector_index
        .search(&[&query_vector], None, 10, None, &Default::default())
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].len(), 0);
}

#[test]
fn sparse_vector_index_persistence_test() {
    let stopped = AtomicBool::new(false);

    let dim = 8;
    let num_vectors: u64 = 5_000;
    let top = 3;
    let mut rnd = StdRng::seed_from_u64(42);

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let config = SegmentConfig {
        vector_data: Default::default(),
        sparse_vector_data: HashMap::from([(
            SPARSE_VECTOR_NAME.to_owned(),
            SparseVectorDataConfig {
                index: SparseIndexConfig {
                    full_scan_threshold: Some(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD),
                    index_type: SparseIndexType::MutableRam,
                    datatype: Some(VectorStorageDatatype::Float32),
                },
                storage_type: SparseVectorStorageType::default(),
                modifier: None,
            },
        )]),
        payload_storage_type: Default::default(),
    };
    let mut segment = build_segment(dir.path(), &config, true).unwrap();

    let hw_counter = HardwareCounterCell::new();

    for n in 0..num_vectors {
        let vector: VectorInternal = random_sparse_vector(&mut rnd, dim).into();
        let mut named_vector = NamedVectors::default();
        named_vector.insert(SPARSE_VECTOR_NAME.to_owned(), vector);
        let idx = n.into();
        segment
            .upsert_point(n as SeqNumberType, idx, named_vector, &hw_counter)
            .unwrap();
    }
    segment.flush(false).unwrap();

    let search_vector = random_sparse_vector(&mut rnd, dim);
    let query_vector: QueryVector = search_vector.into();

    let search_result = segment
        .search(
            SPARSE_VECTOR_NAME,
            &query_vector,
            &Default::default(),
            &Default::default(),
            None,
            top,
            None,
        )
        .unwrap();

    assert_eq!(search_result.len(), top);

    let path = segment.current_path.clone();
    drop(segment);

    // persistence using rebuild of inverted index
    // for appendable segment vector index has to be rebuilt
    let segment = load_segment(&path, &stopped).unwrap().unwrap();
    let search_after_reload_result = segment
        .search(
            SPARSE_VECTOR_NAME,
            &query_vector,
            &Default::default(),
            &Default::default(),
            None,
            top,
            None,
        )
        .unwrap();

    assert_eq!(search_after_reload_result.len(), top);
    assert_eq!(search_result, search_after_reload_result);

    fixture_for_all_indices!(check_persistence::<_>(
        &segment,
        &search_result,
        &query_vector,
        top
    ));
}

fn check_persistence<TInvertedIndex: InvertedIndex>(
    segment: &Segment,
    search_result: &[ScoredPoint],
    query_vector: &QueryVector,
    top: usize,
) {
    let stopped = AtomicBool::new(false);

    let inverted_index_dir = Builder::new()
        .prefix("inverted_index_ram")
        .tempdir()
        .unwrap();

    let open_index = || -> SparseVectorIndex<TInvertedIndex> {
        SparseVectorIndex::open(SparseVectorIndexOpenArgs {
            config: SparseIndexConfig {
                full_scan_threshold: Some(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD),
                index_type: SparseIndexType::Mmap,
                datatype: Some(VectorStorageDatatype::Float32),
            },
            id_tracker: segment.id_tracker.clone(),
            vector_storage: segment.vector_data[SPARSE_VECTOR_NAME]
                .vector_storage
                .clone(),
            payload_index: segment.payload_index.clone(),
            path: inverted_index_dir.path(),
            stopped: &stopped,
            tick_progress: || (),
        })
        .unwrap()
    };

    let check_search = |sparse_vector_index: &SparseVectorIndex<TInvertedIndex>| {
        // check that the loaded index performs the same search
        let search_after_reload_result = sparse_vector_index
            .search(&[query_vector], None, top, None, &Default::default())
            .unwrap();
        assert_eq!(search_after_reload_result[0].len(), top);
        for (search_1, search_2) in search_result
            .iter()
            .zip(search_after_reload_result[0].iter())
        {
            let id_1 = segment
                .id_tracker
                .borrow_mut()
                .internal_id(search_1.id)
                .unwrap();
            assert_eq!(id_1, search_2.idx);
        }
    };

    let sparse_vector_index = open_index();

    let version_file = inverted_index_dir.path().join(VERSION_FILE);
    assert!(version_file.exists());

    // reload sparse index from file
    drop(sparse_vector_index);
    let sparse_vector_index = open_index();
    check_search(&sparse_vector_index);

    // drop version file and reload index
    drop(sparse_vector_index);
    fs::remove_file(&version_file).unwrap();
    let sparse_vector_index = open_index();
    assert!(version_file.exists(), "version file should be recreated");
    check_search(&sparse_vector_index);
}

#[test]
fn sparse_vector_index_files() {
    fixture_for_all_indices!(check_sparse_vector_index_files::<_>());
}

fn check_sparse_vector_index_files<I: InvertedIndex>() {
    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();
    let index = fixture_sparse_index::<I, _>(
        &mut StdRng::seed_from_u64(42),
        1,
        MAX_SPARSE_DIM,
        LOW_FULL_SCAN_THRESHOLD,
        data_dir.path(),
    );

    let files = index.files();
    // sparse index config + version + inverted index config + inverted index data + tracker
    assert_eq!(files.len(), 5);
    for file in files.iter() {
        assert!(file.exists(), "file {file:?} does not exist");
    }
}

#[test]
fn sparse_vector_test_large_index() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let config = SegmentConfig {
        vector_data: Default::default(),
        sparse_vector_data: HashMap::from([(
            SPARSE_VECTOR_NAME.to_owned(),
            SparseVectorDataConfig {
                index: SparseIndexConfig {
                    full_scan_threshold: Some(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD),
                    index_type: SparseIndexType::MutableRam,
                    datatype: Some(VectorStorageDatatype::Float32),
                },
                storage_type: SparseVectorStorageType::Mmap,
                modifier: None,
            },
        )]),
        payload_storage_type: Default::default(),
    };
    let mut segment = build_segment(dir.path(), &config, true).unwrap();

    let hw_counter = HardwareCounterCell::new();

    let vector: VectorInternal = SparseVector {
        indices: vec![DimId::MAX],
        values: vec![0.0],
    }
    .into();
    let mut named_vector = NamedVectors::default();
    named_vector.insert(SPARSE_VECTOR_NAME.to_owned(), vector);
    let idx = 0.into();
    segment
        .upsert_point(0 as SeqNumberType, idx, named_vector, &hw_counter)
        .unwrap();

    let borrowed_vector_index = segment.vector_data[SPARSE_VECTOR_NAME]
        .vector_index
        .borrow();
    match &*borrowed_vector_index {
        VectorIndexEnum::SparseRam(sparse_vector_index) => {
            assert!(
                sparse_vector_index
                    .indices_tracker()
                    .remap_index(DimId::MAX)
                    .is_some(),
            );
            assert_eq!(sparse_vector_index.inverted_index().max_index().unwrap(), 0);
        }
        _ => panic!("unexpected vector index type"),
    }
}
