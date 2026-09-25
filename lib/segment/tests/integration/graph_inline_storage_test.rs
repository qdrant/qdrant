use std::assert_matches;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::generic_consts::Random;
use common::types::DeferredBehavior;
use common::universal_io::MmapFs;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rstest::rstest;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, QueryVector, VectorInternal, only_default_vector,
};
use segment::entry::entry_point::{NonAppendableSegmentEntry, ReadSegmentEntry, SegmentEntry};
use segment::segment::Segment;
use segment::segment::update_only::LookupSegment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::{build_segment, get_vector_storage_path};
use segment::types::{
    Condition, Distance, Filter, HasVectorCondition, HnswConfig, HnswGlobalConfig, Indexes,
    PayloadStorageType, QuantizationConfig, ScalarQuantization, ScalarQuantizationConfig,
    ScalarType, SearchParams, SegmentConfig, VectorDataConfig, VectorStorageDatatype,
    VectorStorageType, WithPayload, WithVector,
};
use segment::vector_storage::VectorStorageRead;
use tap::Tap;
use tempfile::Builder;

const DIM: usize = 16;
const NUM_VECTORS: usize = 600;
const TOP: usize = 10;
const DISTANCE: Distance = Distance::Dot;

fn indexed_config(
    inline_storage: bool,
    datatype: Option<VectorStorageDatatype>,
    storage_type: VectorStorageType,
) -> SegmentConfig {
    #[expect(deprecated, reason = "HnswConfig still carries the on_disk flag")]
    let hnsw_config = HnswConfig {
        m: 16,
        ef_construct: 64,
        full_scan_threshold: 1,
        max_indexing_threads: 1,
        on_disk: None,
        memory: None,
        payload_m: None,
        inline_storage: Some(inline_storage),
    };
    #[expect(
        deprecated,
        reason = "ScalarQuantizationConfig still carries always_ram"
    )]
    let quantization_config = QuantizationConfig::Scalar(ScalarQuantization {
        scalar: ScalarQuantizationConfig {
            r#type: ScalarType::Int8,
            quantile: Some(0.99),
            always_ram: None,
            memory: None,
        },
    });

    SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: DISTANCE,
                storage_type,
                index: Indexes::Hnsw(hnsw_config),
                quantization_config: Some(quantization_config),
                multivector_config: None,
                datatype,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: PayloadStorageType::default(),
        id_tracker_memory: None,
    }
}

fn random_vectors(count: usize, datatype: Option<VectorStorageDatatype>) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(4242);
    let element = move |rng: &mut StdRng| match datatype {
        Some(VectorStorageDatatype::Uint8) => rng.random_range(0.0f32..255.0).round(),
        None
        | Some(
            VectorStorageDatatype::Float32
            | VectorStorageDatatype::Float16
            | VectorStorageDatatype::Turbo4,
        ) => rng.random_range(-1.0..1.0),
    };
    (0..count)
        .map(|_| (0..DIM).map(|_| element(&mut rng)).collect())
        .collect()
}

fn plain_segment(
    dir: &std::path::Path,
    vectors: &[Vec<f32>],
    offset: u64,
    datatype: Option<VectorStorageDatatype>,
) -> Segment {
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: DISTANCE,
                storage_type: VectorStorageType::InRamChunkedMmap,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: PayloadStorageType::default(),
        id_tracker_memory: None,
    };
    let hw_counter = HardwareCounterCell::new();
    let (mut segment, _) = build_segment(dir, &config, None, true).unwrap();
    for (idx, vector) in vectors.iter().enumerate() {
        segment
            .upsert_point(
                idx as u64 + offset + 1,
                (idx as u64 + offset).into(),
                only_default_vector(vector),
                &hw_counter,
            )
            .unwrap();
    }
    segment
}

fn build_indexed(dir: &std::path::Path, sources: &[&Segment], config: &SegmentConfig) -> Segment {
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();
    builder
        .update(
            sources,
            &AtomicBool::new(false),
            &HardwareCounterCell::new(),
        )
        .unwrap();
    builder.build_for_test(dir)
}

fn search(segment: &impl ReadSegmentEntry, query: &[f32], filter: Option<&Filter>) -> Vec<u64> {
    search_with(segment, query, filter, None)
}

fn exact_search(segment: &impl ReadSegmentEntry, query: &[f32]) -> Vec<u64> {
    let params = SearchParams {
        exact: true,
        ..Default::default()
    };
    search_with(segment, query, None, Some(&params))
}

fn search_with(
    segment: &impl ReadSegmentEntry,
    query: &[f32],
    filter: Option<&Filter>,
    params: Option<&SearchParams>,
) -> Vec<u64> {
    let query = QueryVector::from(query.to_vec());
    let query_context = QueryContext::default();
    let results = segment
        .search_batch(
            DEFAULT_VECTOR_NAME,
            &[&query],
            &WithPayload::default(),
            &WithVector::Bool(false),
            filter,
            TOP,
            params,
            &query_context.get_segment_query_context(),
        )
        .unwrap();
    let mut ids: Vec<u64> = results[0]
        .iter()
        .map(|point| match point.id {
            segment::types::PointIdType::NumId(id) => id,
            segment::types::PointIdType::Uuid(_) => unreachable!("numeric ids only"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn stored_vector(segment: &impl ReadSegmentEntry, id: u64) -> Vec<f32> {
    let vector = segment
        .vector(DEFAULT_VECTOR_NAME, id.into(), &HardwareCounterCell::new())
        .unwrap()
        .unwrap();
    match vector {
        VectorInternal::Dense(dense) => dense,
        VectorInternal::Sparse(_) | VectorInternal::MultiDense(_) => {
            unreachable!("dense vectors only")
        }
    }
}

#[rstest]
#[case::f32(None, VectorStorageType::Mmap)]
#[case::f16(Some(VectorStorageDatatype::Float16), VectorStorageType::Mmap)]
#[case::u8(Some(VectorStorageDatatype::Uint8), VectorStorageType::Mmap)]
#[case::turbo4(Some(VectorStorageDatatype::Turbo4), VectorStorageType::Mmap)]
#[case::f32_chunked(None, VectorStorageType::InRamChunkedMmap)]
#[case::turbo4_chunked(
    Some(VectorStorageDatatype::Turbo4),
    VectorStorageType::InRamChunkedMmap
)]
fn test_graph_inline_storage_contract(
    #[case] datatype: Option<VectorStorageDatatype>,
    #[case] storage_type: VectorStorageType,
) {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let source_dir = Builder::new().prefix("source_dir").tempdir().unwrap();
    let vectors = random_vectors(NUM_VECTORS, datatype);
    let source = plain_segment(source_dir.path(), &vectors, 0, datatype);

    let graph_backed = build_indexed(
        dir.path(),
        &[&source],
        &indexed_config(true, datatype, storage_type),
    );
    let vector_config = graph_backed
        .segment_config
        .vector_data
        .get(DEFAULT_VECTOR_NAME)
        .unwrap();
    assert_eq!(vector_config.storage_type, VectorStorageType::GraphInline);
    let storage_dir = get_vector_storage_path(&graph_backed.segment_path, DEFAULT_VECTOR_NAME);
    for name in ["matrix.dat", "vectors.dat"] {
        let path = storage_dir.join(name);
        assert!(!path.exists(), "{path:?} should be gone");
    }

    let control_dir = Builder::new().prefix("control_dir").tempdir().unwrap();
    let control = build_indexed(
        control_dir.path(),
        &[&source],
        &indexed_config(false, datatype, storage_type),
    );
    assert_eq!(
        control
            .segment_config
            .vector_data
            .get(DEFAULT_VECTOR_NAME)
            .unwrap()
            .storage_type,
        storage_type,
    );

    for query in vectors.iter().take(5) {
        assert_eq!(
            exact_search(&graph_backed, query),
            exact_search(&control, query),
        );
        let approximate = search(&graph_backed, query, None);
        let reference = search(&control, query, None);
        let common = approximate
            .iter()
            .filter(|id| reference.contains(id))
            .count();
        assert!(common + 2 >= TOP, "{approximate:?} vs {reference:?}");
    }

    for id in [0u64, 1, 42, NUM_VECTORS as u64 - 1] {
        assert_eq!(
            stored_vector(&graph_backed, id),
            stored_vector(&control, id)
        );
    }

    let scrolled = graph_backed
        .retrieve(
            &[0.into(), 7.into(), 99.into()],
            &WithPayload::default(),
            &WithVector::Bool(true),
            &HardwareCounterCell::new(),
            &AtomicBool::new(false),
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();
    assert_eq!(scrolled.len(), 3);

    let ids = graph_backed
        .read_filtered(
            None,
            Some(NUM_VECTORS),
            None,
            &AtomicBool::new(false),
            &HardwareCounterCell::new(),
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();
    assert_eq!(ids.len(), NUM_VECTORS);

    graph_inline_lookup_segment_opens(&graph_backed);

    graph_inline_deletes_and_remerge(&vectors, graph_backed, datatype, storage_type);
}

fn graph_inline_lookup_segment_opens(writable: &Segment) {
    let lookup = LookupSegment::<MmapFs>::open(MmapFs, &writable.segment_path, None).unwrap();

    let storage = lookup.vector_data[DEFAULT_VECTOR_NAME].borrow();
    assert!(!lookup.appendable);
    assert_eq!(storage.total_vector_count(), NUM_VECTORS);
    let vector: Vec<f32> = storage
        .get_vector::<Random>(42)
        .to_owned()
        .try_into()
        .unwrap();
    assert_eq!(vector, stored_vector(writable, 42));
}

fn graph_inline_deletes_and_remerge(
    vectors: &[Vec<f32>],
    mut graph_backed: Segment,
    datatype: Option<VectorStorageDatatype>,
    storage_type: VectorStorageType,
) {
    let hw_counter = HardwareCounterCell::new();
    let deleted: Vec<u64> = vec![3, 17, 200];
    for &id in &deleted {
        assert_matches!(
            graph_backed.delete_point(NUM_VECTORS as u64 + 10, id.into(), &hw_counter),
            Ok(true)
        );
    }
    assert_eq!(
        graph_backed.available_point_count(),
        NUM_VECTORS - deleted.len(),
    );
    for &id in &deleted {
        assert!(!search(&graph_backed, &vectors[id as usize], None).contains(&id));
    }

    let has_vector = Filter::new_must(Condition::HasVector(HasVectorCondition::from(
        DEFAULT_VECTOR_NAME.to_owned(),
    )));
    let with_vector = graph_backed
        .read_filtered(
            None,
            Some(NUM_VECTORS),
            Some(&has_vector),
            &AtomicBool::new(false),
            &hw_counter,
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();
    assert_eq!(with_vector.len(), NUM_VECTORS - deleted.len());

    let extra_dir = Builder::new().prefix("extra_dir").tempdir().unwrap();
    let extra_vectors = random_vectors(64, datatype);
    let extra = plain_segment(
        extra_dir.path(),
        &extra_vectors,
        NUM_VECTORS as u64,
        datatype,
    );

    let merged_dir = Builder::new().prefix("merged_dir").tempdir().unwrap();
    let merged = build_indexed(
        merged_dir.path(),
        &[&graph_backed, &extra],
        &indexed_config(true, datatype, storage_type),
    );
    assert_eq!(
        merged.available_point_count(),
        NUM_VECTORS - deleted.len() + extra_vectors.len(),
    );
    let merged_control_dir = Builder::new().prefix("merged_control").tempdir().unwrap();
    let merged_control = build_indexed(
        merged_control_dir.path(),
        &[&graph_backed, &extra],
        &indexed_config(false, datatype, storage_type),
    );
    for id in [0u64, 42, NUM_VECTORS as u64] {
        assert_eq!(
            stored_vector(&merged, id),
            stored_vector(&merged_control, id)
        );
    }
}

#[test]
fn test_graph_inline_storage_flag_off() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let source_dir = Builder::new().prefix("source_dir").tempdir().unwrap();
    let vectors = random_vectors(NUM_VECTORS, None);
    let source = plain_segment(source_dir.path(), &vectors, 0, None);

    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &indexed_config(true, None, VectorStorageType::Mmap),
        &HnswGlobalConfig::default(),
        FeatureFlags::default().tap_mut(|flags| flags.combined_vector_storage = false),
    )
    .unwrap();
    builder
        .update(
            &[&source],
            &AtomicBool::new(false),
            &HardwareCounterCell::new(),
        )
        .unwrap();
    let built = builder.build_for_test(dir.path());

    assert_eq!(
        built.segment_config.vector_data[DEFAULT_VECTOR_NAME].storage_type,
        VectorStorageType::Mmap,
    );
    let storage_dir = get_vector_storage_path(&built.segment_path, DEFAULT_VECTOR_NAME);
    assert!(storage_dir.join("matrix.dat").exists());
    for query in vectors.iter().take(5) {
        assert_eq!(exact_search(&built, query), exact_search(&source, query));
    }
}
