#![allow(deprecated, reason = "always_ram remains part of the public config")]

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::types::ScoredPointOffset;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::index::VectorIndexRead;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::types::{
    BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding, Distance, Indexes,
    QuantizationConfig, QuantizationSearchParams, SearchParams, SegmentConfig, VectorDataConfig,
    VectorStorageDatatype, VectorStorageType,
};
use segment::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use tempfile::TempDir;

const DIM: usize = 8;

fn quantization_config() -> QuantizationConfig {
    QuantizationConfig::Binary(BinaryQuantization {
        binary: BinaryQuantizationConfig {
            always_ram: Some(true),
            memory: None,
            encoding: Some(BinaryQuantizationEncoding::OneBit),
            query_encoding: None,
        },
    })
}

fn segment_config() -> SegmentConfig {
    SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Cosine,
                storage_type: VectorStorageType::InRamChunkedMmap,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: Some(VectorStorageDatatype::Uint8),
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    }
}

fn vectors() -> Vec<Vec<f32>> {
    vec![
        vec![255.0, 255.0, 255.0, 1.0, 1.0, 1.0, 1.0, 255.0],
        vec![255.0, 255.0, 255.0, 255.0, 1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 1.0, 1.0, 255.0, 255.0, 255.0, 255.0],
        vec![255.0, 1.0, 1.0, 1.0, 1.0, 255.0, 255.0, 255.0],
    ]
}

fn attach_quantization(segment: &mut Segment, path: &std::path::Path) {
    fs_err::create_dir_all(path).unwrap();
    let vector_data = segment
        .vector_data
        .get_mut(DEFAULT_VECTOR_NAME)
        .expect("default vector data must exist");
    let quantized = QuantizedVectors::create(
        &vector_data.vector_storage.borrow(),
        &quantization_config(),
        QuantizedVectorsStorageType::Mutable,
        path,
        1,
        &AtomicBool::new(false),
    )
    .unwrap();
    *vector_data.quantized_vectors.borrow_mut() = Some(quantized);
}

fn insert_vectors(segment: &mut Segment) {
    for (id, vector) in vectors().iter().enumerate() {
        segment
            .upsert_point(id as u64, (id as u64).into(), only_default_vector(vector))
            .unwrap();
    }
}

fn search(segment: &Segment) -> Vec<ScoredPointOffset> {
    let query: QueryVector = vec![255.0, 255.0, 255.0, 255.0, 1.0, 1.0, 1.0, 1.0].into();
    segment.vector_data[DEFAULT_VECTOR_NAME]
        .vector_index
        .borrow()
        .search(
            &[&query],
            None,
            4,
            Some(&SearchParams {
                quantization: Some(QuantizationSearchParams {
                    rescore: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            &Default::default(),
        )
        .unwrap()
        .remove(0)
}

fn tied_id_groups(result: &[ScoredPointOffset]) -> Vec<BTreeSet<u32>> {
    let mut groups: Vec<(f32, BTreeSet<u32>)> = Vec::new();
    for point in result {
        if let Some((score, ids)) = groups.last_mut()
            && *score == point.score
        {
            ids.insert(point.idx);
        } else {
            groups.push((point.score, BTreeSet::from([point.idx])));
        }
    }
    groups.into_iter().map(|(_, ids)| ids).collect()
}

#[test]
fn appendable_uint8_binary_cosine_matches_build_then_quantize() {
    let early_dir = TempDir::with_prefix("qdrant11646_early_quantization").unwrap();
    let late_dir = TempDir::with_prefix("qdrant11646_late_quantization").unwrap();
    let _scope = ambient::test_guard();

    let (mut early, _) = build_segment(early_dir.path(), &segment_config(), None, true).unwrap();
    attach_quantization(&mut early, &early_dir.path().join("quantized"));
    insert_vectors(&mut early);

    let (mut late, _) = build_segment(late_dir.path(), &segment_config(), None, true).unwrap();
    insert_vectors(&mut late);
    attach_quantization(&mut late, &late_dir.path().join("quantized"));

    let early_result = search(&early);
    let late_result = search(&late);
    assert_eq!(late_result.first().map(|point| point.idx), Some(1));
    assert_eq!(late_result.last().map(|point| point.idx), Some(2));
    assert_eq!(
        tied_id_groups(&early_result),
        tied_id_groups(&late_result),
        "QDRANT11646_O2_PARITY_MISMATCH: early={early_result:?}, late={late_result:?}",
    );
}
