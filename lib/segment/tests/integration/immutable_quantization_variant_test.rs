//! Regression test for PR #8606 (deep memory reporting).
//!
//! PR #8606 changed `DenseVectorStorageImpl::is_on_disk()` from hard-coded
//! `true` to `!self.populated`. `quantized_vectors.rs::is_ram()` reads that
//! method to decide whether a quantized blob goes to `QuantizedRamStorage`
//! (heap) or `QuantizedMmapStorage` (disk):
//!
//!     in_ram = !on_disk_vector_storage || always_ram == Some(true)
//!
//! Crasher hits the immutable path via `SegmentBuilder`, which materializes
//! an `InRamMmap` vector storage backed by `DenseVectorStorageImpl` with
//! `populated=true`. The flip causes the quantized storage to land in RAM
//! instead of on disk, driving 4x transient disk spikes under crasher's
//! restart loop.
//!
//! This test drives the full appendable → immutable build path and asserts
//! that binary quantization with `always_ram: false` stays on disk.
//!
//! * Pre-fix (PR #8606 semantics): picks `BinaryRam` → this test fails.
//! * Post-fix (`is_on_disk() -> true` restored): picks `BinaryMmap` → passes.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::types::{
    BinaryQuantizationConfig, Distance, HnswGlobalConfig, Indexes, QuantizationConfig,
    SegmentConfig, VectorDataConfig, VectorStorageType,
};
use segment::vector_storage::quantized::quantized_vectors::QuantizedVectorStorage;
use tempfile::Builder;

#[test]
fn immutable_built_segment_quantizes_binary_to_disk_when_always_ram_false() {
    let _ = env_logger::builder().is_test(true).try_init();

    let source_dir = Builder::new().prefix("src").tempdir().unwrap();
    let builder_tmp = Builder::new().prefix("bld_tmp").tempdir().unwrap();
    let target_dir = Builder::new().prefix("tgt").tempdir().unwrap();

    // Source: appendable, on_disk=false, binary quantization with always_ram=false.
    // Mirrors the crasher `dense-vector-memory-bq-*` configs that hit the bug.
    let quantization: QuantizationConfig = BinaryQuantizationConfig {
        always_ram: Some(false),
        encoding: None,
        query_encoding: None,
    }
    .into();

    let source_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: 8,
                distance: Distance::Dot,
                storage_type: VectorStorageType::InRamChunkedMmap, // appendable, on_disk=false
                index: Indexes::Plain {},
                quantization_config: Some(quantization.clone()),
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: HashMap::new(),
        payload_storage_type: Default::default(),
    };

    let mut source = build_segment(source_dir.path(), &source_config, None, true).unwrap();

    let hw = HardwareCounterCell::new();
    for id in 0..128u64 {
        let v: Vec<f32> = (0..8)
            .map(|i| (((id as i32 + i) % 4) - 2) as f32 * 0.25)
            .collect();
        source
            .upsert_point(id, id.into(), only_default_vector(&v), &hw)
            .unwrap();
    }

    // Target (the merged segment) uses the immutable `InRamMmap` storage --
    // this is what the optimizer produces when consolidating appendable
    // `on_disk: false` segments in production. This is the path that goes
    // through `DenseVectorStorageImpl` with `populated=true`, which is where
    // PR #8606's `is_on_disk()` change materially affects behavior.
    let target_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: 8,
                distance: Distance::Dot,
                storage_type: VectorStorageType::InRamMmap,
                index: Indexes::Plain {},
                quantization_config: Some(quantization),
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: HashMap::new(),
        payload_storage_type: Default::default(),
    };

    let stopped = AtomicBool::new(false);
    let mut builder = SegmentBuilder::new(
        builder_tmp.path(),
        &target_config,
        &HnswGlobalConfig::default(),
    )
    .unwrap();

    builder.update(&[&source], &stopped, &hw).unwrap();

    let _permit = ResourcePermit::dummy(2);
    let merged = builder.build_for_test(target_dir.path());

    let vd = merged.vector_data.values().next().unwrap();
    let quantized = vd.quantized_vectors.borrow();
    let quantized = quantized
        .as_ref()
        .expect("target segment must have quantized vectors");

    let variant = match quantized.get_storage() {
        QuantizedVectorStorage::BinaryRam(_) => "BinaryRam (heap)",
        QuantizedVectorStorage::BinaryMmap(_) => "BinaryMmap (disk)",
        QuantizedVectorStorage::BinaryChunkedMmap(_) => "BinaryChunkedMmap",
        other => panic!("unexpected quantization variant: {other:?}"),
    };
    eprintln!("Quantization variant on immutable built segment: {variant}");

    assert!(
        matches!(
            quantized.get_storage(),
            QuantizedVectorStorage::BinaryMmap(_),
        ),
        "binary quantization with always_ram=false should land on disk \
         (got {variant}) -- PR #8606 regression if this fails",
    );
}
