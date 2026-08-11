//! Writes through the update-only quantized overlay, then reads the persisted bytes back
//! through the ordinary (non-update-only) reader — proving the two share an on-disk format —
//! and compares them against a reference storage fed the same vectors one at a time through
//! `EncodedVectors::upsert_vector` directly.
//!
//! The reference is built the same way, vector by vector, rather than via a single batch
//! `encode()` call over the whole dataset: TurboQuant's quantizer is calibrated from whatever
//! data it has *at creation time* (empty, for a fresh appendable segment — the same as
//! `create_empty_overlay` below), so a quantizer fit from the full dataset up front would
//! legitimately encode differently. That calibration gap is an existing, documented property of
//! appendable quantization in general (see `quantized_vectors/load.rs`'s auto-create comment),
//! not something this test should be asserting away — what it must prove is that this module's
//! storage adapter places bytes identically to the reference *given the same quantizer state*.
#![allow(
    deprecated,
    reason = "always_ram is deprecated but still constructible"
)]

use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::{MmapFile, MmapFs, UniversalWriteFileOps as _};
use quantization::encoded_vectors_binary::{self, EncodedVectorsBin};
use quantization::encoded_vectors_tq::{self, EncodedVectorsTQ};
use quantization::turboquant::{TQMode, TQRotation};
use quantization::{EncodedStorage as _, EncodedVectors as _};
use tempfile::TempDir;

use super::{UpdateOnlyQuantizedVectorStorage, UpdateOnlyQuantizedVectors};
use crate::data_types::vectors::VectorRef;
use crate::types::{
    BinaryQuantization, BinaryQuantizationConfig, Distance, QuantizationConfig, TurboQuantBitSize,
    TurboQuantQuantizationConfig, TurboQuantization,
};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::{
    QuantizedChunkedStorage, UpdateOnlyQuantizedChunkedStorageBuilder,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorageBuilder;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsConfig, QuantizedVectorsStorageType,
};

const DIM: usize = 8;

fn binary_config() -> QuantizationConfig {
    QuantizationConfig::Binary(BinaryQuantization {
        binary: BinaryQuantizationConfig {
            always_ram: None,
            memory: None,
            encoding: None,
            query_encoding: None,
        },
    })
}

fn turbo_config() -> QuantizationConfig {
    QuantizationConfig::Turbo(TurboQuantization {
        turbo: TurboQuantQuantizationConfig {
            always_ram: None,
            memory: None,
            bits: Some(TurboQuantBitSize::Bits4),
        },
    })
}

fn some_vectors(n: usize) -> Vec<Vec<f32>> {
    (0..n)
        .map(|i| {
            (0..DIM)
                .map(|d| (((i * 7 + d * 3) % 11) as f32) - 5.0)
                .collect()
        })
        .collect()
}

/// Build the empty overlay a fresh appendable segment's vector would start from — there is no
/// production code that does this yet (nothing in this stack constructs the first appendable
/// segment of a collection today), so this test fixture goes straight through the `quantization`
/// crate's `encode` the same way that future caller eventually will, rather than through
/// `UpdateOnlyQuantizedVectors`, which only ever reopens what already exists on disk.
fn create_empty_overlay(
    config: &QuantizationConfig,
    path: &std::path::Path,
) -> UpdateOnlyQuantizedVectors<MmapFile> {
    let storage_type = QuantizedVectorsStorageType::Mutable;
    let vector_parameters =
        QuantizedVectors::construct_vector_parameters(config, Distance::Dot, DIM, 0, storage_type);
    let meta_path = QuantizedVectors::get_meta_path(path);
    let data_path = QuantizedVectors::get_data_path(path, storage_type);
    let stopped = AtomicBool::new(false);
    let no_vectors = std::iter::empty::<&[f32]>();

    let storage = match config {
        QuantizationConfig::Binary(BinaryQuantization { binary }) => {
            let encoding = QuantizedVectors::convert_binary_encoding(binary.encoding);
            let query_encoding =
                QuantizedVectors::convert_binary_query_encoding(binary.query_encoding);
            let quantized_vector_size =
                encoded_vectors_binary::get_quantized_vector_size_from_params::<u128>(
                    vector_parameters.dim,
                    encoding,
                );
            let storage_builder = UpdateOnlyQuantizedChunkedStorageBuilder::new(
                MmapFs,
                data_path.as_path(),
                quantized_vector_size,
            )
            .unwrap();
            let encoded = EncodedVectorsBin::encode(
                no_vectors,
                storage_builder,
                &vector_parameters,
                encoding,
                query_encoding,
                Some(meta_path.as_path()),
                &stopped,
            )
            .unwrap();
            UpdateOnlyQuantizedVectorStorage::Binary(Box::new(encoded))
        }
        QuantizationConfig::Turbo(TurboQuantization { turbo }) => {
            let bits = QuantizedVectors::convert_tq_bits(turbo.bits.unwrap_or_default());
            let mode = TQMode::Plus;
            let quantized_vector_size =
                encoded_vectors_tq::get_quantized_vector_size(&vector_parameters, bits, mode);
            let storage_builder = UpdateOnlyQuantizedChunkedStorageBuilder::new(
                MmapFs,
                data_path.as_path(),
                quantized_vector_size,
            )
            .unwrap();
            let encoded = EncodedVectorsTQ::encode(
                no_vectors,
                storage_builder,
                &vector_parameters,
                0,
                bits,
                mode,
                TQRotation::Padded,
                false,
                1,
                Some(meta_path.as_path()),
                &stopped,
            )
            .unwrap();
            UpdateOnlyQuantizedVectorStorage::Turbo(Box::new(encoded))
        }
        QuantizationConfig::Scalar(_) | QuantizationConfig::Product(_) => {
            panic!("test fixture only builds Binary/Turbo overlays")
        }
    };

    let overlay_config = QuantizedVectorsConfig {
        quantization_config: config.clone(),
        vector_parameters,
        storage_type,
    };
    let bytes = serde_json::to_vec(&overlay_config).unwrap();
    MmapFs
        .atomic_save(&QuantizedVectors::get_config_path(path), &bytes)
        .unwrap();

    UpdateOnlyQuantizedVectors { storage }
}

/// First writer: created fresh (as whatever builds a new segment would), writes half the batch,
/// then dropped, then reopened through `open` — proving a second writer resumes correctly,
/// mirroring `dense/update_only/tests.rs::batches_resume`. Reopening goes through
/// `EncodedVectorsBin`/`TQ::reopen_for_write`, not `load`: a resuming writer only needs the
/// fitted metadata, not a validating read of already-stored data.
fn write_all(config: &QuantizationConfig, path: &std::path::Path, vectors: &[Vec<f32>]) {
    let hw_counter = HardwareCounterCell::new();

    let split = vectors.len() / 2;
    let mut writer = create_empty_overlay(config, path);
    for (id, vector) in vectors[..split].iter().enumerate() {
        writer
            .upsert_vector(id as u32, VectorRef::from(vector.as_slice()), &hw_counter)
            .unwrap();
    }
    drop(writer);

    let mut writer = UpdateOnlyQuantizedVectors::<MmapFile>::open(MmapFs, path)
        .unwrap()
        .expect("overlay was already created by the first writer");
    for (offset, vector) in vectors[split..].iter().enumerate() {
        let id = (split + offset) as u32;
        writer
            .upsert_vector(id, VectorRef::from(vector.as_slice()), &hw_counter)
            .unwrap();
    }
    drop(writer);
}

#[test]
fn binary_bytes_match_the_standard_batch_encode_path() {
    let dir = TempDir::with_prefix("update_only_quantized_binary").unwrap();
    let config = binary_config();
    let vectors = some_vectors(6);

    write_all(&config, dir.path(), &vectors);

    let vector_parameters = QuantizedVectors::construct_vector_parameters(
        &config,
        Distance::Dot,
        DIM,
        0,
        QuantizedVectorsStorageType::Mutable,
    );
    let (encoding, query_encoding) = match &config {
        QuantizationConfig::Binary(BinaryQuantization { binary }) => (
            QuantizedVectors::convert_binary_encoding(binary.encoding),
            QuantizedVectors::convert_binary_query_encoding(binary.query_encoding),
        ),
        QuantizationConfig::Scalar(_)
        | QuantizationConfig::Product(_)
        | QuantizationConfig::Turbo(_) => unreachable!(),
    };
    let quantized_vector_size =
        encoded_vectors_binary::get_quantized_vector_size_from_params::<u128>(DIM, encoding);
    let data_path =
        QuantizedVectors::get_data_path(dir.path(), QuantizedVectorsStorageType::Mutable);
    let meta_path = QuantizedVectors::get_meta_path(dir.path());

    // Read the persisted bytes back through the standard (non-update-only) reader.
    let storage =
        QuantizedChunkedStorage::<MmapFile>::new(MmapFs, &data_path, quantized_vector_size, false)
            .unwrap();
    let persisted: EncodedVectorsBin<u128, QuantizedChunkedStorage<MmapFile>> =
        EncodedVectorsBin::load(&MmapFs, storage, &meta_path).unwrap();
    assert_eq!(persisted.vectors_count(), vectors.len());

    // Reference: the same empty-data-fit quantizer (matching a fresh appendable segment),
    // then the same vectors pushed one at a time — isolating what this test actually checks:
    // that this module's storage adapter places bytes identically to a RAM-backed reference,
    // given the same quantizer state.
    let reference_builder = QuantizedRamStorageBuilder::new(
        &dir.path().join("reference"),
        vectors.len(),
        quantized_vector_size,
    )
    .unwrap();
    let mut reference: EncodedVectorsBin<u128, _> = EncodedVectorsBin::encode(
        std::iter::empty::<&[f32]>(),
        reference_builder,
        &vector_parameters,
        encoding,
        query_encoding,
        None,
        &AtomicBool::new(false),
    )
    .unwrap();
    let hw_counter = HardwareCounterCell::new();
    for (id, vector) in vectors.iter().enumerate() {
        reference
            .upsert_vector(id as u32, vector, &hw_counter)
            .unwrap();
    }

    for id in 0..vectors.len() as u32 {
        assert_eq!(
            persisted.storage().get_vector_data(id),
            reference.storage().get_vector_data(id),
            "quantized bytes for point {id} diverge between the update-only writer and the \
             standard batch-encode path",
        );
    }
}

#[test]
fn turbo_bytes_match_the_standard_batch_encode_path() {
    let dir = TempDir::with_prefix("update_only_quantized_turbo").unwrap();
    let config = turbo_config();
    let vectors = some_vectors(6);

    write_all(&config, dir.path(), &vectors);

    let vector_parameters = QuantizedVectors::construct_vector_parameters(
        &config,
        Distance::Dot,
        DIM,
        0,
        QuantizedVectorsStorageType::Mutable,
    );
    let bits = match &config {
        QuantizationConfig::Turbo(TurboQuantization { turbo }) => {
            QuantizedVectors::convert_tq_bits(turbo.bits.unwrap_or_default())
        }
        QuantizationConfig::Scalar(_)
        | QuantizationConfig::Product(_)
        | QuantizationConfig::Binary(_) => unreachable!(),
    };
    let mode = TQMode::Plus;
    let quantized_vector_size =
        encoded_vectors_tq::get_quantized_vector_size(&vector_parameters, bits, mode);
    let data_path =
        QuantizedVectors::get_data_path(dir.path(), QuantizedVectorsStorageType::Mutable);
    let meta_path = QuantizedVectors::get_meta_path(dir.path());

    let storage =
        QuantizedChunkedStorage::<MmapFile>::new(MmapFs, &data_path, quantized_vector_size, false)
            .unwrap();
    let persisted: EncodedVectorsTQ<QuantizedChunkedStorage<MmapFile>> =
        EncodedVectorsTQ::load(&MmapFs, storage, &meta_path).unwrap();
    assert_eq!(persisted.vectors_count(), vectors.len());

    // Reference: the same empty-data-fit quantizer (matching a fresh appendable segment), then
    // the same vectors pushed one at a time — see the module doc comment for why.
    let reference_builder = QuantizedRamStorageBuilder::new(
        &dir.path().join("reference"),
        vectors.len(),
        quantized_vector_size,
    )
    .unwrap();
    let mut reference: EncodedVectorsTQ<_> = EncodedVectorsTQ::encode(
        std::iter::empty::<&[f32]>(),
        reference_builder,
        &vector_parameters,
        0,
        bits,
        mode,
        TQRotation::Padded,
        false,
        1,
        None,
        &AtomicBool::new(false),
    )
    .unwrap();
    let hw_counter = HardwareCounterCell::new();
    for (id, vector) in vectors.iter().enumerate() {
        reference
            .upsert_vector(id as u32, vector, &hw_counter)
            .unwrap();
    }

    for id in 0..vectors.len() as u32 {
        assert_eq!(
            persisted.storage().get_vector_data(id),
            reference.storage().get_vector_data(id),
            "quantized bytes for point {id} diverge between the update-only writer and the \
             standard batch-encode path",
        );
    }
}

/// Nothing persisted yet: `open` returns `None` rather than creating anything — it only reopens
/// what a prior write already persisted.
#[test]
fn open_returns_none_when_nothing_persisted() {
    let dir = TempDir::with_prefix("update_only_quantized_no_config").unwrap();
    let overlay = UpdateOnlyQuantizedVectors::<MmapFile>::open(MmapFs, dir.path()).unwrap();
    assert!(overlay.is_none());
}

/// Reopening an overlay that already has data works, through `reopen_for_write` rather than
/// `load` — covered end to end by `write_all`'s two-writer split (used by both byte-comparison
/// tests above). This test isolates just the `open` call: it must return `Some`, not error or
/// panic, once a prior writer already stored vectors.
#[test]
fn reopening_a_nonempty_overlay_works() {
    let dir = TempDir::with_prefix("update_only_quantized_reopen_nonempty").unwrap();
    let config = binary_config();
    let hw_counter = HardwareCounterCell::new();

    let mut writer = create_empty_overlay(&config, dir.path());
    let vector = some_vectors(1).remove(0);
    writer
        .upsert_vector(0, VectorRef::from(vector.as_slice()), &hw_counter)
        .unwrap();
    drop(writer);

    let reopened = UpdateOnlyQuantizedVectors::<MmapFile>::open(MmapFs, dir.path()).unwrap();
    assert!(reopened.is_some());
}
