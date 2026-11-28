#![allow(
    deprecated,
    reason = "always_ram remains part of the regression matrix"
)]

use std::sync::atomic::AtomicBool;

use common::ambient;
use common::generic_consts::Sequential;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::MmapFs;
use quantization::EncodedStorage as _;
use quantization::encoded_vectors_binary::EncoderBin;
use quantization::encoded_vectors_tq::EncoderTQ;
use tempfile::TempDir;

use super::{QuantizedVectorStorage, QuantizedVectors, QuantizedVectorsStorageType};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    TypedMultiDenseVectorRef, VectorElementType, VectorElementTypeByte, VectorElementTypeHalf,
    VectorRef,
};
use crate::types::{
    BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding, Distance,
    MultiVectorConfig, QuantizationConfig, TurboQuantQuantizationConfig, TurboQuantization,
    VectorStorageDatatype,
};
use crate::vector_storage::dense::volatile_dense_vector_storage::{
    new_volatile_dense_byte_vector_storage, new_volatile_dense_half_vector_storage,
    new_volatile_dense_vector_storage,
};
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::{
    open_appendable_memmap_multi_vector_storage_byte,
    open_appendable_memmap_multi_vector_storage_half,
};
use crate::vector_storage::multi_dense::volatile_multi_dense_vector_storage::new_volatile_multi_dense_vector_storage;
use crate::vector_storage::{
    DenseVectorStorageRead, MultiVectorStorage, VectorStorage, VectorStorageEnum, VectorStorageRead,
};

const DIM: usize = 8;
const O1_MARKER: &str = "QDRANT11646_O1_CODE_MISMATCH";

enum ReferenceEncoder {
    Binary(EncoderBin<u128>),
    BinaryMulti(EncoderBin<u8>),
    Turbo(Box<EncoderTQ>),
}

impl ReferenceEncoder {
    fn load(config: &QuantizationConfig, multi: bool, path: &std::path::Path) -> Self {
        match config {
            QuantizationConfig::Binary(_) if multi => {
                Self::BinaryMulti(EncoderBin::load(&MmapFs, path).unwrap())
            }
            QuantizationConfig::Binary(_) => Self::Binary(EncoderBin::load(&MmapFs, path).unwrap()),
            QuantizationConfig::Turbo(_) => {
                Self::Turbo(Box::new(EncoderTQ::load(&MmapFs, path).unwrap()))
            }
            QuantizationConfig::Scalar(_) | QuantizationConfig::Product(_) => {
                unreachable!("the regression matrix uses Binary and TurboQuant only")
            }
        }
    }

    fn encode(&mut self, vector: &[VectorElementType]) -> Vec<u8> {
        match self {
            Self::Binary(encoder) => encoder.encode(vector).as_bytes().to_vec(),
            Self::BinaryMulti(encoder) => encoder.encode(vector).as_bytes().to_vec(),
            Self::Turbo(encoder) => encoder.encode(vector),
        }
    }
}

fn binary_config(encoding: BinaryQuantizationEncoding, always_ram: bool) -> QuantizationConfig {
    QuantizationConfig::Binary(BinaryQuantization {
        binary: BinaryQuantizationConfig {
            always_ram: Some(always_ram),
            memory: None,
            encoding: Some(encoding),
            query_encoding: None,
        },
    })
}

fn turbo_config(always_ram: bool) -> QuantizationConfig {
    QuantizationConfig::Turbo(TurboQuantization {
        turbo: TurboQuantQuantizationConfig {
            always_ram: Some(always_ram),
            memory: None,
            bits: None,
        },
    })
}

fn dense_storage(
    datatype: VectorStorageDatatype,
    distance: Distance,
    vectors: &[Vec<f32>],
) -> VectorStorageEnum {
    let mut storage = match datatype {
        VectorStorageDatatype::Float32 => new_volatile_dense_vector_storage(DIM, distance),
        VectorStorageDatatype::Float16 => new_volatile_dense_half_vector_storage(DIM, distance),
        VectorStorageDatatype::Uint8 => new_volatile_dense_byte_vector_storage(DIM, distance),
        VectorStorageDatatype::Turbo4
        | VectorStorageDatatype::Turbo8
        | VectorStorageDatatype::Turbo16 => unreachable!("primitive storage fixture only"),
    };
    let _scope = ambient::test_guard();
    for (id, vector) in vectors.iter().enumerate() {
        storage
            .insert_vector(id as PointOffsetType, VectorRef::from(vector.as_slice()))
            .unwrap();
    }
    storage
}

fn multi_storage(
    datatype: VectorStorageDatatype,
    distance: Distance,
    flattened: &[f32],
) -> (VectorStorageEnum, Option<TempDir>) {
    let directory = match datatype {
        VectorStorageDatatype::Float32 => None,
        VectorStorageDatatype::Float16 | VectorStorageDatatype::Uint8 => {
            Some(TempDir::with_prefix("appendable_multi_storage").unwrap())
        }
        VectorStorageDatatype::Turbo4
        | VectorStorageDatatype::Turbo8
        | VectorStorageDatatype::Turbo16 => unreachable!("primitive storage fixture only"),
    };
    let mut storage = match datatype {
        VectorStorageDatatype::Float32 => {
            new_volatile_multi_dense_vector_storage(DIM, distance, MultiVectorConfig::default())
        }
        VectorStorageDatatype::Float16 => open_appendable_memmap_multi_vector_storage_half(
            directory.as_ref().unwrap().path(),
            DIM,
            distance,
            MultiVectorConfig::default(),
            AdviceSetting::Global,
            false,
        )
        .unwrap(),
        VectorStorageDatatype::Uint8 => open_appendable_memmap_multi_vector_storage_byte(
            directory.as_ref().unwrap().path(),
            DIM,
            distance,
            MultiVectorConfig::default(),
            AdviceSetting::Global,
            false,
        )
        .unwrap(),
        VectorStorageDatatype::Turbo4
        | VectorStorageDatatype::Turbo8
        | VectorStorageDatatype::Turbo16 => unreachable!("primitive storage fixture only"),
    };
    if !flattened.is_empty() {
        let _scope = ambient::test_guard();
        storage
            .insert_vector(0, TypedMultiDenseVectorRef::new(flattened, DIM).into())
            .unwrap();
    }
    (storage, directory)
}

fn typed_dense_rows<T: PrimitiveVectorElement>(
    storage: &impl DenseVectorStorageRead<T>,
    config: &QuantizationConfig,
    distance: Distance,
) -> Vec<Vec<f32>> {
    (0..storage.total_vector_count() as PointOffsetType)
        .map(|id| {
            T::quantization_preprocess(config, distance, storage.get_dense::<Sequential>(id))
                .into_owned()
        })
        .collect()
}

fn reference_dense_rows(
    storage: &VectorStorageEnum,
    config: &QuantizationConfig,
    distance: Distance,
) -> Vec<Vec<f32>> {
    match storage {
        VectorStorageEnum::DenseVolatile(storage) => {
            typed_dense_rows::<VectorElementType>(storage, config, distance)
        }
        VectorStorageEnum::DenseVolatileHalf(storage) => {
            typed_dense_rows::<VectorElementTypeHalf>(storage, config, distance)
        }
        VectorStorageEnum::DenseVolatileByte(storage) => {
            typed_dense_rows::<VectorElementTypeByte>(storage, config, distance)
        }
        VectorStorageEnum::DenseMemmap(_)
        | VectorStorageEnum::DenseMemmapByte(_)
        | VectorStorageEnum::DenseMemmapHalf(_)
        | VectorStorageEnum::DenseGraphInline(_)
        | VectorStorageEnum::DenseGraphInlineByte(_)
        | VectorStorageEnum::DenseGraphInlineHalf(_)
        | VectorStorageEnum::DenseAppendableMemmap(_)
        | VectorStorageEnum::DenseAppendableMemmapByte(_)
        | VectorStorageEnum::DenseAppendableMemmapHalf(_)
        | VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboGraphInline(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_)
        | VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::MultiDenseTurbo(_)
        | VectorStorageEnum::EmptyDense(_)
        | VectorStorageEnum::EmptySparse(_) => unreachable!("dense primitive fixture only"),
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(_)
        | VectorStorageEnum::DenseUringByte(_)
        | VectorStorageEnum::DenseUringHalf(_)
        | VectorStorageEnum::DenseTurboUring(_) => unreachable!("dense primitive fixture only"),
    }
}

fn typed_multi_rows<T: PrimitiveVectorElement>(
    storage: &(impl MultiVectorStorage<T> + VectorStorageRead),
    config: &QuantizationConfig,
    distance: Distance,
) -> Vec<Vec<f32>> {
    storage
        .iterate_inner_vectors()
        .map(|vector| T::quantization_preprocess(config, distance, vector).into_owned())
        .collect()
}

fn reference_multi_rows(
    storage: &VectorStorageEnum,
    config: &QuantizationConfig,
    distance: Distance,
) -> Vec<Vec<f32>> {
    match storage {
        VectorStorageEnum::MultiDenseVolatile(storage) => {
            typed_multi_rows::<VectorElementType>(storage, config, distance)
        }
        VectorStorageEnum::MultiDenseVolatileHalf(storage) => {
            typed_multi_rows::<VectorElementTypeHalf>(storage, config, distance)
        }
        VectorStorageEnum::MultiDenseVolatileByte(storage) => {
            typed_multi_rows::<VectorElementTypeByte>(storage, config, distance)
        }
        VectorStorageEnum::MultiDenseAppendableMemmap(storage) => {
            typed_multi_rows::<VectorElementType>(storage.as_ref(), config, distance)
        }
        VectorStorageEnum::MultiDenseAppendableMemmapHalf(storage) => {
            typed_multi_rows::<VectorElementTypeHalf>(storage.as_ref(), config, distance)
        }
        VectorStorageEnum::MultiDenseAppendableMemmapByte(storage) => {
            typed_multi_rows::<VectorElementTypeByte>(storage.as_ref(), config, distance)
        }
        VectorStorageEnum::DenseVolatile(_)
        | VectorStorageEnum::DenseVolatileByte(_)
        | VectorStorageEnum::DenseVolatileHalf(_)
        | VectorStorageEnum::DenseMemmap(_)
        | VectorStorageEnum::DenseMemmapByte(_)
        | VectorStorageEnum::DenseMemmapHalf(_)
        | VectorStorageEnum::DenseGraphInline(_)
        | VectorStorageEnum::DenseGraphInlineByte(_)
        | VectorStorageEnum::DenseGraphInlineHalf(_)
        | VectorStorageEnum::DenseAppendableMemmap(_)
        | VectorStorageEnum::DenseAppendableMemmapByte(_)
        | VectorStorageEnum::DenseAppendableMemmapHalf(_)
        | VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboGraphInline(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_)
        | VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::MultiDenseTurbo(_)
        | VectorStorageEnum::EmptyDense(_)
        | VectorStorageEnum::EmptySparse(_) => {
            unreachable!("multivector primitive fixture only")
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(_)
        | VectorStorageEnum::DenseUringByte(_)
        | VectorStorageEnum::DenseUringHalf(_)
        | VectorStorageEnum::DenseTurboUring(_) => {
            unreachable!("multivector primitive fixture only")
        }
    }
}

fn subject_multi_codes(subject: &QuantizedVectors, point: PointOffsetType) -> Vec<Vec<u8>> {
    macro_rules! codes {
        ($storage:expr) => {{
            let offset = $storage.inner_vector_offset(point);
            (offset.start..offset.start + offset.count)
                .map(|row| {
                    $storage
                        .inner_storage()
                        .storage()
                        .get_vector_data(row)
                        .to_vec()
                })
                .collect()
        }};
    }
    match &subject.storage_impl {
        QuantizedVectorStorage::BinaryRamMulti(storage) => codes!(storage),
        QuantizedVectorStorage::BinaryChunkedMmapMulti(storage) => codes!(storage),
        QuantizedVectorStorage::TQRamMulti(storage) => codes!(storage),
        QuantizedVectorStorage::TQChunkedMmapMulti(storage) => codes!(storage),
        QuantizedVectorStorage::ScalarRam(_)
        | QuantizedVectorStorage::ScalarMmap(_)
        | QuantizedVectorStorage::ScalarChunkedMmap(_)
        | QuantizedVectorStorage::PQRam(_)
        | QuantizedVectorStorage::PQMmap(_)
        | QuantizedVectorStorage::PQChunkedMmap(_)
        | QuantizedVectorStorage::BinaryRam(_)
        | QuantizedVectorStorage::BinaryMmap(_)
        | QuantizedVectorStorage::BinaryChunkedMmap(_)
        | QuantizedVectorStorage::TQRam(_)
        | QuantizedVectorStorage::TQMmap(_)
        | QuantizedVectorStorage::TQChunkedMmap(_)
        | QuantizedVectorStorage::ScalarRamMulti(_)
        | QuantizedVectorStorage::ScalarMmapMulti(_)
        | QuantizedVectorStorage::ScalarChunkedMmapMulti(_)
        | QuantizedVectorStorage::PQRamMulti(_)
        | QuantizedVectorStorage::PQMmapMulti(_)
        | QuantizedVectorStorage::PQChunkedMmapMulti(_)
        | QuantizedVectorStorage::BinaryMmapMulti(_)
        | QuantizedVectorStorage::TQMmapMulti(_) => {
            unreachable!("mutable Binary/Turbo multivector fixture only")
        }
    }
}

fn assert_dense_codes_match_build_path(
    datatype: VectorStorageDatatype,
    distance: Distance,
    config: QuantizationConfig,
    vectors: Vec<Vec<f32>>,
) {
    let quant_dir = TempDir::with_prefix("appendable_dense_subject").unwrap();
    let empty = dense_storage(datatype, distance, &[]);
    let mut subject = QuantizedVectors::create(
        &empty,
        &config,
        QuantizedVectorsStorageType::Mutable,
        quant_dir.path(),
        1,
        &AtomicBool::new(false),
    )
    .unwrap();
    for (id, vector) in vectors.iter().enumerate() {
        subject
            .upsert_vector(id as PointOffsetType, VectorRef::from(vector.as_slice()))
            .unwrap();
    }

    let raw_reference = dense_storage(datatype, distance, &vectors);
    let rows = reference_dense_rows(&raw_reference, &config, distance);
    let mut encoder = ReferenceEncoder::load(
        &config,
        false,
        &QuantizedVectors::get_meta_path(quant_dir.path()),
    );
    for (id, row) in rows.iter().enumerate() {
        let expected = encoder.encode(row);
        assert_eq!(
            subject.get_quantized_vector(id as PointOffsetType).as_ref(),
            expected.as_slice(),
            "{O1_MARKER}: dense point {id}",
        );
    }
}

fn assert_multi_codes_match_build_path(
    datatype: VectorStorageDatatype,
    distance: Distance,
    config: QuantizationConfig,
    flattened: Vec<f32>,
) {
    let quant_dir = TempDir::with_prefix("appendable_multi_subject").unwrap();
    let (empty, _empty_directory) = multi_storage(datatype, distance, &[]);
    assert_eq!(
        empty.datatype(),
        datatype,
        "appendable multivector subject must report its primitive element datatype",
    );
    let mut subject = QuantizedVectors::create(
        &empty,
        &config,
        QuantizedVectorsStorageType::Mutable,
        quant_dir.path(),
        1,
        &AtomicBool::new(false),
    )
    .unwrap();
    subject
        .upsert_vector(0, TypedMultiDenseVectorRef::new(&flattened, DIM).into())
        .unwrap();

    let (raw_reference, _reference_directory) = multi_storage(datatype, distance, &flattened);
    assert_eq!(
        raw_reference.datatype(),
        datatype,
        "appendable multivector reference must report its primitive element datatype",
    );
    let rows = reference_multi_rows(&raw_reference, &config, distance);
    let mut encoder = ReferenceEncoder::load(
        &config,
        true,
        &QuantizedVectors::get_meta_path(quant_dir.path()),
    );
    let expected: Vec<_> = rows.iter().map(|row| encoder.encode(row)).collect();
    assert_eq!(
        subject_multi_codes(&subject, 0),
        expected,
        "{O1_MARKER}: multivector point 0",
    );
}

fn uint8_vectors() -> Vec<Vec<f32>> {
    vec![
        vec![0.0, 32.0, 64.0, 96.0, 128.0, 160.0, 224.0, 255.0],
        vec![255.0, 224.0, 160.0, 128.0, 96.0, 64.0, 32.0, 0.0],
    ]
}

fn float32_vectors() -> Vec<Vec<f32>> {
    vec![
        vec![-4.0, -3.0, -2.0, -1.0, 1.0, 2.0, 3.0, 4.0],
        vec![4.0, 3.0, 2.0, 1.0, -1.0, -2.0, -3.0, -4.0],
    ]
}

macro_rules! dense_binary_case {
    ($name:ident, $datatype:expr, $encoding:expr, $always_ram:expr, $vectors:expr) => {
        #[test]
        fn $name() {
            let _scope = ambient::test_guard();
            assert_dense_codes_match_build_path(
                $datatype,
                Distance::Dot,
                binary_config($encoding, $always_ram),
                $vectors,
            );
        }
    };
}

dense_binary_case!(
    appendable_dense_uint8_one_bit_ram_codes_match_build_path,
    VectorStorageDatatype::Uint8,
    BinaryQuantizationEncoding::OneBit,
    true,
    uint8_vectors()
);
dense_binary_case!(
    appendable_dense_uint8_one_bit_mmap_codes_match_build_path,
    VectorStorageDatatype::Uint8,
    BinaryQuantizationEncoding::OneBit,
    false,
    uint8_vectors()
);
dense_binary_case!(
    appendable_dense_uint8_two_bits_ram_codes_match_build_path,
    VectorStorageDatatype::Uint8,
    BinaryQuantizationEncoding::TwoBits,
    true,
    uint8_vectors()
);
dense_binary_case!(
    appendable_dense_uint8_two_bits_mmap_codes_match_build_path,
    VectorStorageDatatype::Uint8,
    BinaryQuantizationEncoding::TwoBits,
    false,
    uint8_vectors()
);
dense_binary_case!(
    appendable_dense_uint8_one_and_half_bits_ram_codes_match_build_path,
    VectorStorageDatatype::Uint8,
    BinaryQuantizationEncoding::OneAndHalfBits,
    true,
    uint8_vectors()
);
dense_binary_case!(
    appendable_dense_uint8_one_and_half_bits_mmap_codes_match_build_path,
    VectorStorageDatatype::Uint8,
    BinaryQuantizationEncoding::OneAndHalfBits,
    false,
    uint8_vectors()
);

dense_binary_case!(
    appendable_dense_float32_one_bit_ram_codes_match_build_path,
    VectorStorageDatatype::Float32,
    BinaryQuantizationEncoding::OneBit,
    true,
    float32_vectors()
);
dense_binary_case!(
    appendable_dense_float32_one_bit_mmap_codes_match_build_path,
    VectorStorageDatatype::Float32,
    BinaryQuantizationEncoding::OneBit,
    false,
    float32_vectors()
);
dense_binary_case!(
    appendable_dense_float32_two_bits_ram_codes_match_build_path,
    VectorStorageDatatype::Float32,
    BinaryQuantizationEncoding::TwoBits,
    true,
    float32_vectors()
);
dense_binary_case!(
    appendable_dense_float32_two_bits_mmap_codes_match_build_path,
    VectorStorageDatatype::Float32,
    BinaryQuantizationEncoding::TwoBits,
    false,
    float32_vectors()
);
dense_binary_case!(
    appendable_dense_float32_one_and_half_bits_ram_codes_match_build_path,
    VectorStorageDatatype::Float32,
    BinaryQuantizationEncoding::OneAndHalfBits,
    true,
    float32_vectors()
);
dense_binary_case!(
    appendable_dense_float32_one_and_half_bits_mmap_codes_match_build_path,
    VectorStorageDatatype::Float32,
    BinaryQuantizationEncoding::OneAndHalfBits,
    false,
    float32_vectors()
);

#[test]
fn appendable_dense_float16_underflow_one_bit_codes_match_build_path() {
    let _scope = ambient::test_guard();
    assert_dense_codes_match_build_path(
        VectorStorageDatatype::Float16,
        Distance::Dot,
        binary_config(BinaryQuantizationEncoding::OneBit, true),
        vec![vec![1.0e-8; DIM]],
    );
}

macro_rules! multi_binary_case {
    ($name:ident, $encoding:expr) => {
        #[test]
        fn $name() {
            let _scope = ambient::test_guard();
            assert_multi_codes_match_build_path(
                VectorStorageDatatype::Uint8,
                Distance::Dot,
                binary_config($encoding, true),
                uint8_vectors().into_iter().flatten().collect(),
            );
        }
    };
}

multi_binary_case!(
    appendable_multivector_uint8_one_bit_codes_match_build_path,
    BinaryQuantizationEncoding::OneBit
);
multi_binary_case!(
    appendable_multivector_uint8_two_bits_codes_match_build_path,
    BinaryQuantizationEncoding::TwoBits
);
multi_binary_case!(
    appendable_multivector_uint8_one_and_half_bits_codes_match_build_path,
    BinaryQuantizationEncoding::OneAndHalfBits
);

#[test]
fn appendable_multivector_float16_underflow_one_bit_codes_match_build_path() {
    let _scope = ambient::test_guard();
    assert_multi_codes_match_build_path(
        VectorStorageDatatype::Float16,
        Distance::Dot,
        binary_config(BinaryQuantizationEncoding::OneBit, true),
        vec![1.0e-8; DIM * 2],
    );
}

#[test]
fn appendable_dense_uint8_turbo_codes_match_build_path() {
    let _scope = ambient::test_guard();
    assert_dense_codes_match_build_path(
        VectorStorageDatatype::Uint8,
        Distance::Cosine,
        turbo_config(true),
        uint8_vectors(),
    );
}

#[test]
fn appendable_multivector_uint8_turbo_codes_match_build_path() {
    let _scope = ambient::test_guard();
    assert_multi_codes_match_build_path(
        VectorStorageDatatype::Uint8,
        Distance::Cosine,
        turbo_config(true),
        uint8_vectors().into_iter().flatten().collect(),
    );
}
