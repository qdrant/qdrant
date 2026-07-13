//! Regression tests for storages whose `get_vector_data` returns `Cow::Owned` (e.g. the
//! disk-cache / uring backends), as opposed to the borrowed views of mmap-based storages.
//!
//! `EncodedVectorsU8` used to extract a raw pointer from the returned buffer and drop the
//! buffer before dereferencing it — a use-after-free on any owning storage. These tests run
//! the scoring and vector-access paths over an owning storage and check they agree with the
//! borrowed baseline, so the owned code path stays exercised (and fails under Miri/ASAN if
//! the buffer lifetime is ever mishandled again).

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use common::mmap::MmapFlusher;
    use common::types::PointOffsetType;
    use quantization::encoded_storage::{
        EncodedStorage, EncodedStorageBuilder, TestEncodedStorage, TestEncodedStorageBuilder,
    };
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_u8;
    use quantization::encoded_vectors_u8::{EncodedVectorsU8, ScalarQuantizationMethod};
    use rand::{RngExt, SeedableRng};

    /// Wraps a storage so every read returns a freshly allocated `Cow::Owned` buffer.
    struct OwnedStorage(TestEncodedStorage);

    impl EncodedStorage for OwnedStorage {
        fn get_vector_data(&self, index: PointOffsetType) -> Cow<'_, [u8]> {
            let Self(inner) = self;
            Cow::Owned(inner.get_vector_data(index).into_owned())
        }

        fn get_vector_data_opt(&self, index: PointOffsetType) -> Option<Cow<'_, [u8]>> {
            let Self(inner) = self;
            Some(Cow::Owned(inner.get_vector_data_opt(index)?.into_owned()))
        }

        fn is_in_ram_or_mmap() -> bool {
            TestEncodedStorage::is_in_ram_or_mmap()
        }

        fn is_on_disk(&self) -> bool {
            let Self(inner) = self;
            inner.is_on_disk()
        }

        fn upsert_vector(
            &mut self,
            id: PointOffsetType,
            vector: &[u8],
            hw_counter: &HardwareCounterCell,
        ) -> std::io::Result<()> {
            let Self(inner) = self;
            inner.upsert_vector(id, vector, hw_counter)
        }

        fn vectors_count(&self) -> usize {
            let Self(inner) = self;
            inner.vectors_count()
        }

        fn flusher(&self) -> MmapFlusher {
            let Self(inner) = self;
            inner.flusher()
        }

        fn files(&self) -> Vec<PathBuf> {
            let Self(inner) = self;
            inner.files()
        }

        fn immutable_files(&self) -> Vec<PathBuf> {
            let Self(inner) = self;
            inner.immutable_files()
        }

        fn heap_size_bytes(&self) -> usize {
            let Self(inner) = self;
            inner.heap_size_bytes()
        }
    }

    struct OwnedStorageBuilder(TestEncodedStorageBuilder);

    impl EncodedStorageBuilder for OwnedStorageBuilder {
        type Storage = OwnedStorage;
        type Error = std::io::Error;

        fn build(self) -> std::io::Result<Self::Storage> {
            let Self(inner) = self;
            Ok(OwnedStorage(inner.build()?))
        }

        fn push_vector_data(&mut self, other: &[u8]) -> std::io::Result<()> {
            let Self(inner) = self;
            inner.push_vector_data(other)
        }
    }

    #[test]
    fn test_u8_owned_storage_matches_borrowed() {
        let vectors_count = 65;
        let vector_dim = 65;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let vector_data: Vec<Vec<f32>> = (0..vectors_count)
            .map(|_| (0..vector_dim).map(|_| rng.random()).collect())
            .collect();
        let query: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();

        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);

        let encoded_owned = EncodedVectorsU8::encode(
            vector_data.iter(),
            OwnedStorageBuilder(TestEncodedStorageBuilder::new(None, quantized_vector_size)),
            &vector_parameters,
            vectors_count,
            None,
            ScalarQuantizationMethod::Int8,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let encoded_borrowed = EncodedVectorsU8::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            vectors_count,
            None,
            ScalarQuantizationMethod::Int8,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();

        // Offset + code accessor must return identical data through owning and borrowed
        // storages, and the code must have the quantized vector size minus the offset constant.
        let code_size = encoded_borrowed.quantized_vector_size() - size_of::<f32>();
        for i in 0..vectors_count as PointOffsetType {
            let (offset_owned, code_owned) = encoded_owned.get_quantized_vector_offset_and_code(i);
            let (offset_borrowed, code_borrowed) =
                encoded_borrowed.get_quantized_vector_offset_and_code(i);
            assert_eq!(offset_owned, offset_borrowed);
            assert_eq!(code_owned, code_borrowed);
            assert_eq!(code_owned.len(), code_size);
        }

        // Internal (point-to-point) scoring reads two vectors at once from the storage.
        for i in 0..vectors_count as PointOffsetType {
            let j = (i + 7) % vectors_count as PointOffsetType;
            assert_eq!(
                encoded_owned.score_internal(i, j, &counter),
                encoded_borrowed.score_internal(i, j, &counter),
            );
        }

        // Queries encoded from a stored vector must score identically as well.
        let query_owned = encoded_owned.encode_internal_vector(0).unwrap();
        let query_borrowed = encoded_borrowed.encode_internal_vector(0).unwrap();
        for i in 0..vectors_count as PointOffsetType {
            assert_eq!(
                encoded_owned.score_point(&query_owned, i, &counter),
                encoded_borrowed.score_point(&query_borrowed, i, &counter),
            );
        }

        // External query scoring by point id.
        let query_u8_owned = encoded_owned.encode_query(&query);
        let query_u8_borrowed = encoded_borrowed.encode_query(&query);
        for i in 0..vectors_count as PointOffsetType {
            assert_eq!(
                encoded_owned.score_point(&query_u8_owned, i, &counter),
                encoded_borrowed.score_point(&query_u8_borrowed, i, &counter),
            );
        }
    }
}
