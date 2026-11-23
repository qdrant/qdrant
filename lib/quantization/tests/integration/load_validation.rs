#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, VectorParameters};
    use quantization::encoded_vectors_pq::{self, CENTROIDS_COUNT, EncodedVectorsPQ};
    use quantization::encoded_vectors_u8;
    use quantization::encoded_vectors_u8::{EncodedVectorsU8, ScalarQuantizationMethod};
    use tempfile::Builder;

    /// The hot-path `score_bytes` asserts (in debug builds) that each encoded vector is at least
    /// as large as the metadata expects. `load` performs a stricter, exact-size check for
    /// storage-derived vectors once at load time, so the invariant also holds in release builds.
    /// This verifies that a storage whose vector stride does not exactly match the metadata is
    /// rejected at load time instead of reaching the hot path and reading out of bounds.
    #[test]
    fn load_requires_storage_vector_size_to_match_metadata() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let vectors_count = 4;
        let vector_dim = 256;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let vector_data: Vec<Vec<f32>> = (0..vectors_count)
            .map(|i| vec![i as f32; vector_dim])
            .collect();

        let data_path = dir.path().join("data.bin");
        let meta_path = dir.path().join("meta.json");
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
        EncodedVectorsU8::encode(
            vector_data.iter(),
            std::iter::repeat_n(true, vectors_count),
            TestEncodedStorageBuilder::new(Some(data_path.as_path()), quantized_vector_size),
            &vector_parameters,
            None,
            ScalarQuantizationMethod::Int8,
            Some(meta_path.as_path()),
            &AtomicBool::new(false),
        )
        .unwrap();

        let try_load = |stride: usize| {
            EncodedVectorsU8::<TestEncodedStorage>::load(
                &common::universal_io::MmapFs,
                TestEncodedStorage::from_file(data_path.as_path(), stride).unwrap(),
                meta_path.as_path(),
            )
        };

        // Loading with the exact stride succeeds.
        try_load(quantized_vector_size).unwrap();

        // Loading with a stride that does not exactly match the metadata must be rejected,
        // whether it is smaller or larger than expected. Both strides still divide the data
        // file, so they pass the storage's own divisibility check and only our load-time
        // consistency check catches the mismatch.
        for stride in [quantized_vector_size / 2, quantized_vector_size * 2] {
            match try_load(stride) {
                Ok(_) => panic!(
                    "loading a storage with vector stride {stride} should fail, \
                     metadata expects {quantized_vector_size}"
                ),
                Err(err) => assert!(
                    matches!(&err, common::universal_io::UniversalIoError::Io(e) if e.kind() == std::io::ErrorKind::InvalidData),
                    "unexpected error: {err:?}",
                ),
            }
        }
    }

    /// Regression test for BBP-827 (vulnerability 1): heap out-of-bounds read via crafted
    /// quantization metadata in a malicious snapshot.
    ///
    /// A snapshot's `quantized.meta.json` is attacker-controlled. The scalar-quantization SIMD
    /// scoring functions use `metadata.actual_dim` as the iteration count for raw pointer
    /// arithmetic over each stored vector (e.g. `impl_score_dot(q_ptr, v_ptr, actual_dim)`). If
    /// `actual_dim` is inflated far beyond the real vector size, every score reads heap memory
    /// past the end of the (small) stored vector — leaking adjacent heap data into scores, or
    /// crashing outright for large enough values.
    ///
    /// This crafts that malicious snapshot: a storage honestly encoded for dimension 128 whose
    /// `quantized.meta.json` is then patched to claim a far larger `actual_dim`, exactly as an
    /// attacker would inside the snapshot archive. It asserts that `load` rejects the inconsistent
    /// storage, so search queries can never reach the out-of-bounds read.
    #[test]
    fn bbp_827_load_rejects_inflated_actual_dim() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let vectors_count = 4;
        let vector_dim = 128;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Cosine,
            invert: false,
        };
        let vector_data: Vec<Vec<f32>> = (0..vectors_count)
            .map(|i| vec![i as f32; vector_dim])
            .collect();

        let data_path = dir.path().join("data.bin");
        let meta_path = dir.path().join("quantized.meta.json");
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
        EncodedVectorsU8::encode(
            vector_data.iter(),
            std::iter::repeat_n(true, vectors_count),
            TestEncodedStorageBuilder::new(Some(data_path.as_path()), quantized_vector_size),
            &vector_parameters,
            None,
            ScalarQuantizationMethod::Int8,
            Some(meta_path.as_path()),
            &AtomicBool::new(false),
        )
        .unwrap();

        let load = || {
            EncodedVectorsU8::<TestEncodedStorage>::load(
                &common::universal_io::MmapFs,
                TestEncodedStorage::from_file(data_path.as_path(), quantized_vector_size).unwrap(),
                meta_path.as_path(),
            )
        };

        // The honest snapshot loads fine.
        load().unwrap();

        // Sanity check: the encoded metadata records the real dimension under `actual_dim`. This
        // guards against the field being renamed, which would silently make the patch below (and
        // thus this regression test) a no-op.
        let original = fs_err::read_to_string(&meta_path).unwrap();
        let metadata: serde_json::Value = serde_json::from_str(&original).unwrap();
        assert_eq!(metadata["actual_dim"].as_u64(), Some(vector_dim as u64));

        let patch_actual_dim = |actual_dim: u64| {
            let contents = fs_err::read_to_string(&meta_path).unwrap();
            let mut metadata: serde_json::Value = serde_json::from_str(&contents).unwrap();
            metadata["actual_dim"] = serde_json::Value::from(actual_dim);
            fs_err::write(&meta_path, serde_json::to_string(&metadata).unwrap()).unwrap();
        };

        // Each stored vector is only `quantized_vector_size` bytes, but the patched metadata
        // claims `actual_dim` per vector — so scoring would iterate `actual_dim` times over that
        // small buffer, reading well past its allocation. `8192` is the report's score-inflation
        // value; `10_000_000` is its crash value. Both must be rejected at load time.
        for inflated_actual_dim in [8192, 10_000_000] {
            patch_actual_dim(inflated_actual_dim);
            match load() {
                Ok(_) => panic!(
                    "malicious snapshot with actual_dim={inflated_actual_dim} was accepted — \
                     heap OOB read is reachable (BBP-827 regression)"
                ),
                Err(err) => assert!(
                    matches!(&err, common::universal_io::UniversalIoError::Io(e) if e.kind() == std::io::ErrorKind::InvalidData),
                    "unexpected error: {err:?}",
                ),
            }
        }
    }

    /// PQ scoring uses each stored byte as an unchecked index into a query LUT row of
    /// `centroids.len()` entries. A snapshot's `quantized.meta.json` is attacker-controlled, so
    /// metadata with fewer than `CENTROIDS_COUNT` centroids would let a stored code read past the
    /// LUT. This patches the centroid count of an honestly encoded storage and asserts that `load`
    /// rejects it.
    #[test]
    fn pq_load_rejects_wrong_centroids_count() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let vectors_count = 4;
        let vector_dim = 8;
        let chunk_size = 2;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let vector_data: Vec<Vec<f32>> = (0..vectors_count)
            .map(|i| vec![i as f32; vector_dim])
            .collect();

        let data_path = dir.path().join("data.bin");
        let meta_path = dir.path().join("quantized.meta.json");
        let quantized_vector_size =
            encoded_vectors_pq::get_quantized_vector_size(&vector_parameters, chunk_size);
        EncodedVectorsPQ::encode(
            vector_data.iter(),
            std::iter::repeat_n(true, vectors_count),
            TestEncodedStorageBuilder::new(Some(data_path.as_path()), quantized_vector_size),
            &vector_parameters,
            chunk_size,
            1,
            Some(meta_path.as_path()),
            &AtomicBool::new(false),
        )
        .unwrap();

        let load = || {
            EncodedVectorsPQ::<TestEncodedStorage>::load(
                &common::universal_io::MmapFs,
                TestEncodedStorage::from_file(data_path.as_path(), quantized_vector_size).unwrap(),
                meta_path.as_path(),
            )
        };

        // The honest storage loads fine, and the encoder wrote the full codebook, even though
        // there are fewer vectors than centroids.
        load().unwrap();
        let original = fs_err::read_to_string(&meta_path).unwrap();
        let metadata: serde_json::Value = serde_json::from_str(&original).unwrap();
        assert_eq!(
            metadata["centroids"].as_array().map(Vec::len),
            Some(CENTROIDS_COUNT),
        );

        for centroids_count in [0, 1, CENTROIDS_COUNT - 1, CENTROIDS_COUNT + 1] {
            let mut metadata: serde_json::Value = serde_json::from_str(&original).unwrap();
            let centroids = metadata["centroids"].as_array_mut().unwrap();
            centroids.resize(centroids_count, centroids[0].clone());
            fs_err::write(&meta_path, serde_json::to_string(&metadata).unwrap()).unwrap();

            match load() {
                Ok(_) => panic!(
                    "PQ metadata with {centroids_count} centroids was accepted, \
                     but scoring may then read past the query LUT"
                ),
                Err(err) => assert!(
                    matches!(&err, common::universal_io::UniversalIoError::Io(e) if e.kind() == std::io::ErrorKind::InvalidData),
                    "unexpected error: {err:?}",
                ),
            }
        }
    }

    /// Query encoding, `score_internal` and the GPU shader slice every centroid with the
    /// division ranges. This patches the centroids and divisions of an honestly encoded storage
    /// and asserts that `load` rejects metadata whose slices would fall outside a centroid or the
    /// query, instead of panicking (or reading out of bounds on the GPU) at search time.
    #[test]
    fn pq_load_rejects_inconsistent_centroids_and_divisions() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        // An odd dimension, so the last division is shorter than the others: 0..2, 2..4, 4..6, 6..7.
        let vectors_count = 4;
        let vector_dim = 7;
        let chunk_size = 2;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let vector_data: Vec<Vec<f32>> = (0..vectors_count)
            .map(|i| vec![i as f32; vector_dim])
            .collect();

        let data_path = dir.path().join("data.bin");
        let meta_path = dir.path().join("quantized.meta.json");
        let quantized_vector_size =
            encoded_vectors_pq::get_quantized_vector_size(&vector_parameters, chunk_size);
        EncodedVectorsPQ::encode(
            vector_data.iter(),
            std::iter::repeat_n(true, vectors_count),
            TestEncodedStorageBuilder::new(Some(data_path.as_path()), quantized_vector_size),
            &vector_parameters,
            chunk_size,
            1,
            Some(meta_path.as_path()),
            &AtomicBool::new(false),
        )
        .unwrap();

        let load = || {
            EncodedVectorsPQ::<TestEncodedStorage>::load(
                &common::universal_io::MmapFs,
                TestEncodedStorage::from_file(data_path.as_path(), quantized_vector_size).unwrap(),
                meta_path.as_path(),
            )
        };

        load().unwrap();
        let original = fs_err::read_to_string(&meta_path).unwrap();

        type Patch = fn(&mut serde_json::Value);
        let cases: [(&str, Patch); 8] = [
            ("a centroid is too short", |m| {
                m["centroids"][200].as_array_mut().unwrap().truncate(1);
            }),
            ("a centroid is too long", |m| {
                m["centroids"][0].as_array_mut().unwrap().push(0.0.into());
            }),
            ("all centroids are empty", |m| {
                for centroid in m["centroids"].as_array_mut().unwrap() {
                    centroid.as_array_mut().unwrap().clear();
                }
            }),
            ("a division ends past the dimension", |m| {
                m["vector_division"][3]["end"] = 1000.into();
            }),
            ("a division is reversed", |m| {
                m["vector_division"][0]["start"] = 2.into();
                m["vector_division"][0]["end"] = 0.into();
            }),
            ("a division is empty", |m| {
                m["vector_division"][1]["end"] = 2.into();
            }),
            ("divisions do not cover the whole vector", |m| {
                m["vector_division"][3]["end"] = 6.into();
                m["vector_division"][3]["start"] = 4.into();
            }),
            ("the dimension does not match the centroids", |m| {
                m["vector_parameters"]["dim"] = 1.into();
            }),
        ];

        for (name, patch) in cases {
            let mut metadata: serde_json::Value = serde_json::from_str(&original).unwrap();
            patch(&mut metadata);
            fs_err::write(&meta_path, serde_json::to_string(&metadata).unwrap()).unwrap();

            match load() {
                Ok(_) => panic!("PQ metadata was accepted, but {name}"),
                Err(err) => assert!(
                    matches!(&err, common::universal_io::UniversalIoError::Io(e) if e.kind() == std::io::ErrorKind::InvalidData),
                    "{name}: unexpected error: {err:?}",
                ),
            }
        }
    }
}
