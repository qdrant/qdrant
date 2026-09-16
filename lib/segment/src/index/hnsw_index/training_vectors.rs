//! On-disk store of HNSW *training vectors*.
//!
//! Training vectors are a **build input** for the query-aware projection edges
//! (see [`crate::index::hnsw_index::query_aware_edges`]). They are not points,
//! they are never searchable and they are never returned by any query: they are
//! a rehearsal audience that tells the builder which stored points tend to be
//! retrieved *together* by the queries the index will really be asked.
//!
//! They live in the **collection** directory, not in a segment, so that every
//! segment build of that collection — including the ones an optimizer triggers
//! later, and the ones on other shards of the same node — reads the same set:
//!
//! ```text
//! <storage>/collections/<collection>/hnsw_training_vectors/
//!     hnsw_training_vectors.json           header of the unnamed (default) vector
//!     hnsw_training_vectors.f32            its rows, row-major little-endian f32
//!     hnsw_training_vectors-<name>.json    header of the named vector `<name>`
//!     hnsw_training_vectors-<name>.f32     its rows
//! ```
//!
//! The `.f32` file is a raw `num_vectors * dim` little-endian `f32` matrix with
//! no header of its own, so it can be produced by `numpy.tofile` directly. The
//! sidecar `.json` carries the row count and the dimension, and is the file that
//! decides whether a training set exists at all.

use std::io::{BufWriter, Read as _, Write as _};
use std::path::{Path, PathBuf};

use fs_err as fs;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::segment_constructor::get_vector_name_with_prefix;
use crate::types::VectorName;

/// Name of the directory holding the training vectors inside a collection directory.
pub const HNSW_TRAINING_VECTORS_DIR: &str = "hnsw_training_vectors";

/// Common stem of both files of one vector name.
const FILE_STEM: &str = "hnsw_training_vectors";

/// Refuse anything above this to keep a hostile or mistaken upload from filling the disk.
pub const MAX_TRAINING_VECTOR_DIM: usize = 65536;

/// The directory holding the training vectors of a collection.
pub fn training_vectors_dir(collection_path: &Path) -> PathBuf {
    collection_path.join(HNSW_TRAINING_VECTORS_DIR)
}

/// Path of the raw `f32` matrix of one vector name.
pub fn training_vectors_data_path(dir: &Path, vector_name: &VectorName) -> PathBuf {
    dir.join(format!(
        "{}.f32",
        get_vector_name_with_prefix(FILE_STEM, vector_name)
    ))
}

/// Path of the JSON header of one vector name.
pub fn training_vectors_header_path(dir: &Path, vector_name: &VectorName) -> PathBuf {
    dir.join(format!(
        "{}.json",
        get_vector_name_with_prefix(FILE_STEM, vector_name)
    ))
}

/// Sidecar header describing the `.f32` matrix next to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HnswTrainingVectorsHeader {
    /// Vector name these training vectors belong to (`""` for the unnamed vector).
    pub vector_name: String,
    /// Dimensionality of a single training vector.
    pub dim: usize,
    /// Number of rows in the `.f32` file.
    pub num_vectors: usize,
}

impl HnswTrainingVectorsHeader {
    fn load(path: &Path) -> OperationResult<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(path)?;
        let header: Self = serde_json::from_str(&content).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to read HNSW training vectors header {}: {err}",
                path.display(),
            ))
        })?;
        Ok(Some(header))
    }

    fn save(&self, path: &Path) -> OperationResult<()> {
        let content = serde_json::to_string_pretty(self).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to serialize HNSW training vectors header: {err}",
            ))
        })?;
        fs::write(path, content)?;
        Ok(())
    }
}

/// Where a vector index build should read its training vectors from.
///
/// Carried through [`VectorIndexBuildArgs`](crate::segment_constructor::VectorIndexBuildArgs)
/// so the HNSW builder — which only knows its own segment — can still reach the
/// collection-level training set of the vector name it is building.
#[derive(Debug, Clone, Copy)]
pub struct HnswTrainingVectorsSource<'a> {
    /// The collection's `hnsw_training_vectors` directory.
    pub dir: &'a Path,
    /// Vector name being built (`""` for the unnamed vector).
    pub vector_name: &'a VectorName,
}

impl HnswTrainingVectorsSource<'_> {
    /// Load the training set this source points at, if it exists.
    pub fn load(&self) -> OperationResult<Option<HnswTrainingVectors>> {
        HnswTrainingVectors::load(self.dir, self.vector_name)
    }
}

/// A loaded training set: `num_vectors` rows of `dim` `f32` each, row-major.
#[derive(Debug, Clone, Default)]
pub struct HnswTrainingVectors {
    dim: usize,
    data: Vec<f32>,
}

impl HnswTrainingVectors {
    pub fn new(dim: usize, data: Vec<f32>) -> OperationResult<Self> {
        if dim == 0 || !data.len().is_multiple_of(dim) {
            return Err(OperationError::service_error(format!(
                "HNSW training vectors: {} floats are not a multiple of dim {dim}",
                data.len(),
            )));
        }
        Ok(Self { dim, data })
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn len(&self) -> usize {
        self.data.len().checked_div(self.dim).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The `i`-th training vector.
    pub fn get(&self, i: usize) -> &[f32] {
        &self.data[i * self.dim..(i + 1) * self.dim]
    }

    /// Read the training set of `vector_name` from `dir`.
    ///
    /// Returns `Ok(None)` when no training set was ever uploaded for that name.
    pub fn load(dir: &Path, vector_name: &VectorName) -> OperationResult<Option<Self>> {
        let header_path = training_vectors_header_path(dir, vector_name);
        let Some(header) = HnswTrainingVectorsHeader::load(&header_path)? else {
            return Ok(None);
        };
        let data_path = training_vectors_data_path(dir, vector_name);
        let expected_bytes = header
            .num_vectors
            .checked_mul(header.dim)
            .and_then(|floats| floats.checked_mul(size_of::<f32>()))
            .ok_or_else(|| {
                OperationError::service_error(
                    "HNSW training vectors: header describes an impossibly large matrix",
                )
            })?;

        let actual_bytes = fs::metadata(&data_path)?.len() as usize;
        if actual_bytes != expected_bytes {
            return Err(OperationError::service_error(format!(
                "HNSW training vectors {} is {actual_bytes} bytes, but its header says \
                 {} x {} f32 = {expected_bytes} bytes",
                data_path.display(),
                header.num_vectors,
                header.dim,
            )));
        }

        let mut bytes = Vec::with_capacity(expected_bytes);
        fs::File::open(&data_path)?.read_to_end(&mut bytes)?;
        let mut data = vec![0.0f32; header.num_vectors * header.dim];
        for (float, chunk) in data.iter_mut().zip(bytes.chunks_exact(size_of::<f32>())) {
            *float = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        }

        Ok(Some(Self {
            dim: header.dim,
            data,
        }))
    }

    /// Header of the training set of `vector_name`, without reading the matrix.
    pub fn info(
        dir: &Path,
        vector_name: &VectorName,
    ) -> OperationResult<Option<HnswTrainingVectorsHeader>> {
        HnswTrainingVectorsHeader::load(&training_vectors_header_path(dir, vector_name))
    }

    /// Append `rows` (row-major, `dim` floats each) to the training set of `vector_name`,
    /// creating it if needed. Returns the header after the append.
    ///
    /// When `append` is false the previous content is dropped first.
    pub fn store(
        dir: &Path,
        vector_name: &VectorName,
        dim: usize,
        rows: &[f32],
        append: bool,
    ) -> OperationResult<HnswTrainingVectorsHeader> {
        if dim == 0 || dim > MAX_TRAINING_VECTOR_DIM {
            return Err(OperationError::validation_error(format!(
                "HNSW training vectors: unsupported dimension {dim}",
            )));
        }
        if !rows.len().is_multiple_of(dim) {
            return Err(OperationError::validation_error(format!(
                "HNSW training vectors: {} floats are not a multiple of dim {dim}",
                rows.len(),
            )));
        }

        fs::create_dir_all(dir)?;
        let header_path = training_vectors_header_path(dir, vector_name);
        let data_path = training_vectors_data_path(dir, vector_name);

        let existing = if append {
            HnswTrainingVectorsHeader::load(&header_path)?
        } else {
            None
        };
        if let Some(existing) = &existing
            && existing.dim != dim
        {
            return Err(OperationError::validation_error(format!(
                "HNSW training vectors for vector `{vector_name}` already have dim {}, \
                 got {dim}",
                existing.dim,
            )));
        }

        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(existing.is_some())
            .truncate(existing.is_none())
            .open(&data_path)?;
        {
            let mut writer = BufWriter::new(&mut file);
            for value in rows {
                writer.write_all(&value.to_le_bytes())?;
            }
            writer.flush()?;
        }
        file.sync_all()?;
        drop(file);

        let header = HnswTrainingVectorsHeader {
            vector_name: vector_name.to_string(),
            dim,
            num_vectors: existing.map_or(0, |h| h.num_vectors) + rows.len() / dim,
        };
        header.save(&header_path)?;
        Ok(header)
    }

    /// Drop the training set of `vector_name`. Returns true if anything was removed.
    pub fn clear(dir: &Path, vector_name: &VectorName) -> OperationResult<bool> {
        let mut removed = false;
        for path in [
            training_vectors_header_path(dir, vector_name),
            training_vectors_data_path(dir, vector_name),
        ] {
            if path.exists() {
                fs::remove_file(&path)?;
                removed = true;
            }
        }
        Ok(removed)
    }

    /// Deterministically sample down to at most `max` rows.
    ///
    /// The projection post-pass does an exact `n_train x n_points x dim` scan, so the training
    /// set has to be bounded. Sampling is seeded, so every segment of a collection — and a
    /// rebuild of the same segment — sees the same subset.
    #[must_use]
    pub fn sampled(self, max: usize, seed: u64) -> Self {
        let len = self.len();
        if len <= max {
            return self;
        }
        let mut indices: Vec<usize> = (0..len).collect();
        let mut rng = StdRng::seed_from_u64(seed);
        indices.shuffle(&mut rng);
        indices.truncate(max);
        indices.sort_unstable();

        let mut data = Vec::with_capacity(max * self.dim);
        for i in indices {
            data.extend_from_slice(&self.data[i * self.dim..(i + 1) * self.dim]);
        }
        Self {
            dim: self.dim,
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;

    #[test]
    fn store_load_append_clear_roundtrip() {
        let dir = Builder::new().prefix("training_vectors").tempdir().unwrap();
        let dir = dir.path();

        assert!(HnswTrainingVectors::load(dir, "").unwrap().is_none());
        assert!(HnswTrainingVectors::info(dir, "").unwrap().is_none());

        let header = HnswTrainingVectors::store(dir, "", 2, &[1.0, 2.0, 3.0, 4.0], true).unwrap();
        assert_eq!(header.num_vectors, 2);
        assert_eq!(header.dim, 2);

        let header = HnswTrainingVectors::store(dir, "", 2, &[5.0, 6.0], true).unwrap();
        assert_eq!(header.num_vectors, 3);

        let loaded = HnswTrainingVectors::load(dir, "").unwrap().unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.dim(), 2);
        assert_eq!(loaded.get(0), &[1.0, 2.0]);
        assert_eq!(loaded.get(2), &[5.0, 6.0]);

        // A different vector name is a different file.
        assert!(HnswTrainingVectors::load(dir, "other").unwrap().is_none());
        HnswTrainingVectors::store(dir, "other", 3, &[1.0, 1.0, 1.0], true).unwrap();
        assert_eq!(
            HnswTrainingVectors::load(dir, "other")
                .unwrap()
                .unwrap()
                .len(),
            1,
        );
        assert_eq!(
            HnswTrainingVectors::load(dir, "").unwrap().unwrap().len(),
            3
        );

        // Overwrite instead of append.
        let header = HnswTrainingVectors::store(dir, "", 2, &[9.0, 9.0], false).unwrap();
        assert_eq!(header.num_vectors, 1);
        assert_eq!(
            HnswTrainingVectors::load(dir, "").unwrap().unwrap().get(0),
            &[9.0, 9.0],
        );

        assert!(HnswTrainingVectors::clear(dir, "").unwrap());
        assert!(!HnswTrainingVectors::clear(dir, "").unwrap());
        assert!(HnswTrainingVectors::load(dir, "").unwrap().is_none());
    }

    #[test]
    fn dim_mismatch_is_rejected() {
        let dir = Builder::new().prefix("training_vectors").tempdir().unwrap();
        let dir = dir.path();
        HnswTrainingVectors::store(dir, "", 2, &[1.0, 2.0], true).unwrap();
        assert!(HnswTrainingVectors::store(dir, "", 3, &[1.0, 2.0, 3.0], true).is_err());
        // Not a whole number of rows.
        assert!(HnswTrainingVectors::store(dir, "", 2, &[1.0, 2.0, 3.0], true).is_err());
    }

    #[test]
    fn sampling_is_deterministic_and_bounded() {
        let dim = 2;
        let data: Vec<f32> = (0..20).map(|i| i as f32).collect();
        let vectors = HnswTrainingVectors::new(dim, data).unwrap();
        assert_eq!(vectors.len(), 10);

        let a = vectors.clone().sampled(4, 7);
        let b = vectors.clone().sampled(4, 7);
        let c = vectors.clone().sampled(4, 8);
        assert_eq!(a.len(), 4);
        assert_eq!(a.data, b.data);
        assert_ne!(a.data, c.data);

        // Nothing to do when already small enough.
        assert_eq!(vectors.clone().sampled(10, 7).len(), 10);
        assert_eq!(vectors.sampled(100, 7).len(), 10);
    }
}
