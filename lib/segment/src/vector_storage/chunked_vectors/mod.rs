//! Fixed-dimension vectors stored flattened across a directory of
//! equally-sized chunk files.
//!
//! The directory holds a config file, a status file carrying the vector count,
//! and `chunk_<n>.mmap` files preallocated to the configured chunk size. A
//! vector never straddles a chunk boundary.
//!
//! Two types share that layout:
//!
//! - [`ChunkedVectors`] — the writable storage. Its impls are split across
//!   [`lifecycle`] (open, config creation, flush) and [`write_ops`] (insert,
//!   push).
//! - [`read_only::ReadOnlyChunkedVectors`] — the read-only view, which
//!   [`ChunkedVectors`] wraps and derefs to for every read.
//!
//! [`chunks`] and [`config`] hold what both sides need: the chunk files and
//! the on-disk metadata files respectively.

pub mod update_only;
mod chunks;
mod config;
mod lifecycle;
pub mod read_only;
mod write_ops;

use std::ops::Deref;

use common::universal_io::{StoredStruct, UniversalWrite};

use self::config::Status;
use self::read_only::ReadOnlyChunkedVectors;

/// Writable chunked vectors.
///
/// Wraps the read-only view — every read goes through the [`Deref`] — and adds
/// the writable status mapping, so appends update the stored vector count in
/// the same place they extend the chunks.
#[derive(Debug)]
pub struct ChunkedVectors<T, S>
where
    T: bytemuck::Pod + Send,
    S: UniversalWrite + Send + 'static,
{
    inner: ReadOnlyChunkedVectors<T, S>,
    status: StoredStruct<S, Status>,
    fs: S::Fs,
}

impl<T, S> Deref for ChunkedVectors<T, S>
where
    T: bytemuck::Pod + Send,
    S: UniversalWrite + Send + 'static,
{
    type Target = ReadOnlyChunkedVectors<T, S>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use std::iter::zip;

    use common::counter::hardware_counter::HardwareCounterCell;
    use common::mmap::AdviceSetting;
    use common::universal_io::{MmapFile, MmapFs, Populate};
    use rand::SeedableRng;
    use rand::prelude::StdRng;
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::VectorElementType;
    use crate::fixtures::index_fixtures::random_vector;

    #[test]
    fn test_chunked_mmap() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let dim = 500;
        let num_vectors = 1000;
        let mut rng = StdRng::seed_from_u64(42);

        let hw_counter = HardwareCounterCell::new();

        let mut vectors: Vec<_> = (0..num_vectors)
            .map(|_| random_vector(&mut rng, dim))
            .collect();

        {
            let mut chunked_mmap: ChunkedVectors<VectorElementType, MmapFile> =
                ChunkedVectors::open(
                    MmapFs,
                    dir.path(),
                    dim,
                    AdviceSetting::Global,
                    Populate::Blocking,
                )
                .unwrap();

            for vec in &vectors {
                chunked_mmap.push(vec, &hw_counter).unwrap();
            }

            let random_offset = 666;
            let batch_size = 10;

            let batch_ids = (random_offset as u32..random_offset as u32 + batch_size as u32)
                .collect::<Vec<_>>();
            let mut vectors_buffer = Vec::with_capacity(batch_size);
            chunked_mmap
                .for_each_in_batch(&batch_ids, |i, vec| {
                    assert_eq!(i, vectors_buffer.len());
                    vectors_buffer.push(vec.to_vec());
                })
                .unwrap();

            for (i, (vec, loaded_vec)) in zip(
                &vectors[random_offset..random_offset + batch_size],
                &vectors_buffer[..batch_size],
            )
            .enumerate()
            {
                assert_eq!(
                    vec, loaded_vec,
                    "Vectors at index {i} in chunked_mmap are not equal to vectors",
                );
            }

            vectors[0] = random_vector(&mut rng, dim);
            vectors[150] = random_vector(&mut rng, dim);
            vectors[44] = random_vector(&mut rng, dim);
            vectors[999] = random_vector(&mut rng, dim);

            chunked_mmap.insert(0, &vectors[0], &hw_counter).unwrap();
            chunked_mmap
                .insert(150, &vectors[150], &hw_counter)
                .unwrap();
            chunked_mmap.insert(44, &vectors[44], &hw_counter).unwrap();
            chunked_mmap
                .insert(999, &vectors[999], &hw_counter)
                .unwrap();

            assert!(
                chunked_mmap.chunks.len() > 1,
                "must have multiple chunks to test",
            );

            chunked_mmap.flusher()().unwrap();
        }
    }
}
