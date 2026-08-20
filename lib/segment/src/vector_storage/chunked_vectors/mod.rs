//! Fixed-dimension vectors stored flattened across a directory of chunk files.
//!
//! The directory holds a config file, a status file carrying the vector count,
//! and `chunk_<n>.mmap` files. A single vector never straddles a chunk
//! boundary, a run of them may.
//!
//! Three types share that layout:
//!
//! - [`ChunkedVectors`] — the writable storage, which preallocates chunks to
//!   the configured size and writes in place. Its impls are split across
//!   [`lifecycle`] (open, flush) and [`write_ops`] (insert, push).
//! - [`update_only::UpdateOnlyChunkedVectors`] — a short-lived batch writer
//!   that grows chunks by appends instead, so chunk files end at the data.
//! - [`read_only::ReadOnlyChunkedVectors`] — the read-only view over either
//!   writer's output, which [`ChunkedVectors`] wraps and derefs to for every
//!   read.
//!
//! [`chunks`] and [`config`] hold what all sides need: the chunk files and
//! the on-disk metadata files respectively.

mod chunks;
mod config;
mod lifecycle;
pub mod read_only;
#[cfg(test)]
mod test_utils;
pub mod update_only;
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
    use std::borrow::Cow;
    use std::iter::zip;

    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::mmap::AdviceSetting;
    use common::types::PointOffsetType;
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

    /// A run of vectors crossing a chunk boundary is written to both chunks and
    /// read back as one copied slice.
    #[test]
    fn run_across_chunk_boundary_round_trips() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let dim = 500;
        let hw_counter = HardwareCounterCell::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut chunked_mmap: ChunkedVectors<VectorElementType, MmapFile> = ChunkedVectors::open(
            MmapFs,
            dir.path(),
            dim,
            AdviceSetting::Global,
            Populate::Blocking,
        )
        .unwrap();

        // Start the run two vectors before the boundary, so it spans both chunks
        let per_chunk = chunked_mmap.config.chunk_size_vectors;
        let start = per_chunk - 2;
        let count = 5;
        let run: Vec<VectorElementType> = (0..count)
            .flat_map(|_| random_vector(&mut rng, dim))
            .collect();

        chunked_mmap
            .insert_many(start, &run, count, &hw_counter)
            .unwrap();

        assert_eq!(chunked_mmap.chunks.len(), 2);
        assert_eq!(chunked_mmap.len(), start + count);

        let read = chunked_mmap.get_many::<Random>(start, count).unwrap();
        assert!(matches!(read, Cow::Owned(_)), "straddling read must copy");
        assert_eq!(read.as_ref(), run.as_slice());

        // The parts are readable on their own too, borrowed from their chunk
        for (i, vector) in run.chunks_exact(dim).enumerate() {
            let one = chunked_mmap.get::<Random>(start + i).unwrap();
            assert!(matches!(one, Cow::Borrowed(_)));
            assert_eq!(one.as_ref(), vector);
        }
    }

    /// The batched path schedules one read per chunk a run covers and stitches
    /// them once they land, alongside runs that take a single read.
    #[test]
    fn for_each_vector_stitches_straddling_runs() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let dim = 500;
        let hw_counter = HardwareCounterCell::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut chunked_mmap: ChunkedVectors<VectorElementType, MmapFile> = ChunkedVectors::open(
            MmapFs,
            dir.path(),
            dim,
            AdviceSetting::Global,
            Populate::Blocking,
        )
        .unwrap();

        let per_chunk = chunked_mmap.config.chunk_size_vectors;
        let straddle_start = per_chunk - 2;
        let straddle_count = 5;

        // A run before the boundary, one across it, one after it
        let runs = [
            (0, 1),
            (straddle_start, straddle_count),
            (straddle_start + straddle_count, 3),
        ];
        let expected: Vec<Vec<VectorElementType>> = runs
            .iter()
            .map(|&(_, count)| {
                (0..count)
                    .flat_map(|_| random_vector(&mut rng, dim))
                    .collect()
            })
            .collect();

        for (&(start, count), vectors) in zip(&runs, &expected) {
            chunked_mmap
                .insert_many(start, vectors, count, &hw_counter)
                .unwrap();
        }

        let mut read = vec![None; runs.len()];
        chunked_mmap
            .for_each_vector::<Random, _>(
                runs.iter()
                    .enumerate()
                    .map(|(i, &(start, count))| (i, start as PointOffsetType, count as u32)),
                |i, vectors| {
                    read[i] = Some(vectors.to_vec());
                    Ok(())
                },
            )
            .unwrap();

        for (i, expected) in expected.iter().enumerate() {
            assert_eq!(read[i].as_ref(), Some(expected), "run {i}");
        }
    }
}
