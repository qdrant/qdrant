use std::cmp::max;
use std::collections::HashMap;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::JoinHandle;

use common::types::PointOffsetType;
use log::debug;
use rayon::prelude::*;

use crate::common::sparse_vector::{RemappedSparseVector, SparseVector};
use crate::common::types::DimId;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::posting_list::PostingBuilder;
use crate::index::posting_list_common::PostingElementEx;

/// Builder for InvertedIndexRam
pub struct InvertedIndexBuilder {
    pub posting_builders: Vec<PostingBuilder>,
    pub vector_count: usize,
    pub total_sparse_size: usize,
}

/// Builds postings while vectors are read from storage, without retaining all remapped vectors.
///
/// Dimensions are assigned by their original id. The caller supplies the final dense mapping only
/// when finishing, after the sequential storage scan has discovered every dimension.
pub struct StreamingInvertedIndexBuilder {
    senders: Vec<SyncSender<Vec<RawPostingRecord>>>,
    batches: Vec<Vec<RawPostingRecord>>,
    owner_handles: Vec<JoinHandle<HashMap<DimId, PostingBuilder>>>,
    vector_count: usize,
    total_sparse_size: usize,
}

const STREAMING_BATCH_SIZE: usize = 1_024;

impl StreamingInvertedIndexBuilder {
    pub fn new(num_threads: usize) -> Self {
        // The caller performs the storage scan and routing on one of the permitted CPUs.
        // Single-threaded builds use InvertedIndexBuilder directly.
        assert!(
            num_threads >= 2,
            "streaming requires a producer and an owner"
        );
        let owners = num_threads - 1;
        let (senders, receivers) = (0..owners)
            .map(|_| sync_channel::<Vec<RawPostingRecord>>(2))
            .unzip::<_, _, Vec<_>, Vec<_>>();
        // Construct the guard before spawning: even a partial startup failure must join every
        // worker that was already started.
        let mut builder = Self {
            batches: (0..owners).map(|_| Vec::new()).collect(),
            senders,
            owner_handles: Vec::with_capacity(owners),
            vector_count: 0,
            total_sparse_size: 0,
        };
        for receiver in receivers {
            builder.owner_handles.push(std::thread::spawn(move || {
                let mut postings = HashMap::<DimId, PostingBuilder>::new();
                for batch in receiver {
                    for record in batch {
                        postings
                            .entry(record.dim_id)
                            .or_default()
                            .add(record.id, record.weight);
                    }
                }
                postings
            }));
        }
        builder
    }

    pub fn add(&mut self, id: PointOffsetType, vector: SparseVector) {
        self.total_sparse_size = self
            .total_sparse_size
            .saturating_add(vector.len() * size_of::<PostingElementEx>());
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values) {
            let owner = posting_owner(dim_id, self.batches.len());
            let batch = &mut self.batches[owner];
            batch.push(RawPostingRecord { id, dim_id, weight });
            if batch.len() == STREAMING_BATCH_SIZE {
                self.senders[owner]
                    .send(std::mem::take(batch))
                    .expect("posting owner must outlive the storage scan");
            }
        }
        self.vector_count += 1;
    }

    pub fn finish(
        mut self,
        dimension_count: usize,
        remap: impl Fn(DimId) -> Option<usize>,
    ) -> InvertedIndexBuilder {
        for (owner, batch) in self.batches.drain(..).enumerate() {
            if !batch.is_empty() {
                self.senders[owner]
                    .send(batch)
                    .expect("posting owner must outlive the storage scan");
            }
        }
        self.senders.clear();

        let mut posting_builders = Vec::with_capacity(dimension_count);
        posting_builders.resize_with(dimension_count, PostingBuilder::new);
        // Pop individually so a panic leaves the remaining handles in the Drop guard.
        while let Some(owner) = self.owner_handles.pop() {
            for (dim_id, builder) in owner.join().expect("posting owner worker must not panic") {
                let offset = remap(dim_id)
                    .expect("every streamed dimension must be registered in the indices tracker");
                posting_builders[offset] = builder;
            }
        }
        InvertedIndexBuilder {
            posting_builders,
            vector_count: self.vector_count,
            total_sparse_size: self.total_sparse_size,
        }
    }
}

impl Drop for StreamingInvertedIndexBuilder {
    fn drop(&mut self) {
        // Disconnect before joining so receivers can finish even on scan errors/cancellation.
        self.senders.clear();
        self.batches.clear();
        while let Some(owner) = self.owner_handles.pop() {
            // Do not cause a second panic during unwinding. finish reports worker panics.
            let _ = owner.join();
        }
    }
}

fn posting_owner(dim_id: DimId, owners: usize) -> usize {
    // SplitMix64 finalizer spreads regular/strided IDs over owners rather than using their
    // low bits directly. A single hot dimension still necessarily belongs to one owner.
    let mut mixed = u64::from(dim_id);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
    mixed ^= mixed >> 31;
    (mixed % owners as u64) as usize
}

impl Default for InvertedIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndexBuilder {
    pub fn new() -> InvertedIndexBuilder {
        InvertedIndexBuilder {
            posting_builders: Vec::new(),
            vector_count: 0,
            total_sparse_size: 0,
        }
    }

    /// Add a vector to the inverted index builder
    pub fn add(&mut self, id: PointOffsetType, vector: RemappedSparseVector) {
        let sparse_size = vector.len() * size_of::<PostingElementEx>();
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values) {
            let dim_id = dim_id as usize;
            self.posting_builders.resize_with(
                max(dim_id + 1, self.posting_builders.len()),
                PostingBuilder::new,
            );
            self.posting_builders[dim_id].add(id, weight);
        }
        self.vector_count += 1;
        self.total_sparse_size = self.total_sparse_size.saturating_add(sparse_size);
    }

    /// Accumulate vectors by routing every dimension to one owner worker. Each posting list is
    /// therefore built by exactly one worker, with no per-dimension fragment merge. Finalization
    /// remains a separate operation in [`Self::build_with_threads`].
    pub fn from_vectors_with_threads(
        vectors: Vec<(PointOffsetType, RemappedSparseVector)>,
        dimension_count: usize,
        num_threads: usize,
    ) -> Self {
        if vectors.is_empty() || dimension_count == 0 || num_threads <= 1 {
            let mut builder = Self::new();
            for (id, vector) in vectors {
                builder.add(id, vector);
            }
            return builder;
        }

        // Reserve part of the permit for owners, which continuously consume routed batches, and
        // use the remainder to map source vectors. Keeping these roles disjoint avoids a Rayon
        // pool deadlock when producers block on a bounded channel.
        let owner_threads = (num_threads / 2).max(1).min(dimension_count);
        let mapper_threads = (num_threads - owner_threads).max(1).min(vectors.len());
        let vector_count = vectors.len();
        let total_sparse_size = vectors.iter().fold(0usize, |total, (_, vector)| {
            total.saturating_add(vector.len() * size_of::<PostingElementEx>())
        });

        // Several tasks per mapper keep the producer side balanced without making any routed
        // batch so large that it defeats the bounded channels below.
        let chunk_size = vectors.len().div_ceil(mapper_threads.saturating_mul(8));
        let mut vector_iter = vectors.into_iter();
        let chunks = (0..mapper_threads.saturating_mul(8))
            .filter_map(|_| {
                let chunk = vector_iter.by_ref().take(chunk_size).collect::<Vec<_>>();
                (!chunk.is_empty()).then_some(chunk)
            })
            .collect::<Vec<_>>();

        let (senders, receivers) = (0..owner_threads)
            .map(|_| sync_channel::<Vec<PostingRecord>>(2))
            .unzip::<_, _, Vec<_>, Vec<_>>();
        let owners = std::thread::scope(|scope| {
            let owner_handles = receivers
                .into_iter()
                .enumerate()
                .map(|(owner, receiver)| {
                    scope.spawn(move || {
                        // These ceil boundaries are the inverse of the owner calculation above:
                        // `dim_id * owner_threads / dimension_count`.
                        let start = (owner * dimension_count).div_ceil(owner_threads);
                        let end = ((owner + 1) * dimension_count).div_ceil(owner_threads);
                        let mut posting_builders = Vec::with_capacity(end - start);
                        posting_builders.resize_with(end - start, PostingBuilder::new);
                        for batch in receiver {
                            for record in batch {
                                posting_builders[record.dim_id - start]
                                    .add(record.id, record.weight);
                            }
                        }
                        posting_builders
                    })
                })
                .collect::<Vec<_>>();

            let mapper_senders = senders.clone();
            drop(senders);
            rayon::ThreadPoolBuilder::new()
                .num_threads(mapper_threads)
                .build()
                .expect("a positive sparse-index thread count must create a Rayon pool")
                .install(|| {
                    chunks
                        .into_par_iter()
                        .for_each_with(mapper_senders, |senders, chunk| {
                            let mut buckets =
                                (0..owner_threads).map(|_| Vec::new()).collect::<Vec<_>>();
                            for (id, vector) in chunk {
                                for (dim_id, weight) in
                                    vector.indices.into_iter().zip(vector.values)
                                {
                                    let dim_id = dim_id as usize;
                                    let owner = dim_id * owner_threads / dimension_count;
                                    buckets[owner].push(PostingRecord { id, dim_id, weight });
                                }
                            }
                            for (owner, batch) in buckets.into_iter().enumerate() {
                                if !batch.is_empty() {
                                    senders[owner]
                                        .send(batch)
                                        .expect("posting owner must outlive mapper workers");
                                }
                            }
                        });
                });

            owner_handles
                .into_iter()
                .map(|handle| handle.join().expect("posting owner worker must not panic"))
                .collect::<Vec<_>>()
        });

        let mut posting_builders = Vec::with_capacity(dimension_count);
        for mut owner_postings in owners {
            posting_builders.append(&mut owner_postings);
        }
        Self {
            posting_builders,
            vector_count,
            total_sparse_size,
        }
    }

    /// Consumes the builder and returns an InvertedIndexRam
    pub fn build(self) -> InvertedIndexRam {
        self.build_with_threads(1)
    }

    /// Finalize independent posting lists using at most `num_threads` workers.
    ///
    /// Indexed Rayon collection retains the dimension order, so this produces the same inverted
    /// index as [`Self::build`] while parallelizing the per-posting sort and WAND-bound pass.
    pub fn build_with_threads(self, num_threads: usize) -> InvertedIndexRam {
        if self.posting_builders.is_empty() {
            return InvertedIndexRam {
                postings: vec![],
                total_sparse_size: self.total_sparse_size,
                vector_count: self.vector_count,
                // The one-pass build always computes exact bounds; whether they are then
                // maintained across later writes is the caller's policy, set afterwards via
                // `set_maintain_max_next_weight`.
                maintain_max_next_weight: true,
            };
        }

        debug!(
            "building inverted index with {} sparse vectors in {} posting lists",
            self.vector_count,
            self.posting_builders.len(),
        );

        let threads = num_threads.clamp(1, self.posting_builders.len());
        let postings = if threads == 1 {
            self.posting_builders
                .into_iter()
                .map(PostingBuilder::build)
                .collect()
        } else {
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("a positive sparse-index thread count must create a Rayon pool")
                .install(|| {
                    self.posting_builders
                        .into_par_iter()
                        .map(PostingBuilder::build)
                        .collect()
                })
        };

        let vector_count = self.vector_count;
        let total_sparse_size = self.total_sparse_size;
        InvertedIndexRam {
            postings,
            vector_count,
            total_sparse_size,
            maintain_max_next_weight: true,
        }
    }

    /// Creates an [InvertedIndexRam] from an iterator of (id, vector) pairs.
    pub fn build_from_iterator(
        iter: impl Iterator<Item = (PointOffsetType, RemappedSparseVector)>,
    ) -> InvertedIndexRam {
        let mut builder = InvertedIndexBuilder::new();
        for (id, vector) in iter {
            builder.add(id, vector);
        }
        builder.build()
    }
}

struct PostingRecord {
    id: PointOffsetType,
    dim_id: usize,
    weight: f32,
}

struct RawPostingRecord {
    id: PointOffsetType,
    dim_id: DimId,
    weight: f32,
}

#[cfg(test)]
mod tests {
    use super::{InvertedIndexBuilder, StreamingInvertedIndexBuilder};
    use crate::common::sparse_vector::{RemappedSparseVector, SparseVector};

    #[test]
    fn streaming_full_batches_match_serial_and_buffered_compression() {
        use std::borrow::Cow;

        use common::universal_io::MmapFs;

        use crate::index::inverted_index::InvertedIndex;
        use crate::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;

        // Strided original IDs and a reversed dense mapping exercise routing independently of
        // dense-ID order. Every dimension crosses several full batches plus a partial batch.
        let dimensions = 65usize;
        let vectors = (0..3_077u32)
            .map(|id| {
                let indices = (0..dimensions as u32).map(|dim| dim * 32).collect();
                let values = (0..dimensions)
                    .map(|dim| ((id as usize + dim) % 101) as f32 / 7.0)
                    .collect();
                (id * 2, SparseVector { indices, values })
            })
            .collect::<Vec<_>>();
        let remap = |dim| Some(dimensions - 1 - (dim / 32) as usize);
        let remapped = vectors
            .iter()
            .map(|(id, vector)| {
                let mut v = RemappedSparseVector {
                    indices: vector
                        .indices
                        .iter()
                        .map(|&dim| remap(dim).unwrap() as u32)
                        .collect(),
                    values: vector.values.clone(),
                };
                v.sort_by_indices();
                (*id, v)
            })
            .collect::<Vec<_>>();
        let serial = InvertedIndexBuilder::build_from_iterator(remapped.clone().into_iter());
        let dir = tempfile::tempdir().unwrap();
        let compressed = InvertedIndexCompressedImmutableRam::<f32>::from_ram_index(
            &MmapFs,
            Cow::Borrowed(&serial),
            dir.path(),
        )
        .unwrap();
        for threads in [2, 4, 8, 16, 32] {
            let mut stream = StreamingInvertedIndexBuilder::new(threads);
            assert_eq!(stream.owner_handles.len() + 1, threads);
            for (id, vector) in &vectors {
                stream.add(*id, vector.clone());
            }
            let streamed = stream.finish(dimensions, remap).build_with_threads(threads);
            let buffered = InvertedIndexBuilder::from_vectors_with_threads(
                remapped.clone(),
                dimensions,
                threads,
            )
            .build_with_threads(threads);
            for actual in [&streamed, &buffered] {
                assert_eq!(actual, &serial, "threads={threads}");
                let actual = InvertedIndexCompressedImmutableRam::<f32>::from_ram_index_parallel(
                    &MmapFs,
                    Cow::Borrowed(actual),
                    dir.path(),
                    threads,
                )
                .unwrap();
                assert_eq!(actual, compressed, "threads={threads}");
            }
        }
    }

    #[test]
    fn streaming_strided_dimensions_use_all_owners() {
        for owners in [3, 7, 15, 31, 32] {
            let mut counts = vec![0usize; owners];
            for dim in 0..4096 {
                counts[super::posting_owner(dim * 32, owners)] += 1;
            }
            assert!(counts.iter().all(|&count| count > 0));
            assert!(*counts.iter().max().unwrap() < 2 * 4096 / owners);
        }
    }

    #[test]
    fn streaming_drop_joins_workers_on_error_and_unwind() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        for unwind in [false, true] {
            let completed = Arc::new(AtomicBool::new(false));
            let flag = completed.clone();
            let result = std::panic::catch_unwind(move || {
                let mut builder = StreamingInvertedIndexBuilder::new(2);
                // An extra sentinel receiver can exit only after Drop disconnects senders.
                let (sender, receiver) = std::sync::mpsc::sync_channel(2);
                builder.senders.push(sender);
                builder.owner_handles.push(std::thread::spawn(move || {
                    for _ in receiver {}
                    flag.store(true, Ordering::SeqCst);
                    std::collections::HashMap::new()
                }));
                if unwind {
                    panic!("simulate scan panic");
                }
                Err::<(), _>("simulate scan error")
            });
            assert!(completed.load(Ordering::SeqCst));
            assert_eq!(result.is_err(), unwind);
        }
    }

    #[test]
    fn streaming_builder_matches_remapped_builder() {
        let raw_vectors = vec![
            (
                0,
                SparseVector {
                    indices: vec![11, 99],
                    values: vec![1.0, 2.0],
                },
            ),
            (
                1,
                SparseVector {
                    indices: vec![7, 99],
                    values: vec![3.0, 4.0],
                },
            ),
            (
                2,
                SparseVector {
                    indices: vec![11],
                    values: vec![5.0],
                },
            ),
        ];
        let remap = |dim_id| match dim_id {
            7 => Some(0usize),
            11 => Some(1),
            99 => Some(2),
            _ => None,
        };

        let remapped_vectors = raw_vectors
            .iter()
            .map(|(id, vector)| {
                let mut remapped = RemappedSparseVector {
                    indices: vector
                        .indices
                        .iter()
                        .map(|&dim_id| remap(dim_id).unwrap() as u32)
                        .collect(),
                    values: vector.values.clone(),
                };
                remapped.sort_by_indices();
                (*id, remapped)
            })
            .collect::<Vec<_>>();

        let mut serial_builder = InvertedIndexBuilder::new();
        for (id, vector) in remapped_vectors.clone() {
            serial_builder.add(id, vector);
        }
        let serial = serial_builder.build();
        let buffered_parallel =
            InvertedIndexBuilder::from_vectors_with_threads(remapped_vectors, 3, 4)
                .build_with_threads(4);

        let mut streaming = StreamingInvertedIndexBuilder::new(4);
        for (id, vector) in raw_vectors {
            streaming.add(id, vector);
        }
        let streaming = streaming.finish(3, remap).build_with_threads(4);

        assert_eq!(buffered_parallel, serial);
        assert_eq!(streaming, serial);
    }
}
