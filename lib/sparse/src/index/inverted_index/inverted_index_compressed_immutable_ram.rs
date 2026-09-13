use std::borrow::Cow;
use std::path::Path;
use std::time::Instant;

use blink_alloc::Blink;
use common::counter::hardware_counter::HardwareCounterCell;
use common::ext::VecExt;
use common::types::PointOffsetType;
use common::universal_io::{
    MmapFs, UioResult, UniversalRead, UniversalReadFs, UniversalWrite, UserData,
};
use rayon::prelude::*;

use super::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use super::inverted_index_ram::InvertedIndexRam;
use super::{InvertedIndex, out_of_bounds};
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::{DimOffset, Weight};
use crate::index::compressed_posting_list::{
    CompressedPostingBuilder, CompressedPostingList, CompressedPostingListIterator,
    CompressedPostingListView,
};
use crate::index::inverted_index::inverted_index_compressed_mmap::Version;
use crate::index::inverted_index::{InvertedIndexReadOnly, InvertedIndexReadWrite};
use crate::index::posting_list::PostingList;

#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexCompressedImmutableRam<W: Weight> {
    pub(super) postings: Vec<CompressedPostingList<W>>,
    pub(super) vector_count: usize,
    pub(super) total_sparse_size: usize,
}

type Storage = common::universal_io::MmapFile;

impl<W: Weight, S: UniversalRead + 'static> InvertedIndexReadOnly<S>
    for InvertedIndexCompressedImmutableRam<W>
{
    fn open_ro_impl<Fs: UniversalReadFs<File = S>>(fs: &Fs, path: &Path) -> UioResult<Self> {
        let mmap_inverted_index = InvertedIndexCompressedMmap::<W, S>::open_ro(fs, path)?;
        Self::from_mmap_index(mmap_inverted_index)
    }
}

impl<W: Weight, S: UniversalWrite + 'static> InvertedIndexReadWrite<S>
    for InvertedIndexCompressedImmutableRam<W>
{
    fn open_rw_impl(fs: &<S as UniversalRead>::Fs, path: &Path) -> UioResult<Self> {
        let mmap_inverted_index = InvertedIndexCompressedMmap::<W, S>::open_rw(fs, path)?;
        Self::from_mmap_index(mmap_inverted_index)
    }

    fn from_ram_index_impl<P: AsRef<Path>>(
        _fs: &S::Fs,
        ram_index: Cow<InvertedIndexRam>,
        _path: P,
        num_threads: usize,
    ) -> UioResult<Self> {
        let compression_started = Instant::now();
        let threads = num_threads.clamp(1, ram_index.postings.len().max(1));
        let postings: Vec<CompressedPostingList<W>> = if threads == 1 {
            ram_index
                .postings
                .iter()
                .map(compress_posting::<W>)
                .collect()
        } else {
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("a positive sparse-index thread count must create a Rayon pool")
                .install(|| {
                    ram_index
                        .postings
                        .par_iter()
                        .map(compress_posting::<W>)
                        .collect()
                })
        };

        let hw_counter = HardwareCounterCell::disposable();

        let total_sparse_size = postings
            .iter()
            .map(|p| p.view(&hw_counter).store_size().total)
            .sum();

        log::info!(
            "sparse index build: compressed {} posting list(s) in {:.1?}",
            postings.len(),
            compression_started.elapsed(),
        );

        Ok(InvertedIndexCompressedImmutableRam {
            postings,
            vector_count: ram_index.vector_count,
            total_sparse_size,
        })
    }
}

/// Compress one independent posting list. The caller preserves the outer dimension order.
fn compress_posting<W: Weight>(old_posting_list: &PostingList) -> CompressedPostingList<W> {
    let mut new_posting_list = CompressedPostingBuilder::new();
    for elem in &old_posting_list.elements {
        new_posting_list.add(elem.record_id, elem.weight);
    }
    new_posting_list.build()
}

impl<W: Weight> InvertedIndex for InvertedIndexCompressedImmutableRam<W> {
    type Iter<'a> = CompressedPostingListIterator<'a, W>;

    type Version = Version;

    fn is_on_disk(&self) -> bool {
        false
    }

    fn save(&self, path: &Path) -> UioResult<()> {
        InvertedIndexCompressedMmap::<W, Storage>::convert_and_save(&MmapFs, self, path)?;
        Ok(())
    }

    fn get_batch<'a, U: UserData>(
        &'a self,
        ids: impl Iterator<Item = (U, DimOffset)>,
        _arena: &'a Blink,
        hw_counter: &'a HardwareCounterCell, // Ignored for in-ram index
        mut callback: impl FnMut(U, Self::Iter<'a>) -> UioResult<()>,
    ) -> UioResult<()> {
        for (user_data, id) in ids {
            callback(user_data, self.get(id, hw_counter)?.iter())?;
        }
        Ok(())
    }

    fn len(&self) -> usize {
        self.postings.len()
    }

    fn posting_list_len_batch<U: UserData>(
        &self,
        ids: impl Iterator<Item = (U, DimOffset)>,
        hw_counter: &HardwareCounterCell,
        mut callback: impl FnMut(U, usize) -> UioResult<()>,
    ) -> UioResult<()> {
        for (user_data, id) in ids {
            callback(user_data, self.get(id, hw_counter)?.len())?;
        }
        Ok(())
    }

    fn files(path: &Path) -> Vec<std::path::PathBuf> {
        InvertedIndexCompressedMmap::<W, Storage>::files(path)
    }

    fn immutable_files(path: &Path) -> Vec<std::path::PathBuf> {
        // `InvertedIndexCompressedImmutableRam` is always immutable
        InvertedIndexCompressedMmap::<W, Storage>::immutable_files(path)
    }

    fn remove(&mut self, _id: PointOffsetType, _old_vector: RemappedSparseVector) {
        panic!("Cannot remove from a read-only RAM inverted index")
    }

    fn upsert(
        &mut self,
        _id: PointOffsetType,
        _vector: RemappedSparseVector,
        _old_vector: Option<RemappedSparseVector>,
    ) {
        panic!("Cannot upsert into a read-only RAM inverted index")
    }

    fn vector_count(&self) -> usize {
        self.vector_count
    }

    fn total_sparse_vectors_size(&self) -> usize {
        self.total_sparse_size
    }

    fn max_index(&self) -> Option<DimOffset> {
        self.postings
            .len()
            .checked_sub(1)
            .map(|len| len as DimOffset)
    }
}

impl<W: Weight> InvertedIndexCompressedImmutableRam<W> {
    /// Materialize an mmap-layout index into owned in-RAM postings.
    fn from_mmap_index<S>(mmap_inverted_index: InvertedIndexCompressedMmap<W, S>) -> UioResult<Self>
    where
        S: UniversalRead + 'static,
    {
        let hw_counter = HardwareCounterCell::disposable();
        let mut postings = vec![None; mmap_inverted_index.file_header.posting_count];
        mmap_inverted_index.for_each_view(&hw_counter, |id, view| {
            postings[id as usize] = Some(view.to_owned());
            Ok(())
        })?;

        mmap_inverted_index.clear_cache()?;

        Ok(InvertedIndexCompressedImmutableRam {
            postings: postings.transform_in_place(Option::unwrap),
            vector_count: mmap_inverted_index.file_header.vector_count,
            // populated by `load`/`load_universal` when missing from a legacy header
            total_sparse_size: mmap_inverted_index
                .file_header
                .total_sparse_size
                .unwrap_or(0),
        })
    }

    #[inline]
    fn get<'a>(
        &'a self,
        id: DimOffset,
        hw_counter: &'a HardwareCounterCell,
    ) -> UioResult<CompressedPostingListView<'a, W>> {
        let Some(posting) = self.postings.get(id as usize) else {
            return Err(out_of_bounds(id, self.len()));
        };
        Ok(posting.view(hw_counter))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use common::universal_io::MmapFile;
    use rand::SeedableRng as _;
    use rand::rngs::SmallRng;
    use tempfile::Builder;

    use super::*;
    use crate::common::sparse_vector_fixture::random_sparse_vector;
    use crate::common::types::QuantizedU8;
    use crate::index::inverted_index::INDEX_FILE_NAME;
    use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

    #[test]
    fn test_save_load_tiny() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, vec![(1, 10.0), (2, 10.0), (3, 10.0)].try_into().unwrap());
        builder.add(2, vec![(1, 20.0), (2, 20.0), (3, 20.0)].try_into().unwrap());
        builder.add(3, vec![(1, 30.0), (2, 30.0), (3, 30.0)].try_into().unwrap());
        let inverted_index_ram = builder.build();

        check_save_load::<f32>(&inverted_index_ram);
        check_save_load::<half::f16>(&inverted_index_ram);
        check_save_load::<u8>(&inverted_index_ram);
        check_save_load::<QuantizedU8>(&inverted_index_ram);
    }

    #[test]
    fn test_save_load_large() {
        let mut rnd_gen = rand::rng();
        let mut builder = InvertedIndexBuilder::new();
        // Enough elements to put some of them into chunks
        for i in 0..1024 {
            builder.add(i, random_sparse_vector(&mut rnd_gen, 3).into_remapped());
        }
        let inverted_index_ram = builder.build();

        check_save_load::<f32>(&inverted_index_ram);
        check_save_load::<half::f16>(&inverted_index_ram);
        check_save_load::<u8>(&inverted_index_ram);
        check_save_load::<QuantizedU8>(&inverted_index_ram);
    }

    #[test]
    fn parallel_finalization_and_compression_match_single_threaded_output() {
        let build_ram_index = |num_threads| {
            let vectors = (0..1_024)
                .map(|point_id| {
                    // Each worker's contiguous range reaches a different maximum dimension.
                    // This catches merge implementations that accidentally shrink a previously
                    // merged posting-builder vector.
                    let dim_id = match point_id / 256 {
                        0 => 1_024,
                        1 => 64,
                        2 => 8,
                        _ => 1,
                    };
                    (
                        point_id,
                        vec![(dim_id, point_id as f32)].try_into().unwrap(),
                    )
                })
                .collect();
            let builder =
                InvertedIndexBuilder::from_vectors_with_threads(vectors, 1_025, num_threads);
            builder.build_with_threads(num_threads)
        };

        let single_threaded = build_ram_index(1);
        let parallel = build_ram_index(4);
        assert_eq!(single_threaded, parallel);

        let output_dir = Builder::new()
            .prefix("sparse-parallel-output")
            .tempdir()
            .unwrap();
        let single_threaded = InvertedIndexCompressedImmutableRam::<f32>::from_ram_index(
            &MmapFs,
            Cow::Borrowed(&single_threaded),
            output_dir.path(),
        )
        .unwrap();
        let parallel = InvertedIndexCompressedImmutableRam::<f32>::from_ram_index_parallel(
            &MmapFs,
            Cow::Borrowed(&parallel),
            output_dir.path(),
            4,
        )
        .unwrap();
        assert_eq!(single_threaded, parallel);
    }

    /// Local sizing/profile aid for the shard builder's immutable sparse-index path.
    ///
    /// Run with:
    /// `cargo test -p sparse --release profile_sparse_index_build_scaling -- --ignored --nocapture`
    ///
    /// This deliberately starts with `(point id, sparse vector)` pairs, after the segment layer
    /// has read storage and remapped dimensions. It therefore profiles the one-pass posting
    /// build, compression, and mmap persistence — the phases relevant to deciding whether
    /// posting-list parallelism is worthwhile — but not storage scan/remapping.
    #[test]
    #[ignore = "local performance profile; run explicitly in release mode"]
    fn profile_sparse_index_build_scaling() {
        const MAX_DIMENSION: usize = 10_000;

        for vector_count in [25_000usize, 100_000, 200_000, 300_000] {
            let mut rng = SmallRng::seed_from_u64(0x5A17_5EED);

            let generate_started = Instant::now();
            let vectors = (0..vector_count)
                .map(|_| random_sparse_vector(&mut rng, MAX_DIMENSION).into_remapped())
                .collect::<Vec<_>>();
            let generate_elapsed = generate_started.elapsed();

            let build = |num_threads| {
                let worker_vectors = vectors
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(point_id, vector)| (point_id as u32, vector))
                    .collect();
                let accumulate_started = Instant::now();
                let builder = InvertedIndexBuilder::from_vectors_with_threads(
                    worker_vectors,
                    MAX_DIMENSION,
                    num_threads,
                );
                let accumulate_elapsed = accumulate_started.elapsed();

                let finalize_started = Instant::now();
                let ram_index = builder.build_with_threads(num_threads);
                (ram_index, accumulate_elapsed, finalize_started.elapsed())
            };

            let (single_threaded_ram, single_accumulate, single_finalize) = build(1);
            let compress_started = Instant::now();
            let single_threaded = InvertedIndexCompressedImmutableRam::<f32>::from_ram_index(
                &MmapFs,
                Cow::Borrowed(&single_threaded_ram),
                Builder::new()
                    .prefix("sparse-profile-ram")
                    .tempdir()
                    .unwrap()
                    .path(),
            )
            .unwrap();
            let single_compress = compress_started.elapsed();
            drop(single_threaded);
            drop(single_threaded_ram);

            let (ram_index, parallel_accumulate, parallel_finalize) = build(4);
            let compress_started = Instant::now();
            let immutable = InvertedIndexCompressedImmutableRam::<f32>::from_ram_index_parallel(
                &MmapFs,
                Cow::Borrowed(&ram_index),
                Builder::new()
                    .prefix("sparse-profile-ram")
                    .tempdir()
                    .unwrap()
                    .path(),
                4,
            )
            .unwrap();
            let parallel_compress = compress_started.elapsed();

            drop(immutable);
            drop(ram_index);

            let (ram_index, parallel_16_accumulate, parallel_16_finalize) = build(16);
            let compress_started = Instant::now();
            let immutable = InvertedIndexCompressedImmutableRam::<f32>::from_ram_index_parallel(
                &MmapFs,
                Cow::Borrowed(&ram_index),
                Builder::new()
                    .prefix("sparse-profile-ram")
                    .tempdir()
                    .unwrap()
                    .path(),
                16,
            )
            .unwrap();
            let parallel_16_compress = compress_started.elapsed();

            let output = Builder::new()
                .prefix("sparse-profile-mmap")
                .tempdir()
                .unwrap();
            let write_started = Instant::now();
            InvertedIndexCompressedMmap::<f32, MmapFile>::convert_and_save(
                &MmapFs,
                &immutable,
                output.path(),
            )
            .unwrap();
            let write_elapsed = write_started.elapsed();
            let bytes = fs_err::metadata(output.path().join(INDEX_FILE_NAME))
                .unwrap()
                .len();

            eprintln!(
                "sparse profile: vectors={vector_count}, postings={}, bytes={bytes}, \
                 generate={generate_elapsed:.3?}, \
                 single(accumulate={single_accumulate:.3?}, finalize={single_finalize:.3?}, compress={single_compress:.3?}), \
                 parallel-4(accumulate={parallel_accumulate:.3?}, finalize={parallel_finalize:.3?}, compress={parallel_compress:.3?}), \
                 parallel-16(accumulate={parallel_16_accumulate:.3?}, finalize={parallel_16_finalize:.3?}, compress={parallel_16_compress:.3?}), \
                 write={write_elapsed:.3?}",
                ram_index.postings.len(),
            );
        }
    }

    fn check_save_load<W: Weight>(inverted_index_ram: &InvertedIndexRam) {
        let tmp_dir_path = Builder::new().prefix("test_index_dir").tempdir().unwrap();
        let inverted_index_immutable_ram =
            InvertedIndexCompressedImmutableRam::<W>::from_ram_index(
                &MmapFs,
                Cow::Borrowed(inverted_index_ram),
                tmp_dir_path.path(),
            )
            .unwrap();
        inverted_index_immutable_ram
            .save(tmp_dir_path.path())
            .unwrap();

        let loaded_inverted_index =
            InvertedIndexCompressedImmutableRam::<W>::open_ro(&MmapFs, tmp_dir_path.path())
                .unwrap();
        assert_eq!(inverted_index_immutable_ram, loaded_inverted_index);
    }
}
