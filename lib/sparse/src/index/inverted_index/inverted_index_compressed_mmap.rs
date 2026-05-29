use std::borrow::Cow;
use std::fmt::Debug;
use std::io::{BufWriter, Write as _};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::ext::aligned_vec::ACow;
use common::fs::{atomic_save_json, read_json};
use common::generic_consts::Random;
use common::mmap::{Advice, AdviceSetting, create_and_ensure_length};
#[expect(deprecated, reason = "legacy code")]
use common::mmap::{transmute_to_u8, transmute_to_u8_slice};
use common::storage_version::StorageVersion;
use common::types::PointOffsetType;
use common::universal_io::{OpenOptions, Populate, Result, UniversalRead, UniversalReadFs};
use serde::{Deserialize, Serialize};
use zerocopy::{FromBytes, Immutable, KnownLayout};

use super::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use super::{INDEX_FILE_NAME, corrupted_index, out_of_bounds};
use crate::SearchScratchArena;
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::{DimId, DimOffset, Weight};
use crate::index::compressed_posting_list::{
    CHUNK_SIZE, CompressedPostingChunk, CompressedPostingListIterator, CompressedPostingListView,
};
use crate::index::inverted_index::InvertedIndex;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::posting_list_common::GenericPostingElement;

const INDEX_CONFIG_FILE_NAME: &str = "inverted_index_config.json";

pub struct Version;

impl StorageVersion for Version {
    fn current_raw() -> &'static str {
        "0.2.0"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InvertedIndexFileHeader {
    /// Number of posting lists
    pub posting_count: usize,
    /// Number of unique vectors indexed
    pub vector_count: usize,
    /// Total size of all searchable sparse vectors in bytes
    // This is an option because earlier versions of the index did not store this information.
    // In case it is not present, it will be calculated on load.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_sparse_size: Option<usize>,
}

/// Inverted flatten index from dimension id to posting list
#[derive(Debug)]
pub struct InvertedIndexCompressedMmap<W, S: UniversalRead> {
    path: PathBuf,
    storage: S,
    pub file_header: InvertedIndexFileHeader,
    _phantom: PhantomData<W>,
}

#[derive(Debug, Default, Copy, Clone, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
struct PostingListFileHeader<W: Weight> {
    pub ids_start: u64,
    pub last_id: u32,
    /// Possible values: 0, 4, 8, ..., 512.
    /// Step = 4 = `BLOCK_LEN / u32::BITS` = `128 / 32`.
    /// Max = 512 = `BLOCK_LEN * size_of::<u32>()` = `128 * 4`.
    pub ids_len: u32,
    pub chunks_count: u32,
    pub quantization_params: W::QuantizationParams,
}

impl<W: Weight> PostingListFileHeader<W> {
    fn postings_count(&self, remainders_end: u64) -> Option<usize> {
        let chunks_bytes = self.chunks_count as usize * size_of::<CompressedPostingChunk<W>>();
        let data_len = remainders_end.checked_sub(self.ids_start)? as usize;
        let remainders_bytes = data_len.checked_sub(self.ids_len as usize + chunks_bytes)?;
        let elem_size = size_of::<GenericPostingElement<W>>();
        if !remainders_bytes.is_multiple_of(elem_size) {
            return None;
        }
        let remainders_count = remainders_bytes / elem_size;
        Some(self.chunks_count as usize * CHUNK_SIZE + remainders_count)
    }
}

impl<W: Weight, S: UniversalRead + 'static> InvertedIndex for InvertedIndexCompressedMmap<W, S>
where
    S::Fs: Default,
{
    type Iter<'a> = CompressedPostingListIterator<'a, W>;

    type Version = Version;

    fn is_on_disk(&self) -> bool {
        true
    }

    fn open(path: &Path) -> Result<Self> {
        Self::load(&S::Fs::default(), path)
    }

    fn save(&self, path: &Path) -> Result<()> {
        debug_assert_eq!(path, self.path);

        // If Self instance exists, it's either constructed by using `open()` (which reads index
        // files), or using `from_ram_index()` (which writes them). Both assume that the files
        // exist. If any of the files are missing, then something went wrong.
        for file in Self::files(path) {
            debug_assert!(file.exists());
        }

        Ok(())
    }

    fn get<'a>(
        &'a self,
        id: DimOffset,
        arena: &'a SearchScratchArena,
        hw_counter: &'a HardwareCounterCell,
    ) -> Result<CompressedPostingListIterator<'a, W>> {
        Ok(self.get(id, arena, hw_counter)?.iter())
    }

    fn len(&self) -> usize {
        self.file_header.posting_count
    }

    fn posting_list_len(&self, id: DimOffset, hw_counter: &HardwareCounterCell) -> Result<usize> {
        let (header, remainders_end) = self.read_posting_header(id, hw_counter)?;
        header
            .postings_count(remainders_end)
            .ok_or_else(corrupted_index)
    }

    fn files(path: &Path) -> Vec<PathBuf> {
        vec![
            Self::index_file_path(path),
            Self::index_config_file_path(path),
        ]
    }

    fn immutable_files(path: &Path) -> Vec<PathBuf> {
        // `InvertedIndexCompressedMmap` is always immutable
        Self::files(path)
    }

    fn remove(&mut self, _id: PointOffsetType, _old_vector: RemappedSparseVector) {
        panic!("Cannot remove from a read-only Mmap inverted index")
    }

    fn upsert(
        &mut self,
        _id: PointOffsetType,
        _vector: RemappedSparseVector,
        _old_vector: Option<RemappedSparseVector>,
    ) {
        panic!("Cannot upsert into a read-only Mmap inverted index")
    }

    fn from_ram_index<P: AsRef<Path>>(ram_index: Cow<InvertedIndexRam>, path: P) -> Result<Self> {
        let index = InvertedIndexCompressedImmutableRam::from_ram_index(ram_index, &path)?;
        Self::convert_and_save(&S::Fs::default(), &index, path)
    }

    fn vector_count(&self) -> usize {
        self.file_header.vector_count
    }

    fn total_sparse_vectors_size(&self) -> usize {
        debug_assert!(
            self.file_header.total_sparse_size.is_some(),
            "The field should be populated from the file, or on load"
        );
        self.file_header.total_sparse_size.unwrap_or(0)
    }

    fn max_index(&self) -> Option<DimId> {
        match self.file_header.posting_count {
            0 => None,
            len => Some(len as DimId - 1),
        }
    }
}

impl<W: Weight, S: UniversalRead + Debug + 'static> InvertedIndexCompressedMmap<W, S> {
    const HEADER_SIZE: usize = size_of::<PostingListFileHeader<W>>();

    pub fn index_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_FILE_NAME)
    }

    pub fn index_config_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_CONFIG_FILE_NAME)
    }

    pub fn get<'a>(
        &'a self,
        id: DimId,
        arena: &'a SearchScratchArena,
        hw_counter: &'a HardwareCounterCell,
    ) -> Result<CompressedPostingListView<'a, W>> {
        let (header, remainders_end) = self.read_posting_header(id, hw_counter)?;

        let data_bytes = self.storage.read_bytes::<Random>(
            header.ids_start..remainders_end,
            align_of::<(CompressedPostingChunk<W>, GenericPostingElement<W>)>(),
        )?;
        let data = match data_bytes {
            ACow::Borrowed(b) => b,
            ACow::Owned(avec) => arena.alloc(avec),
        };

        let ids_len = header.ids_len as usize;
        let chunks_bytes = header.chunks_count as usize * size_of::<CompressedPostingChunk<W>>();
        let id_data = data.get(..ids_len).ok_or_else(corrupted_index)?;
        let chunks = <[CompressedPostingChunk<W>]>::ref_from_bytes(
            data.get(ids_len..ids_len + chunks_bytes)
                .ok_or_else(corrupted_index)?,
        )
        .map_err(|_| corrupted_index())?;
        let remainders = <[GenericPostingElement<W>]>::ref_from_bytes(
            data.get(ids_len + chunks_bytes..)
                .ok_or_else(corrupted_index)?,
        )
        .map_err(|_| corrupted_index())?;

        Ok(CompressedPostingListView::new(
            id_data,
            chunks,
            remainders,
            header.last_id.checked_sub(1),
            header.quantization_params,
            hw_counter,
        ))
    }

    /// Read the header for `id`, plus the next header's `ids_start`
    /// (which doubles as this entry's remainders end).
    fn read_posting_header(
        &self,
        id: DimId,
        hw_counter: &HardwareCounterCell,
    ) -> Result<(PostingListFileHeader<W>, u64)> {
        if id >= self.file_header.posting_count as DimId {
            return Err(out_of_bounds(id, self.file_header.posting_count));
        }

        let header_start = u64::from(id) * Self::HEADER_SIZE as u64;
        let has_next = id + 1 < self.file_header.posting_count as DimId;
        let read_size = Self::HEADER_SIZE + if has_next { size_of::<u64>() } else { 0 };
        let header_bytes = self.storage.read_bytes::<Random>(
            header_start..header_start + read_size as u64,
            align_of::<PostingListFileHeader<W>>(),
        )?;
        let (&header, rest) = PostingListFileHeader::<W>::ref_from_prefix(&header_bytes)
            .map_err(|_| corrupted_index())?;
        let remainders_end = if has_next {
            *u64::ref_from_bytes(rest).map_err(|_| corrupted_index())?
        } else {
            self.storage.len::<u8>()?
        };

        hw_counter.vector_io_read().incr_delta(read_size);

        Ok((header, remainders_end))
    }

    pub fn convert_and_save<P: AsRef<Path>>(
        fs: &S::Fs,
        index: &InvertedIndexCompressedImmutableRam<W>,
        path: P,
    ) -> Result<Self> {
        let total_posting_headers_size =
            index.postings.as_slice().len() * size_of::<PostingListFileHeader<W>>();

        // Ignore HW on load
        let hw_counter = HardwareCounterCell::disposable();

        let file_length = total_posting_headers_size
            + index
                .postings
                .as_slice()
                .iter()
                .map(|p| p.view(&hw_counter).store_size().total)
                .sum::<usize>();
        let file_path = Self::index_file_path(path.as_ref());
        // TODO(uio): use UIO for writing as well.
        let file = create_and_ensure_length(file_path.as_ref(), file_length)?;

        let mut buf = BufWriter::new(file);

        // Save posting headers
        let mut offset: usize = total_posting_headers_size;
        for posting in index.postings.as_slice() {
            let store_size = posting.view(&hw_counter).store_size();
            let posting_header = PostingListFileHeader::<W> {
                ids_start: offset as u64,
                ids_len: store_size.id_data_bytes as u32,
                chunks_count: store_size.chunks_count as u32,
                last_id: posting.view(&hw_counter).last_id().map_or(0, |id| id + 1),
                quantization_params: posting.view(&hw_counter).multiplier(),
            };
            // TODO Safety
            #[expect(deprecated, reason = "legacy code")]
            buf.write_all(unsafe { transmute_to_u8(&posting_header) })?;
            offset += store_size.total;
        }

        // Save posting elements
        for posting in index.postings.as_slice() {
            let posting_view = posting.view(&hw_counter);
            let (id_data, chunks, remainders) = posting_view.parts();
            buf.write_all(id_data)?;
            // TODO Safety
            #[expect(deprecated, reason = "legacy code")]
            buf.write_all(unsafe { transmute_to_u8_slice(chunks) })?;
            // TODO Safety
            #[expect(deprecated, reason = "legacy code")]
            buf.write_all(unsafe { transmute_to_u8_slice(remainders) })?;
        }

        // Explicitly fsync file contents to ensure durability
        buf.flush()?;
        let file = buf.into_inner().unwrap();
        file.sync_all()?;

        // save header properties
        let file_header = InvertedIndexFileHeader {
            posting_count: index.postings.as_slice().len(),
            vector_count: index.vector_count,
            total_sparse_size: Some(index.total_sparse_size),
        };

        atomic_save_json(&Self::index_config_file_path(path.as_ref()), &file_header)?;

        let storage = fs.open(
            &file_path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;

        Ok(Self {
            path: path.as_ref().to_owned(),
            storage,
            file_header,
            _phantom: PhantomData,
        })
    }

    pub fn load<P: AsRef<Path>>(fs: &S::Fs, path: P) -> Result<Self> {
        // read index config file
        let config_file_path = Self::index_config_file_path(path.as_ref());
        // if the file header does not exist, the index is malformed
        let file_header: InvertedIndexFileHeader = read_json(&config_file_path)?;
        // open index data via universal IO
        let file_path = Self::index_file_path(path.as_ref());
        let storage = fs.open(
            &file_path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Advice(Advice::Normal),
            },
            Default::default(),
        )?;

        let mut index = Self {
            path: path.as_ref().to_owned(),
            storage,
            file_header,
            _phantom: PhantomData,
        };

        let hw_counter = HardwareCounterCell::disposable();

        if index.file_header.total_sparse_size.is_none() {
            index.file_header.total_sparse_size =
                Some(index.calculate_total_sparse_size(&hw_counter)?);
            atomic_save_json(&config_file_path, &index.file_header)?;
        }

        Ok(index)
    }

    fn calculate_total_sparse_size(&self, hw_counter: &HardwareCounterCell) -> Result<usize> {
        let mut total = 0;
        let mut arena = SearchScratchArena::new_slow();
        for id in 0..self.file_header.posting_count as DimId {
            total += self.get(id, &arena, hw_counter)?.store_size().total;
            arena.gc();
        }
        Ok(total)
    }

    /// Populate the underlying storage in RAM cache. Block until completed.
    pub fn populate(&self) -> Result<()> {
        self.storage.populate()
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> Result<()> {
        self.storage.clear_ram_cache()
    }
}

#[cfg(test)]
mod tests {
    use common::universal_io::MmapFile;
    use tempfile::Builder;

    use super::*;
    use crate::common::types::QuantizedU8;
    use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

    fn compare_indexes<S: UniversalRead + 'static, W: Weight>(
        inverted_index_ram: &InvertedIndexCompressedImmutableRam<W>,
        inverted_index_mmap: &InvertedIndexCompressedMmap<W, S>,
    ) {
        let hw_counter = HardwareCounterCell::new();
        let arena = SearchScratchArena::new_slow();
        for id in 0..inverted_index_ram.postings.len() as DimId {
            let posting_list_ram = inverted_index_ram
                .postings
                .get(id as usize)
                .unwrap()
                .view(&hw_counter);
            let posting_list_mmap = inverted_index_mmap.get(id, &arena, &hw_counter).unwrap();

            let mmap_parts = posting_list_mmap.parts();
            let ram_parts = posting_list_ram.parts();

            assert_eq!(mmap_parts, ram_parts);
        }
    }

    #[test]
    fn test_inverted_index_mmap() {
        check_inverted_index_mmap::<MmapFile, f32>();
        check_inverted_index_mmap::<MmapFile, half::f16>();
        check_inverted_index_mmap::<MmapFile, u8>();
        check_inverted_index_mmap::<MmapFile, QuantizedU8>();

        #[cfg(target_os = "linux")]
        {
            use common::universal_io::IoUringFile;
            check_inverted_index_mmap::<IoUringFile, f32>();
            check_inverted_index_mmap::<IoUringFile, half::f16>();
            check_inverted_index_mmap::<IoUringFile, u8>();
            check_inverted_index_mmap::<IoUringFile, QuantizedU8>();
        }
    }

    fn check_inverted_index_mmap<S: UniversalRead + 'static, W: Weight>()
    where
        S::Fs: Default,
    {
        let hw_counter = HardwareCounterCell::new();

        // skip 4th dimension
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0), (5, 10.0)].into());
        builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0), (5, 20.0)].into());
        builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
        builder.add(4, [(1, 1.0), (2, 1.0)].into());
        builder.add(5, [(1, 2.0)].into());
        builder.add(6, [(1, 3.0)].into());
        builder.add(7, [(1, 4.0)].into());
        builder.add(8, [(1, 5.0)].into());
        builder.add(9, [(1, 6.0)].into());
        let inverted_index_ram = builder.build();
        let tmp_dir_path = Builder::new().prefix("test_index_dir1").tempdir().unwrap();
        let inverted_index_ram = InvertedIndexCompressedImmutableRam::from_ram_index(
            Cow::Borrowed(&inverted_index_ram),
            &tmp_dir_path,
        )
        .unwrap();

        let tmp_dir_path = Builder::new().prefix("test_index_dir2").tempdir().unwrap();

        {
            let inverted_index_mmap = InvertedIndexCompressedMmap::<W, S>::convert_and_save(
                &S::Fs::default(),
                &inverted_index_ram,
                &tmp_dir_path,
            )
            .unwrap();

            compare_indexes(&inverted_index_ram, &inverted_index_mmap);
        }
        let index =
            InvertedIndexCompressedMmap::<W, MmapFile>::load(&Default::default(), &tmp_dir_path)
                .unwrap();
        // posting_count: 0th entry is always empty + 1st + 2nd + 3rd + 4th empty + 5th
        assert_eq!(index.file_header.posting_count, 6);
        assert_eq!(index.file_header.vector_count, 9);

        compare_indexes(&inverted_index_ram, &index);

        let arena = SearchScratchArena::new_slow();
        assert!(index.get(0, &arena, &hw_counter).unwrap().is_empty()); // the first entry is always empty as dimension ids start at 1
        assert_eq!(index.get(1, &arena, &hw_counter).unwrap().len(), 9);
        assert_eq!(index.get(2, &arena, &hw_counter).unwrap().len(), 4);
        assert_eq!(index.get(3, &arena, &hw_counter).unwrap().len(), 3);
        assert!(index.get(4, &arena, &hw_counter).unwrap().is_empty()); // return empty posting list info for intermediary empty ids
        assert_eq!(index.get(5, &arena, &hw_counter).unwrap().len(), 2);
        // index after the last values are errors (out of bounds)
        assert!(index.get(6, &arena, &hw_counter).is_err());
        assert!(index.get(7, &arena, &hw_counter).is_err());
        assert!(index.get(100, &arena, &hw_counter).is_err());
    }
}
