use std::borrow::Cow;
use std::io::{BufWriter, Write as _};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::types::PointOffsetType;
use io::file_operations::{atomic_save_json, read_json};
use io::storage_version::StorageVersion;
use memmap2::Mmap;
use memory::madvise::{Advice, AdviceSetting};
use memory::mmap_ops::{
    create_and_ensure_length, open_read_mmap, transmute_from_u8_to_slice, transmute_to_u8,
    transmute_to_u8_slice,
};
use serde::{Deserialize, Serialize};

use super::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use super::INDEX_FILE_NAME;
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::{DimId, DimOffset, Weight};
use crate::index::compressed_posting_list::{
    CompressedPostingChunk, CompressedPostingListIterator, CompressedPostingListView,
};
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::inverted_index::InvertedIndex;
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
    pub posting_count: usize, // number of posting lists
    pub vector_count: usize,  // number of unique vectors indexed
}

/// Inverted flatten index from dimension id to posting list
#[derive(Debug)]
pub struct InvertedIndexCompressedMmap<W> {
    path: PathBuf,
    mmap: Arc<Mmap>,
    pub file_header: InvertedIndexFileHeader,
    _phantom: PhantomData<W>,
}

#[derive(Debug, Default, Clone)]
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

impl<W: Weight> InvertedIndex for InvertedIndexCompressedMmap<W> {
    type Iter<'a> = CompressedPostingListIterator<'a, W>;

    type Version = Version;

    fn open(path: &Path) -> std::io::Result<Self> {
        Self::load(path)
    }

    fn save(&self, path: &Path) -> std::io::Result<()> {
        debug_assert_eq!(path, self.path);

        // If Self instance exists, it's either constructed by using `open()` (which reads index
        // files), or using `from_ram_index()` (which writes them). Both assume that the files
        // exist. If any of the files are missing, then something went wrong.
        for file in Self::files(path) {
            debug_assert!(file.exists());
        }

        Ok(())
    }

    fn get<'a>(&'a self, id: &DimId) -> Option<CompressedPostingListIterator<'a, W>> {
        self.get(id).map(|posting_list| posting_list.iter())
    }

    fn len(&self) -> usize {
        self.file_header.posting_count
    }

    fn posting_list_len(&self, id: &DimOffset) -> Option<usize> {
        self.get(id).map(|posting_list| posting_list.len())
    }

    fn files(path: &Path) -> Vec<PathBuf> {
        vec![
            Self::index_file_path(path),
            Self::index_config_file_path(path),
        ]
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

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        path: P,
    ) -> std::io::Result<Self> {
        let index = InvertedIndexCompressedImmutableRam::from_ram_index(ram_index, &path)?;
        Self::convert_and_save(&index, path)
    }

    fn vector_count(&self) -> usize {
        self.file_header.vector_count
    }

    fn max_index(&self) -> Option<DimId> {
        match self.file_header.posting_count {
            0 => None,
            len => Some(len as DimId - 1),
        }
    }
}

impl<W: Weight> InvertedIndexCompressedMmap<W> {
    pub fn index_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_FILE_NAME)
    }

    pub fn index_config_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_CONFIG_FILE_NAME)
    }

    pub fn get<'a>(&'a self, id: &DimId) -> Option<CompressedPostingListView<'a, W>> {
        // check that the id is not out of bounds (posting_count includes the empty zeroth entry)
        if *id >= self.file_header.posting_count as DimId {
            return None;
        }

        let header: PostingListFileHeader<W> = self.slice_part::<PostingListFileHeader<W>>(
            u64::from(*id) * size_of::<PostingListFileHeader<W>>() as u64,
            1u32,
        )[0]
        .clone();

        let remainders_start = header.ids_start
            + u64::from(header.ids_len)
            + u64::from(header.chunks_count) * size_of::<CompressedPostingChunk<W>>() as u64;

        let remainders_end = if *id + 1 < self.file_header.posting_count as DimId {
            self.slice_part::<PostingListFileHeader<W>>(
                u64::from(*id + 1) * size_of::<PostingListFileHeader<W>>() as u64,
                1u32,
            )[0]
            .ids_start
        } else {
            self.mmap.len() as u64
        };

        if remainders_end
            .checked_sub(remainders_start)
            .is_some_and(|len| len % size_of::<GenericPostingElement<W>>() as u64 != 0)
        {
            return None;
        }

        Some(CompressedPostingListView::new(
            self.slice_part(header.ids_start, header.ids_len),
            self.slice_part(
                header.ids_start + u64::from(header.ids_len),
                header.chunks_count,
            ),
            transmute_from_u8_to_slice(
                &self.mmap[remainders_start as usize..remainders_end as usize],
            ),
            header.last_id.checked_sub(1),
            header.quantization_params,
        ))
    }

    fn slice_part<T>(&self, start: impl Into<u64>, count: impl Into<u64>) -> &[T] {
        let start = start.into() as usize;
        let end = start + count.into() as usize * size_of::<T>();
        transmute_from_u8_to_slice(&self.mmap[start..end])
    }

    pub fn convert_and_save<P: AsRef<Path>>(
        index: &InvertedIndexCompressedImmutableRam<W>,
        path: P,
    ) -> std::io::Result<Self> {
        let total_posting_headers_size =
            index.postings.as_slice().len() * size_of::<PostingListFileHeader<W>>();

        let file_length = total_posting_headers_size
            + index
                .postings
                .as_slice()
                .iter()
                .map(|p| p.view().store_size().total)
                .sum::<usize>();
        let file_path = Self::index_file_path(path.as_ref());
        let file = create_and_ensure_length(file_path.as_ref(), file_length)?;

        let mut buf = BufWriter::new(file);

        // Save posting headers
        let mut offset: usize = total_posting_headers_size;
        for posting in index.postings.as_slice() {
            let store_size = posting.view().store_size();
            let posting_header = PostingListFileHeader::<W> {
                ids_start: offset as u64,
                ids_len: store_size.id_data_bytes as u32,
                chunks_count: store_size.chunks_count as u32,
                last_id: posting.view().last_id().map_or(0, |id| id + 1),
                quantization_params: posting.view().multiplier(),
            };
            buf.write_all(transmute_to_u8(&posting_header))?;
            offset += store_size.total;
        }

        // Save posting elements
        for posting in index.postings.as_slice() {
            let posting_view = posting.view();
            let (id_data, chunks, remainders) = posting_view.parts();
            buf.write_all(id_data)?;
            buf.write_all(transmute_to_u8_slice(chunks))?;
            buf.write_all(transmute_to_u8_slice(remainders))?;
        }

        buf.flush()?;
        drop(buf);

        // save header properties
        let file_header = InvertedIndexFileHeader {
            posting_count: index.postings.as_slice().len(),
            vector_count: index.vector_count,
        };
        atomic_save_json(&Self::index_config_file_path(path.as_ref()), &file_header)?;

        Ok(Self {
            path: path.as_ref().to_owned(),
            mmap: Arc::new(open_read_mmap(
                file_path.as_ref(),
                AdviceSetting::Global,
                false,
            )?),
            file_header,
            _phantom: PhantomData,
        })
    }

    pub fn load<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        // read index config file
        let config_file_path = Self::index_config_file_path(path.as_ref());
        // if the file header does not exist, the index is malformed
        let file_header: InvertedIndexFileHeader = read_json(&config_file_path)?;
        // read index data into mmap
        let file_path = Self::index_file_path(path.as_ref());
        let mmap = open_read_mmap(
            file_path.as_ref(),
            AdviceSetting::from(Advice::Normal),
            false,
        )?;
        Ok(Self {
            path: path.as_ref().to_owned(),
            mmap: Arc::new(mmap),
            file_header,
            _phantom: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::common::types::QuantizedU8;
    use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

    fn compare_indexes<W: Weight>(
        inverted_index_ram: &InvertedIndexCompressedImmutableRam<W>,
        inverted_index_mmap: &InvertedIndexCompressedMmap<W>,
    ) {
        for id in 0..inverted_index_ram.postings.len() as DimId {
            let posting_list_ram = inverted_index_ram.postings.get(id as usize).unwrap().view();
            let posting_list_mmap = inverted_index_mmap.get(&id).unwrap();
            assert_eq!(posting_list_ram, posting_list_mmap);
        }
    }

    #[test]
    fn test_inverted_index_mmap() {
        check_inverted_index_mmap::<f32>();
        check_inverted_index_mmap::<half::f16>();
        check_inverted_index_mmap::<u8>();
        check_inverted_index_mmap::<QuantizedU8>();
    }

    fn check_inverted_index_mmap<W: Weight>() {
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
            let inverted_index_mmap = InvertedIndexCompressedMmap::<W>::convert_and_save(
                &inverted_index_ram,
                &tmp_dir_path,
            )
            .unwrap();

            compare_indexes(&inverted_index_ram, &inverted_index_mmap);
        }
        let inverted_index_mmap = InvertedIndexCompressedMmap::<W>::load(&tmp_dir_path).unwrap();
        // posting_count: 0th entry is always empty + 1st + 2nd + 3rd + 4th empty + 5th
        assert_eq!(inverted_index_mmap.file_header.posting_count, 6);
        assert_eq!(inverted_index_mmap.file_header.vector_count, 9);

        compare_indexes(&inverted_index_ram, &inverted_index_mmap);

        assert!(inverted_index_mmap.get(&0).unwrap().is_empty()); // the first entry is always empty as dimension ids start at 1
        assert_eq!(inverted_index_mmap.get(&1).unwrap().len(), 9);
        assert_eq!(inverted_index_mmap.get(&2).unwrap().len(), 4);
        assert_eq!(inverted_index_mmap.get(&3).unwrap().len(), 3);
        assert!(inverted_index_mmap.get(&4).unwrap().is_empty()); // return empty posting list info for intermediary empty ids
        assert_eq!(inverted_index_mmap.get(&5).unwrap().len(), 2);
        // index after the last values are None
        assert!(inverted_index_mmap.get(&6).is_none());
        assert!(inverted_index_mmap.get(&7).is_none());
        assert!(inverted_index_mmap.get(&100).is_none());
    }
}
