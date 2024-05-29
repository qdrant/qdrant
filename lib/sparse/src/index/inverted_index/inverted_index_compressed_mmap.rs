use std::io::{BufWriter, Write as _};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::types::PointOffsetType;
use io::file_operations::{atomic_save_json, read_json};
use memmap2::Mmap;
use memory::madvise;
use memory::mmap_ops::{
    create_and_ensure_length, open_read_mmap, transmute_from_u8_to_slice, transmute_to_u8,
    transmute_to_u8_slice,
};
use serde::{Deserialize, Serialize};

use super::inverted_index_compressed_immutable_ram::InvertedIndexImmutableRam;
use super::INDEX_FILE_NAME;
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::{DimId, DimOffset};
use crate::index::compressed_posting_list::{
    CompressedPostingChunk, CompressedPostingListIterator, CompressedPostingListView,
};
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::inverted_index::InvertedIndex;
use crate::index::posting_list_common::PostingElement;

const POSTING_HEADER_SIZE: usize = size_of::<PostingListFileHeader>();
const INDEX_CONFIG_FILE_NAME: &str = "inverted_index_config.json";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InvertedIndexFileHeader {
    pub posting_count: usize, // number oof posting lists
    pub vector_count: usize,  // number of unique vectors indexed
}

/// Inverted flatten index from dimension id to posting list
pub struct InvertedIndexMmap {
    path: PathBuf,
    mmap: Arc<Mmap>,
    pub file_header: InvertedIndexFileHeader,
}

#[derive(Debug, Default, Clone)]
#[repr(C)]
struct PostingListFileHeader {
    pub ids_start: u64,
    pub last_id: u32,
    pub ids_len: u32, // TODO: divide by 16?
    pub chunks_count: u32,
}

impl InvertedIndex for InvertedIndexMmap {
    type Iter<'a> = CompressedPostingListIterator<'a>;

    fn open(path: &Path) -> std::io::Result<Self> {
        Self::load(path)
    }

    fn save(&self, path: &Path) -> std::io::Result<()> {
        debug_assert_eq!(path, self.path);
        Ok(())
    }

    fn get<'a>(&'a self, id: &DimId) -> Option<CompressedPostingListIterator<'a>> {
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

    fn upsert(&mut self, _id: PointOffsetType, _vector: RemappedSparseVector) {
        panic!("Cannot upsert into a read-only Mmap inverted index")
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: InvertedIndexRam,
        path: P,
    ) -> std::io::Result<Self> {
        let index = InvertedIndexImmutableRam::from_ram_index(ram_index, &path)?;
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

impl InvertedIndexMmap {
    pub fn index_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_FILE_NAME)
    }

    pub fn index_config_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_CONFIG_FILE_NAME)
    }

    pub fn get<'a>(&'a self, id: &DimId) -> Option<CompressedPostingListView<'a>> {
        // check that the id is not out of bounds (posting_count includes the empty zeroth entry)
        if *id >= self.file_header.posting_count as DimId {
            return None;
        }

        let header: PostingListFileHeader = self
            .slice_part::<PostingListFileHeader>(*id as u64 * POSTING_HEADER_SIZE as u64, 1u32)[0]
            .clone();

        let remainders_start = header.ids_start
            + header.ids_len as u64
            + header.chunks_count as u64 * size_of::<CompressedPostingChunk>() as u64;

        let remainders_end = if *id + 1 < self.file_header.posting_count as DimId {
            self.slice_part::<PostingListFileHeader>(
                (*id + 1) as u64 * POSTING_HEADER_SIZE as u64,
                1u32,
            )[0]
            .ids_start
        } else {
            self.mmap.len() as u64
        };

        if remainders_end
            .checked_sub(remainders_start)
            .map_or(false, |len| len % size_of::<PostingElement>() as u64 != 0)
        {
            return None;
        }

        Some(CompressedPostingListView::new(
            self.slice_part(header.ids_start, header.ids_len),
            self.slice_part(
                header.ids_start + header.ids_len as u64,
                header.chunks_count,
            ),
            transmute_from_u8_to_slice(
                &self.mmap[remainders_start as usize..remainders_end as usize],
            ),
            header.last_id.checked_sub(1),
        ))
    }

    fn slice_part<T>(&self, start: impl Into<u64>, count: impl Into<u64>) -> &[T] {
        let start = start.into() as usize;
        let end = start + count.into() as usize * size_of::<T>();
        transmute_from_u8_to_slice(&self.mmap[start..end])
    }

    pub fn convert_and_save<P: AsRef<Path>>(
        index: &InvertedIndexImmutableRam,
        path: P,
    ) -> std::io::Result<Self> {
        let total_posting_headers_size = index.postings.as_slice().len() * POSTING_HEADER_SIZE;

        let file_length = total_posting_headers_size
            + index
                .postings
                .as_slice()
                .iter()
                .map(|p| p.view().store_size().total())
                .sum::<usize>();
        let file_path = Self::index_file_path(path.as_ref());
        let file = create_and_ensure_length(file_path.as_ref(), file_length)?;

        let mut buf = BufWriter::new(file);

        // Save posting headers
        let mut offset: usize = total_posting_headers_size;
        for posting in index.postings.as_slice() {
            let store_size = posting.view().store_size();
            let posting_header = PostingListFileHeader {
                ids_start: offset as u64,
                ids_len: store_size.id_data_bytes as u32,
                chunks_count: store_size.chunks_count as u32,
                last_id: posting.view().last_id().map_or(0, |id| id + 1),
            };
            buf.write_all(transmute_to_u8(&posting_header))?;
            offset += store_size.total();
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
            mmap: Arc::new(open_read_mmap(file_path.as_ref())?),
            file_header,
        })
    }

    pub fn load<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        // read index config file
        let config_file_path = Self::index_config_file_path(path.as_ref());
        // if the file header does not exist, the index is malformed
        let file_header: InvertedIndexFileHeader = read_json(&config_file_path)?;
        // read index data into mmap
        let file_path = Self::index_file_path(path.as_ref());
        let mmap = open_read_mmap(file_path.as_ref())?;
        madvise::madvise(&mmap, madvise::Advice::Normal)?;
        Ok(Self {
            path: path.as_ref().to_owned(),
            mmap: Arc::new(mmap),
            file_header,
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

    fn compare_indexes(
        inverted_index_ram: &InvertedIndexImmutableRam,
        inverted_index_mmap: &InvertedIndexMmap,
    ) {
        for id in 0..inverted_index_ram.postings.len() as DimId {
            let posting_list_ram = inverted_index_ram.postings.get(id as usize).unwrap().view();
            let posting_list_mmap = inverted_index_mmap.get(&id).unwrap();
            assert_eq!(posting_list_ram, posting_list_mmap);
        }
    }

    #[test]
    fn test_inverted_index_mmap() {
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
        let inverted_index_ram =
            InvertedIndexImmutableRam::from_ram_index(inverted_index_ram, &tmp_dir_path).unwrap();

        let tmp_dir_path = Builder::new().prefix("test_index_dir2").tempdir().unwrap();

        {
            let inverted_index_mmap =
                InvertedIndexMmap::convert_and_save(&inverted_index_ram, &tmp_dir_path).unwrap();

            compare_indexes(&inverted_index_ram, &inverted_index_mmap);
        }
        let inverted_index_mmap = InvertedIndexMmap::load(&tmp_dir_path).unwrap();
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
