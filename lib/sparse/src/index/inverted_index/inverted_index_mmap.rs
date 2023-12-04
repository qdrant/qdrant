use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::types::PointOffsetType;
use io::file_operations::{atomic_save_json, read_json};
use memmap2::{Mmap, MmapMut};
use memory::madvise;
use memory::mmap_ops::{
    create_and_ensure_length, open_read_mmap, open_write_mmap, transmute_from_u8,
    transmute_from_u8_to_slice, transmute_to_u8, transmute_to_u8_slice,
};
use serde::{Deserialize, Serialize};

use crate::common::sparse_vector::SparseVector;
use crate::common::types::DimId;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::inverted_index::InvertedIndex;
use crate::index::posting_list::{PostingElement, PostingListIterator};

const POSTING_HEADER_SIZE: usize = size_of::<PostingListFileHeader>();
const INDEX_FILE_NAME: &str = "inverted_index.data";
const INDEX_CONFIG_FILE_NAME: &str = "inverted_index_config.json";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InvertedIndexFileHeader {
    pub posting_count: usize, // number oof posting lists
    vector_count: usize,      // number of unique vectors indexed
}

/// Inverted flatten index from dimension id to posting list
pub struct InvertedIndexMmap {
    path: PathBuf,
    mmap: Arc<Mmap>,
    file_header: InvertedIndexFileHeader,
}

#[derive(Debug, Default, Clone)]
struct PostingListFileHeader {
    pub start_offset: u64,
    pub end_offset: u64,
}

impl InvertedIndex for InvertedIndexMmap {
    fn open(path: &Path) -> std::io::Result<Option<Self>> {
        Self::load(path).map(Some)
    }

    fn get(&self, id: &DimId) -> Option<PostingListIterator> {
        self.get(id).map(PostingListIterator::new)
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![
            Self::index_file_path(&self.path),
            Self::index_config_file_path(&self.path),
        ]
    }

    fn upsert(&mut self, _id: PointOffsetType, _vector: SparseVector) {
        panic!("Cannot upsert into a read-only Mmap inverted index")
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: InvertedIndexRam,
        path: P,
    ) -> std::io::Result<Self> {
        Self::convert_and_save(&ram_index, path)
    }

    fn vector_count(&self) -> usize {
        self.file_header.vector_count
    }
}

impl InvertedIndexMmap {
    pub fn index_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_FILE_NAME)
    }

    pub fn index_config_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_CONFIG_FILE_NAME)
    }

    pub fn get(&self, id: &DimId) -> Option<&[PostingElement]> {
        // check that the id is not out of bounds (posting_count includes the empty zeroth entry)
        if *id >= self.file_header.posting_count as DimId {
            return None;
        }
        let header_start = *id as usize * POSTING_HEADER_SIZE;
        let header = transmute_from_u8::<PostingListFileHeader>(
            &self.mmap[header_start..header_start + POSTING_HEADER_SIZE],
        )
        .clone();
        let elements_bytes = &self.mmap[header.start_offset as usize..header.end_offset as usize];
        Some(transmute_from_u8_to_slice(elements_bytes))
    }

    pub fn convert_and_save<P: AsRef<Path>>(
        inverted_index_ram: &InvertedIndexRam,
        path: P,
    ) -> std::io::Result<Self> {
        let total_posting_headers_size = Self::total_posting_headers_size(inverted_index_ram);
        let total_posting_elements_size = Self::total_posting_elements_size(inverted_index_ram);

        let file_length = total_posting_headers_size + total_posting_elements_size;
        let file_path = Self::index_file_path(path.as_ref());
        create_and_ensure_length(file_path.as_ref(), file_length)?;

        let mut mmap = open_write_mmap(file_path.as_ref())?;
        madvise::madvise(&mmap, madvise::get_global())?;

        // file index data
        Self::save_posting_headers(&mut mmap, inverted_index_ram, total_posting_headers_size);
        Self::save_posting_elements(&mut mmap, inverted_index_ram, total_posting_headers_size);

        // save header properties
        let posting_count = inverted_index_ram.postings.len();
        let vector_count = inverted_index_ram.vector_count();

        // finalize data with index file.
        let file_header = InvertedIndexFileHeader {
            posting_count,
            vector_count,
        };
        let config_file_path = Self::index_config_file_path(path.as_ref());
        atomic_save_json(&config_file_path, &file_header)?;

        Ok(Self {
            path: path.as_ref().to_owned(),
            mmap: Arc::new(mmap.make_read_only()?),
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
        madvise::madvise(&mmap, madvise::get_global())?;
        Ok(Self {
            path: path.as_ref().to_owned(),
            mmap: Arc::new(mmap),
            file_header,
        })
    }

    fn total_posting_headers_size(inverted_index_ram: &InvertedIndexRam) -> usize {
        inverted_index_ram.postings.len() * POSTING_HEADER_SIZE
    }

    fn total_posting_elements_size(inverted_index_ram: &InvertedIndexRam) -> usize {
        let mut total_posting_elements_size = 0;
        for posting in &inverted_index_ram.postings {
            total_posting_elements_size += posting.elements.len() * size_of::<PostingElement>();
        }

        total_posting_elements_size
    }

    fn save_posting_headers(
        mmap: &mut MmapMut,
        inverted_index_ram: &InvertedIndexRam,
        total_posting_headers_size: usize,
    ) {
        let mut elements_offset: usize = total_posting_headers_size;
        for (id, posting) in inverted_index_ram.postings.iter().enumerate() {
            let posting_elements_size = posting.elements.len() * size_of::<PostingElement>();
            let posting_header = PostingListFileHeader {
                start_offset: elements_offset as u64,
                end_offset: (elements_offset + posting_elements_size) as u64,
            };
            elements_offset = posting_header.end_offset as usize;

            // save posting header
            let posting_header_bytes = transmute_to_u8(&posting_header);
            let start_posting_offset = id * POSTING_HEADER_SIZE;
            let end_posting_offset = (id + 1) * POSTING_HEADER_SIZE;
            mmap[start_posting_offset..end_posting_offset].copy_from_slice(posting_header_bytes);
        }
    }

    fn save_posting_elements(
        mmap: &mut MmapMut,
        inverted_index_ram: &InvertedIndexRam,
        total_posting_headers_size: usize,
    ) {
        let mut offset = total_posting_headers_size;
        for posting in &inverted_index_ram.postings {
            // save posting element
            let posting_elements_bytes = transmute_to_u8_slice(&posting.elements);
            mmap[offset..offset + posting_elements_bytes.len()]
                .copy_from_slice(posting_elements_bytes);
            offset += posting_elements_bytes.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::index::inverted_index::inverted_index_ram::InvertedIndexBuilder;
    use crate::index::posting_list::PostingList;

    fn compare_indexes(
        inverted_index_ram: &InvertedIndexRam,
        inverted_index_mmap: &InvertedIndexMmap,
    ) {
        for id in 0..inverted_index_ram.postings.len() as DimId {
            let posting_list_ram = inverted_index_ram.get(&id).unwrap().elements.as_slice();
            let posting_list_mmap = inverted_index_mmap.get(&id).unwrap();
            assert_eq!(posting_list_ram.len(), posting_list_mmap.len());
            for i in 0..posting_list_ram.len() {
                assert_eq!(posting_list_ram[i], posting_list_mmap[i]);
            }
        }
    }

    #[test]
    fn test_inverted_index_mmap() {
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(
                1,
                PostingList::from(vec![
                    (1, 10.0),
                    (2, 20.0),
                    (3, 30.0),
                    (4, 1.0),
                    (5, 2.0),
                    (6, 3.0),
                    (7, 4.0),
                    (8, 5.0),
                    (9, 6.0),
                ]),
            )
            .add(
                2,
                PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0), (4, 1.0)]),
            )
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(5, PostingList::from(vec![(1, 10.0), (2, 20.0)])) // skip 4
            .build();

        let tmp_dir_path = Builder::new().prefix("test_index_dir").tempdir().unwrap();

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
