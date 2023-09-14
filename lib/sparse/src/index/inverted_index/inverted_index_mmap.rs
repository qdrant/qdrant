use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;
use memmap2::{Mmap, MmapMut};

use crate::common::types::DimId;
use crate::index::posting_list::PostingElement;

use super::inverted_index_ram::InvertedIndexRam;

const POSTING_HEADER_SIZE: usize = size_of::<PostingListFileHeader>();

/// Inverted flatten index from dimension id to posting list
pub struct InvertedIndexMmap {
    mmap: Arc<Mmap>,
}

#[derive(Default, Clone)]
struct PostingListFileHeader {
    pub start_offset: u64,
    pub end_offset: u64,
}

impl InvertedIndexMmap {
    pub fn get(&self, id: &DimId) -> Option<&[PostingElement]> {
        // TODO: check if id is in range
        let header = transmute_from_u8::<PostingListFileHeader>(&self.mmap[
            *id as usize * POSTING_HEADER_SIZE
            ..
            (*id as usize + 1) * POSTING_HEADER_SIZE
        ]).clone();
        let elements_bytes = &self.mmap[
            header.start_offset as usize
            ..
            header.end_offset as usize];
        Some(transmute_from_u8_to_slice(elements_bytes))
    }

    pub fn convert_and_save<P: AsRef<Path>>(
        inverted_index_ram: &InvertedIndexRam,
        path: P,
    ) -> std::io::Result<Self> {
        let file_length = Self::calculate_file_length(inverted_index_ram);
        Self::create_and_ensure_length(path.as_ref(), file_length)?;
        let mut mmap = Self::open_write_mmap(path.as_ref())?;

        Self::save_posting_headers(&mut mmap, inverted_index_ram);

        Self::save_posting_elements(&mut mmap, inverted_index_ram);

        Ok(Self {
            mmap: Arc::new(mmap.make_read_only()?),
        })
    }

    pub fn load<P: AsRef<Path>>(
        path: P,
    ) -> std::io::Result<Self> {
        Ok(Self {
            mmap: Arc::new(Self::open_read_mmap(path.as_ref())?),
        })
    }

    fn calculate_file_length(
        inverted_index_ram: &InvertedIndexRam,
    ) -> usize {
        let total_posting_headers_size = inverted_index_ram.postings.len() * POSTING_HEADER_SIZE;

        let mut total_posting_elements_size = 0;
        for posting in &inverted_index_ram.postings {
            total_posting_elements_size += posting.elements.len() * size_of::<PostingElement>();
        }

        total_posting_headers_size + total_posting_elements_size
    }

    fn save_posting_headers(
        mmap: &mut MmapMut,
        inverted_index_ram: &InvertedIndexRam,
    ) {
        let total_posting_headers_size = inverted_index_ram.postings.len() * POSTING_HEADER_SIZE;

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
    ) {
        // todo: avoid total_posting_headers_size duplication
        let total_posting_headers_size = inverted_index_ram.postings.len() * POSTING_HEADER_SIZE;

        let mut offset = total_posting_headers_size;
        for posting in &inverted_index_ram.postings {
            // save posting element
            let posting_elements_bytes = transmute_to_u8_slice(&posting.elements);
            mmap[offset..offset + posting_elements_bytes.len()].copy_from_slice(
                posting_elements_bytes
            );
            offset += posting_elements_bytes.len();
        }
    }

    // OOOhhhh noo, we don't want tyo copypaste here!!!!!!
    fn open_read_mmap(path: &Path) -> std::io::Result<Mmap> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .append(true)
            .create(true)
            .open(path)?;
        unsafe { Mmap::map(&file) }
    }

    // OOOhhhh noo, we don't want tyo copypaste here!!!!!!
    pub fn open_write_mmap(path: &Path) -> std::io::Result<MmapMut> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
    
        unsafe { MmapMut::map_mut(&file) }
    }

    // OOOhhhh noo, we don't want tyo copypaste here!!!!!!
    pub fn create_and_ensure_length(path: &Path, length: usize) -> std::io::Result<()> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
    
        file.set_len(length as u64)?;
        Ok(())
    }
}

pub fn transmute_to_u8<T>(v: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v as *const T as *const u8, std::mem::size_of_val(v)) }
}

pub fn transmute_from_u8<T>(v: &[u8]) -> &T {
    unsafe { &*(v.as_ptr() as *const T) }
}

pub fn transmute_from_u8_to_slice<T>(data: &[u8]) -> &[T] {
    debug_assert_eq!(data.len() % size_of::<T>(), 0);
    let len = data.len() / size_of::<T>();
    let ptr = data.as_ptr() as *const T;
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

pub fn transmute_from_u8_to_mut_slice<T>(data: &mut [u8]) -> &mut [T] {
    debug_assert_eq!(data.len() % size_of::<T>(), 0);
    let len = data.len() / size_of::<T>();
    let ptr = data.as_mut_ptr() as *mut T;
    unsafe { std::slice::from_raw_parts_mut(ptr, len) }
}

pub fn transmute_to_u8_slice<T>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use crate::index::{inverted_index::inverted_index_ram::InvertedIndexBuilder, posting_list::PostingList};
    use super::*;

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
        .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
        .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
        .build();

        let tmp_path = Builder::new()
            .prefix("test_serialize_dir")
            .tempfile()
            .unwrap();

        {
            let inverted_index_mmap = InvertedIndexMmap::convert_and_save(
                &inverted_index_ram,
                &tmp_path,
            ).unwrap();

            compare_indexes(
                &inverted_index_ram,
                &inverted_index_mmap,
            );
        }
        let inverted_index_mmap = InvertedIndexMmap::load(
            &tmp_path,
        ).unwrap();

        compare_indexes(
            &inverted_index_ram,
            &inverted_index_mmap,
        );
    }
}
