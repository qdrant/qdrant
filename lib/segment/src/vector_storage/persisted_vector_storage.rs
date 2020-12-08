use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use memmap::{Mmap, MmapMut};

use crate::entry::entry_point::OperationResult;
use std::intrinsics::size_of;
use std::ops;


const MAX_SEGMENT_SIZE: usize = 32 * 1024 * 1024; // 32 mb
const SEGMENT_MAGIC: &'static [u8; 4] = b"vect";

const SEGMENT_HEADER_SIZE: usize = size_of::<usize> + SEGMENT_MAGIC.len();



/// Storage for fixed-length records
///
struct Segment {
    mmap: MmapMut,
    path: PathBuf,

    record_size: usize,
    /// Number of records which could be stored in total in this segment
    capacity: usize,
    /// Number of currently stored vectors
    occupancy: usize,
    unflashed: uszie
}

impl Segment {
    pub fn new(path: &Path, record_size: usize) -> OperationResult<Self> {
        let max_records = MAX_SEGMENT_SIZE / record_size;
        let storage_size = max_records * record_size;

        let file = OpenOptions::new().read(true).write(true).create(true).open(&path)?;
        file.allocate(storage_size as u64)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Segment {
            mmap,
            path: path.to_owned(),
            record_size,
            capacity: max_records,
            occupancy: 0,
            unflashed: 0
        })
    }

    pub fn append<T>(&mut self, record: &T) -> OperationResult<usize> where T: ops::Deref<Target=[u8]> {
        self.unflashed += 1;
        unimplemented!()
    }

    pub fn update<T>(&mut self, idx: usize, record: &T) -> OperationResult<()> where T: ops::Deref<Target=[u8]> {
        self.unflashed += 1;
        unimplemented!()
    }

        /// Flushes recently written entries to durable storage.
    pub fn flush(&mut self) -> OperationResult<()> {
        if self.unflashed == 0 {
            Ok(())
        } else {
            self.mmap.flush()?;
            self.unflashed = 0;
        }
    }

    pub fn read<T>(&self, idx: usize) -> &[u8] {
        unimplemented!()
    }
}

struct PersistedVectorStorage {
    storage_path: PathBuf,
    vector_size: usize,
    segments: Vec<Segment>,
}