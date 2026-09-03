use std::mem;
use std::path::Path;

use common::bitvec::{BitSlice, BitSliceExt as _};
use common::mmap;
use common::mmap::{AdviceSetting, MmapBitSlice, MmapFlusher};
use common::types::PointOffsetType;

use crate::common::error_logging::LogError;
use crate::common::operation_error::OperationResult;
use crate::vector_storage::common::ensure_mmap_file_size;

const HEADER_SIZE: usize = 4;
const DELETED_HEADER: &[u8; HEADER_SIZE] = b"drop";

#[derive(Debug)]
pub struct DeletedFlags {
    bitslice: MmapBitSlice,
    count: usize,
}

impl DeletedFlags {
    /// Open the flags file, creating and/or resizing it to hold `num_vectors` flags.
    pub fn open_or_create(path: &Path, num_vectors: usize) -> OperationResult<Self> {
        let size = deleted_mmap_size(num_vectors);
        ensure_mmap_file_size(path, DELETED_HEADER, Some(size as u64))
            .describe("Create mmap deleted file")?;
        let deleted_mmap = mmap::open_write_mmap(path, AdviceSetting::Global, false)
            .describe("Open mmap deleted for writing")?;

        // Advise kernel that we'll need this page soon so the kernel can prepare
        #[cfg(unix)]
        if let Err(err) = deleted_mmap.advise(memmap2::Advice::WillNeed) {
            log::error!("Failed to advise MADV_WILLNEED for deleted flags: {err}");
        }

        // Transform into mmap BitSlice
        let bitslice = MmapBitSlice::try_from(deleted_mmap, deleted_mmap_data_start())?;
        let count = bitslice.count_ones();

        Ok(Self { bitslice, count })
    }

    pub fn bitslice(&self) -> &BitSlice {
        &self.bitslice
    }

    pub fn is_deleted(&self, key: PointOffsetType) -> bool {
        self.bitslice.get_bit(key as usize).unwrap_or(false)
    }

    /// Marks the key as deleted.
    ///
    /// Returns true if the key was not deleted before, and it is now deleted.
    pub fn delete(&mut self, key: PointOffsetType) -> bool {
        let is_deleted = !self.bitslice.replace(key as usize, true);
        if is_deleted {
            self.count += 1;
        }
        is_deleted
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn flusher(&self) -> MmapFlusher {
        self.bitslice.flusher()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.bitslice.clear_cache()?;
        Ok(())
    }
}

/// Get start position of flags `BitSlice` in deleted mmap.
#[inline]
pub(crate) const fn deleted_mmap_data_start() -> usize {
    let align = mem::align_of::<usize>();
    HEADER_SIZE.div_ceil(align) * align
}

/// Calculate size for deleted mmap to hold the given number of vectors.
///
/// The mmap will hold a file header and an aligned `BitSlice`.
fn deleted_mmap_size(num: usize) -> usize {
    let unit_size = mem::size_of::<usize>();
    let num_bytes = num.div_ceil(8);
    let num_usizes = num_bytes.div_ceil(unit_size);
    let data_size = num_usizes * unit_size;
    deleted_mmap_data_start() + data_size
}
