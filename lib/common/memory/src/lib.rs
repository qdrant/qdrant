pub mod checkfs {
    pub use common::fs::{FsCheckResult, check_fs_info, check_mmap_functionality};
}

pub mod chunked_utils {
    pub use common::mmap::UniversalMmapChunk;
    pub use common::mmap::chunked::{chunk_name, create_chunk, read_mmaps};
}

pub mod fadvise {
    pub use common::fs::{OneshotFile, clear_disk_cache};
}

pub mod madvise {
    pub use common::mmap::advice::{get_global, madvise, set_global, will_need_multiple_pages};
    pub use common::mmap::{Advice, AdviceSetting, Madviseable};
}

pub mod mmap_ops {
    pub use common::mmap::{
        MULTI_MMAP_IS_SUPPORTED, MULTI_MMAP_SUPPORT_CHECK_RESULT, TEMP_FILE_EXTENSION,
        create_and_ensure_length, open_read_mmap, open_write_mmap,
    };
    #[expect(deprecated)]
    pub use common::mmap::{
        transmute_from_u8, transmute_from_u8_to_slice, transmute_to_u8, transmute_to_u8_slice,
    };
}

pub mod mmap_type {
    pub use common::mmap::{Error, MmapBitSlice, MmapFlusher, MmapSlice, MmapType};
}

pub mod mmap_type_readonly {
    pub use common::mmap::{MmapSliceReadOnly, MmapTypeReadOnly};
}
