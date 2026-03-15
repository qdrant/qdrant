pub mod advice;
mod mmap_readonly;
#[cfg(not(target_arch = "wasm32"))]
mod mmap_rw;
#[cfg(target_arch = "wasm32")]
mod mmap_rw_wasm;
mod ops;

pub use advice::{Advice, AdviceSetting, Madviseable};
pub use mmap_readonly::{MmapSliceReadOnly, MmapTypeReadOnly};
#[cfg(not(target_arch = "wasm32"))]
pub use mmap_rw::{
    Error, Mmap, MmapBitSlice, MmapFlusher, MmapMut, MmapOptions, MmapSlice, MmapType,
};
#[cfg(target_arch = "wasm32")]
pub use mmap_rw_wasm::{
    Error, Mmap, MmapBitSlice, MmapFlusher, MmapMut, MmapOptions, MmapSlice, MmapType,
};
pub use ops::{
    MULTI_MMAP_IS_SUPPORTED, MULTI_MMAP_SUPPORT_CHECK_RESULT, TEMP_FILE_EXTENSION,
    create_and_ensure_length, open_read_mmap, open_write_mmap,
};
#[expect(deprecated, reason = "Re-exports of deprecated items")]
pub use ops::{
    transmute_from_u8, transmute_from_u8_to_slice, transmute_to_u8, transmute_to_u8_slice,
};
