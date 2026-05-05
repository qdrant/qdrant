use crate::universal_io::UniversalRead;

/// Type-family marker for [`UniversalRead`] backends.
///
/// Rust has no higher-kinded types, so we cannot write `S<T>` where `S` is a
/// generic parameter. This trait stands in: `S: UniversalReadFamily` then
/// `S::Read<T>` yields the concrete `UniversalRead<T>` for element type `T`.
pub trait UniversalReadFamily {
    type Read<T: bytemuck::Pod + 'static>: UniversalRead<T>;
}

pub struct MmapFamily;
impl UniversalReadFamily for MmapFamily {
    type Read<T: bytemuck::Pod + 'static> = crate::universal_io::mmap::MmapFile;
}

#[cfg(target_os = "linux")]
pub struct IoUringFamily;
#[cfg(target_os = "linux")]
impl UniversalReadFamily for IoUringFamily {
    type Read<T: bytemuck::Pod + 'static> = crate::universal_io::io_uring::IoUringFile;
}

#[cfg(not(target_os = "windows"))]
pub struct CachedSliceFamily;
#[cfg(not(target_os = "windows"))]
impl UniversalReadFamily for CachedSliceFamily {
    type Read<T: bytemuck::Pod + 'static> = crate::universal_io::disk_cache::CachedSlice<T>;
}
