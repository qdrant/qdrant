//! Platform-independent abstractions over [`memmap2::Mmap::advise`]/[`memmap2::MmapMut::advise`]
//! and [`memmap2::Advice`].

use std::io;

use serde::Deserialize;

/// Global [`Advice`] value, to trivially set [`Advice`] value
/// used by all memmaps created by the `segment` crate.
///
/// See [`set_global`] and [`get_global`].
static ADVICE: parking_lot::RwLock<Advice> = parking_lot::RwLock::new(Advice::Random);

/// Set global [`Advice`] value.
///
/// When the `segment` crate creates [`memmap2::Mmap`] or [`memmap2::MmapMut`]
/// _for a memory-mapped, on-disk HNSW index or vector storage access_
/// it will "advise" the created memmap with the current global [`Advice`] value
/// (obtained with [`get_global`]).
///
/// It is recommended to set the desired [`Advice`] value before calling any other function
/// from the `segment` crate and not to change it afterwards.
///
/// The `segment` crate itself does not modify the global [`Advice`] value.
///
/// The default global [`Advice`] value is [`Advice::Random`].
pub fn set_global(advice: Advice) {
    *ADVICE.write() = advice;
}

/// Get current global [`Advice`] value.
pub fn get_global() -> Advice {
    *ADVICE.read()
}

/// Platform-independent version of [`memmap2::Advice`].
/// See [`memmap2::Advice`] and [`madvise(2)`] man page.
///
/// [`madvise(2)`]: https://man7.org/linux/man-pages/man2/madvise.2.html
#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Advice {
    /// See [`memmap2::Advice::Normal`].
    Normal,

    /// See [`memmap2::Advice::Random`].
    Random,

    /// See [`memmap2::Advice::Sequential`].
    Sequential,

    /// See [`memmap2::Advice::PopulateRead`].
    PopulateRead,
}

// `memmap2::Advice` is only supported on Unix platforms.
//
// It's enabled/disabled with `#[cfg(unix)]` conditional compilation directive in `memmap2` crate,
// so, unfortunately, a bit of conditional compilation is also required in our code.
//
// The most ergonomic way to "integrate" conditionally compiled parts is to abstract them
// into a no-op and/or runtime error, so:
//
// - the `Advice` enum defined in this module is *not* conditionally compiled, and always present
//   on any platform
// - but the `Madviseable::madvise` implementation is a no-op on non-Unix platforms
// - and trying to use `Advice::PopulateRead` is a runtime error on non-Linux platforms
#[cfg(unix)]
impl TryFrom<Advice> for memmap2::Advice {
    type Error = io::Error;

    fn try_from(advice: Advice) -> io::Result<Self> {
        match advice {
            Advice::Normal => Ok(memmap2::Advice::Normal),
            Advice::Random => Ok(memmap2::Advice::Random),
            Advice::Sequential => Ok(memmap2::Advice::Sequential),

            #[cfg(target_os = "linux")]
            Advice::PopulateRead => Ok(memmap2::Advice::PopulateRead),

            #[cfg(not(target_os = "linux"))]
            Advice::PopulateRead => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "MADV_POPULATE_READ is only supported on Linux",
            )),
        }
    }
}

/// Advise OS how given memory map will be accessed. On non-Unix platforms this is a no-op.
pub fn madvise(madviseable: &impl Madviseable, advice: Advice) -> io::Result<()> {
    madviseable.madvise(advice)
}

/// Generic, platform-independent abstraction
/// over [`memmap2::Mmap::advise`] and [`memmap2::MmapMut::advise`].
pub trait Madviseable {
    /// Advise OS how given memory map will be accessed. On non-Unix platforms this is a no-op.
    fn madvise(&self, advice: Advice) -> io::Result<()>;
}

impl Madviseable for memmap2::Mmap {
    fn madvise(&self, advice: Advice) -> io::Result<()> {
        #[cfg(unix)]
        self.advise(advice.try_into()?)?;

        #[cfg(windows)]
        if advice == Advice::PopulateRead {
            win::prefetch_virtual_memory(self)?;
        }

        Ok(())
    }
}

impl Madviseable for memmap2::MmapMut {
    fn madvise(&self, advice: Advice) -> io::Result<()> {
        #[cfg(unix)]
        self.advise(advice.try_into()?)?;

        #[cfg(windows)]
        if advice == Advice::PopulateRead {
            win::prefetch_virtual_memory(self)?;
        }

        Ok(())
    }
}

#[cfg(windows)]
mod win {
    use std::{io, ops};

    // Documentation for the `windows` crate is hosted by Microsoft. The one on Docs.rs is fudged. :/
    // https://microsoft.github.io/windows-docs-rs/doc/windows/index.html
    use windows::Win32::System::Memory::{PrefetchVirtualMemory, WIN32_MEMORY_RANGE_ENTRY};
    use windows::Win32::System::Threading::GetCurrentProcess;

    pub fn prefetch_virtual_memory(mmap: &impl ops::Deref<Target = [u8]>) -> io::Result<()> {
        let ptr = mmap.deref().as_ptr().cast_mut().cast();
        let len = mmap.deref().len();

        if len == 0 {
            return Ok(());
        }

        // - https://microsoft.github.io/windows-docs-rs/doc/windows/Win32/System/Memory/struct.WIN32_MEMORY_RANGE_ENTRY.html
        // - https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/ns-memoryapi-win32_memory_range_entry
        let memory_range_entry = WIN32_MEMORY_RANGE_ENTRY {
            VirtualAddress: ptr,
            NumberOfBytes: len,
        };

        let result = unsafe {
            // - https://microsoft.github.io/windows-docs-rs/doc/windows/Win32/System/Memory/fn.PrefetchVirtualMemory.html
            // - https://microsoft.github.io/windows-docs-rs/doc/windows/Win32/System/Threading/fn.GetCurrentProcess.html
            // - https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-prefetchvirtualmemory
            // - https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-getcurrentprocess
            PrefetchVirtualMemory(GetCurrentProcess(), &[memory_range_entry], 0)
        };

        // - https://microsoft.github.io/windows-docs-rs/doc/windows/Win32/Foundation/struct.BOOL.html
        // - https://microsoft.github.io/windows-docs-rs/doc/windows/core/type.Result.html
        // - https://microsoft.github.io/windows-docs-rs/doc/windows/core/struct.Error.html
        result.ok().map_err(From::from)
    }
}
