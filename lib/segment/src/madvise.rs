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
    use std::{ops, ptr, slice};

    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::System::Diagnostics::Debug::{
        FormatMessageW, FORMAT_MESSAGE_ALLOCATE_BUFFER, FORMAT_MESSAGE_FROM_SYSTEM,
    };
    use windows_sys::Win32::System::Memory::{
        GetProcessHeap, HeapFree, PrefetchVirtualMemory, WIN32_MEMORY_RANGE_ENTRY,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    use super::*;

    pub fn prefetch_virtual_memory(mmap: &impl ops::Deref<Target = [u8]>) -> io::Result<()> {
        let ptr = mmap.deref().as_ptr().cast_mut().cast();
        let len = mmap.deref().len();

        if len == 0 {
            return Ok(());
        }

        // https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/ns-memoryapi-win32_memory_range_entry
        let memory_range_entry = WIN32_MEMORY_RANGE_ENTRY {
            VirtualAddress: ptr,
            NumberOfBytes: len,
        };

        let result = unsafe {
            // - https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-prefetchvirtualmemory
            // - https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-getcurrentprocess
            PrefetchVirtualMemory(GetCurrentProcess(), 1, &memory_range_entry, 0)
        };

        if result != 0 {
            return Ok(());
        }

        // https://learn.microsoft.com/en-us/windows/win32/api/errhandlingapi/nf-errhandlingapi-getlasterror
        let error_code = unsafe { GetLastError() };

        let error_message = match error_message(error_code) {
            Ok(error_message) => format!(
                "PrefetchVirtualMemory failed with {error_code} error code: \
                 {error_message}"
            ),

            Err(format_message_error_code) => format!(
                "PrefetchVirtualMemory failed with {error_code} error code \
                 (FormatMessageW failed to format error message with {format_message_error_code} error code)"
            ),
        };

        Err(io::Error::new(io::ErrorKind::Other, error_message))
    }

    // - https://learn.microsoft.com/en-us/windows/win32/debug/retrieving-the-last-error-code
    // - `windows_core::hresult::Hresult::message` implementation
    //   - https://github.com/microsoft/windows-rs/blob/311b080/crates/libs/core/src/hresult.rs#L83-L92
    fn error_message(error_code: u32) -> Result<String, u32> {
        let mut error_message_buf_ptr = ptr::null_mut();

        let error_message_buf_len = unsafe {
            // https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-formatmessagew
            FormatMessageW(
                FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
                ptr::null(),
                error_code,
                // Equivalent of `MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT)` in C/C++
                //
                // - https://learn.microsoft.com/en-us/windows/win32/api/winnt/nf-winnt-makelangid
                // - https://github.com/microsoft/windows-rs/issues/829#issuecomment-851553538
                0x0000_0400,
                // This is super confusing, but correct, as far as I can tell:
                // - `error_message_ptr` is a NULL-pointer of `*mut u16` type
                // - we take pointer to `error_message_ptr`
                //   - which is of `*mut *mut u16` type
                // - and pass it as `lpbuffer` argument of `FormatMessageW`
                //   - `which is of `*mut u16` type
                //   - so we cast pointer to `error_message_ptr` from `*mut *mut u16` to `*mut u16`
                // - because we pass `FORMAT_MESSAGE_ALLOCATE_BUFFER` flag to `FormatMessageW`,
                //   it would cast `lpbuffer` back to `*mut *mut u16` and write an address
                //   of a UTF-16 message buffer (i.e., `*mut u16` value) to `error_message_ptr`
                //
                // - https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-formatmessagew#parameters
                //   - `lpBuffer` parameter
                // - https://learn.microsoft.com/en-us/windows/win32/debug/retrieving-the-last-error-code
                // - `windows_core::hresult::Hresult::message` implementation
                //   - https://github.com/microsoft/windows-rs/blob/311b080/crates/libs/core/src/hresult.rs#L83-L92
                &mut error_message_buf_ptr as *mut _ as *mut _,
                0,
                ptr::null_mut(),
            )
        };

        if error_message_buf_len == 0 {
            // https://learn.microsoft.com/en-us/windows/win32/api/errhandlingapi/nf-errhandlingapi-getlasterror
            return Err(unsafe { GetLastError() });
        }

        let error_message_buf = unsafe {
            slice::from_raw_parts(
                error_message_buf_ptr as *const _,
                error_message_buf_len as _,
            )
        };

        let error_message = String::from_utf16_lossy(error_message_buf);

        unsafe {
            // - https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-localfree
            //   - note at the top regarding "heap functions"
            // - https://learn.microsoft.com/en-us/windows/win32/api/heapapi/nf-heapapi-heapfree
            // - https://learn.microsoft.com/en-us/windows/win32/api/heapapi/nf-heapapi-getprocessheap
            // - `windows_core::strings::hstrng::HSTRING` drop implementation:
            //   - https://github.com/microsoft/windows-rs/blob/311b080/crates/libs/core/src/strings/hstring.rs#L119-L136
            // - `windows_core::imp::heap::heap_free` implementation:
            //   - https://github.com/microsoft/windows-rs/blob/311b080/crates/libs/core/src/imp/heap.rs#L26-L35
            HeapFree(GetProcessHeap(), 0, error_message_buf_ptr);
        }

        Ok(error_message)
    }
}
