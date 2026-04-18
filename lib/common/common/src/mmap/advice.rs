//! Platform-independent abstractions over [`memmap2::Mmap::advise`]/[`memmap2::MmapMut::advise`]
//! and [`memmap2::Advice`].

use std::hint::black_box;
use std::num::Wrapping;
use std::{io, slice};

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
#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Advice {
    /// See [`memmap2::Advice::Normal`].
    Normal,

    /// See [`memmap2::Advice::Random`].
    Random,

    /// See [`memmap2::Advice::Sequential`].
    Sequential,
}

#[cfg(unix)]
impl From<Advice> for memmap2::Advice {
    fn from(advice: Advice) -> Self {
        match advice {
            Advice::Normal => memmap2::Advice::Normal,
            Advice::Random => memmap2::Advice::Random,
            Advice::Sequential => memmap2::Advice::Sequential,
        }
    }
}

/// Either the global [`Advice`] value or a specific [`Advice`] value.
#[derive(Copy, Clone, Debug)]
pub enum AdviceSetting {
    /// Use the global [`Advice`] value (see [`set_global`] and [`get_global`]).
    Global,

    /// Use the specific [`Advice`] value.
    Advice(Advice),
}

impl From<Advice> for AdviceSetting {
    fn from(advice: Advice) -> Self {
        AdviceSetting::Advice(advice)
    }
}

impl AdviceSetting {
    /// Get the specific [`Advice`] value.
    pub fn resolve(self) -> Advice {
        match self {
            AdviceSetting::Global => get_global(),
            AdviceSetting::Advice(advice) => advice,
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
    fn madvise(&self, advice: Advice) -> io::Result<()> {
        #[cfg(unix)]
        self.advise_impl(advice.into())?;

        #[cfg(not(unix))]
        log::debug!("Madvice {advice:?} is ignored on non-unix platforms");

        Ok(())
    }

    #[cfg(unix)]
    fn advise_impl(&self, advice: memmap2::Advice) -> io::Result<()>;

    fn populate(&self) {
        // Low-memory mode `no_populate` suppresses mmap prefault globally.
        // Pages will be faulted in on demand when queries touch them.
        if crate::low_memory::low_memory_mode().skip_populate() {
            return;
        }

        #[cfg(target_os = "linux")]
        {
            use std::sync::LazyLock;

            /// True if `MADV_POPULATE_READ` is supported (added in Linux 5.14)
            static POPULATE_READ_IS_SUPPORTED: LazyLock<bool> =
                LazyLock::new(|| memmap2::Advice::PopulateRead.is_supported());

            if *POPULATE_READ_IS_SUPPORTED {
                match self.advise_impl(memmap2::Advice::PopulateRead) {
                    Ok(()) => return,
                    Err(err) => log::warn!(
                        "Failed to populate with MADV_POPULATE_READ: {err}. \
                         Falling back to naive approach."
                    ),
                }
            }
        }

        self.populate_simple_impl();
    }

    fn populate_simple_impl(&self);

    /// Hint to the OS that pages backing this memory map can be reclaimed.
    ///
    /// Uses `madvise(MADV_PAGEOUT)` on Linux 5.4+, which writes back any dirty
    /// pages and frees the resident memory while keeping the mapping valid.
    /// On older kernels or non-Linux platforms this is a no-op, since there is
    /// no portable userspace equivalent.
    fn clear_cache(&self) {
        #[cfg(target_os = "linux")]
        {
            use std::sync::LazyLock;

            /// True if `MADV_PAGEOUT` is supported (added in Linux 5.4).
            /// Probed by calling `madvise` with a zero-length range, which
            /// validates the advice value without touching any memory.
            ///
            /// As shown in madvise man pages:
            /// > `madvise(0, 0, advice)` will return zero iff advice is supported by the kernel
            /// > and can be relied on to probe for support.
            static PAGEOUT_IS_SUPPORTED: LazyLock<bool> = LazyLock::new(|| {
                let res =
                    unsafe { nix::libc::madvise(std::ptr::null_mut(), 0, nix::libc::MADV_PAGEOUT) };
                res == 0
            });

            if *PAGEOUT_IS_SUPPORTED {
                self.pageout_impl();
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn pageout_impl(&self);
}

/// Issue `madvise(MADV_PAGEOUT)` for the given memory region.
///
/// Mmap base addresses are always page-aligned, so callers do not need to
/// adjust the slice. The kernel will write back any dirty pages and reclaim
/// the resident memory while keeping the mapping valid.
#[cfg(target_os = "linux")]
fn pageout_slice(slice: &[u8]) {
    if slice.is_empty() {
        return;
    }
    let res = unsafe {
        nix::libc::madvise(
            slice.as_ptr() as *mut _,
            slice.len(),
            nix::libc::MADV_PAGEOUT,
        )
    };
    if res != 0 {
        let err = io::Error::last_os_error();
        log::warn!("Failed to call madvise(MADV_PAGEOUT): {err}");
    }
}

impl Madviseable for memmap2::Mmap {
    #[cfg(unix)]
    fn advise_impl(&self, advice: memmap2::Advice) -> io::Result<()> {
        self.advise(advice)
    }

    fn populate_simple_impl(&self) {
        populate_simple(self);
    }

    #[cfg(target_os = "linux")]
    fn pageout_impl(&self) {
        pageout_slice(self);
    }
}

impl Madviseable for memmap2::MmapMut {
    #[cfg(unix)]
    fn advise_impl(&self, advice: memmap2::Advice) -> io::Result<()> {
        self.advise(advice)
    }

    fn populate_simple_impl(&self) {
        populate_simple(self);
    }

    #[cfg(target_os = "linux")]
    fn pageout_impl(&self) {
        pageout_slice(self);
    }
}

impl Madviseable for memmap2::MmapRaw {
    #[cfg(unix)]
    fn advise_impl(&self, advice: memmap2::Advice) -> io::Result<()> {
        self.advise(advice)
    }

    fn populate_simple_impl(&self) {
        let mmap = unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) };
        populate_simple(mmap);
    }

    #[cfg(target_os = "linux")]
    fn pageout_impl(&self) {
        let mmap = unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) };
        pageout_slice(mmap);
    }
}

/// On older Linuxes and non-Unix platforms, we just read every 512th byte to
/// populate the page cache. This is not as efficient as `madvise(2)` with
/// `MADV_POPULATE_READ` but it's better than nothing.
fn populate_simple(slice: &[u8]) {
    black_box(
        slice
            .iter()
            .copied()
            .map(Wrapping)
            .step_by(512)
            .sum::<Wrapping<u8>>(),
    );
}

/// Trigger readahead for a memory-mapped region by calling
/// `madvise(MADV_WILLNEED)` on it.
///
/// Use-case: the `region` is inside `MADV_RANDOM` memory map, but it spans
/// across more than one 4KiB page. If you read it in sequence, it will cause
/// multiple page faults, thus multiple 4KiB I/O operations. Avoid this by
/// calling this function before reading the region. It will prefetch the whole
/// region in a single I/O operation. (if possible)
///
/// Note: if the region fits within a single page, this function is a no-op.
#[cfg(unix)]
pub fn will_need_multiple_pages(region: &[u8]) {
    let Some(page_mask) = page_size().map(|s| s - 1) else {
        return;
    };

    // `madvise()` requires the address to be page-aligned.
    let addr = region.as_ptr().map_addr(|addr| addr & !page_mask);
    let length = region.len() + (region.as_ptr().addr() & page_mask);

    if length <= page_mask {
        // Data fits within a single page, do nothing.
        return;
    }

    // Safety: madvise(MADV_WILLNEED) is harmless. If the address is not valid
    // (not file-baked mmap or even if it is an arbitrary invalid address), it
    // will return an error, but it won't crash or cause an undefined behavior.
    let res = unsafe { nix::libc::madvise(addr as *mut _, length, nix::libc::MADV_WILLNEED) };
    if res != 0 {
        #[cfg(debug_assertions)]
        {
            let err = io::Error::last_os_error();
            panic!("Failed to call madvise(MADV_WILLNEED): {err}");
        }
    }
}

#[cfg(not(unix))]
pub fn will_need_multiple_pages(_region: &[u8]) {}

/// Returns the system page size in bytes, or `None` if it could not be determined.
///
/// Cached after first call. Typically 4096 on x86_64, 16384 on aarch64 macOS.
#[cfg(unix)]
pub fn page_size() -> Option<usize> {
    *CACHED_PAGE_SIZE
}

/// System page size. Must be a power of two.
#[cfg(unix)]
static CACHED_PAGE_SIZE: std::sync::LazyLock<Option<usize>> =
    std::sync::LazyLock::new(|| get_page_size().inspect_err(|err| log::warn!("{err}")).ok());

#[cfg(unix)]
fn get_page_size() -> Result<usize, String> {
    let page_size = nix::unistd::sysconf(nix::unistd::SysconfVar::PAGE_SIZE)
        .map_err(|err| format!("Failed to get page size: {err}"))?
        .ok_or_else(|| "sysconf(PAGE_SIZE) returned None".to_string())?;
    let page_size = usize::try_from(page_size)
        .map_err(|_| format!("Failed to convert page size {page_size} to usize"))?;
    if !page_size.is_power_of_two() {
        return Err(format!("Page size {page_size} is not a power of two"));
    }
    Ok(page_size)
}
