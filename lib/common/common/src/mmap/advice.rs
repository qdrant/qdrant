//! Platform-independent abstractions over [`memmap2::Mmap::advise`]/[`memmap2::MmapMut::advise`]
//! and [`memmap2::Advice`].

use std::hint::black_box;
use std::io;
use std::num::Wrapping;

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
    fn madvise(&self, advice: Advice) -> io::Result<()>;

    fn populate(&self);
}

impl Madviseable for memmap2::Mmap {
    fn madvise(&self, advice: Advice) -> io::Result<()> {
        #[cfg(unix)]
        self.advise(advice.into())?;
        #[cfg(not(unix))]
        log::debug!("Ignore {advice:?} on this platform");
        Ok(())
    }

    fn populate(&self) {
        #[cfg(target_os = "linux")]
        if *POPULATE_READ_IS_SUPPORTED {
            match self.advise(memmap2::Advice::PopulateRead) {
                Ok(()) => return,
                Err(err) => log::warn!(
                    "Failed to populate with MADV_POPULATE_READ: {err}. \
                     Falling back to naive approach."
                ),
            }
        }
        populate_simple(self);
    }
}

impl Madviseable for memmap2::MmapMut {
    fn madvise(&self, advice: Advice) -> io::Result<()> {
        #[cfg(unix)]
        self.advise(advice.into())?;
        #[cfg(not(unix))]
        log::debug!("Ignore {advice:?} on this platform");
        Ok(())
    }

    fn populate(&self) {
        #[cfg(target_os = "linux")]
        if *POPULATE_READ_IS_SUPPORTED {
            match self.advise(memmap2::Advice::PopulateRead) {
                Ok(()) => return,
                Err(err) => log::warn!(
                    "Failed to populate with MADV_POPULATE_READ: {err}. \
                     Falling back to naive approach."
                ),
            }
        }
        populate_simple(self);
    }
}

/// True if `MADV_POPULATE_READ` is supported (added in Linux 5.14).
#[cfg(target_os = "linux")]
static POPULATE_READ_IS_SUPPORTED: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| memmap2::Advice::PopulateRead.is_supported());

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
    let Some(page_mask) = *PAGE_SIZE_MASK else {
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

/// Page size mask. Typically 0xfff for 4KiB pages.
#[cfg(unix)]
static PAGE_SIZE_MASK: std::sync::LazyLock<Option<usize>> =
    std::sync::LazyLock::new(|| get_page_mask().inspect_err(|err| log::warn!("{err}")).ok());

#[cfg(unix)]
fn get_page_mask() -> Result<usize, String> {
    let page_size = nix::unistd::sysconf(nix::unistd::SysconfVar::PAGE_SIZE)
        .map_err(|err| format!("Failed to get page size: {err}"))?
        .ok_or_else(|| "sysconf(PAGE_SIZE) returned None".to_string())?;
    let page_size = usize::try_from(page_size)
        .map_err(|_| format!("Failed to convert page size {page_size} to usize"))?;
    if !page_size.is_power_of_two() {
        // Assuming that page size is a power of two (which is true for all
        // known platforms) simplifies computations.
        return Err(format!("Page size {page_size} is not a power of two"));
    }
    Ok(page_size - 1)
}
