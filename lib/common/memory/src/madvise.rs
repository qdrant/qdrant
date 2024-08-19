//! Platform-independent abstractions over [`memmap2::Mmap::advise`]/[`memmap2::MmapMut::advise`]
//! and [`memmap2::Advice`].

#[cfg(not(unix))]
use std::hint::black_box;
use std::io;

use serde::Deserialize;

#[cfg(not(unix))]
const PAGE_SIZE: usize = 4096;

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

    fn populate(&self) -> io::Result<()>;
}

impl Madviseable for memmap2::Mmap {
    fn madvise(&self, advice: Advice) -> io::Result<()> {
        #[cfg(unix)]
        self.advise(advice.into())?;
        #[cfg(not(unix))]
        log::debug!("Ignore {advice:?} on this platform");
        Ok(())
    }

    fn populate(&self) -> io::Result<()> {
        #[cfg(unix)]
        self.advise(memmap2::Advice::PopulateRead)?;
        #[cfg(not(unix))]
        {
            // On non-Unix platforms, we just iterate over the memory to populate it.
            // This is not as efficient as `madvise(2)` with `PopulateRead` but it's better than nothing.
            let mut dst = [0; PAGE_SIZE * 2];

            for chunk in self.chunks(dst.len()) {
                dst[..chunk.len()].copy_from_slice(chunk);
            }

            black_box(dst);
        }
        Ok(())
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

    fn populate(&self) -> io::Result<()> {
        #[cfg(unix)]
        self.advise(memmap2::Advice::PopulateRead)?;
        #[cfg(not(unix))]
        {
            // On non-Unix platforms, we just iterate over the memory to populate it.
            // This is not as efficient as `madvise(2)` with `PopulateRead` but it's better than nothing.
            let mut dst = [0; PAGE_SIZE * 2];

            for chunk in self.chunks(dst.len()) {
                dst[..chunk.len()].copy_from_slice(chunk);
            }

            black_box(dst);
        }
        Ok(())
    }
}
