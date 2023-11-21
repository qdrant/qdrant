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
        self.advise(advice.into())?;
        #[cfg(not(unix))]
        log::debug!("Ignore {advice:?} on this platform");
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
}
