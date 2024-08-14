use crate::areas::MemoryAreas;
use crate::error::Error;
use bitflags::bitflags;
use std::fs::File;
use std::ops::{Deref, DerefMut, Range};

#[cfg(unix)]
use crate::os_impl::unix as platform;

#[cfg(windows)]
use crate::os_impl::windows as platform;

bitflags! {
    /// The available flags to configure the allocated mapping.
    pub struct MmapFlags: u32 {
        /// Maps the pages as shared such that any modifcations are visible between processes.
        ///
        /// When mapping a file without specifying this flag, the pages may initially be mapped as
        /// shared, but a private copy will be created when any process writes to the memory
        /// mapping, such that any modification is not visible to other processes.
        const SHARED                 = 1 << 0;

        /// Ensure the allocated pages are populated, such that they do not cause page faults.
        const POPULATE               = 1 << 1;

        /// Do not reserve swap space for this allocation.
        ///
        /// This flag acts as a no-op on platforms that do not support this feature.
        const NO_RESERVE             = 1 << 2;

        /// Use huge pages for this allocation.
        const HUGE_PAGES             = 1 << 3;

        /// The region grows downward like a stack on certain Unix platforms (e.g. FreeBSD).
        ///
        /// This flag acts as a no-op on platforms that do not support this feature.
        const STACK                  = 1 << 4;

        /// The pages will not be included in a core dump.
        ///
        /// This flag acts as a no-op on platforms that do not support this feature.
        const NO_CORE_DUMP           = 1 << 5;

        /// Lock the physical memory to prevent page faults from happening when accessing the
        /// pages.
        const LOCKED                 = 1 << 6;

        /// Suggest to use transparent huge pages for this allocation by calling `madvise()`.
        ///
        /// This flag acts as a no-op on platforms that do not support this feature.
        const TRANSPARENT_HUGE_PAGES = 1 << 7;

        /// Suggest that the mapped region will be accessed sequentially by calling `madvise()`.
        ///
        /// This flag acts as a no-op on platforms that do not support this feature.
        const SEQUENTIAL = 1 << 8;

        /// Suggest that the mapped region will be accessed randomly by calling `madvise()`.
        ///
        /// This flag acts as a no-op on platforms that do not support this feature.
        const RANDOM_ACCESS = 1 << 9;
    }

    /// The available flags to configure the allocated mapping, but that are considered unsafe to
    /// use.
    pub struct UnsafeMmapFlags: u32 {
        /// Maps the memory mapping at the address specified, replacing any pages that have been
        /// mapped at that address range.
        ///
        /// This is not supported on Microsoft Windows.
        const MAP_FIXED = 1 << 0;

        /// Allows mapping the page as RWX. While this may seem useful for self-modifying code and
        /// JIT engines, it is instead recommended to convert between mutable and executable
        /// mappings using [`Mmap::make_mut()`] and [`MmapMut::make_exec()`] instead.
        ///
        /// As it may be tempting to use this flag, this flag has been (indirectly) marked as
        /// **unsafe**. Make sure to read the text below to understand the complications of this
        /// flag before using it.
        ///
        /// RWX pages are an interesting targets to attackers, e.g. for buffer overflow attacks, as
        /// RWX mappings can potentially simplify such attacks. Without RWX mappings, attackers
        /// instead have to resort to return-oriented programming (ROP) gadgets. To prevent buffer
        /// overflow attacks, contemporary CPUs allow pages to be marked as non-executable which is
        /// then used by the operating system to ensure that pages are either marked as writeable
        /// or as executable, but not both. This is also known as W^X.
        ///
        /// While the x86 and x86-64 architectures guarantee cache coherency between the L1
        /// instruction and the L1 data cache, other architectures such as Arm and AArch64 do not.
        /// If the user modified the pages, then executing the code may result in undefined
        /// behavior. To ensure correct behavior a user has to flush the instruction cache after
        /// modifying and before executing the page.
        const JIT       = 1 << 1;
    }

    /// A set of (supported) page sizes.
    pub struct PageSizes: usize {
        /// 4 KiB pages.
        const _4K   = 1 << 12;
        /// 8 KiB pages.
        const _8K   = 1 << 13;
        /// 16 KiB pages.
        const _16K  = 1 << 14;
        /// 32 KiB pages.
        const _32K  = 1 << 15;
        /// 64 KiB pages.
        const _64K  = 1 << 16;
        /// 128 KiB pages.
        const _128K = 1 << 17;
        /// 256 KiB pages.
        const _256K = 1 << 18;
        /// 512 KiB pages.
        const _512K = 1 << 19;
        /// 1 MiB pages.
        const _1M   = 1 << 20;
        /// 2 MiB pages.
        const _2M   = 1 << 21;
        /// 4 MiB pages.
        const _4M   = 1 << 22;
        /// 8 MiB pages.
        const _8M   = 1 << 23;
        /// 16 MiB pages.
        const _16M  = 1 << 24;
        /// 32 MiB pages.
        const _32M = 1 << 25;
        /// 64 MiB pages.
        const _64M = 1 << 26;
        /// 128 MiB pages.
        const _128M = 1 << 27;
        /// 256 MiB pages.
        const _256M = 1 << 28;
        /// 512 MiB pages.
        const _512M = 1 << 29;
        /// 1 GiB pages.
        const _1G   = 1 << 30;
        /// 2 GiB pages.
        const _2G   = 1 << 31;
        #[cfg(target_pointer_width = "64")]
        /// 4 GiB pages.
        const _4G   = 1 << 32;
        #[cfg(target_pointer_width = "64")]
        /// 8 GiB pages.
        const _8G   = 1 << 33;
        #[cfg(target_pointer_width = "64")]
        /// 16 GiB pages.
        const _16G  = 1 << 34;
    }
}

/// The preferred size of the pages uses, where the size is in log2 notation.
///
/// Note that not all the offered page sizes may be available on the current platform.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PageSize(pub usize);

impl PageSize {
    /// Map the mapping using 4 KiB pages.
    pub const _4K: Self = Self(12);
    /// Map the mapping using 64 KiB pages.
    pub const _64K: Self = Self(16);
    /// Map the mapping using 512 KiB pages.
    pub const _512K: Self = Self(19);
    /// Map the mapping using 1 MiB pages.
    pub const _1M: Self = Self(20);
    /// Map the mapping using 2 MiB pages.
    pub const _2M: Self = Self(21);
    /// Map the mapping using 4 MiB pages.
    pub const _4M: Self = Self(22);
    /// Map the mapping using 8 MiB pages.
    pub const _8M: Self = Self(23);
    /// Map the mapping using 16 MiB pages.
    pub const _16M: Self = Self(24);
    /// Map the mapping using 32 MiB pages.
    pub const _32M: Self = Self(25);
    /// Map the mapping using 256 MiB pages.
    pub const _256M: Self = Self(28);
    /// Map the mapping using 512 MiB pages.
    pub const _512M: Self = Self(29);
    /// Map the mapping using 1 GiB pages.
    pub const _1G: Self = Self(30);
    /// Map the mapping using 2 GiB pages.
    pub const _2G: Self = Self(31);
    /// Map the mapping using 16 GiB pages.
    pub const _16G: Self = Self(34);
}

impl TryFrom<PageSizes> for PageSize {
    type Error = Error;

    fn try_from(page_sizes: PageSizes) -> Result<PageSize, Error> {
        if page_sizes.bits().count_ones() != 1 {
            return Err(Error::InvalidSize);
        }

        Ok(PageSize(page_sizes.bits()))
    }
}

macro_rules! reserved_mmap_impl {
    ($t:ident) => {
        impl $t {
            /// Returns the start address of this mapping.
            #[inline]
            pub fn start(&self) -> usize {
                self.inner.as_ptr() as usize
            }

            /// Returns the end address of this mapping.
            #[inline]
            pub fn end(&self) -> usize {
                self.start() + self.size()
            }

            /// Yields a raw immutable pointer of this mapping.
            #[inline]
            pub fn as_ptr(&self) -> *const u8 {
                self.inner.as_ptr()
            }

            /// Yields a raw mutable pointer of this mapping.
            #[inline]
            pub fn as_mut_ptr(&mut self) -> *mut u8 {
                self.inner.as_mut_ptr()
            }

            /// Yields the size of this mapping.
            #[inline]
            pub fn size(&self) -> usize {
                self.inner.size()
            }

            /// Merges the memory maps into one. The memory maps must be adjacent to each other and
            /// share the same attributes and backing. On success, this consumes the other memory map
            /// object. Otherwise, this returns an error together with the original memory map that
            /// failed to be merged.
            pub fn merge(&mut self, other: Self) -> Result<(), (Error, Self)> {
                // Ensure the memory maps are adjacent.
                if self.end() != other.start() {
                    return Err((Error::MustBeAdjacent, other));
                }

                // Ensure the protection attributes match.
                let region = match MemoryAreas::query(self.start()) {
                    Ok(Some(region)) => region,
                    Ok(None) => return Err((Error::AttributeMismatch, other)),
                    Err(e) => return Err((e, other)),
                };

                let other_region = match MemoryAreas::query(other.start()) {
                    Ok(Some(region)) => region,
                    Ok(None) => return Err((Error::AttributeMismatch, other)),
                    Err(e) => return Err((e, other)),
                };

                if region.protection != other_region.protection {
                    return Err((Error::AttributeMismatch, other));
                }

                if let Err(e) = self.inner.merge(&other.inner) {
                    return Err((e, other));
                }

                std::mem::forget(other);

                Ok(())
            }

            /// Splits the memory map into two at the given byte offset. The byte offset must be
            /// page size aligned.
            ///
            /// Afterwards `self` is limited to the range `[0, at)`, and the returning memory
            /// mapping is limited to `[at, len)`.
            pub fn split_off(&mut self, at: usize) -> Result<Self, Error> {
                let inner = self.inner.split_off(at)?;

                Ok(Self { inner })
            }

            /// Splits the memory map into two at the given byte offset. The byte offset must be
            /// page size aligned.
            ///
            /// Afterwards `self` is limited to the range `[at, len)`, and the returning memory
            /// mapping is limited to `[0, at)`.
            pub fn split_to(&mut self, at: usize) -> Result<Self, Error> {
                let inner = self.inner.split_to(at)?;

                Ok(Self { inner })
            }
        }
    };
}

macro_rules! mmap_impl {
    ($t:ident) => {
        impl $t {
            /// Locks the physical pages in memory such that accessing the mapping causes no page faults.
            pub fn lock(&mut self) -> Result<(), Error> {
                self.inner.lock()
            }

            /// Unlocks the physical pages in memory, allowing the operating system to swap out the pages
            /// backing this memory mapping.
            pub fn unlock(&mut self) -> Result<(), Error> {
                self.inner.unlock()
            }

            /// Flushes a range of the memory mapping, i.e. this initiates writing dirty pages
            /// within that range to the disk. Dirty pages are those whose contents have changed
            /// since the file was mapped.
            ///
            /// On Microsoft Windows, this function does not flush the file metadata. Thus, it must
            /// be followed with a call to [`File::sync_all`] to flush the file metadata. This also
            /// causes the flush operaton to be synchronous.
            ///
            /// On other platforms, the flush operation is synchronous, i.e. this waits until the
            /// flush operation completes.
            pub fn flush(&self, range: Range<usize>) -> Result<(), Error> {
                self.inner.flush(range)
            }

            /// Flushes a range of the memory mapping asynchronously, i.e. this initiates writing
            /// dirty pages within that range to the disk without waiting for the flush operation
            /// to complete. Dirty pages are those whose contents have changed since the file was
            /// mapped.
            pub fn flush_async(&self, range: Range<usize>) -> Result<(), Error> {
                self.inner.flush_async(range)
            }

            /// This function can be used to flush the instruction cache on architectures where
            /// this is required.
            ///
            /// While the x86 and x86-64 architectures guarantee cache coherency between the L1 instruction
            /// and the L1 data cache, other architectures such as Arm and AArch64 do not. If the user
            /// modified the pages, then executing the code may result in undefined behavior. To ensure
            /// correct behavior a user has to flush the instruction cache after modifying and before
            /// executing the page.
            pub fn flush_icache(&self) -> Result<(), Error> {
                self.inner.flush_icache()
            }

            /// Remaps this memory mapping as inaccessible.
            ///
            /// In case of failure, this returns the ownership of `self`.
            pub fn make_none(mut self) -> Result<MmapNone, (Self, Error)> {
                if let Err(e) = self.inner.make_none() {
                    return Err((self, e));
                }

                Ok(MmapNone { inner: self.inner })
            }

            /// Remaps this memory mapping as immutable.
            ///
            /// In case of failure, this returns the ownership of `self`. If you are
            /// not interested in this feature, you can use the implementation of
            /// the [`TryFrom`] trait instead.
            pub fn make_read_only(mut self) -> Result<Mmap, (Self, Error)> {
                if let Err(e) = self.inner.make_read_only() {
                    return Err((self, e));
                }

                Ok(Mmap { inner: self.inner })
            }

            /// Remaps this memory mapping as executable.
            ///
            /// In case of failure, this returns the ownership of `self`.
            pub fn make_exec(mut self) -> Result<Mmap, (Self, Error)> {
                if let Err(e) = self.inner.make_exec() {
                    return Err((self, e));
                }

                if let Err(e) = self.inner.flush_icache() {
                    return Err((self, e));
                }

                Ok(Mmap { inner: self.inner })
            }

            /// Remaps this memory mapping as executable, but does not flush the instruction cache.
            ///
            /// # Safety
            ///
            /// While the x86 and x86-64 architectures guarantee cache coherency between the L1 instruction
            /// and the L1 data cache, other architectures such as Arm and AArch64 do not. If the user
            /// modified the pages, then executing the code may result in undefined behavior. To ensure
            /// correct behavior a user has to flush the instruction cache after modifying and before
            /// executing the page.
            ///
            /// In case of failure, this returns the ownership of `self`.
            pub unsafe fn make_exec_no_flush(mut self) -> Result<Mmap, (Self, Error)> {
                if let Err(e) = self.inner.make_exec() {
                    return Err((self, e));
                }

                Ok(Mmap { inner: self.inner })
            }

            /// Remaps this mapping to be mutable.
            ///
            /// In case of failure, this returns the ownership of `self`. If you are
            /// not interested in this feature, you can use the implementation of
            /// the [`TryFrom`] trait instead.
            pub fn make_mut(mut self) -> Result<MmapMut, (Self, Error)> {
                if let Err(e) = self.inner.make_mut() {
                    return Err((self, e));
                }

                Ok(MmapMut { inner: self.inner })
            }

            /// Remaps this mapping to be executable and mutable.
            ///
            /// While this may seem useful for self-modifying
            /// code and JIT engines, it is instead recommended to convert between mutable and executable
            /// mappings using [`Mmap::make_mut()`] and [`MmapMut::make_exec()`] instead.
            ///
            /// Make sure to read the text below to understand the complications of this function before
            /// using it. The [`UnsafeMmapFlags::JIT`] flag must be set for this function to succeed.
            ///
            /// # Safety
            ///
            /// RWX pages are an interesting targets to attackers, e.g. for buffer overflow attacks, as RWX
            /// mappings can potentially simplify such attacks. Without RWX mappings, attackers instead
            /// have to resort to return-oriented programming (ROP) gadgets. To prevent buffer overflow
            /// attacks, contemporary CPUs allow pages to be marked as non-executable which is then used by
            /// the operating system to ensure that pages are either marked as writeable or as executable,
            /// but not both. This is also known as W^X.
            ///
            /// While the x86 and x86-64 architectures guarantee cache coherency between the L1 instruction
            /// and the L1 data cache, other architectures such as Arm and AArch64 do not. If the user
            /// modified the pages, then executing the code may result in undefined behavior. To ensure
            /// correct behavior a user has to flush the instruction cache after modifying and before
            /// executing the page.
            ///
            /// In case of failure, this returns the ownership of `self`.
            pub unsafe fn make_exec_mut(mut self) -> Result<MmapMut, (Self, Error)> {
                if let Err(e) = self.inner.make_exec_mut() {
                    return Err((self, e));
                }

                Ok(MmapMut { inner: self.inner })
            }
        }
    };
}

/// Represents an inaccessible memory mapping.
#[derive(Debug)]
pub struct MmapNone {
    inner: platform::Mmap,
}

mmap_impl!(MmapNone);
reserved_mmap_impl!(MmapNone);

/// Represents an immutable memory mapping.
#[derive(Debug)]
pub struct Mmap {
    inner: platform::Mmap,
}

mmap_impl!(Mmap);
reserved_mmap_impl!(Mmap);

impl Mmap {
    /// Extracts a slice containing the entire mapping.
    ///
    /// This is equivalent to `&mapping[..]`.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self[..]
    }
}

impl Deref for Mmap {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.size()) }
    }
}

impl AsRef<[u8]> for Mmap {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.size()) }
    }
}

impl TryFrom<MmapMut> for Mmap {
    type Error = Error;
    fn try_from(mmap_mut: MmapMut) -> Result<Self, Self::Error> {
        match mmap_mut.make_read_only() {
            Ok(mmap) => Ok(mmap),
            Err((_, e)) => Err(e),
        }
    }
}

impl TryFrom<MmapNone> for Mmap {
    type Error = Error;
    fn try_from(mmap_none: MmapNone) -> Result<Self, Self::Error> {
        match mmap_none.make_read_only() {
            Ok(mmap) => Ok(mmap),
            Err((_, e)) => Err(e),
        }
    }
}

/// Represents a mutable memory mapping.
#[derive(Debug)]
pub struct MmapMut {
    inner: platform::Mmap,
}

mmap_impl!(MmapMut);
reserved_mmap_impl!(MmapMut);

impl MmapMut {
    /// Extracts a slice containing the entire mapping.
    ///
    /// This is equivalent to `&mapping[..]`.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self[..]
    }

    /// Extracts a mutable slice containing the entire mapping.
    ///
    /// This is equivalent to `&mut mapping[..]`.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self[..]
    }
}

impl TryFrom<Mmap> for MmapMut {
    type Error = Error;
    fn try_from(mmap: Mmap) -> Result<Self, Self::Error> {
        match mmap.make_mut() {
            Ok(mmap_mut) => Ok(mmap_mut),
            Err((_, e)) => Err(e),
        }
    }
}

impl TryFrom<MmapNone> for MmapMut {
    type Error = Error;
    fn try_from(mmap_none: MmapNone) -> Result<Self, Self::Error> {
        match mmap_none.make_mut() {
            Ok(mmap_mut) => Ok(mmap_mut),
            Err((_, e)) => Err(e),
        }
    }
}

impl Deref for MmapMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.size()) }
    }
}

impl DerefMut for MmapMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.size()) }
    }
}

impl AsRef<[u8]> for MmapMut {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.size()) }
    }
}

impl AsMut<[u8]> for MmapMut {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.size()) }
    }
}

/// Represents the options for the memory mapping.
#[derive(Debug)]
pub struct MmapOptions<'a> {
    inner: platform::MmapOptions<'a>,
}

impl<'a> MmapOptions<'a> {
    /// Constructs the `MmapOptions` builder. The size specified is the size of the mapping to be
    /// allocated in bytes.
    pub fn new(size: usize) -> Result<Self, Error> {
        Ok(Self {
            inner: platform::MmapOptions::new(size)?,
        })
    }

    /// Returns the smallest possible page size for the current platform. The allocation size must
    /// be aligned to the page size for the allocation to succeed.
    pub fn page_size() -> usize {
        platform::MmapOptions::page_size()
    }

    /// Returns the set of supported page sizes for the current platform.
    pub fn page_sizes() -> Result<PageSizes, Error> {
        platform::MmapOptions::page_sizes()
    }

    /// Returns the allocation granularity for the current platform. On some platforms the
    /// allocation granularity may be a multiple of the page size. The start address of the
    /// allocation must be aligned to `max(allocation_granularity, page_size)`.
    pub fn allocation_granularity() -> usize {
        platform::MmapOptions::allocation_granularity()
    }

    /// The desired address at which the memory should be mapped.
    pub fn with_address(self, address: usize) -> Self {
        Self {
            inner: self.inner.with_address(address),
        }
    }

    /// Whether the memory mapping should be backed by a [`File`] or not. If the memory mapping
    /// should be mapped by a [`File`], then the user can also specify the offset within the file
    /// at which the mapping should start.
    ///
    /// On Microsoft Windows, it may not be possible to extend the protection beyond the access
    /// mask that has been used to open the file. For instance, if a file has been opened with read
    /// access, then [`Mmap::make_mut()`] will not work. Furthermore, [`std::fs::OpenOptions`] does
    /// not in itself provide a standardized way to open the file with executable access. However,
    /// if the file is not opened with executable access, then it may not be possible to use
    /// [`Mmap::make_exec()`]. Fortunately, Rust provides [`OpenOptionsExt`] that allows you to
    /// open the file with executable access rights. See [`access_mode`] for more information.
    ///
    /// # Safety
    ///
    /// This function is marked as **unsafe** as the user should be aware that even in the case
    /// that a file is mapped as immutable in the address space of the current process, it does not
    /// guarantee that there does not exist any other mutable mapping to the file.
    ///
    /// On Microsoft Windows, it is possible to limit the access to shared reading or to be fully
    /// exclusive using [`share_mode`].
    ///
    /// On most Unix systems, it is possible to use [`nix::fcntl::flock`]. However, keep in mind
    /// that this provides an **advisory** locking scheme, and that implementations are therefore
    /// required to be co-operative.
    ///
    /// On Linux, it is also possible to mark the file as immutable. See `man 2 ioctl_iflags` and
    /// `man 1 chattr` for more information.
    ///
    /// [`OpenOptionsExt`]: https://doc.rust-lang.org/std/os/windows/fs/trait.OpenOptionsExt.html
    /// [`access_mode`]: https://doc.rust-lang.org/std/os/windows/fs/trait.OpenOptionsExt.html#tymethod.access_mode
    /// [`share_mode`]: https://doc.rust-lang.org/std/os/windows/fs/trait.OpenOptionsExt.html#tymethod.share_mode
    /// [`nix::fcntl::flock`]: https://docs.rs/nix/latest/nix/fcntl/fn.flock.html
    pub unsafe fn with_file(self, file: &'a File, offset: u64) -> Self {
        Self {
            inner: self.inner.with_file(file, offset),
        }
    }

    /// The desired configuration of the mapping. See [`MmapFlags`] for available options.
    pub fn with_flags(self, flags: MmapFlags) -> Self {
        Self {
            inner: self.inner.with_flags(flags),
        }
    }

    /// The desired configuration of the mapping. See [`UnsafeMmapFlags`] for available options.
    ///
    /// # Safety
    ///
    /// The flags that can be passed to this function have unsafe behavior associated with them.
    pub unsafe fn with_unsafe_flags(self, flags: UnsafeMmapFlags) -> Self {
        Self {
            inner: self.inner.with_unsafe_flags(flags),
        }
    }

    /// Whether this memory mapped should be backed by a specific page size or not.
    pub fn with_page_size(self, page_size: PageSize) -> Self {
        Self {
            inner: self.inner.with_page_size(page_size),
        }
    }

    /// Reserves inaccessible memory.
    pub fn reserve_none(self) -> Result<ReservedNone, Error> {
        Ok(ReservedNone {
            inner: self.inner.reserve_none()?,
        })
    }

    /// Reserves immutable memory.
    pub fn reserve(self) -> Result<Reserved, Error> {
        Ok(Reserved {
            inner: self.inner.reserve()?,
        })
    }

    /// Reserves executable memory.
    pub fn reserve_exec(self) -> Result<Reserved, Error> {
        Ok(Reserved {
            inner: self.inner.reserve_exec()?,
        })
    }

    /// Reserves mutable memory.
    pub fn reserve_mut(self) -> Result<ReservedMut, Error> {
        Ok(ReservedMut {
            inner: self.inner.reserve_mut()?,
        })
    }

    /// Reserves executable and mutable memory.
    ///
    /// # Safety
    ///
    /// See [`MmapOptions::map_exec_mut`] for more information.
    pub unsafe fn reserve_exec_mut(self) -> Result<ReservedMut, Error> {
        Ok(ReservedMut {
            inner: self.inner.reserve_exec_mut()?,
        })
    }

    /// Maps the memory as inaccessible.
    pub fn map_none(self) -> Result<MmapNone, Error> {
        Ok(MmapNone {
            inner: self.inner.map_none()?,
        })
    }

    /// Maps the memory as immutable.
    pub fn map(self) -> Result<Mmap, Error> {
        Ok(Mmap {
            inner: self.inner.map()?,
        })
    }

    /// Maps the memory as executable.
    pub fn map_exec(self) -> Result<Mmap, Error> {
        Ok(Mmap {
            inner: self.inner.map_exec()?,
        })
    }

    /// Maps the memory as mutable.
    pub fn map_mut(self) -> Result<MmapMut, Error> {
        Ok(MmapMut {
            inner: self.inner.map_mut()?,
        })
    }

    /// Maps the memory as executable and mutable. While this may seem useful for self-modifying
    /// code and JIT engines, it is instead recommended to convert between mutable and executable
    /// mappings using [`Mmap::make_mut()`] and [`MmapMut::make_exec()`] instead.
    ///
    /// Make sure to read the text below to understand the complications of this function before
    /// using it. The [`UnsafeMmapFlags::JIT`] flag must be set for this function to succeed.
    ///
    /// # Safety
    ///
    /// RWX pages are an interesting targets to attackers, e.g. for buffer overflow attacks, as RWX
    /// mappings can potentially simplify such attacks. Without RWX mappings, attackers instead
    /// have to resort to return-oriented programming (ROP) gadgets. To prevent buffer overflow
    /// attacks, contemporary CPUs allow pages to be marked as non-executable which is then used by
    /// the operating system to ensure that pages are either marked as writeable or as executable,
    /// but not both. This is also known as W^X.
    ///
    /// While the x86 and x86-64 architectures guarantee cache coherency between the L1 instruction
    /// and the L1 data cache, other architectures such as Arm and AArch64 do not. If the user
    /// modified the pages, then executing the code may result in undefined behavior. To ensure
    /// correct behavior a user has to flush the instruction cache after  modifying and before
    /// executing the page.
    pub unsafe fn map_exec_mut(self) -> Result<MmapMut, Error> {
        Ok(MmapMut {
            inner: self.inner.map_exec_mut()?,
        })
    }
}

macro_rules! reserved_impl {
    ($t:ident) => {
        impl $t {
            /// Returns `true` if the memory mapping is size 0.
            #[inline]
            pub fn is_empty(&self) -> bool {
                self.inner.size() == 0
            }

            /// Yields the length of this mapping.
            #[inline]
            pub fn len(&self) -> usize {
                self.inner.size()
            }

            /// Remaps this memory mapping as inaccessible.
            ///
            /// In case of failure, this returns the ownership of `self`. If you are
            /// not interested in this feature, you can use the implementation of
            /// the [`TryFrom`] trait instead.
            pub fn make_none(mut self) -> Result<ReservedNone, (Self, Error)> {
                if let Err(e) = self.inner.make_none() {
                    return Err((self, e));
                }

                Ok(ReservedNone { inner: self.inner })
            }

            /// Remaps this memory mapping as immutable.
            ///
            /// In case of failure, this returns the ownership of `self`. If you are
            /// not interested in this feature, you can use the implementation of
            /// the [`TryFrom`] trait instead.
            pub fn make_read_only(mut self) -> Result<Reserved, (Self, Error)> {
                if let Err(e) = self.inner.make_read_only() {
                    return Err((self, e));
                }

                Ok(Reserved { inner: self.inner })
            }

            /// Remaps this memory mapping as executable.
            ///
            /// In case of failure, this returns the ownership of `self`.
            pub fn make_exec(mut self) -> Result<Reserved, (Self, Error)> {
                if let Err(e) = self.inner.make_exec() {
                    return Err((self, e));
                }

                if let Err(e) = self.inner.flush_icache() {
                    return Err((self, e));
                }

                Ok(Reserved { inner: self.inner })
            }

            /// Remaps this memory mapping as executable, but does not flush the instruction cache.
            ///
            /// # Safety
            ///
            /// While the x86 and x86-64 architectures guarantee cache coherency between the L1 instruction
            /// and the L1 data cache, other architectures such as Arm and AArch64 do not. If the user
            /// modified the pages, then executing the code may result in undefined behavior. To ensure
            /// correct behavior a user has to flush the instruction cache after modifying and before
            /// executing the page.
            ///
            /// In case of failure, this returns the ownership of `self`.
            pub unsafe fn make_exec_no_flush(mut self) -> Result<Reserved, (Self, Error)> {
                if let Err(e) = self.inner.make_exec() {
                    return Err((self, e));
                }

                Ok(Reserved { inner: self.inner })
            }

            /// Remaps this mapping to be mutable.
            ///
            /// In case of failure, this returns the ownership of `self`. If you are
            /// not interested in this feature, you can use the implementation of
            /// the [`TryFrom`] trait instead.
            pub fn make_mut(mut self) -> Result<ReservedMut, (Self, Error)> {
                if let Err(e) = self.inner.make_mut() {
                    return Err((self, e));
                }

                Ok(ReservedMut { inner: self.inner })
            }

            /// Remaps this mapping to be executable and mutable.
            ///
            /// While this may seem useful for self-modifying
            /// code and JIT engines, it is instead recommended to convert between mutable and executable
            /// mappings using [`Mmap::make_mut()`] and [`MmapMut::make_exec()`] instead.
            ///
            /// Make sure to read the text below to understand the complications of this function before
            /// using it. The [`UnsafeMmapFlags::JIT`] flag must be set for this function to succeed.
            ///
            /// # Safety
            ///
            /// RWX pages are an interesting targets to attackers, e.g. for buffer overflow attacks, as RWX
            /// mappings can potentially simplify such attacks. Without RWX mappings, attackers instead
            /// have to resort to return-oriented programming (ROP) gadgets. To prevent buffer overflow
            /// attacks, contemporary CPUs allow pages to be marked as non-executable which is then used by
            /// the operating system to ensure that pages are either marked as writeable or as executable,
            /// but not both. This is also known as W^X.
            ///
            /// While the x86 and x86-64 architectures guarantee cache coherency between the L1 instruction
            /// and the L1 data cache, other architectures such as Arm and AArch64 do not. If the user
            /// modified the pages, then executing the code may result in undefined behavior. To ensure
            /// correct behavior a user has to flush the instruction cache after modifying and before
            /// executing the page.
            ///
            /// In case of failure, this returns the ownership of `self`.
            pub unsafe fn make_exec_mut(mut self) -> Result<ReservedMut, (Self, Error)> {
                if let Err(e) = self.inner.make_exec_mut() {
                    return Err((self, e));
                }

                Ok(ReservedMut { inner: self.inner })
            }
        }
    };
}

/// Represents an inaccessible memory mapping in a reserved state, i.e. a memory mapping that is not
/// backed by any physical pages yet.
#[derive(Debug)]
pub struct ReservedNone {
    inner: platform::Mmap,
}

reserved_impl!(ReservedNone);
reserved_mmap_impl!(ReservedNone);

impl TryFrom<ReservedNone> for MmapNone {
    type Error = Error;

    fn try_from(mut reserved_none: ReservedNone) -> Result<MmapNone, Error> {
        reserved_none.inner.commit()?;

        Ok(MmapNone {
            inner: reserved_none.inner,
        })
    }
}

impl TryFrom<ReservedMut> for Reserved {
    type Error = Error;
    fn try_from(mmap_mut: ReservedMut) -> Result<Self, Self::Error> {
        match mmap_mut.make_read_only() {
            Ok(mmap) => Ok(mmap),
            Err((_, e)) => Err(e),
        }
    }
}

impl TryFrom<ReservedNone> for Reserved {
    type Error = Error;
    fn try_from(mmap_none: ReservedNone) -> Result<Self, Self::Error> {
        match mmap_none.make_read_only() {
            Ok(mmap) => Ok(mmap),
            Err((_, e)) => Err(e),
        }
    }
}

/// Represents an immutable memory mapping in a reserved state, i.e. a memory mapping that is not
/// backed by any physical pages yet.
#[derive(Debug)]
pub struct Reserved {
    inner: platform::Mmap,
}

reserved_impl!(Reserved);
reserved_mmap_impl!(Reserved);

impl TryFrom<Reserved> for Mmap {
    type Error = Error;

    fn try_from(mut reserved: Reserved) -> Result<Mmap, Error> {
        reserved.inner.commit()?;

        Ok(Mmap {
            inner: reserved.inner,
        })
    }
}

/// Represents a mutable memory mapping in a reserved state, i.e. a memory mapping that is not
/// backed by any physical pages yet.
#[derive(Debug)]
pub struct ReservedMut {
    inner: platform::Mmap,
}

reserved_impl!(ReservedMut);
reserved_mmap_impl!(ReservedMut);

impl TryFrom<ReservedMut> for MmapMut {
    type Error = Error;

    fn try_from(mut reserved_mut: ReservedMut) -> Result<MmapMut, Error> {
        reserved_mut.inner.commit()?;

        Ok(MmapMut {
            inner: reserved_mut.inner,
        })
    }
}

impl TryFrom<Reserved> for ReservedMut {
    type Error = Error;
    fn try_from(mmap: Reserved) -> Result<Self, Self::Error> {
        match mmap.make_mut() {
            Ok(mmap_mut) => Ok(mmap_mut),
            Err((_, e)) => Err(e),
        }
    }
}

impl TryFrom<ReservedNone> for ReservedMut {
    type Error = Error;
    fn try_from(mmap_none: ReservedNone) -> Result<Self, Self::Error> {
        match mmap_none.make_mut() {
            Ok(mmap_mut) => Ok(mmap_mut),
            Err((_, e)) => Err(e),
        }
    }
}
