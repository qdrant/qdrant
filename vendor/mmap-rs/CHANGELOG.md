# Changelog

All notable changes to mmap-rs will be documented in this file.

## 0.6.1

- Implemented `TryFrom` for `Mmap` and `Reserved` objects for improved ergonomics.
- Added the MSRV (1.67).

## 0.6.0

- Implemented `Debug` for all public types.
- Added `MemoryAreas::query()` and related functions to query a specific address or address range 
- Added `Mmap::split_off()` and related functions to split an existing memory mapping at a page boundary.
- Added `Mmap::merge()` to merge adjacent memory mappings into one.
- Added `MmapOptions::reserve()` and related functions to reserve memory mappings instead of committing them.
- Changed `MmapOptions::with_file()` to require a reference to the file instead of taking ownership instead.
- Documented flags that may have no operation on platforms that do not support the flag, such as `MmapFlags::STACK` on Microsoft Windows.
- Changed `MemoryAreas::query()` to use `mach_vm_region_recurse()` on MacOS to recurse into submappings.
- Changed `MmapFlags::COPY_ON_WRITE` to `MmapFlags::SHARED`, such that private memory mappings are the default and since copy-on-write semantics only make sense for file mappings.
- Fixed an issue where `MemoryAreas::query()` and related functions returned the wrong `ShareMode`.
- Removed `ShareMode::CopyOnWrite` and use `ShareMode::Private` instead.
- Extended `MemoryAreas::query()` to return the allocation base of the memory mapping.
- Updated the windows crate from version 0.44 to 0.48.

## 0.5.0

- Implemented `MmapOptions::page_sizes()` to return the supported page sizes for the platform.
- Separated the functions to get the page size and the allocation granularity.
- Updated the windows crate from version 0.39 to 0.44.
- Changed `MmapOptions::new()` to return `Result<Self, Error>` rather than `Self` to support the use of `NonZeroUsize` in nix.
- Updated the nix crate from version 0.24 to 0.26.
- Added support for `i686-linux-android`, `aarch64-linux-android`, `x86_64-linux-android` and `armv7-linux-androideabi`.
- Added `MapFlags::TRANSPARENT_HUGE_PAGES` to hint the kernel that it may merge pages within the mapping into huge pages if possible when set to `madvise` mode.
- `MmapOptions::with_flags()` appends the flags instead of overriding them.
- Implemented `Send` and `Sync` for `Mmap`.

## 0.4.0

- Added support for `i686-pc-windows-msvc`, `aarch64-pc-windows-msvc`, `aarch64-apple-ios`, `x86_64-apple-ios`, `armv7a-unknown-linux-gnueabihf`, `aarch64-unknown-linux-gnu` and `i686-unknown-linux-gnu`.
- Updated various dependencies.
- Relicensed under MIT and Apache.
- Added the `MemoryAreas` iterator to iterate over the memory areas of a process.
- Changed `UnsafeMmapFlags::MAP_JIT` to `UnsafeMmapFlags::JIT` to fix compilation on Mac OS X.
