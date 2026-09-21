//! `Arr<T>`: a read-only flat array that is either **owned** (a `Vec`, the shape everything had
//! before) or a **window into an mmap** of the file it came from.
//!
//! Every bulk array of a finalized session -- the block codes, the fp16 block means and norms,
//! the CSR level-0 links, the derived node records, the bf16 page blobs -- is written to disk as
//! a plain little-endian run of POD values with no compression and no varints
//! (`crate::persist`), which is exactly the shape a mapping wants. With `--mmap on` a restored
//! session therefore *maps* its files instead of reading them into fresh allocations, and the
//! resident set of a session becomes "the pages the requests actually touched" instead of "every
//! byte the session holds". A session that is still being uploaded, and everything built in
//! memory by `FinalizeSession`, stays owned.
//!
//! Three rules make this safe and cheap:
//!
//! * **the mapping outlives the window** -- every `Arr` that points into a mapping holds an
//!   `Arc<Mmap>` of the whole file, so the last array to go drops the mapping;
//! * **alignment is a property of the format** -- the writer pads every bulk array so its first
//!   byte sits on an 8-byte boundary (64 for the node records, which want a cache line), and an
//!   mmap starts on a page boundary, so a mapped window is always correctly aligned. The reader
//!   checks anyway and falls back to a copy if it ever is not;
//! * **mutation is an owned-only operation** -- [`Arr::as_mut_slice`] panics on a mapped array.
//!   The index build is the only writer of these arrays and it always builds owned ones.
//!
//! Night 6 added a third shape between "owned" and "mapped": a [`Blob::Pinned`] window, i.e. a
//! window into a heap buffer the store read a whole file into on purpose (`--pin-mb`, see
//! [`crate::store::PinTable`]). It is a window like a mapped one -- same reader, same zero-copy
//! clone, same `Arc` keeping the bytes alive -- and it costs heap like an owned one, which is
//! exactly what the pin budget is spent on. The only two differences are that `madvise` on it is
//! a no-op and that [`Arr::heap_bytes`] counts it.

use std::io;
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[cfg(unix)]
pub use memmap2::Advice;
/// `madvise` does not exist off unix; the store only *builds* there (development on Windows),
/// every advice call is a no-op and the server is not meant to serve from such a build.
#[cfg(not(unix))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Advice {
    Normal,
    Random,
    Sequential,
    WillNeed,
    HugePage,
}
use memmap2::Mmap;

// ---------------------------------------------------------------------------------------------
// madvise policy (`--madvise on|off`) and range prefetch (`--prefetch on|off`)
// ---------------------------------------------------------------------------------------------
//
// Both are process-wide switches set once from the command line, because that is exactly what
// they are: a mapping's advice is a property of how the *server* was started, not of a session or
// a request, and every place that maps a file already knows which of the four roles below the
// file plays. Keeping them as two atomics (instead of threading a config through `Store` ->
// `Session` -> `load_payload` -> four readers) is what makes an A/B run a flag flip.
static MADVISE: std::sync::atomic::AtomicU8 =
    std::sync::atomic::AtomicU8::new(Madvise::Bulk as u8);
static PREFETCH: AtomicBool = AtomicBool::new(true);

/// `--madvise`: which policy the store applies to a mapped session file.
///
/// There are two defensible policies and the difference between them is *measured*, not assumed
/// (`bench/night3-io.txt`, table A), which is why both are here:
///
/// * [`Madvise::Random`] is the policy the research suggested: `MADV_RANDOM` for the node
///   records, the HNSW links and the bf16 page logs, `MADV_SEQUENTIAL` for the page-scan block
///   layout, `MADV_HUGEPAGE` for the codes. Its premise is that a beam of width `ef` touches a
///   few thousand scattered records and read-ahead around each of them is waste.
/// * [`Madvise::Bulk`] is what the measurement says instead: `MADV_SEQUENTIAL` on the page logs
///   (the bulk reader of those is `StreamKv`, in order) and nothing at all on the rest, i.e.
///   the kernel's default 128 KB read-ahead window.
///
/// The premise of `Random` turns out to be false at these sizes, and arithmetic says why: a
/// 97k-key head's records are 6.6 MB, which is 1613 pages holding 60 records each, and a beam
/// touches ~4200 records -- so it touches *every page of the file*. `MADV_RANDOM` then does not
/// save any IO, it only forbids the kernel from batching it, and one page per round trip on a
/// latency-bound disk is what makes it slow. On the page logs it is worse than that: it turns a
/// `StreamKv` into a 4 KB-at-a-time read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Madvise {
    /// No advice at all: whatever the kernel's defaults do.
    Off = 0,
    /// `MADV_RANDOM` / `MADV_SEQUENTIAL` / `MADV_HUGEPAGE` per role, as first designed.
    Random = 1,
    /// The measured policy: sequential where a bulk reader exists, defaults everywhere else.
    Bulk = 2,
}

impl Madvise {
    pub fn parse(s: &str) -> Option<Madvise> {
        match s {
            "off" | "0" | "false" | "none" => Some(Madvise::Off),
            "random" => Some(Madvise::Random),
            "bulk" | "on" | "1" | "true" => Some(Madvise::Bulk),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Madvise::Off => "off",
            Madvise::Random => "random",
            Madvise::Bulk => "bulk",
        }
    }
}

pub fn set_madvise(mode: Madvise) {
    MADVISE.store(mode as u8, Ordering::Relaxed);
}

pub fn madvise_mode() -> Madvise {
    match MADVISE.load(Ordering::Relaxed) {
        0 => Madvise::Off,
        1 => Madvise::Random,
        _ => Madvise::Bulk,
    }
}

/// `--prefetch on|off`: does the store issue `MADV_WILLNEED` on the ranges it is about to read?
pub fn set_prefetch(on: bool) {
    PREFETCH.store(on, Ordering::Relaxed);
}

pub fn prefetch_enabled() -> bool {
    PREFETCH.load(Ordering::Relaxed)
}

/// What a mapped session file is *for*, which is what decides its `madvise`.
///
/// The four roles read very differently, and on a cold session the difference is the whole cost:
///
/// * [`MapUse::Random`] -- the node records, the HNSW upper levels and the CSR level 0, walked by
///   a beam;
/// * [`MapUse::Sequential`] -- the page-scan block layout, read end to end by construction;
/// * [`MapUse::Codes`] -- the block codes, read both ways (sequentially by the scan, at random
///   through the records by the beam);
/// * [`MapUse::Pages`] -- the bf16 key and value logs: `R` scattered rows per query for the exact
///   rescoring, whole pages in ascending order for `StreamKv`.
///
/// What each role is told depends on [`Madvise`]; `--prefetch` adds one `MADV_WILLNEED` over the
/// whole mapping for the two roles a request reads most of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapUse {
    Random,
    Sequential,
    Codes,
    Pages,
    /// The page-major generation (`pages/*.pages`, `crate::pages`): 4 KiB pages faulted one at
    /// a time by a beam that asks for the next expansion's pages itself
    /// (`PagesHead::advise_pages`). Always `MADV_RANDOM`, never a whole-file `WILLNEED` -- the
    /// cold-start claim of that mode is "reads the pages it visits", and the first night
    /// matrix showed a whole-file prefetch quietly reading 913 MB behind a 145-page beam.
    PageMajor,
}

impl MapUse {
    /// The advice list for this role under the current mode, in the order it must be applied.
    fn advice(self) -> Vec<Advice> {
        let mut v = match (madvise_mode(), self) {
            (Madvise::Off, _) => Vec::new(),
            (Madvise::Random, MapUse::Random | MapUse::Pages) => vec![Advice::Random],
            (Madvise::Random | Madvise::Bulk, MapUse::PageMajor) => vec![Advice::Random],
            (Madvise::Random, MapUse::Sequential) => {
                vec![Advice::HugePage, Advice::Sequential]
            }
            (Madvise::Random, MapUse::Codes) => vec![Advice::HugePage],
            (Madvise::Bulk, MapUse::Pages | MapUse::Sequential) => vec![Advice::Sequential],
            (Madvise::Bulk, MapUse::Random | MapUse::Codes) => Vec::new(),
        };
        // `--prefetch`: one syscall that starts the whole file moving, for the two roles a
        // request reads (nearly) all of -- the records/links a beam walks and the block layout a
        // scan streams. It populates the page cache, not the heap, so the resident set of the
        // process is unchanged.
        if prefetch_enabled() && matches!(self, MapUse::Random | MapUse::Sequential) {
            v.push(Advice::WillNeed);
        }
        v
    }
}

/// Apply a role's advice to a whole mapping. Best effort: `MADV_HUGEPAGE` on a file mapping is
/// refused by kernels built without `CONFIG_READ_ONLY_THP_FOR_FS`, and a refused hint is not an
/// error -- the mapping is correct either way. Failures are counted, and the count is in the
/// startup line so a run that got none of what it asked for cannot be mistaken for one that did.
fn advise_all(map: &Blob, use_: MapUse) {
    for a in use_.advice() {
        if map.advise_all(a).is_err() {
            ADVISE_FAILED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

static ADVISE_FAILED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many `madvise` calls the kernel refused (see [`advise_all`]).
pub fn advise_failures() -> u64 {
    ADVISE_FAILED.load(Ordering::Relaxed)
}

/// Types the store blits to and from disk verbatim: plain little-endian numbers, no padding.
pub trait Pod: Copy + Default {}
impl Pod for u8 {}
impl Pod for u16 {}
impl Pod for u32 {}
impl Pod for i32 {}
impl Pod for u64 {}
impl Pod for f32 {}

/// `&[T]` -> `&[u8]` for a POD slice (the on-disk form).
pub fn pod_bytes<T: Pod>(v: &[T]) -> &[u8] {
    // SAFETY: `T` is a plain integer/float with no padding and the slice covers the same
    // allocation; the format is little-endian only (asserted in `crate::persist`).
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

/// `&mut [T]` -> `&mut [u8]`, for reading a whole array with one `read_exact`.
pub fn pod_bytes_mut<T: Pod>(v: &mut [T]) -> &mut [u8] {
    unsafe { std::slice::from_raw_parts_mut(v.as_mut_ptr() as *mut u8, std::mem::size_of_val(v)) }
}

/// A heap buffer aligned like a mapping's first byte.
///
/// A `Vec<u8>` is 1-byte aligned, and the format's strictest array is the node records' code
/// table, which `NodeRecords::from_parts` *refuses* unless it starts on a 64-byte boundary (one
/// cache line per key is the whole point of that layout). An mmap starts on a page boundary and
/// therefore satisfies every alignment the writer padded for; a pinned copy has to do the same,
/// so it is allocated as 64-byte chunks and the file's own padding does the rest.
pub struct Aligned {
    chunks: Vec<Align64>,
    len: usize,
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Align64([u8; 64]);

impl Aligned {
    /// A zeroed buffer of `len` bytes, 64-byte aligned.
    fn zeroed(len: usize) -> Aligned {
        Aligned { chunks: vec![Align64([0u8; 64]); len.div_ceil(64)], len }
    }

    fn as_mut(&mut self) -> &mut [u8] {
        // SAFETY: `chunks` is a contiguous run of `64 * chunks.len() >= len` bytes of POD.
        unsafe { std::slice::from_raw_parts_mut(self.chunks.as_mut_ptr() as *mut u8, self.len) }
    }

    fn as_ref(&self) -> &[u8] {
        // SAFETY: as above.
        unsafe { std::slice::from_raw_parts(self.chunks.as_ptr() as *const u8, self.len) }
    }
}

/// The bytes an [`Arr`] window points into: a **mapped** file, or a file the store read into a
/// **pinned** heap buffer (`--pin-mb`, [`crate::store::PinTable`]).
///
/// The two are deliberately the same shape, because the reader that turns a payload file into
/// arrays ([`crate::persist`]'s `MapRd`) is the same code either way: it takes header words out
/// of the bytes and hands every bulk array out as a window. What differs is who owns the pages
/// -- the page cache, or this process's heap -- and that is the only thing the pin budget is
/// about.
pub enum Blob {
    Mapped(Mmap),
    Pinned(Aligned),
}

impl Blob {
    /// Is this the page cache's copy (i.e. free of heap) rather than a pinned one?
    pub fn is_mapped(&self) -> bool {
        matches!(self, Blob::Mapped(_))
    }

    /// `madvise` a byte range of a mapping; a no-op (and `false`) for a pinned buffer, which is
    /// already exactly as resident as it will ever be.
    pub fn advise_range(&self, advice: Advice, off: usize, len: usize) -> bool {
        match self {
            #[cfg(unix)]
            Blob::Mapped(m) => m.advise_range(advice, off, len).is_ok(),
            #[cfg(not(unix))]
            Blob::Mapped(_) => {
                let _ = (advice, off, len);
                false
            }
            Blob::Pinned(_) => false,
        }
    }

    pub fn advise_all(&self, advice: Advice) -> Result<(), io::Error> {
        match self {
            #[cfg(unix)]
            Blob::Mapped(m) => m.advise(advice),
            #[cfg(not(unix))]
            Blob::Mapped(_) => {
                let _ = advice;
                Ok(())
            }
            Blob::Pinned(_) => Ok(()),
        }
    }
}

impl Deref for Blob {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Blob::Mapped(m) => m,
            Blob::Pinned(a) => a.as_ref(),
        }
    }
}

enum Inner<T: Pod> {
    Owned(Vec<T>),
    /// A window `[ptr, ptr + len)` inside `blob` (a mapping, or a pinned heap buffer).
    Mapped {
        blob: Arc<Blob>,
        ptr: *const T,
        len: usize,
    },
}

/// A flat POD array: owned, or a window into a mapped file.
pub struct Arr<T: Pod> {
    inner: Inner<T>,
}

// SAFETY: the mapped variant is a read-only window kept alive by its own `Arc<Mmap>`, and `Mmap`
// is `Send + Sync`; nothing here hands out interior mutability.
unsafe impl<T: Pod + Send> Send for Arr<T> {}
unsafe impl<T: Pod + Sync> Sync for Arr<T> {}

impl<T: Pod> Arr<T> {
    pub fn from_vec(v: Vec<T>) -> Arr<T> {
        Arr { inner: Inner::Owned(v) }
    }

    pub fn empty() -> Arr<T> {
        Arr { inner: Inner::Owned(Vec::new()) }
    }

    /// A window of `len` values starting at byte offset `off` of `blob`.
    ///
    /// Returns `None` when the window would leave the blob or is not aligned for `T` -- the
    /// caller then falls back to a copy, so a hand-edited file can never produce a bad slice.
    pub fn window(map: &Arc<Blob>, off: usize, len: usize) -> Option<Arr<T>> {
        let bytes = len.checked_mul(std::mem::size_of::<T>())?;
        if off.checked_add(bytes)? > map.len() {
            return None;
        }
        let base = map.as_ptr();
        // SAFETY: `off + bytes <= map.len()`, checked above.
        let ptr = unsafe { base.add(off) };
        if !(ptr as usize).is_multiple_of(std::mem::align_of::<T>()) {
            return None;
        }
        Some(Arr {
            inner: Inner::Mapped { blob: map.clone(), ptr: ptr as *const T, len },
        })
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        match &self.inner {
            Inner::Owned(v) => v,
            // SAFETY: the window was bounds- and alignment-checked in `window`, the mapping is
            // kept alive by the `Arc`, and the file is mapped read-only.
            Inner::Mapped { ptr, len, .. } => unsafe { std::slice::from_raw_parts(*ptr, *len) },
        }
    }

    /// Mutable access -- owned arrays only (the index build never mutates a mapped graph).
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        match &mut self.inner {
            Inner::Owned(v) => v,
            Inner::Mapped { .. } => panic!("Arr::as_mut_slice on a mapped (read-only) array"),
        }
    }

    /// Is this array a window into a mapped file (i.e. not counted in the heap)?
    pub fn is_mapped(&self) -> bool {
        match &self.inner {
            Inner::Owned(_) => false,
            Inner::Mapped { blob, .. } => blob.is_mapped(),
        }
    }

    /// Is this array a window into a **pinned** heap buffer (`--pin-mb`)? Neither mapped nor
    /// owned: the bytes are this process's, but they are shared with every other array of the
    /// same file and with the pin table that keeps them.
    pub fn is_pinned(&self) -> bool {
        match &self.inner {
            Inner::Owned(_) => false,
            Inner::Mapped { blob, .. } => !blob.is_mapped(),
        }
    }

    /// `madvise` exactly this window -- what [`crate::persist`] uses to say "I am about to read
    /// these pages" (`MADV_WILLNEED`) or "I am done with them" (`MADV_DONTNEED`).
    ///
    /// A no-op for an owned array and when `--prefetch` is off. Returns whether the kernel took
    /// it. The range is rounded outwards to page boundaries by `madvise` itself, so advising a
    /// 512 B row advises its 4 KB page, which is the point.
    pub fn advise(&self, advice: Advice) -> bool {
        if !prefetch_enabled() {
            return false;
        }
        match &self.inner {
            Inner::Owned(_) => false,
            Inner::Mapped { blob, ptr, len } => {
                let base = blob.as_ptr() as usize;
                let off = (*ptr as usize).saturating_sub(base);
                let bytes = len * std::mem::size_of::<T>();
                blob.advise_range(advice, off, bytes)
            }
        }
    }

    /// `madvise` the sub-range `[start, start + len)` (element indices) of this window.
    pub fn advise_at(&self, start: usize, len: usize, advice: Advice) -> bool {
        if !prefetch_enabled() || start + len > self.len() {
            return false;
        }
        match &self.inner {
            Inner::Owned(_) => false,
            Inner::Mapped { blob, ptr, .. } => {
                let base = blob.as_ptr() as usize;
                let sz = std::mem::size_of::<T>();
                let off = (*ptr as usize).saturating_sub(base) + start * sz;
                blob.advise_range(advice, off, len * sz)
            }
        }
    }

    /// Bytes of **heap** this array costs: 0 when it is mapped (the page cache owns those, and
    /// only the pages that were touched are resident), its own bytes when it is owned or pinned.
    ///
    /// A pinned window counts, even though several windows of one file share one buffer: the
    /// windows of a file partition it (the format writes one array after another, with at most
    /// `ALIGN` bytes of padding between them), so summing them is the file's size to within a
    /// few bytes per array, and the pin budget is charged the file's size on top of that
    /// ([`crate::store::PinTable`]).
    pub fn heap_bytes(&self) -> usize {
        match &self.inner {
            Inner::Owned(v) => std::mem::size_of_val(&v[..]),
            Inner::Mapped { blob, len, .. } => {
                if blob.is_mapped() {
                    0
                } else {
                    len * std::mem::size_of::<T>()
                }
            }
        }
    }
}

impl<T: Pod> Deref for Arr<T> {
    type Target = [T];
    #[inline]
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T: Pod> Clone for Arr<T> {
    /// Cloning a mapped array clones the window (one `Arc` bump), not the bytes.
    fn clone(&self) -> Arr<T> {
        match &self.inner {
            Inner::Owned(v) => Arr::from_vec(v.clone()),
            Inner::Mapped { blob, ptr, len } => Arr {
                inner: Inner::Mapped { blob: blob.clone(), ptr: *ptr, len: *len },
            },
        }
    }
}

impl<T: Pod> Default for Arr<T> {
    fn default() -> Arr<T> {
        Arr::empty()
    }
}

impl<T: Pod + std::fmt::Debug> std::fmt::Debug for Arr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Arr(mapped={}, len={})", self.is_mapped(), self.len())
    }
}

impl<T: Pod> From<Vec<T>> for Arr<T> {
    fn from(v: Vec<T>) -> Arr<T> {
        Arr::from_vec(v)
    }
}

/// Map a whole file read-only, and tell the kernel how it is going to be read.
pub fn map_file_for(path: &Path, use_: MapUse) -> io::Result<Arc<Blob>> {
    let f = std::fs::File::open(path)?;
    // SAFETY: the store only ever maps its own session files, which are immutable once the
    // manifest is finalized; a concurrent truncation would be a foreign process editing the
    // data dir underneath a running server, which is out of contract either way.
    let map = Blob::Mapped(unsafe { Mmap::map(&f)? });
    advise_all(&map, use_);
    Ok(Arc::new(map))
}

/// Map a whole file read-only with no advice (the shape everything had before `--madvise`).
pub fn map_file(path: &Path) -> io::Result<Arc<Blob>> {
    let f = std::fs::File::open(path)?;
    // SAFETY: see `map_file_for`.
    Ok(Arc::new(Blob::Mapped(unsafe { Mmap::map(&f)? })))
}

/// Read a whole file into a **pinned**, 64-byte aligned heap buffer -- the `--pin-mb` half of
/// [`Blob`].
///
/// One `read_exact` of the whole file, which is the cheapest way to get it: the file is a plain
/// run of arrays with no parsing, and the pages are wanted in the heap anyway, so there is
/// nothing for a mapping to defer. Returns the buffer and its FNV-1a-64, because a pin reads
/// every byte and can therefore verify the manifest's checksum for free -- the thing `--mmap on`
/// gave up (`crate::persist`, module docs).
pub fn pin_file(path: &Path) -> io::Result<(Arc<Blob>, u64)> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let len = f.metadata()?.len() as usize;
    let mut buf = Aligned::zeroed(len);
    f.read_exact(buf.as_mut())?;
    let fnv = crate::tq4::fnv1a64(buf.as_ref());
    Ok((Arc::new(Blob::Pinned(buf)), fnv))
}

/// A pinned blob over bytes the caller already holds -- the shape [`pin_file`] produces, for a
/// caller that got the bytes some other way (the pin table's tests).
pub fn pin_bytes(src: &[u8]) -> Arc<Blob> {
    let mut buf = Aligned::zeroed(src.len());
    buf.as_mut().copy_from_slice(src);
    Arc::new(Blob::Pinned(buf))
}

/// Bytes of heap a pinned blob holds (0 for a mapped one): what the pin budget is charged.
pub fn blob_heap_bytes(b: &Blob) -> u64 {
    if b.is_mapped() {
        0
    } else {
        b.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_arrays_behave_like_slices() {
        let mut a = Arr::from_vec(vec![1u32, 2, 3]);
        assert_eq!(&a[..], &[1, 2, 3]);
        assert!(!a.is_mapped());
        assert_eq!(a.heap_bytes(), 12);
        a.as_mut_slice()[0] = 9;
        assert_eq!(a[0], 9);
        assert_eq!(a.clone().to_vec(), vec![9, 2, 3]);
    }

    #[test]
    fn a_window_is_the_files_bytes_and_costs_no_heap() {
        let dir = std::env::temp_dir().join(format!("kvstore-arr-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.bin");
        let want: Vec<u32> = (0..64).collect();
        let mut bytes = vec![0u8; 8];
        bytes.extend_from_slice(pod_bytes(&want));
        std::fs::write(&path, &bytes).unwrap();
        let map = map_file(&path).unwrap();
        let a: Arr<u32> = Arr::window(&map, 8, 64).unwrap();
        assert_eq!(&a[..], &want[..]);
        assert!(a.is_mapped());
        assert_eq!(a.heap_bytes(), 0);
        // the clone is another window, not a copy
        let b = a.clone();
        assert_eq!(b.as_ptr(), a.as_ptr());
        // out of range and misaligned windows are refused, not silently wrong
        assert!(Arr::<u32>::window(&map, 8, 65).is_none());
        assert!(Arr::<u32>::window(&map, 9, 4).is_none());
        // a byte window has no alignment requirement
        assert!(Arr::<u8>::window(&map, 9, 4).is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_pinned_window_is_the_files_bytes_and_costs_heap() {
        let dir = std::env::temp_dir().join(format!("kvstore-arr-pin-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.bin");
        let want: Vec<u32> = (0..64).collect();
        let mut bytes = vec![0u8; 64];
        bytes.extend_from_slice(pod_bytes(&want));
        std::fs::write(&path, &bytes).unwrap();
        let (blob, fnv) = pin_file(&path).unwrap();
        assert_eq!(fnv, crate::tq4::fnv1a64(&bytes), "a pin hashes what it read");
        assert_eq!(blob_heap_bytes(&blob), bytes.len() as u64);
        // the buffer starts where a mapping would: 64-byte aligned, so every array the writer
        // padded to 8 or to 64 is aligned inside it too (`NodeRecords::from_parts` insists)
        assert!((blob.as_ptr() as usize).is_multiple_of(64));
        let a: Arr<u32> = Arr::window(&blob, 64, 64).unwrap();
        assert_eq!(&a[..], &want[..]);
        assert!(!a.is_mapped(), "pinned is not mapped");
        assert!(a.is_pinned());
        assert_eq!(a.heap_bytes(), 256, "a pinned window costs heap");
        // the clone is another window into the same buffer, and it keeps that buffer alive
        let b = a.clone();
        drop(a);
        assert_eq!(b.as_ptr() as usize - blob.as_ptr() as usize, 64);
        // `madvise` on a pinned buffer is a no-op that reports it did nothing
        assert!(!b.advise(Advice::WillNeed));
        assert!(!b.advise_at(0, 4, Advice::WillNeed));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[should_panic(expected = "mapped")]
    fn a_mapped_array_cannot_be_mutated() {
        let dir = std::env::temp_dir().join(format!("kvstore-arr-mut-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("b.bin");
        std::fs::write(&path, [0u8; 64]).unwrap();
        let map = map_file(&path).unwrap();
        let mut a: Arr<u32> = Arr::window(&map, 0, 16).unwrap();
        // the mapping keeps the (now unlinked) file alive, so nothing is left behind when the
        // assertion below unwinds
        std::fs::remove_dir_all(&dir).ok();
        let _ = a.as_mut_slice();
    }
}
