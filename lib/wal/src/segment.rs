use std::cmp::{Ordering, min};
use std::io::{Error, ErrorKind, Result};
use std::ops::Deref;
use std::path::{Path, PathBuf};
#[cfg(target_os = "windows")]
use std::time::Duration;
use std::{fmt, mem, ptr, thread};

use byteorder::{ByteOrder, LittleEndian};
use fs_err as fs;
use fs_err::OpenOptions;
#[cfg(not(unix))]
use fs4::fs_std::FileExt;
use log::{debug, error, log_enabled, trace};

use crate::mmap_view_sync::MmapViewSync;

/// The magic bytes and version tag of the segment header.
const SEGMENT_MAGIC: &[u8; 3] = b"wal";
const SEGMENT_VERSION: u8 = 0;

/// The length of both the segment and entry header.
const HEADER_LEN: usize = 8;

/// The length of a CRC value.
const CRC_LEN: usize = 4;

pub struct Entry {
    view: MmapViewSync,
}

impl Deref for Entry {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { self.view.as_slice() }
    }
}

impl fmt::Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Entry {{ len: {} }}", self.view.len())
    }
}

/// An append-only, fixed-length, durable container of entries.
///
/// The segment on-disk format is as simple as possible, while providing
/// backwards compatibility, protection against corruption, and alignment
/// guarantees.
///
/// A version tag allows the internal format to be updated, while still
/// maintaining compatibility with previously written files. CRCs are used
/// to ensure that entries are not corrupted. Padding is used to ensure that
/// entries are always aligned to 8-byte boundaries.
///
/// ## On Disk Format
///
/// All version, length, and CRC integers are serialized in little-endian format.
///
/// All CRC values are computed using
/// [CRC32-C](https://en.wikipedia.org/wiki/Cyclic_redundancy_check).
///
/// ### Segment Header Format
///
/// | component              | type    |
/// | ---------------------- | ------- |
/// | magic bytes ("wal")    | 3 bytes |
/// | segment format version | u8      |
/// | random CRC seed        | u32     |
///
/// The segment header is 8 bytes long: three magic bytes ("wal") followed by a
/// segment format version `u8`, followed by a random `u32` CRC seed. The CRC
/// seed ensures that if a segment file is reused for a new segment, the old
/// entries will be ignored (since the CRC will not match).
///
/// ### Entry Format
///
/// | component                    | type |
/// | ---------------------------- | ---- |
/// | length                       | u64  |
/// | data                         |      |
/// | padding                      |      |
/// | CRC(length + data + padding) | u32  |
///
/// Entries are serialized to the log with a fixed-length header, followed by
/// the data itself, and finally a variable length footer. The header includes
/// the length of the entry as a u64. The footer includes between 0 and 7 bytes
/// of padding to extend the total length of the entry to a multiple of 8,
/// followed by the CRC code of the length, data, and padding.
///
/// ### Logging
///
/// Segment modifications are logged through the standard Rust [logging
/// facade](https://crates.io/crates/log/). Metadata operations (create, open,
/// resize, file rename) are logged at `info` level, flush events are logged at
/// `debug` level, and entry events (append and truncate) are logged at `trace`
/// level. Long-running or multi-step operations will log a message at a lower
/// level when beginning, with a final completion message at a higher level.
pub struct Segment {
    /// The segment file buffer.
    mmap: MmapViewSync,
    /// The segment file path.
    path: PathBuf,
    /// Index of entry offset and lengths.
    index: Vec<(usize, usize)>,
    /// The crc of the last appended entry.
    crc: u32,
    /// Offset of last flush.
    flush_offset: usize,
}

impl Segment {
    pub fn close(&self) {
        self.mmap.close()
    }

    /// Creates a new segment.
    ///
    /// The initial capacity must be at least 8 bytes.
    ///
    /// If a file already exists at the path it will be overwritten, and the
    /// allocated space will be reused.
    ///
    /// The caller is responsible for flushing the containing directory in order
    /// to guarantee that the segment is durable in the event of a crash.
    pub fn create<P>(path: P, capacity: usize) -> Result<Segment>
    where
        P: AsRef<Path>,
    {
        let file_name = path
            .as_ref()
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .expect("Path to WAL segment file provided");

        let tmp_file_path = match path.as_ref().parent() {
            Some(parent) => parent.join(format!("tmp-{file_name}")),
            None => PathBuf::from(format!("tmp-{file_name}")),
        };

        // Round capacity down to the nearest 8-byte alignment, since the
        // segment would not be able to take advantage of the space.
        let capacity = capacity & !7;
        if capacity < HEADER_LEN {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("invalid segment capacity: {capacity}"),
            ));
        }
        let seed = rand::random();

        {
            // Prepare properly formatted segment in a temporary file, so in case of failure it won't be corrupted.
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                // Don't truncate now, we do it manually later to allocate the right size directly
                .truncate(false)
                .open(&tmp_file_path)?;

            // fs4 provides some cross-platform bindings which help for Windows.
            #[cfg(not(unix))]
            file.file().allocate(capacity as u64)?;
            // For all unix systems WAL can just use ftruncate directly
            #[cfg(unix)]
            {
                rustix::fs::ftruncate(&file, capacity as u64)?;
            }

            let mut mmap = MmapViewSync::from_file(&file, 0, capacity)?;
            {
                let segment = unsafe { &mut mmap.as_mut_slice() };
                copy_memory(SEGMENT_MAGIC, segment);
                segment[3] = SEGMENT_VERSION;
                LittleEndian::write_u32(&mut segment[4..], seed);
            }

            // From "man 2 close":
            // > A successful close does not guarantee that the data has been successfully saved to disk, as the kernel defers writes.
            // So we need to flush magic header manually to ensure that it is written to disk.
            mmap.flush()?;

            // Manually sync each file in Windows since sync-ing cannot be done for the whole directory.
            #[cfg(target_os = "windows")]
            {
                file.sync_all()?;
            }
        };

        // File renames are atomic, so we can safely rename the temporary file to the final file.
        fs::rename(&tmp_file_path, &path)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path.as_ref())?;

        let mmap = MmapViewSync::from_file(&file, 0, capacity)?;

        let segment = Segment {
            mmap,
            path: path.as_ref().to_path_buf(),
            index: Vec::new(),
            crc: seed,
            flush_offset: 0,
        };
        debug!("{segment:?}: created");
        Ok(segment)
    }

    /// Opens the segment file at the specified path.
    ///
    /// An individual file must only be opened by one segment at a time.
    pub fn open<P>(path: P) -> Result<Segment>
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path.as_ref())?;
        let capacity = file.metadata()?.len();
        if capacity > usize::MAX as u64 || capacity < HEADER_LEN as u64 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("invalid segment capacity: {capacity}"),
            ));
        }

        // Round capacity down to the nearest 8-byte alignment, since the
        // segment would not be able to take advantage of the space.
        let capacity = capacity as usize & !7;
        let mmap = MmapViewSync::from_file(&file, 0, capacity)?;

        let mut index = Vec::new();
        let mut crc;
        {
            // Parse the segment, filling out the index containing the offset
            // and length of each entry, as well as the latest CRC value.
            //
            // If the CRC of any entry does not match, then parsing stops and
            // the remainder of the file is considered empty.
            let segment = unsafe { mmap.as_slice() };

            if &segment[0..3] != SEGMENT_MAGIC {
                return Err(Error::new(ErrorKind::InvalidData, "Illegal segment header"));
            }

            if segment[3] != 0 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Segment version unsupported: {}", segment[3]),
                ));
            }

            crc = LittleEndian::read_u32(&segment[4..]);
            let mut offset = HEADER_LEN;

            while offset + HEADER_LEN + CRC_LEN < capacity {
                let len = LittleEndian::read_u64(&segment[offset..]) as usize;
                let padding = padding(len);
                let padded_len = len + padding;
                if offset + HEADER_LEN + padded_len + CRC_LEN > capacity {
                    break;
                }
                let entry_crc = crc32c::crc32c_append(
                    !crc.reverse_bits(),
                    &segment[offset..offset + HEADER_LEN + padded_len],
                );
                let stored_crc =
                    LittleEndian::read_u32(&segment[offset + HEADER_LEN + padded_len..]);
                if entry_crc != stored_crc {
                    if stored_crc != 0 {
                        log::warn!("CRC mismatch at offset {offset}: {entry_crc} != {stored_crc}");
                    }
                    break;
                }

                crc = entry_crc;
                index.push((offset + HEADER_LEN, len));
                offset += HEADER_LEN + padded_len + CRC_LEN;
            }
        }

        let mut segment = Segment {
            mmap,
            path: path.as_ref().to_path_buf(),
            index,
            crc,
            flush_offset: 0,
        };

        // Bump flush offset next to last entry, we don't need to flush any existing data
        segment.flush_offset = segment.size();

        debug!("{segment:?}: opened");
        Ok(segment)
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { self.mmap.as_slice() }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { self.mmap.as_mut_slice() }
    }

    /// Returns the segment entry at the specified index, or `None` if no such
    /// entry exists.
    pub fn entry(&self, index: usize) -> Option<Entry> {
        self.index.get(index).map(|&(offset, len)| {
            let mut view = unsafe { self.mmap.clone() };
            // restrict only fails on bounds errors, but an invariant of
            // Segment is that the index always holds valid offset and
            // length bounds.
            view.restrict(offset, len)
                .expect("illegal segment offset or length");
            Entry { view }
        })
    }

    /// Appends an entry to the segment, returning the index of the new entry,
    /// or `None` if there is insufficient space for the entry.
    ///
    /// The entry may be immediately read from the log, but it is not guaranteed
    /// to be durably stored on disk until the segment is flushed.
    pub fn append<T>(&mut self, entry: &T) -> Option<usize>
    where
        T: Deref<Target = [u8]>,
    {
        if !self.sufficient_capacity(entry.len()) {
            return None;
        }
        trace!("{:?}: appending {} byte entry", self, entry.len());

        let padding = padding(entry.len());
        let padded_len = entry.len() + padding;

        let offset = self.size();

        let mut crc = self.crc;

        LittleEndian::write_u64(&mut self.as_mut_slice()[offset..], entry.len() as u64);
        copy_memory(
            entry.deref(),
            &mut self.as_mut_slice()[offset + HEADER_LEN..],
        );

        if padding > 0 {
            let start = offset + HEADER_LEN + entry.len();
            self.as_mut_slice()[start..start + padding].fill(0)
        }
        crc = crc32c::crc32c_append(
            !crc.reverse_bits(),
            &self.as_slice()[offset..offset + HEADER_LEN + padded_len],
        );

        LittleEndian::write_u32(
            &mut self.as_mut_slice()[offset + HEADER_LEN + padded_len..],
            crc,
        );

        self.crc = crc;
        self.index.push((offset + HEADER_LEN, entry.len()));
        Some(self.index.len() - 1)
    }

    fn _read_seed_crc(&self) -> u32 {
        LittleEndian::read_u32(&self.as_slice()[4..])
    }

    fn _read_entry_crc(&self, entry_id: usize) -> u32 {
        let (offset, len) = self.index[entry_id];
        let padding = padding(len);
        let padded_len = len + padding;
        LittleEndian::read_u32(&self.as_slice()[offset + padded_len..])
    }

    /// Truncates the entries in the segment beginning with `from`.
    ///
    /// The entries are not guaranteed to be removed until the segment is
    /// flushed.
    pub fn truncate(&mut self, from: usize) {
        if from >= self.index.len() {
            return;
        }
        trace!("{self:?}: truncating from position {from}");

        let zero_end = self.size();

        // Remove the index entries.
        let deleted = self.index.drain(from..).count();
        trace!("{self:?}: truncated {deleted} entries");

        // Update the CRC.
        if self.index.is_empty() {
            self.crc = self._read_seed_crc(); // Seed
        } else {
            // Read CRC of the last entry.
            self.crc = self._read_entry_crc(self.index.len() - 1);
        }

        // Zero all deleted entries so that we will not read the data back after a crash
        let zero_start = self.size();
        self.as_mut_slice()[zero_start..zero_end].fill(0);

        // Move flush offset back to write new changes on next flush
        self.flush_offset = min(self.flush_offset, zero_start);
    }

    /// Flushes recently written entries to durable storage.
    pub fn flush(&mut self) -> Result<()> {
        trace!("{self:?}: flushing");
        let start = self.flush_offset;
        let end = self.size();

        match start.cmp(&end) {
            Ordering::Equal => {
                // nothing to flush
                trace!("{self:?}: nothing to flush");
            }
            Ordering::Less => {
                // flush new elements added since last flush
                trace!("{self:?}: flushing byte range [{start}, {end})");
                let mut view = unsafe { self.mmap.clone() };
                view.restrict(start, end - start)?;
                view.flush()?;
            }
            Ordering::Greater => {
                // most likely truncated in between flushes
                // register new flush offset & flush the whole segment
                error!("{self:?}: invalid flush range, flushing everything");
                self.mmap.flush()?;
            }
        }

        self.flush_offset = end;
        Ok(())
    }

    /// Flushes recently written entries to durable storage.
    pub fn flush_async(&mut self) -> thread::JoinHandle<Result<()>> {
        trace!("{self:?}: async flushing");
        let start = self.flush_offset;
        let end = self.size();

        match start.cmp(&end) {
            Ordering::Equal => thread::spawn(move || Ok(())), // nothing to flush
            Ordering::Less => {
                // flush new elements added since last flush
                let mut view = unsafe { self.mmap.clone() };
                self.flush_offset = end;

                let log_msg = if log_enabled!(log::Level::Trace) {
                    format!(
                        "{:?}: async flushing byte range [{}, {})",
                        &self, start, end
                    )
                } else {
                    String::new()
                };

                thread::spawn(move || {
                    trace!("{log_msg}");
                    view.restrict(start, end - start).and_then(|_| view.flush())
                })
            }
            Ordering::Greater => {
                // most likely truncated in between flushes
                // register new flush offset & flush the whole segment
                let view = unsafe { self.mmap.clone() };
                self.flush_offset = end;

                let log_msg =
                    format!("{self:?}: invalid flush range, flushing everything asynchronously");

                thread::spawn(move || {
                    error!("{log_msg}");
                    view.flush()
                })
            }
        }
    }

    /// Ensure that the segment can store an entry of the provided size.
    ///
    /// If the current segment length is insufficient then it is resized. This
    /// is potentially a very slow operation.
    pub fn ensure_capacity(&mut self, entry_size: usize) -> Result<()> {
        let required_capacity =
            entry_size + padding(entry_size) + HEADER_LEN + CRC_LEN + self.size();
        // Sanity check the 8-byte alignment invariant.
        assert_eq!(required_capacity & !7, required_capacity);
        if required_capacity > self.capacity() {
            debug!("{self:?}: resizing to {required_capacity} bytes");
            self.flush()?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(&self.path)?;
            // fs4 provides some cross-platform bindings which help for Windows.
            #[cfg(not(unix))]
            file.file().allocate(required_capacity as u64)?;
            // For all unix systems WAL can just use ftruncate directly
            #[cfg(unix)]
            {
                rustix::fs::ftruncate(&file, required_capacity as u64)?;
            }

            let mut mmap = MmapViewSync::from_file(&file, 0, required_capacity)?;
            mem::swap(&mut mmap, &mut self.mmap);
        }
        Ok(())
    }

    /// Returns the number of entries in the segment.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns true if the segment has no entries.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Returns the capacity (file-size) of the segment in bytes
    ///
    /// Each entry is stored with a header and padding, so the entire capacity
    /// will not be available for entry data.
    pub fn capacity(&self) -> usize {
        self.mmap.len()
    }

    /// Returns the total number of bytes used to store existing entries,
    /// including header and padding overhead.
    pub fn size(&self) -> usize {
        self.index.last().map_or(HEADER_LEN, |&(offset, len)| {
            offset + len + padding(len) + CRC_LEN
        })
    }

    /// Returns `true` if the segment has sufficient remaining capacity to
    /// append an entry of size `entry_len`.
    pub fn sufficient_capacity(&self, entry_len: usize) -> bool {
        (self.capacity() - self.size())
            .checked_sub(HEADER_LEN + CRC_LEN)
            .is_some_and(|rem| rem >= entry_len + padding(entry_len))
    }

    /// Returns the path to the segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Renames the segment file.
    ///
    /// The caller is responsible for syncing the directory in order to
    /// guarantee that the rename is durable in the event of a crash.
    pub fn rename<P>(&mut self, path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        debug!("{:?}: renaming file to {:?}", self, path.as_ref());
        fs::rename(&self.path, &path).map_err(|e| {
            error!("{:?}: failed to rename segment {}", self.path, e);
            e
        })?;
        self.path = path.as_ref().to_path_buf();
        Ok(())
    }

    /// Deletes the segment file.
    pub fn delete(self) -> Result<()> {
        debug!("{self:?}: deleting file");

        #[cfg(not(target_os = "windows"))]
        {
            self.delete_unix()
        }

        #[cfg(target_os = "windows")]
        {
            self.delete_windows()
        }
    }

    #[cfg(not(target_os = "windows"))]
    fn delete_unix(self) -> Result<()> {
        fs::remove_file(&self.path).map_err(|e| {
            error!("{self:?}: failed to delete segment {e}");
            e
        })
    }

    #[cfg(target_os = "windows")]
    fn delete_windows(self) -> Result<()> {
        const DELETE_TRIES: u32 = 3;

        let Segment {
            mmap,
            path,
            index,
            crc: _,
            flush_offset,
        } = self;
        let mmap_len = mmap.len();

        // Unmaps the file before `fs::remove_file` else access will be denied
        mmap.flush()?;
        std::mem::drop(mmap);

        let mut tries = 0;
        loop {
            tries += 1;
            match fs::remove_file(&path) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if tries >= DELETE_TRIES {
                        error!(
                            "{:?}: failed to delete segment {}",
                            // `self` was destructured when mmap was yoinked out so `fmt::Debug`
                            // cannot be used
                            format_args!(
                                "Segment {{ path: {:?}, flush_offset: {}, entries: {}, \
                                space: ({}/{}) }}",
                                path,
                                flush_offset,
                                index.len(),
                                index.last().map_or(HEADER_LEN, |&(offset, len)| {
                                    offset + len + padding(len) + CRC_LEN
                                }),
                                mmap_len
                            ),
                            e
                        );
                        return Err(e);
                    } else {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        }
    }

    pub(crate) fn copy_to_path<P>(&self, path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        if path.as_ref().exists() {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("Path {:?} already exists", path.as_ref()),
            ));
        }

        let mut other = Self::create(path, self.capacity())?;
        unsafe {
            other
                .mmap
                .as_mut_slice()
                .copy_from_slice(self.mmap.as_slice());
        }
        Ok(())
    }
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Segment {{ path: {:?}, flush_offset: {}, entries: {}, space: ({}/{}) }}",
            &self.path,
            self.flush_offset,
            self.len(),
            self.size(),
            self.capacity()
        )
    }
}

/// Copies data from `src` to `dst`
///
/// Panics if the length of `dst` is less than the length of `src`.
pub fn copy_memory(src: &[u8], dst: &mut [u8]) {
    let len_src = src.len();
    assert!(dst.len() >= len_src);
    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), len_src);
    }
}

/// Returns the number of padding bytes to add to a buffer to ensure 8-byte alignment.
fn padding(len: usize) -> usize {
    4usize.wrapping_sub(len) & 7
}

/// Returns total number of bytes used on disk to store an entry of length `len`.
///
/// Includes header, padding and CRC.
///
/// | component                    | type |
/// | ---------------------------- | ---- |
/// | length                       | u64  |
/// | data                         |      |
/// | padding                      |      |
/// | CRC(length + data + padding) | u32  |
#[cfg(test)]
pub fn entry_size_disk(len: usize) -> usize {
    len + entry_overhead(len)
}

/// Returns the overhead of storing an entry of length `len`.
pub fn entry_overhead(len: usize) -> usize {
    padding(len) + HEADER_LEN + CRC_LEN
}

/// Returns the fixed-overhead of segment metadata.
pub fn segment_overhead() -> usize {
    HEADER_LEN
}

#[cfg(test)]
mod test {
    use std::io::ErrorKind;
    use std::path::Path;

    use rand::Rng;
    use tempfile::{Builder, TempDir};

    use super::{Segment, padding};
    use crate::segment::{HEADER_LEN, entry_size_disk};
    use crate::test_utils::EntryGenerator;

    #[test]
    fn test_pad_len() {
        assert_eq!(4, padding(0));
        assert_eq!(3, padding(1));
        assert_eq!(2, padding(2));
        assert_eq!(1, padding(3));
        assert_eq!(0, padding(4));
        assert_eq!(7, padding(5));
        assert_eq!(6, padding(6));
        assert_eq!(5, padding(7));

        assert_eq!(4, padding(8));
        assert_eq!(3, padding(9));
        assert_eq!(2, padding(10));
        assert_eq!(1, padding(11));
        assert_eq!(0, padding(12));
        assert_eq!(7, padding(13));
        assert_eq!(6, padding(14));
        assert_eq!(5, padding(15));
    }

    fn create_segment(len: usize) -> (Segment, TempDir) {
        let dir = Builder::new().prefix("segment").tempdir().unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("sync-segment");
        (Segment::create(path, len).unwrap(), dir)
    }

    fn load_segment(dir: impl AsRef<Path>) -> Segment {
        let mut path = dir.as_ref().to_path_buf();
        path.push("sync-segment");
        Segment::open(path).unwrap()
    }

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    /// Checks that entries can be appended to a segment.
    fn check_append(segment: &mut Segment) {
        assert_eq!(0, segment.len());

        let entries: Vec<Vec<u8>> =
            EntryGenerator::with_segment_capacity(segment.capacity()).collect();

        for (idx, entry) in entries.iter().enumerate() {
            assert_eq!(idx, segment.append(entry).unwrap());
            assert_eq!(&entry[..], &*segment.entry(idx).unwrap());
        }

        for (idx, entry) in entries.iter().enumerate() {
            assert_eq!(&entry[..], &*segment.entry(idx).unwrap());
        }
    }

    #[test]
    fn test_append() {
        init_logger();
        check_append(&mut create_segment(8).0);
        check_append(&mut create_segment(9).0);
        check_append(&mut create_segment(32).0);
        check_append(&mut create_segment(100).0);
        check_append(&mut create_segment(1023).0);
        check_append(&mut create_segment(1024).0);
        check_append(&mut create_segment(1025).0);
        check_append(&mut create_segment(4096).0);
        check_append(&mut create_segment(8 * 1024 * 1024).0);
    }

    #[test]
    fn test_truncate() {
        init_logger();
        let (mut segment, dir) = create_segment(4096);

        segment.append(&b"0".as_slice()).unwrap();
        segment.append(&b"1".as_slice()).unwrap();
        segment.append(&b"2".as_slice()).unwrap();
        segment.append(&b"3".as_slice()).unwrap();

        // Truncate beyond the end is a no-op
        assert_eq!(segment.len(), 4);
        segment.truncate(4);
        assert_eq!(segment.len(), 4);

        // Until we flush, flush offset remains zero
        assert_eq!(segment.flush_offset, 0);
        segment.flush().unwrap();
        assert_eq!(segment.flush_offset, HEADER_LEN + entry_size_disk(1) * 4);

        // Truncate to keep one entry
        segment.truncate(1);
        assert_eq!(segment.len(), 1);
        assert_eq!(segment.flush_offset, HEADER_LEN + entry_size_disk(1));

        // Add a new items (index 2, 3), flush offset remains at index 1 until we flush
        segment.append(&b"12345".as_slice()).unwrap();
        segment.append(&b"67890".as_slice()).unwrap();
        assert_eq!(segment.len(), 3);
        assert_eq!(segment.flush_offset, HEADER_LEN + entry_size_disk(1));

        // Flush and reload
        // This was broken before <https://github.com/qdrant/wal/pull/99>, as it wouldn't fully
        // flush the last two appended operations
        segment.flush().unwrap();
        assert_eq!(
            segment.flush_offset,
            HEADER_LEN + entry_size_disk(1) + entry_size_disk(5) * 2,
        );
        let mut segment = load_segment(&dir);
        assert_eq!(segment.len(), 3);
        assert_eq!(
            segment.flush_offset,
            HEADER_LEN + entry_size_disk(1) + entry_size_disk(5) * 2,
        );

        // Truncate all (clear) and assert flush offset is bumped
        segment.truncate(0);
        assert_eq!(segment.len(), 0);
        assert_eq!(segment.flush_offset, HEADER_LEN);

        // Flush and reload
        segment.flush().unwrap();
        assert_eq!(segment.flush_offset, HEADER_LEN);
        let segment = load_segment(&dir);
        assert_eq!(segment.len(), 0);
        assert_eq!(segment.flush_offset, HEADER_LEN);
    }

    #[test]
    fn test_create_dir_path() {
        init_logger();
        let dir = Builder::new().prefix("segment").tempdir().unwrap();
        assert!(Segment::open(dir.path()).is_err());
    }

    #[test]
    fn test_entries() {
        init_logger();
        let (mut segment, _dir) = create_segment(4096);
        let entries: &[&[u8]] = &[
            b"",
            b"0",
            b"01",
            b"012",
            b"0123",
            b"01234",
            b"012345",
            b"0123456",
            b"01234567",
            b"012345678",
            b"0123456789",
        ];

        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(index, segment.append(entry).unwrap());
        }

        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(*entry, &*segment.entry(index).unwrap());
        }
    }

    #[test]
    fn test_open() {
        init_logger();
        let dir = Builder::new().prefix("segment").tempdir().unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("test-open");

        let entries: &[&[u8]] = &[
            b"",
            b"0",
            b"01",
            b"012",
            b"0123",
            b"01234",
            b"012345",
            b"0123456",
            b"01234567",
            b"012345678",
            b"0123456789",
        ];

        let capacity = 4096;

        {
            let segment_res = Segment::create(&path, capacity);
            if let Ok(mut segment) = segment_res {
                for (i, entry) in entries.iter().enumerate() {
                    let index = segment.append(entry).unwrap();
                    assert_eq!(i, index);
                }
                segment.flush().unwrap();
            } else {
                eprintln!("segment_res = {segment_res:#?}");
                panic!("Failed to create segment");
            }
        }

        let segment = Segment::open(&path).unwrap();
        assert_eq!(capacity, segment.capacity());
        assert_eq!(entries.len(), segment.len());

        for (index, &entry) in entries.iter().enumerate() {
            assert_eq!(entry, &*segment.entry(index).unwrap());
        }
    }

    /// Tests that when overwriting an existing segment file with a new segment,
    /// the old entries will not be indexed.
    #[test]
    fn test_overwrite() {
        init_logger();
        let dir = Builder::new().prefix("segment").tempdir().unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("test-overwrite");

        let entries: &[&[u8]] = &[b"abcdefgh", b"abcdefgh", b"abcdefgh"];

        {
            let mut segment = Segment::create(&path, 4096).unwrap();
            for (i, entry) in entries.iter().enumerate() {
                let index = segment.append(entry).unwrap();
                assert_eq!(i, index);
            }
        }

        Segment::create(&path, 4096).unwrap();
        let segment = Segment::open(&path).unwrap();

        assert_eq!(0, segment.len());
    }

    /// Tests that opening a non-existent segment file will fail.
    #[test]
    fn test_open_nonexistent() {
        init_logger();
        let dir = Builder::new().prefix("segment").tempdir().unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("test-open-nonexistent");
        assert_eq!(
            ErrorKind::NotFound,
            Segment::open(&path).unwrap_err().kind()
        );
    }

    use std::hash::Hasher;

    #[test]
    fn test_crc32c() {
        let message = b"123456789";
        let crc = crc32c::crc32c(message);
        assert_eq!(crc, crc::CRC_32_ISCSI.check);

        let mut hasher = crc32c::Crc32cHasher::default();
        hasher.write(message);
        assert_eq!(hasher.finish() as u32, crc::CRC_32_ISCSI.check);
    }

    #[test]
    fn test_crc32c_accuracy() {
        let mut buffer = [0u8; 8192];
        let castagnoli = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

        let mut rng = rand::rng();
        (0..1024).for_each(|_| {
            rng.fill_bytes(&mut buffer);
            let mut digest = castagnoli.digest();
            digest.update(&buffer);
            let crc1 = digest.finalize();
            let crc2 = crc32c::crc32c(&buffer);
            assert_eq!(crc1, crc2);
        });
    }

    #[test]
    fn test_crc32c_with_arbitrary_initial_value() {
        let mut buffer = [0u8; 8192];
        let castagnoli = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

        let mut rng = rand::rng();
        (0..1024).for_each(|seed| {
            rng.fill_bytes(&mut buffer);
            let mut digest = castagnoli.digest_with_initial(seed);
            digest.update(&buffer);
            let crc1 = digest.finalize();

            let crc2 = crc32c::crc32c_append(!seed.reverse_bits(), &buffer);
            assert_eq!(crc1, crc2);
        });
    }
}
