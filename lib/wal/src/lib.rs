use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::{fmt, mem, ops, result, thread};

use fs_err as fs;
use fs_err::File;
use fs4::fs_std::FileExt;
use log::{debug, info, trace};
pub use segment::{Entry, Segment};

use crate::segment_creator::SegmentCreatorV2;

mod mmap_view_sync;
mod segment;
mod segment_creator;
pub mod test_utils;

#[cfg(test)]
mod test_segment_recovery;

#[derive(Debug)]
pub struct WalOptions {
    /// The segment capacity. Defaults to 32MiB.
    pub segment_capacity: usize,

    /// The number of segments to create ahead of time, so that appends never
    /// need to wait on creating a new segment.
    pub segment_queue_len: usize,

    /// The number of "closed-*" wal files to retain. Defaults to 1.
    pub retain_closed: NonZeroUsize,
}

impl Default for WalOptions {
    fn default() -> WalOptions {
        WalOptions {
            segment_capacity: 32 * 1024 * 1024,
            segment_queue_len: 0,
            retain_closed: NonZeroUsize::new(1).unwrap(),
        }
    }
}

/// An open segment and its ID.
#[derive(Debug)]
struct OpenSegment {
    pub id: u64,
    pub segment: Segment,
}

/// A closed segment, and the associated start and stop indices.
#[derive(Debug)]
struct ClosedSegment {
    pub start_index: u64,
    pub segment: Segment,
}

enum WalSegment {
    Open(OpenSegment),
    Closed(ClosedSegment),
}

/// A write ahead log.
///
/// ### Logging
///
/// Wal operations are logged. Metadata operations (open) are logged at `info`
/// level. Segment operations (create, close, delete) are logged at `debug`
/// level. Flush operations are logged at `debug` level. Entry operations
/// (append, truncate) are logged at `trace` level. Long-running or multi-step
/// operations will log a message at a lower level when beginning, and a final
/// completion message.
pub struct Wal {
    /// The segment currently being appended to.
    open_segment: OpenSegment,
    closed_segments: Vec<ClosedSegment>,
    creator: SegmentCreatorV2,

    /// The number of closed segments to retain.
    retain_closed: NonZeroUsize,

    /// The directory which contains the write ahead log. Used to hold an open
    /// file lock for the lifetime of the log.
    #[allow(dead_code)]
    dir: File,

    /// The directory path.
    path: PathBuf,

    /// Tracks the flush status of recently closed segments between user calls
    /// to `Wal::flush`.
    flush: Option<thread::JoinHandle<Result<()>>>,
}

impl Wal {
    pub fn open<P>(path: P) -> Result<Wal>
    where
        P: AsRef<Path>,
    {
        Wal::with_options(path, &WalOptions::default())
    }

    pub fn generate_empty_wal_starting_at_index(
        path: impl Into<PathBuf>,
        options: &WalOptions,
        index: u64,
    ) -> Result<()> {
        let open_id = 0;
        let mut path_buf = path.into();
        path_buf.push(format!("open-{open_id}"));
        let segment = OpenSegment {
            id: index + 1,
            segment: Segment::create(&path_buf, options.segment_capacity)?,
        };

        let mut close_segment = close_segment(segment, index + 1)?;

        close_segment.segment.flush()
    }

    pub fn with_options<P>(path: P, options: &WalOptions) -> Result<Wal>
    where
        P: AsRef<Path>,
    {
        debug!("Wal {{ path: {:?} }}: opening", path.as_ref());

        #[cfg(not(target_os = "windows"))]
        let path = path.as_ref().to_path_buf();
        #[cfg(not(target_os = "windows"))]
        let dir = File::open(&path)?;

        // Windows workaround. Directories cannot be exclusively held so we create a proxy file
        // inside the tmp directory which is used for locking. This is done because:
        // - A Windows directory is not a file unlike in Linux, so we cannot open it with
        //   `File::open` nor lock it with `try_lock_exclusive`
        // - We want this to be auto-deleted together with the `TempDir`
        #[cfg(target_os = "windows")]
        let mut path = path.as_ref().to_path_buf();
        #[cfg(target_os = "windows")]
        let dir = {
            path.push(".wal");
            let dir = File::options()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&path)?;
            path.pop();
            dir
        };

        if !dir.file().try_lock_exclusive()? {
            return Err(fs4::lock_contended_error());
        }

        // Holds open segments in the directory.
        let mut open_segments: Vec<OpenSegment> = Vec::new();
        let mut closed_segments: Vec<ClosedSegment> = Vec::new();

        for entry in fs::read_dir(&path)? {
            match open_dir_entry(entry?)? {
                Some(WalSegment::Open(open_segment)) => open_segments.push(open_segment),
                Some(WalSegment::Closed(closed_segment)) => closed_segments.push(closed_segment),
                None => {}
            }
        }

        // Validate the closed segments. They must be non-overlapping, and contiguous.
        closed_segments.sort_by(|a, b| a.start_index.cmp(&b.start_index));
        let mut next_start_index = closed_segments
            .first()
            .map_or(0, |segment| segment.start_index);
        for &ClosedSegment {
            start_index,
            ref segment,
        } in &closed_segments
        {
            match start_index.cmp(&next_start_index) {
                Ordering::Less => {
                    // TODO: figure out what to do here.
                    // Current thinking is the previous segment should be truncated.
                    unimplemented!()
                }
                Ordering::Equal => {
                    next_start_index = start_index + segment.len() as u64;
                }
                Ordering::Greater => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "missing segment(s) containing wal entries {next_start_index} to {start_index}"
                        ),
                    ));
                }
            }
        }

        // Validate the open segments.
        open_segments.sort_by(|a, b| a.id.cmp(&b.id));

        // The latest open segment, may already have segments.
        let mut open_segment: Option<OpenSegment> = None;
        // Unused open segments.
        let mut unused_segments: Vec<OpenSegment> = Vec::new();

        for segment in open_segments {
            if !segment.segment.is_empty() {
                // This segment has already been written to. If a previous open
                // segment has also already been written to, we close it out and
                // replace it with this new one. This may happen because when a
                // segment is closed it is renamed, but the directory is not
                // sync'd, so the operation is not guaranteed to be durable.
                let stranded_segment = open_segment.take();
                open_segment = Some(segment);
                if let Some(segment) = stranded_segment {
                    let closed_segment = close_segment(segment, next_start_index)?;
                    next_start_index += closed_segment.segment.len() as u64;
                    closed_segments.push(closed_segment);
                }
            } else if open_segment.is_none() {
                open_segment = Some(segment);
            } else {
                unused_segments.push(segment);
            }
        }

        let mut creator = SegmentCreatorV2::new(
            &path,
            open_segment.as_ref(),
            unused_segments,
            options.segment_capacity,
            options.segment_queue_len,
        );

        let open_segment = match open_segment {
            Some(segment) => segment,
            None => creator.next()?,
        };

        let wal = Wal {
            open_segment,
            closed_segments,
            retain_closed: options.retain_closed,
            creator,
            dir,
            path,
            flush: None,
        };
        info!("{wal:?}: opened");
        Ok(wal)
    }

    fn retire_open_segment(&mut self) -> Result<()> {
        trace!("{self:?}: retiring open segment");
        let mut segment = self.creator.next()?;
        mem::swap(&mut self.open_segment, &mut segment);

        if let Some(flush) = self.flush.take() {
            flush
                .join()
                .map_err(|err| Error::other(format!("wal flush thread panicked: {err:?}")))??;
        };

        self.flush = Some(segment.segment.flush_async());

        let start_index = self.open_segment_start_index();

        // If there is an empty closed segment, remove it before adding the new one.
        if let Some(last_closed) = self.closed_segments.last()
            && last_closed.segment.is_empty()
        {
            let empty_segment = self.closed_segments.pop().unwrap();
            empty_segment.segment.delete()?;
        }

        self.closed_segments
            .push(close_segment(segment, start_index)?);
        debug!("{self:?}: open segment retired. start_index: {start_index}");
        Ok(())
    }

    pub fn append<T>(&mut self, entry: &T) -> Result<u64>
    where
        T: ops::Deref<Target = [u8]>,
    {
        trace!("{:?}: appending entry of length {}", self, entry.len());
        if !self.open_segment.segment.sufficient_capacity(entry.len()) {
            if !self.open_segment.segment.is_empty() {
                self.retire_open_segment()?;
            }
            self.open_segment.segment.ensure_capacity(entry.len())?;
        }

        Ok(self.open_segment_start_index()
            + self.open_segment.segment.append(entry).unwrap() as u64)
    }

    pub fn flush_open_segment(&mut self) -> Result<()> {
        trace!("{self:?}: flushing open segments");
        self.open_segment.segment.flush()?;
        Ok(())
    }

    pub fn flush_open_segment_async(&mut self) -> thread::JoinHandle<Result<()>> {
        trace!("{self:?}: flushing open segments");
        self.open_segment.segment.flush_async()
    }

    /// Retrieve the entry with the provided index from the log.
    pub fn entry(&self, index: u64) -> Option<Entry> {
        let open_start_index = self.open_segment_start_index();
        if index >= open_start_index {
            return self
                .open_segment
                .segment
                .entry((index - open_start_index) as usize);
        }

        match self.find_closed_segment(index) {
            Ok(segment_index) => {
                let segment = &self.closed_segments[segment_index];
                segment
                    .segment
                    .entry((index - segment.start_index) as usize)
            }
            Err(i) => {
                // Sanity check that the missing index is less than the start of the log.
                assert_eq!(0, i);
                None
            }
        }
    }

    /// Truncates entries in the log beginning with `from`.
    ///
    /// Entries can be immediately appended to the log once this method returns,
    /// but the truncated entries are not guaranteed to be removed until the
    /// wal is flushed.
    pub fn truncate(&mut self, from: u64) -> Result<()> {
        trace!("{self:?}: truncate from entry {from}");
        let open_start_index = self.open_segment_start_index();
        if from >= open_start_index {
            self.open_segment
                .segment
                .truncate((from - open_start_index) as usize);
        } else {
            // Truncate the open segment completely.
            self.open_segment.segment.truncate(0);

            match self.find_closed_segment(from) {
                Ok(index) => {
                    if from == self.closed_segments[index].start_index {
                        for segment in self.closed_segments.drain(index..) {
                            // TODO: this should be async
                            segment.segment.delete()?;
                        }
                    } else {
                        {
                            let segment = &mut self.closed_segments[index];
                            segment
                                .segment
                                .truncate((from - segment.start_index) as usize);
                            // flushing closed segment after truncation
                            segment.segment.flush()?;
                        }
                        if index + 1 < self.closed_segments.len() {
                            for segment in self.closed_segments.drain(index + 1..) {
                                // TODO: this should be async
                                segment.segment.delete()?;
                            }
                        }
                    }
                }
                Err(index) => {
                    // The truncate index is before the first entry of the wal
                    assert!(
                        from <= self
                            .closed_segments
                            .get(index)
                            .map_or(0, |segment| segment.start_index)
                    );
                    for segment in self.closed_segments.drain(..) {
                        // TODO: this should be async
                        segment.segment.delete()?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Possibly removes entries from the beginning of the log before the given index.
    ///
    /// After calling this method, the `first_index` will be between the current
    /// `first_index` (inclusive), and `until` (exclusive).
    ///
    /// This always keeps at least one closed segment.
    pub fn prefix_truncate(&mut self, until: u64) -> Result<()> {
        trace!("{self:?}: prefix_truncate until entry {until}");

        // Return early if everything up to `until` has already been truncated
        if until
            <= self
                .closed_segments
                .first()
                .map_or(0, |segment| segment.start_index)
        {
            return Ok(());
        }

        // Retain closed segments starting from this index.
        // If `retain_closed` is 1 (default), only the last closed segment will be preserved.
        let retain_start_index = self
            .closed_segments
            .len()
            .saturating_sub(self.retain_closed.get());

        // If `until` goes into or above our open segment, delete till preserved closed segment index
        if until >= self.open_segment_start_index() {
            for segment in self.closed_segments.drain(..retain_start_index) {
                segment.segment.delete()?
            }
            return Ok(());
        }

        // Delete all closed segments before the one `until` is in
        let index = self.find_closed_segment(until).unwrap();
        let truncate_until_index = index.min(retain_start_index);
        trace!("{self:?}: prefix truncating until closed segment {truncate_until_index}");
        for segment in self.closed_segments.drain(..truncate_until_index) {
            segment.segment.delete()?
        }
        Ok(())
    }

    /// Returns the start index of the open segment.
    fn open_segment_start_index(&self) -> u64 {
        self.closed_segments
            .last()
            .map_or(0, |segment: &ClosedSegment| {
                segment.start_index + segment.segment.len() as u64
            })
    }

    fn find_closed_segment(&self, index: u64) -> result::Result<usize, usize> {
        self.closed_segments.binary_search_by(|segment| {
            if index < segment.start_index {
                Ordering::Greater
            } else if index >= segment.start_index + segment.segment.len() as u64 {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn num_segments(&self) -> usize {
        self.closed_segments.len() + 1
    }

    pub fn num_entries(&self) -> u64 {
        self.open_segment_start_index()
            - self
                .closed_segments
                .first()
                .map_or(0, |segment| segment.start_index)
            + self.open_segment.segment.len() as u64
    }

    /// The index of the first entry.
    pub fn first_index(&self) -> u64 {
        self.closed_segments
            .first()
            .map_or(0, |segment| segment.start_index)
    }

    /// The index of the last entry
    pub fn last_index(&self) -> u64 {
        let num_entries = self.num_entries();
        self.first_index() + num_entries.saturating_sub(1)
    }

    /// Remove all entries
    pub fn clear(&mut self) -> Result<()> {
        self.truncate(self.first_index())
    }

    /// Copy all files to the given path directory. directory should exist and be empty
    pub fn copy_to_path<P>(&self, path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        if fs::read_dir(path.as_ref())?.next().is_some() {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("path {:?} not empty", path.as_ref()),
            ));
        };

        let open_segment_file = self.open_segment.segment.path().file_name().unwrap();
        let close_segment_files: HashMap<_, _> = self
            .closed_segments
            .iter()
            .map(|segment| {
                (
                    segment.segment.path().file_name().unwrap(),
                    &segment.segment,
                )
            })
            .collect();

        for entry in fs::read_dir(self.path())? {
            let entry = entry?;
            if !entry.metadata()?.is_file() {
                continue;
            }

            // if file is locked by any Segment, call copy_to_path on it
            let entry_file_name = entry.file_name();
            let dst_path = path.as_ref().to_owned().join(entry_file_name.clone());
            if entry_file_name == open_segment_file {
                self.open_segment.segment.copy_to_path(&dst_path)?;
            } else if let Some(segment) = close_segment_files.get(entry_file_name.as_os_str()) {
                segment.copy_to_path(&dst_path)?;
            } else {
                // if file is not locked by any Segment, just copy it
                fs::copy(entry.path(), &dst_path)?;
            }
        }
        Ok(())
    }

    /// Set how many segments closed segments to retain on prefix truncation.
    ///
    /// Can't be less than 1. If 0 is provided, it will be set to 1.
    pub fn set_retention(&mut self, retain_closed: usize) {
        self.retain_closed = NonZeroUsize::new(retain_closed.max(1)).unwrap();
    }
}

impl fmt::Debug for Wal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let start_index = self
            .closed_segments
            .first()
            .map_or(0, |segment| segment.start_index);
        let end_index = self.open_segment_start_index() + self.open_segment.segment.len() as u64;
        write!(
            f,
            "Wal {{ path: {:?}, segment-count: {}, entries: [{}, {})  }}",
            &self.path,
            self.closed_segments.len() + 1,
            start_index,
            end_index
        )
    }
}

fn close_segment(mut segment: OpenSegment, start_index: u64) -> Result<ClosedSegment> {
    let new_path = segment
        .segment
        .path()
        .with_file_name(format!("closed-{start_index}"));
    segment.segment.rename(new_path)?;
    segment.segment.close();
    Ok(ClosedSegment {
        start_index,
        segment: segment.segment,
    })
}

fn open_dir_entry(entry: fs::DirEntry) -> Result<Option<WalSegment>> {
    let metadata = entry.metadata()?;

    let error = || {
        Error::new(
            ErrorKind::InvalidData,
            format!("unexpected entry in wal directory: {:?}", entry.path()),
        )
    };

    if !metadata.is_file() {
        return Ok(None); // ignore non-files
    }

    let filename = entry.file_name().into_string().map_err(|_| error())?;
    match filename.split_once('-') {
        Some(("tmp", _)) => {
            // remove temporary files.
            fs::remove_file(entry.path())?;
            Ok(None)
        }
        Some(("open", id)) => {
            let id = u64::from_str(id).map_err(|_| error())?;
            let segment = Segment::open(entry.path())?;
            Ok(Some(WalSegment::Open(OpenSegment { id, segment })))
        }
        Some(("closed", start)) => {
            let start = u64::from_str(start).map_err(|_| error())?;
            let segment = Segment::open(entry.path())?;
            Ok(Some(WalSegment::Closed(ClosedSegment {
                start_index: start,
                segment,
            })))
        }
        _ => Ok(None), // Ignore other files.
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use std::num::{NonZeroU8, NonZeroUsize};

    use fs_err as fs;
    use log::trace;
    use quickcheck::{QuickCheck, TestResult};
    use tempfile::Builder;

    use super::{Wal, WalOptions};
    use crate::test_utils::EntryGenerator;

    /// Windows has very slow IO
    #[cfg(target_os = "windows")]
    const QC_TESTS: u64 = 10;

    #[cfg(not(target_os = "windows"))]
    const QC_TESTS: u64 = 100;

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_generate_empty_wal() {
        init_logger();
        let dir = Builder::new().prefix("wal").tempdir().unwrap();
        let options = WalOptions {
            segment_capacity: 80,
            segment_queue_len: 3,
            retain_closed: NonZeroUsize::new(1).unwrap(),
        };

        // Create empty wal with initial id 10.
        let init_offset = 10;
        Wal::generate_empty_wal_starting_at_index(dir.path(), &options, init_offset).unwrap();

        let mut wal = Wal::with_options(dir.path(), &options).unwrap();

        let first_index = wal.first_index();
        let last_index = wal.last_index();
        let num_entries = wal.num_entries();

        assert!(first_index <= last_index);
        assert_eq!(num_entries, 0);

        let next_entry: Vec<u8> = vec![1, 2, 3];
        let op = wal.append(&next_entry).unwrap();

        assert!(op > init_offset);

        let first_index = wal.first_index();
        let last_index = wal.last_index();
        let num_entries = wal.num_entries();

        assert!(first_index <= last_index);
        assert_eq!(num_entries, 1);

        wal.append(&next_entry).unwrap();
        wal.append(&next_entry).unwrap();

        let first_index = wal.first_index();
        let last_index = wal.last_index();
        let num_entries = wal.num_entries();

        assert!(first_index <= last_index);
        assert_eq!(num_entries, 3);
    }

    #[test]
    fn test_create_empty_wal_with_initial_id() {
        init_logger();
        let dir = Builder::new().prefix("wal").tempdir().unwrap();
        let options = WalOptions {
            segment_capacity: 80,
            segment_queue_len: 3,
            retain_closed: NonZeroUsize::new(1).unwrap(),
        };

        // Create empty wal with initial id 10.
        let init_offset = 10;
        Wal::generate_empty_wal_starting_at_index(dir.path(), &options, init_offset).unwrap();

        let mut wal = Wal::with_options(dir.path(), &options).unwrap();

        let last_index = wal.last_index();

        assert!(last_index > init_offset);

        assert_eq!(wal.num_entries(), 0);

        let next_entry: Vec<u8> = vec![1, 2, 3];

        wal.append(&next_entry).unwrap();

        let last_index = wal.last_index();
        assert_eq!(last_index, init_offset + 1);

        assert_eq!(wal.num_entries(), 1);

        let entry_count = 50;

        let entries = EntryGenerator::new().take(entry_count).collect::<Vec<_>>();

        for entry in &entries {
            wal.append(entry).unwrap();
        }

        let last_index = wal.last_index();
        assert_eq!(last_index, init_offset + 1 + entry_count as u64);

        assert_eq!(wal.num_entries(), 1 + entry_count as u64);

        // read random entry back to make sure it is correct.
        {
            let entry_index = init_offset + 1;
            let entry = wal.entry(entry_index).unwrap();
            assert_eq!(next_entry[..], entry[..]);

            let entry_index = init_offset + 1 + entry_count as u64;
            let entry = wal.entry(entry_index).unwrap();
            assert_eq!(entries[entry_count - 1][..], entry[..]);

            let entry_index = init_offset + 1 + 10;
            let entry = wal.entry(entry_index).unwrap();
            assert_eq!(entries[9][..], entry[..]);
        }

        wal.prefix_truncate(init_offset).unwrap();

        assert_eq!(wal.num_entries(), entry_count as u64 + 1);

        wal.prefix_truncate(init_offset + 20).unwrap();

        assert!(wal.num_entries() < entry_count as u64 + 1);

        let truncate_index = init_offset + 30;
        wal.truncate(truncate_index).unwrap();

        let last_index = wal.last_index();
        assert_eq!(last_index, truncate_index - 1);
    }

    /// Check that entries appended to the write ahead log can be read back.
    #[test]
    fn check_wal() {
        init_logger();
        fn wal(entry_count: u8) -> TestResult {
            let dir = Builder::new().prefix("wal").tempdir().unwrap();
            let mut wal = Wal::with_options(
                dir.path(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                    retain_closed: NonZeroUsize::new(1).unwrap(),
                },
            )
            .unwrap();
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();

            for entry in &entries {
                wal.append(entry).unwrap();
            }

            for (index, expected) in entries.iter().enumerate() {
                match wal.entry(index as u64) {
                    Some(ref entry) if entry[..] != expected[..] => return TestResult::failed(),
                    None => return TestResult::failed(),
                    _ => (),
                }
            }
            TestResult::passed()
        }

        QuickCheck::new()
            .tests(QC_TESTS)
            .quickcheck(wal as fn(u8) -> TestResult);
    }

    #[test]
    fn check_last_index() {
        init_logger();
        fn check(entry_count: u8) -> TestResult {
            let dir = Builder::new().prefix("wal").tempdir().unwrap();
            let mut wal = Wal::with_options(
                dir.path(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                    retain_closed: NonZeroUsize::new(1).unwrap(),
                },
            )
            .unwrap();
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();

            for entry in &entries {
                wal.append(entry).unwrap();
            }
            if entries.is_empty() {
                assert_eq!(wal.last_index(), 0);
            } else {
                assert_eq!(wal.last_index(), entries.len() as u64 - 1);
            }

            let last_index = wal.last_index();
            if wal.entry(last_index).is_none() && wal.num_entries() != 0 {
                return TestResult::failed();
            }
            if wal.entry(last_index + 1).is_some() {
                return TestResult::failed();
            }
            TestResult::passed()
        }

        QuickCheck::new()
            .tests(QC_TESTS)
            .quickcheck(check as fn(u8) -> TestResult)
    }

    #[test]
    fn check_clear() {
        init_logger();
        fn check(entry_count: u8) -> TestResult {
            let dir = Builder::new().prefix("wal").tempdir().unwrap();
            let mut wal = Wal::with_options(
                dir.path(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                    retain_closed: NonZeroUsize::new(1).unwrap(),
                },
            )
            .unwrap();
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();

            for entry in &entries {
                wal.append(entry).unwrap();
            }
            wal.clear().unwrap();
            TestResult::from_bool(wal.num_entries() == 0)
        }

        QuickCheck::new()
            .tests(QC_TESTS)
            .quickcheck(check as fn(u8) -> TestResult)
    }

    /// Check that the Wal will read previously written entries.
    #[test]
    fn check_reopen() {
        init_logger();
        fn wal(entry_count: u8) -> TestResult {
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();
            let dir = Builder::new().prefix("wal").tempdir().unwrap();
            {
                let mut wal = Wal::with_options(
                    dir.path(),
                    &WalOptions {
                        segment_capacity: 80,
                        segment_queue_len: 3,
                        retain_closed: NonZeroUsize::new(1).unwrap(),
                    },
                )
                .unwrap();
                for entry in &entries {
                    let _ = wal.append(entry);
                }
            }

            {
                // Create fake temp file to simulate a crash.
                let mut file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(dir.path().join("tmp-open-123"))
                    .unwrap();

                let _ = file.write(b"123").unwrap();
            }

            let wal = Wal::with_options(
                dir.path(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                    retain_closed: NonZeroUsize::new(1).unwrap(),
                },
            )
            .unwrap();
            // Check that all of the entries are present.
            for (index, expected) in entries.iter().enumerate() {
                match wal.entry(index as u64) {
                    Some(ref entry) if entry[..] != expected[..] => return TestResult::failed(),
                    None => return TestResult::failed(),
                    _ => (),
                }
            }
            TestResult::passed()
        }

        QuickCheck::new()
            .tests(QC_TESTS)
            .quickcheck(wal as fn(u8) -> TestResult);
    }

    #[test]
    fn check_truncate() {
        init_logger();
        fn truncate(entry_count: u8, truncate: u8) -> TestResult {
            if truncate > entry_count {
                return TestResult::discard();
            }
            let dir = Builder::new().prefix("wal").tempdir().unwrap();
            let mut wal = Wal::with_options(
                dir.path(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                    retain_closed: NonZeroUsize::new(1).unwrap(),
                },
            )
            .unwrap();
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();

            for entry in &entries {
                if let Err(error) = wal.append(entry) {
                    return TestResult::error(error.to_string());
                }
            }

            wal.truncate(u64::from(truncate)).unwrap();

            for (index, expected) in entries.iter().take(truncate as usize).enumerate() {
                match wal.entry(index as u64) {
                    Some(ref entry) if entry[..] != expected[..] => return TestResult::failed(),
                    None => return TestResult::failed(),
                    _ => (),
                }
            }

            TestResult::from_bool(wal.entry(u64::from(truncate)).is_none())
        }

        QuickCheck::new()
            .tests(QC_TESTS)
            .quickcheck(truncate as fn(u8, u8) -> TestResult);
    }

    #[test]
    fn check_prefix_truncate() {
        init_logger();
        fn prefix_truncate(entry_count: u8, until: u8, retain_closed: NonZeroU8) -> TestResult {
            trace!(
                "prefix truncate; entry_count: {entry_count}, until: {until}, retain_closed: {retain_closed}",
            );
            if until > entry_count {
                return TestResult::discard();
            }
            let dir = Builder::new().prefix("wal").tempdir().unwrap();
            let mut wal = Wal::with_options(
                dir.path(),
                &WalOptions {
                    segment_capacity: 80,
                    segment_queue_len: 3,
                    retain_closed: NonZeroUsize::from(retain_closed),
                },
            )
            .unwrap();
            let entries = EntryGenerator::new()
                .take(entry_count as usize)
                .collect::<Vec<_>>();

            let mut has_ever_reached_max = false;
            let retain_closed = retain_closed.get() as usize;

            for entry in &entries {
                wal.append(entry).unwrap();
                if wal.closed_segments.len() >= retain_closed {
                    has_ever_reached_max = true;
                }
            }

            wal.prefix_truncate(u64::from(until)).unwrap();

            let retained = if has_ever_reached_max {
                // If it ever reaches max, it should stay there
                if until < entry_count {
                    // If `until` is (much) lower we might retain more to satisfy prefix_truncate
                    wal.closed_segments.len() >= retain_closed
                } else {
                    wal.closed_segments.len() == retain_closed
                }
            } else {
                wal.closed_segments.len() < retain_closed
            };

            let num_entries = wal.num_entries() as u8;
            TestResult::from_bool(
                num_entries <= entry_count && num_entries >= entry_count - until && retained,
            )
        }
        QuickCheck::new()
            .tests(QC_TESTS)
            .quickcheck(prefix_truncate as fn(u8, u8, NonZeroU8) -> TestResult);
    }

    #[test]
    fn test_append() {
        init_logger();
        let dir = Builder::new().prefix("wal").tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();

        let entry: &[u8] = &[42u8; 4096];
        for _ in 1..10 {
            wal.append(&entry).unwrap();
        }
    }

    #[test]
    fn test_truncate() {
        init_logger();
        let dir = Builder::new().prefix("wal").tempdir().unwrap();
        // 2 entries should fit in each segment
        let mut wal = Wal::with_options(
            dir.path(),
            &WalOptions {
                segment_capacity: 4096,
                segment_queue_len: 3,
                retain_closed: NonZeroUsize::new(1).unwrap(),
            },
        )
        .unwrap();

        let entry: [u8; 2000] = [42u8; 2000];

        for truncate_index in 0..10 {
            assert!(wal.entry(0).is_none());
            for i in 0..10 {
                assert_eq!(i, wal.append(&&entry[..]).unwrap());
            }

            wal.truncate(truncate_index).unwrap();

            assert!(wal.entry(truncate_index).is_none());

            if truncate_index > 0 {
                assert!(wal.entry(truncate_index - 1).is_some());
            }
            wal.truncate(0).unwrap();
        }
    }

    fn run_test_with_retain_closed(retain_closed: usize) {
        init_logger();
        let num_entries = 10;
        let dir = Builder::new().prefix("wal").tempdir().unwrap();
        let entry: [u8; 2000] = [42u8; 2000];
        let mut wal = Wal::with_options(
            dir.path(),
            &WalOptions {
                segment_capacity: 4096,
                segment_queue_len: 3,
                retain_closed: NonZeroUsize::new(retain_closed).unwrap(),
            },
        )
        .unwrap();

        let entries_per_segment = 2; // Based on segment_capacity / entry_size

        for truncate_index in 0..num_entries {
            assert!(wal.entry(0).is_none());
            for i in 0..num_entries {
                assert_eq!(i, wal.append(&&entry[..]).unwrap());
            } // for num_entries=10 => 4 closed segments + 1 open segment (each having 2 entries)

            let initial_closed_segments = wal.closed_segments.len();

            // Do the prefix truncation
            wal.prefix_truncate(truncate_index).unwrap();

            // Generalized logic
            let segments_that_can_be_truncated = truncate_index / entries_per_segment;
            let expected_closed_segments = (initial_closed_segments
                - segments_that_can_be_truncated as usize)
                .max(retain_closed);
            let expected_trimmed_until =
                (initial_closed_segments - expected_closed_segments) as u64 * entries_per_segment;

            assert!(wal.entry(truncate_index).is_some()); // truncate_index should be intact

            for i in 0..expected_trimmed_until {
                // before should be trimmed
                assert!(wal.entry(i).is_none());
            }

            for i in expected_trimmed_until..num_entries {
                // after should be intact
                assert!(wal.entry(i).is_some());
            }

            assert_eq!(wal.closed_segments.len(), expected_closed_segments);
            wal.truncate(0).unwrap(); // Clean up for the next test case
        }
    }

    #[test]
    fn test_prefix_truncate_parametric() {
        run_test_with_retain_closed(1);
        run_test_with_retain_closed(2);
        run_test_with_retain_closed(3);
    }

    #[test]
    fn test_truncate_flush() {
        init_logger();
        let dir = Builder::new().prefix("wal").tempdir().unwrap();
        // 2 entries should fit in each segment
        let mut wal = Wal::with_options(
            dir.path(),
            &WalOptions {
                segment_capacity: 4096,
                segment_queue_len: 3,
                retain_closed: NonZeroUsize::new(1).unwrap(),
            },
        )
        .unwrap();

        let entry: [u8; 2000] = [42u8; 2000];
        // wal is empty
        assert!(wal.entry(0).is_none());

        // add 10 entries
        for i in 0..10 {
            assert_eq!(i, wal.append(&&entry[..]).unwrap());
        }

        // 4 closed segments
        assert_eq!(wal.num_entries(), 10);
        assert_eq!(wal.first_index(), 0);
        assert_eq!(wal.last_index(), 9);
        assert_eq!(wal.closed_segments.len(), 4); // 4 x 2 entries
        assert_eq!(wal.closed_segments[0].segment.len(), 2);
        assert_eq!(wal.closed_segments[1].segment.len(), 2);
        assert_eq!(wal.closed_segments[2].segment.len(), 2);
        assert_eq!(wal.closed_segments[3].segment.len(), 2);
        assert_eq!(wal.open_segment.segment.len(), 2); // 1 x 2 entries

        // first flush to set `flush_offset
        wal.flush_open_segment().unwrap();

        // content unchanged after flushing
        assert_eq!(wal.num_entries(), 10);
        assert_eq!(wal.first_index(), 0);
        assert_eq!(wal.last_index(), 9);
        assert_eq!(wal.closed_segments.len(), 4); // 4 x 2 entries
        assert_eq!(wal.closed_segments[0].segment.len(), 2);
        assert_eq!(wal.closed_segments[1].segment.len(), 2);
        assert_eq!(wal.closed_segments[2].segment.len(), 2);
        assert_eq!(wal.closed_segments[3].segment.len(), 2);
        assert_eq!(wal.open_segment.segment.len(), 2); // 1 x 2 entries

        wal.truncate(9).unwrap();

        assert_eq!(wal.open_segment.segment.len(), 1); // 1 x 2 entries

        // truncate half of it
        wal.truncate(5).unwrap();

        // assert truncation
        for i in 5..10 {
            assert!(wal.entry(i).is_none());
        }

        // flush again with `flush_offset` > segment size
        wal.flush_open_segment().unwrap();

        assert_eq!(wal.num_entries(), 5); // 5 entries removed
        assert_eq!(wal.first_index(), 0);
        assert_eq!(wal.last_index(), 4);
        assert_eq!(wal.closed_segments.len(), 3); // (0, 1) + (2, 3) + (4, empty slot)
        assert_eq!(wal.closed_segments[0].segment.len(), 2);
        assert_eq!(wal.closed_segments[1].segment.len(), 2);
        assert_eq!(wal.closed_segments[2].segment.len(), 1);
        assert_eq!(wal.open_segment.segment.len(), 0); // empty open segment

        // add 5 more entries
        for i in 0..5 {
            assert_eq!(i + 5, wal.append(&&entry[..]).unwrap());
        }

        // 5 closed segments
        assert_eq!(wal.num_entries(), 10);
        assert_eq!(wal.first_index(), 0);
        assert_eq!(wal.last_index(), 9);
        assert_eq!(wal.closed_segments.len(), 5);
        assert_eq!(wal.closed_segments[0].segment.len(), 2); // 1,2
        assert_eq!(wal.closed_segments[1].segment.len(), 2); // 3
        assert_eq!(wal.closed_segments[2].segment.len(), 1); // 4 empty slot due to truncation
        assert_eq!(wal.closed_segments[3].segment.len(), 2); // 5, 6
        assert_eq!(wal.closed_segments[4].segment.len(), 2); // 7, 8
        assert_eq!(wal.open_segment.segment.len(), 1); // 9

        eprintln!("wal: {wal:?}");
        eprintln!("wal open: {:?}", wal.open_segment);
        eprintln!("wal closed: {:?}", wal.closed_segments);

        // test persistence
        drop(wal);
        let wal = Wal::open(dir.path()).unwrap();
        assert_eq!(wal.num_entries(), 10);
        assert_eq!(wal.first_index(), 0);
        assert_eq!(wal.last_index(), 9);
        assert_eq!(wal.closed_segments.len(), 5);
        assert_eq!(wal.closed_segments[0].segment.len(), 2);
        assert_eq!(wal.closed_segments[1].segment.len(), 2);
        assert_eq!(wal.closed_segments[2].segment.len(), 1); // previously half truncated
        assert_eq!(wal.closed_segments[3].segment.len(), 2);
        assert_eq!(wal.closed_segments[4].segment.len(), 2);
        assert_eq!(wal.open_segment.segment.len(), 1);
    }

    /// Tests that two Wal instances can not coexist for the same directory.
    #[test]
    fn test_exclusive_lock() {
        init_logger();
        let dir = Builder::new().prefix("wal").tempdir().unwrap();
        let wal = Wal::open(dir.path()).unwrap();
        assert_eq!(
            fs4::lock_contended_error().kind(),
            Wal::open(dir.path()).unwrap_err().kind()
        );
        drop(wal);
        assert!(Wal::open(dir.path()).is_ok());
    }

    #[test]
    fn test_record_id_preserving() {
        init_logger();
        let entry_count = 55;
        let dir = Builder::new().prefix("wal").tempdir().unwrap();
        let options = WalOptions {
            segment_capacity: 512,
            segment_queue_len: 3,
            retain_closed: NonZeroUsize::new(1).unwrap(),
        };

        let mut wal = Wal::with_options(dir.path(), &options).unwrap();
        let entries = EntryGenerator::new().take(entry_count).collect::<Vec<_>>();

        for entry in &entries {
            wal.append(entry).unwrap();
        }
        let closed_segments = wal.closed_segments.len();
        let start_index = wal.open_segment_start_index();

        wal.prefix_truncate(25).unwrap();
        let half_trunk_closed_segments = wal.closed_segments.len();
        let half_trunk_start_index = wal.open_segment_start_index();

        wal.prefix_truncate((entry_count - 2) as u64).unwrap();
        let full_trunk_closed_segments = wal.closed_segments.len();
        let full_trunk_start_index = wal.open_segment_start_index();

        assert!(closed_segments > half_trunk_closed_segments);
        assert!(half_trunk_closed_segments > full_trunk_closed_segments);

        assert_eq!(start_index, half_trunk_start_index);
        assert_eq!(start_index, full_trunk_start_index);
    }

    #[test]
    fn test_offset_after_open() {
        init_logger();
        let entry_count = 55;
        let dir = Builder::new().prefix("wal").tempdir().unwrap();
        let options = WalOptions {
            segment_capacity: 512,
            segment_queue_len: 3,
            retain_closed: NonZeroUsize::new(1).unwrap(),
        };
        let start_index;
        {
            let mut wal = Wal::with_options(dir.path(), &options).unwrap();
            let entries = EntryGenerator::new().take(entry_count).collect::<Vec<_>>();

            for entry in &entries {
                wal.append(entry).unwrap();
            }
            start_index = wal.open_segment_start_index();
            wal.prefix_truncate(25).unwrap();
            assert_eq!(start_index, wal.open_segment_start_index());
        }
        {
            let wal2 = Wal::with_options(dir.path(), &options).unwrap();
            assert_eq!(start_index, wal2.open_segment_start_index());
        }
    }
}
