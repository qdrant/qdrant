use std::cell::UnsafeCell;
use std::fmt;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

use fs_err::File;
use memmap2::{MmapMut, MmapOptions};

/// ported from https://github.com/danburkert/memmap-rs in version 0.5.2
///
/// A thread-safe view of a memory map.
///
/// The view may be split into disjoint ranges, each of which will share the
/// underlying memory map.
pub struct MmapViewSync {
    inner: Arc<UnsafeCell<MmapMut>>,
    offset: usize,
    len: usize,
}

impl MmapViewSync {
    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    pub fn close(&self) {
        #[cfg(target_os = "linux")]
        unsafe {
            let mmap = &mut *self.inner.get();
            if let Err(err) = mmap.unchecked_advise(memmap2::UncheckedAdvice::DontNeed) {
                log::warn!("Error clearing closed wal segment: {err:?}");
            }
        }
    }

    pub fn from_file(file: &File, offset: usize, capacity: usize) -> Result<MmapViewSync> {
        let mmap = unsafe {
            MmapOptions::new()
                .offset(offset as u64)
                .len(capacity)
                .map_mut(file)?
        };

        Ok(mmap.into())
    }

    #[allow(dead_code)]
    pub fn anonymous(capacity: usize) -> Result<MmapViewSync> {
        let mmap = MmapOptions::new().len(capacity).map_anon()?;

        Ok(mmap.into())
    }

    /// Split the view into disjoint pieces at the specified offset.
    ///
    /// The provided offset must be less than the view's length.
    #[allow(dead_code)]
    pub fn split_at(self, offset: usize) -> Result<(MmapViewSync, MmapViewSync)> {
        if self.len < offset {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mmap view split offset must be less than the view length",
            ));
        }
        let MmapViewSync {
            inner,
            offset: self_offset,
            len: self_len,
        } = self;
        Ok((
            MmapViewSync {
                inner: inner.clone(),
                offset: self_offset,
                len: offset,
            },
            MmapViewSync {
                inner,
                offset: self_offset + offset,
                len: self_len - offset,
            },
        ))
    }

    /// Restricts the range of this view to the provided offset and length.
    ///
    /// The provided range must be a subset of the current range (`offset + len < view.len()`).
    pub fn restrict(&mut self, offset: usize, len: usize) -> Result<()> {
        if offset + len > self.len {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mmap view may only be restricted to a subrange \
                                       of the current view",
            ));
        }
        self.offset += offset;
        self.len = len;
        Ok(())
    }

    /// Get a reference to the inner mmap.
    ///
    /// ## Unsafety
    ///
    /// The caller must ensure that the file is not concurrently modified.
    /// The caller must ensure that memory outside the `offset`/`len` range is not accessed.
    fn inner(&self) -> &MmapMut {
        unsafe { &*self.inner.get() }
    }

    /// Get a mutable reference to the inner mmap.
    ///
    /// ## Unsafety
    ///
    /// The caller must ensure that the file is not concurrently modified.
    /// The caller must ensure that memory outside the `offset`/`len` range is not accessed.
    fn inner_mut(&mut self) -> &mut MmapMut {
        unsafe { &mut *self.inner.get() }
    }

    /// Flushes outstanding view modifications to disk.
    ///
    /// When this returns with a non-error result, all outstanding changes to a file-backed memory
    /// map view are guaranteed to be durably stored. The file's metadata (including last
    /// modification timestamp) may not be updated.
    pub fn flush(&self) -> Result<()> {
        self.inner().flush_range(self.offset, self.len)
    }

    /// Returns the length of the memory map view.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the memory mapped file as an immutable slice.
    ///
    /// ## Unsafety
    ///
    /// The caller must ensure that the file is not concurrently modified.
    pub unsafe fn as_slice(&self) -> &[u8] {
        &self.inner()[self.offset..self.offset + self.len]
    }

    /// Returns the memory mapped file as a mutable slice.
    ///
    /// ## Unsafety
    ///
    /// The caller must ensure that the file is not concurrently accessed.
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        let (offset, len) = (self.offset, self.len);
        &mut self.inner_mut()[offset..offset + len]
    }

    /// Clones the view of the memory map.
    ///
    /// The underlying memory map is shared, and thus the caller must ensure that the memory
    /// underlying the view is not illegally aliased.
    pub unsafe fn clone(&self) -> MmapViewSync {
        MmapViewSync {
            inner: self.inner.clone(),
            offset: self.offset,
            len: self.len,
        }
    }
}

impl From<MmapMut> for MmapViewSync {
    fn from(mmap: MmapMut) -> MmapViewSync {
        let len = mmap.len();
        MmapViewSync {
            #[allow(clippy::arc_with_non_send_sync)]
            inner: Arc::new(UnsafeCell::new(mmap)),
            offset: 0,
            len,
        }
    }
}

impl fmt::Debug for MmapViewSync {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MmapViewSync {{ offset: {}, len: {} }}",
            self.offset, self.len
        )
    }
}

#[cfg(test)]
unsafe impl Sync for MmapViewSync {}
unsafe impl Send for MmapViewSync {}

#[cfg(test)]
mod test {
    use std::io::{Read, Write};
    use std::sync::Arc;
    use std::thread;

    use fs_err as fs;
    use tempfile::Builder;

    use super::*;

    #[test]
    fn view() {
        let len = 128;
        let split = 32;
        let mut view = MmapViewSync::anonymous(len).unwrap();
        let incr = (0..len).map(|n| n as u8).collect::<Vec<_>>();
        // write values into the view
        unsafe { view.as_mut_slice() }.write_all(&incr[..]).unwrap();

        let (mut view1, view2) = view.split_at(32).unwrap();
        assert_eq!(view1.len(), split);
        assert_eq!(view2.len(), len - split);

        assert_eq!(&incr[0..split], unsafe { view1.as_slice() });
        assert_eq!(&incr[split..], unsafe { view2.as_slice() });

        view1.restrict(10, 10).unwrap();
        assert_eq!(&incr[10..20], unsafe { view1.as_slice() })
    }

    #[test]
    fn view_sync() {
        let len = 128;
        let split = 32;
        let mut view = MmapViewSync::anonymous(len).unwrap();
        let incr = (0..len).map(|n| n as u8).collect::<Vec<_>>();
        // write values into the view
        unsafe { view.as_mut_slice() }.write_all(&incr[..]).unwrap();

        let (mut view1, view2) = view.split_at(32).unwrap();
        assert_eq!(view1.len(), split);
        assert_eq!(view2.len(), len - split);

        assert_eq!(&incr[0..split], unsafe { view1.as_slice() });
        assert_eq!(&incr[split..], unsafe { view2.as_slice() });

        view1.restrict(10, 10).unwrap();
        assert_eq!(&incr[10..20], unsafe { view1.as_slice() })
    }

    #[test]
    fn view_write() {
        let len = 131072; // 256KiB
        let split = 66560; // 65KiB + 10B

        let tempdir = Builder::new().prefix("mmap").tempdir().unwrap();
        let path = tempdir.path().join("mmap");

        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        file.set_len(len).unwrap();

        let incr = (0..len).map(|n| n as u8).collect::<Vec<_>>();
        let incr1 = incr[0..split].to_owned();
        let incr2 = incr[split..].to_owned();

        let view: MmapViewSync = MmapViewSync::from_file(&file, 0, len as usize).unwrap();
        let (mut view1, mut view2) = view.split_at(split).unwrap();

        let join1 = thread::spawn(move || {
            let _written = unsafe { view1.as_mut_slice() }.write(&incr1).unwrap();
            view1.flush().unwrap();
        });

        let join2 = thread::spawn(move || {
            let _written = unsafe { view2.as_mut_slice() }.write(&incr2).unwrap();
            view2.flush().unwrap();
        });

        join1.join().unwrap();
        join2.join().unwrap();

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        assert_eq!(incr, &buf[..]);
    }

    #[test]
    fn view_sync_send() {
        let view: Arc<MmapViewSync> = Arc::new(MmapViewSync::anonymous(128).unwrap());
        thread::spawn(move || unsafe {
            view.as_slice();
        });
    }
}
