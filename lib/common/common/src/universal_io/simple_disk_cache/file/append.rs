//! The [`UniversalAppend`] implementation for [`DiskCache`]: append to the
//! remote (the single grow+write operation), then write the same bytes
//! through into the local mirror so tail reads don't re-fetch what was just
//! uploaded.

use std::io::ErrorKind;

use super::{DiskCache, State};
use crate::universal_io::simple_disk_cache::DiskCacheRemote;
use crate::universal_io::{
    ByteOffset, Flusher, Result, UniversalAppend, UniversalFlush, UniversalIoError, UniversalRead,
};

impl<R> UniversalFlush for DiskCache<R>
where
    R: DiskCacheRemote + UniversalAppend,
{
    fn flusher(&self) -> Flusher {
        // The remote is the durable source of truth: delegate to its
        // flusher, which covers remotes whose appends are not durable on
        // acknowledgement (local files need their fsync; object-store
        // flushers are no-ops). The local mirror is rebuilt from scratch on
        // every open (`LocalState::new` truncates), so there is nothing
        // worth flushing locally. A never-materialized cache has made no
        // appends — nothing to flush either.
        if !self.is_ready() {
            return Box::new(|| Ok(()));
        }

        self.state()
            .expect("`is_ready` guarantees the `Ready` state")
            .remote
            .flusher()
    }
}

impl<R> UniversalAppend for DiskCache<R>
where
    R: DiskCacheRemote + UniversalAppend,
{
    fn append<T: bytemuck::Pod>(&mut self, data: &[T]) -> Result<ByteOffset> {
        self.append_impl(&[bytemuck::cast_slice(data)])
    }

    fn append_batch<'a, T: bytemuck::Pod>(
        &mut self,
        items: impl IntoIterator<Item = &'a [T]>,
    ) -> Result<ByteOffset> {
        let slices: Vec<&[u8]> = items.into_iter().map(bytemuck::cast_slice).collect();
        self.append_impl(&slices)
    }
}

impl<R> DiskCache<R>
where
    R: DiskCacheRemote + UniversalAppend,
{
    fn append_impl(&mut self, slices: &[&[u8]]) -> Result<ByteOffset> {
        if !self.open_options.writeable {
            return Err(UniversalIoError::Io(std::io::Error::new(
                ErrorKind::PermissionDenied,
                "append requires a handle opened with writeable=true",
            )));
        }

        // Materialize `State::Ready`, then take it mutably: appending needs
        // `&mut` access to both the remote handle and the mirror, and
        // `&mut self` guarantees no concurrent readers on this handle.
        self.state()?;
        let State::Ready { remote, local } = self.state.get_mut() else {
            unreachable!("`state()` materializes `Ready`")
        };

        // The single remote grow+write operation.
        let offset = remote.append_batch(slices.iter().copied())?;

        let total: u64 = slices.iter().map(|slice| slice.len() as u64).sum();
        if total == 0 {
            return Ok(offset);
        }

        let local_len = local.mmap().len::<u8>()?;
        if offset == local_len {
            // Write-through: the appended bytes are in memory already, so
            // fill the mirror instead of re-fetching them from the remote.
            local.append_local(&self.local_path, offset, slices)?;
        } else {
            // The mirror is out of sync (the remote grew behind our back).
            // Extend to the new length and let the lazy fetch machinery heal
            // the gap on the next read.
            local.resize(&self.local_path, offset + total)?;
        }

        Ok(offset)
    }
}
