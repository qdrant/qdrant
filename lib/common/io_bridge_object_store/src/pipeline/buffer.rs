use std::future::Future;

use common::universal_io::{ReadRange, Result, UniversalIoError};
use futures::StreamExt as _;
use tokio::io::AsyncWriteExt as _;

use crate::file::BlobFile;
use crate::read::AsyncRead;
use crate::writer::AlignedBufWriter;

/// Send-able raw byte pointer used to hand the future a writable view into a
/// `Vec<T>` that is owned by the pipeline's slot map.
///
/// # Why this exists
///
/// The spawned read task writes the response bytes; the originating pipeline
/// lives on a different thread. We want the destination buffer (`Vec<T>`) to be
/// allocated once and written into once — no `Bytes` aggregation, no
/// `pod_collect_to_vec` re-copy. To do that the future needs a `&mut [u8]` view
/// of the buffer, but a normal mutable reference cannot cross threads into a
/// spawned `'static` task. This struct is a `Send` wrapper around the raw
/// pointer + length pair.
///
/// # Safety invariant
///
/// The `Vec<T>` from which the `SendBytePtr` is derived must:
/// 1. Remain alive and at its original heap allocation address until the
///    future referencing this pointer has completed (success or error).
/// 2. Not be reallocated (no `push`, `reserve`, `shrink_to_fit`, etc.).
/// 3. Not be observed by the originating thread (no `&` or `&mut` access) while
///    the future may be using the pointer.
///
/// `PipelineInner::schedule` enforces this by moving the `Vec<T>` into its slot
/// map before dispatching the future and never touching it until `wait()`
/// removes it after the matching `BridgeResponse` arrives. The heap allocation
/// address is stable across `Vec` moves and `AHashMap` rehashes (those only
/// relocate the `(ptr, len, cap)` triplet, not the bytes).
pub(super) struct SendBytePtr {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: Exclusive access to the underlying bytes is guaranteed by the
// pipeline's ownership invariant — the future is the sole accessor for the
// lifetime of `SendBytePtr`.
unsafe impl Send for SendBytePtr {}

impl SendBytePtr {
    /// Derive a `SendBytePtr` from a `Vec<T>`'s heap allocation.
    ///
    /// # Safety
    /// The caller guarantees the `Vec<T>` will not be reallocated, freed, or
    /// otherwise accessed by `&`/`&mut` while any `&mut [u8]` view obtained
    /// through `as_slice_mut` is alive. In practice the pipeline upholds this
    /// by moving the `Vec<T>` into its slot map and not touching it until the
    /// future completes.
    pub(super) unsafe fn from_vec<T: bytemuck::Pod>(buf: &mut Vec<T>) -> Self {
        Self {
            ptr: buf.as_mut_ptr().cast::<u8>(),
            len: buf.len() * size_of::<T>(),
        }
    }

    /// # Safety
    /// The caller (the future) must hold exclusive access to the underlying
    /// buffer per the invariant documented on [`SendBytePtr`]. Only one
    /// `&mut [u8]` view may exist at a time.
    pub(super) unsafe fn as_slice_mut(&mut self) -> &mut [u8] {
        // SAFETY: ptr/len valid per the invariant; exclusivity is the
        // caller's contract.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

/// Allocate the exact-size destination buffer for `range` and build the future
/// that streams the backend read straight into it via a [`SendBytePtr`].
///
/// Returns the buffer — which the caller must move into the pipeline's slot map
/// before driving the future, keeping its heap allocation alive and stable —
/// and the write-future to dispatch. Shared by the borrowed and owned pipeline
/// `schedule` impls.
pub(super) fn read_into_buffer<A: AsyncRead, T: bytemuck::Pod>(
    file: &BlobFile<A>,
    range: ReadRange,
) -> (Vec<T>, impl Future<Output = Result<()>> + Send + 'static) {
    let item_size = size_of::<T>() as u64;
    let start = range.byte_offset;
    let end = start + range.length * item_size;

    let mut buf: Vec<T> = vec![T::zeroed(); range.length as usize];
    // SAFETY: `buf` is moved into the pipeline's slot map by the caller before
    // the returned future runs; the heap allocation address survives the move.
    // The pipeline never reads or mutates the buffer until `wait` removes it
    // after the matching BridgeResponse arrives, at which point the future is
    // no longer running. The pipeline never reallocates `buf`.
    let mut dst = unsafe { SendBytePtr::from_vec(&mut buf) };

    let stream_fut = file.inner.read_range(&file.path, start..end);
    let future = async move {
        let mut stream = stream_fut.await?;
        // SAFETY: this future is the sole accessor of the buffer for its
        // lifetime per the SendBytePtr invariant.
        let slice = unsafe { dst.as_slice_mut() };
        let mut writer = AlignedBufWriter::from_raw_bytes(slice);
        while let Some(chunk) = stream.next().await {
            writer
                .write_all(&chunk?)
                .await
                .map_err(UniversalIoError::s3)?;
        }
        if writer.written() != writer.capacity() {
            return Err(UniversalIoError::S3Config {
                description: format!(
                    "short read: expected {} bytes, got {}",
                    writer.capacity(),
                    writer.written(),
                ),
            });
        }
        Ok(())
    };
    (buf, future)
}
