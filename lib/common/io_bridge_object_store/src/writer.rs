//! [`AsyncWrite`] adapter that lets a backend stream network chunks directly
//! into a caller-provided, `T`-aligned typed buffer. Used by
//! [`AsyncRead::read_range`](crate::AsyncRead::read_range) impls to avoid the
//! intermediate `Bytes` aggregation that `object_store::ObjectStoreExt::get_range`
//! performs internally.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;

/// Writes incoming byte chunks into a `T`-aligned destination slice supplied
/// at construction. The `T` parameter is enforced only at `new`, where
/// [`bytemuck::cast_slice_mut`] reinterprets the typed slice as bytes; after
/// that the writer is type-agnostic, so a single impl serves every Pod target.
pub(crate) struct AlignedBufWriter<'a> {
    dst: &'a mut [u8],
    off: usize,
}

impl<'a> AlignedBufWriter<'a> {
    pub(crate) fn new<T: bytemuck::NoUninit + bytemuck::AnyBitPattern>(buf: &'a mut [T]) -> Self {
        let dst: &'a mut [u8] = bytemuck::cast_slice_mut(buf);
        Self { dst, off: 0 }
    }

    /// Wrap a pre-cast byte slice. Used by the pipeline path where the typed
    /// `&mut [T]` view has already been reconstructed from a raw pointer (see
    /// `SendBytePtr` in `pipeline.rs`) and a fresh `bytemuck::cast_slice_mut`
    /// would re-do work the caller has already done.
    pub(crate) fn from_raw_bytes(buf: &'a mut [u8]) -> Self {
        Self { dst: buf, off: 0 }
    }

    pub(crate) fn written(&self) -> usize {
        self.off
    }

    pub(crate) fn capacity(&self) -> usize {
        self.dst.len()
    }
}

impl AsyncWrite for AlignedBufWriter<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let remaining = this.dst.len().saturating_sub(this.off);
        if remaining == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "AlignedBufWriter is full",
            )));
        }
        let n = data.len().min(remaining);
        let off = this.off;
        this.dst[off..off + n].copy_from_slice(&data[..n]);
        this.off += n;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn writes_single_chunk_into_aligned_buf() {
        let mut buf: Vec<u32> = vec![0u32; 3];
        {
            let mut w = AlignedBufWriter::new(&mut buf);
            w.write_all(&[1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0])
                .await
                .unwrap();
            assert_eq!(w.written(), 12);
            assert_eq!(w.capacity(), 12);
        }
        assert_eq!(buf, vec![1u32, 2, 3]);
    }

    #[tokio::test]
    async fn writes_multiple_chunks_concatenated() {
        let mut buf: Vec<u16> = vec![0u16; 4];
        {
            let mut w = AlignedBufWriter::new(&mut buf);
            w.write_all(&[0x01, 0x00]).await.unwrap();
            w.write_all(&[0x02, 0x00, 0x03, 0x00]).await.unwrap();
            w.write_all(&[0x04, 0x00]).await.unwrap();
            assert_eq!(w.written(), 8);
        }
        assert_eq!(buf, vec![1u16, 2, 3, 4]);
    }

    #[tokio::test]
    async fn writes_past_capacity_return_zero_error() {
        let mut buf: Vec<u8> = vec![0u8; 4];
        let mut w = AlignedBufWriter::new(&mut buf);
        w.write_all(&[0xAA; 4]).await.unwrap();
        let err = w.write(&[0xBB]).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    }

    #[tokio::test]
    async fn from_raw_bytes_writes_into_provided_slice() {
        let mut buf: Vec<u8> = vec![0u8; 6];
        {
            let mut w = AlignedBufWriter::from_raw_bytes(&mut buf);
            w.write_all(&[1, 2, 3]).await.unwrap();
            w.write_all(&[4, 5, 6]).await.unwrap();
            assert_eq!(w.written(), 6);
            assert_eq!(w.capacity(), 6);
        }
        assert_eq!(buf, vec![1, 2, 3, 4, 5, 6]);
    }
}
