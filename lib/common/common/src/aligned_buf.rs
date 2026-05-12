use std::ops::{Deref, RangeTo};

use aligned_vec::{AVec, ConstAlign};

use crate::zeros::ZEROS;

/// Like [`Vec<u8>`], but `&buf[i]` is aligned as if it lived at
/// `file_offset + i` in a file.
///
/// # Motivational example
///
/// Suppose a file that contains fields with mixed types and alignments. We want
/// to read 12-byte entry starting from `file_offset = 0xABC3` into a buffer.
///
/// ```text
///  file_offset=0xABC3  file_offset=0xABC8
///          ↓                   ↓
/// ───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬──
///    │   │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │ 9 │10 │11 │12 │   │
/// ───┴───┼───┴───┼───┴───┴───┼───┴───┴───┴───┴───┴───┴───┴───┼───┴──
///        │ val_1 │  padding  │             val_2             │
///        │[u8; 2]│  [u8; 3]  │              u64              │
///  
/// ```
///
/// For `val_2: u64`, its file offset is aligned by 8 bytes, but its position in
/// the buffer is not. So, to make it work, `&buf[5]` should be aligned.
/// So, the invariant (for some `align` that is large enough):
///
/// `ptr_of(&buf[buf_pos]) % align  ==  (file_offset + buf_pos) % align`
///
/// Files read through mmaps satisfy this invariant: mmaps' natural assignment is
/// 4096 bytes (usually), which is more than enough. This structure mimics that
/// fraction of mmap's power.
pub struct AlignedBuf {
    buf: AVec<u8, ConstAlign<ALIGN>>,
    offset: usize,
}

/// Large enough for our use-cases; we can extend it if needed.
/// [`u128`] is largest entry in persisted_hashmap.
const ALIGN: usize = size_of::<u128>();

impl AlignedBuf {
    /// Creates [`AlignedBuf`]. It will hold the alignment invariant, assuming
    /// we will read starting from `file_offset`.
    pub fn new_for_offset(file_offset: u64) -> Self {
        Self {
            buf: AVec::new(ALIGN),
            offset: (file_offset % ALIGN as u64) as usize,
        }
    }

    /// Just like [`Vec::extend_from_slice`].
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        if self.buf.is_empty() && self.offset > 0 {
            self.buf.reserve(self.offset + data.len());
            self.buf.extend_from_slice(&ZEROS[..self.offset]);
        }
        self.buf.extend_from_slice(data);
    }

    /// Drop the first `count` bytes from the front, like `vec.drain(..count)`.
    ///
    /// Preserves alignment:
    /// `&buf[i]` was aligned before => `&buf[i - count]` will be aligned after.
    pub fn remove_prefix(&mut self, range: RangeTo<usize>) {
        let RangeTo { end: count } = range;
        self.offset += count;
        let to_drop = self.offset - self.offset % ALIGN;
        self.offset %= ALIGN;
        if to_drop > 0 {
            self.buf.copy_within(to_drop.., 0);
            self.buf.truncate(self.buf.len() - to_drop);
        }
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        if self.buf.is_empty() {
            &[]
        } else {
            &self.buf[self.offset..]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buf() {
        let digits = (0..123).collect::<Vec<u8>>();
        let ptr_of = |x: &u8| x as *const _ as usize;

        let mut buf = AlignedBuf::new_for_offset(0xABC3);
        assert_eq!(buf.buf.capacity(), 0, "The constructor shouldn't allocate");
        assert_eq!(&buf[..], [] as [u8; 0]);

        buf.extend_from_slice(&digits[0..12]);
        assert_eq!(&buf[..], &digits[0..12]);
        assert_eq!(ptr_of(&buf[5]) % align_of::<u64>(), 0); // 0xABC3 + 5

        buf.extend_from_slice(&digits[12..123]);
        assert_eq!(&buf[..], &digits[0..123]);
        assert_eq!(ptr_of(&buf[5]) % align_of::<u64>(), 0); // 0xABC3 + 5

        buf.remove_prefix(..26);
        assert_eq!(&buf[..], &digits[26..123]);
        assert_eq!(ptr_of(&buf[3]) % align_of::<u64>(), 0); // 0xABC3 + 26 + 3
    }
}
