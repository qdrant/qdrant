use std::borrow::Cow;
use std::ops::{Deref, DerefMut, RangeTo};

use aligned_vec::{AVec, RuntimeAlign};

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
#[derive(Debug)]
pub struct AlignedBuf {
    inner: AVec<u8, RuntimeAlign>,
    /// The `inner` vec would have such amount of leading zeros.
    /// (unless in the empty state)
    offset: usize,
}

impl AlignedBuf {
    /// Creates [`AlignedBuf`]. It will hold the alignment invariant for `align`,
    /// assuming we will read starting from `file_offset`.
    ///
    /// Implementation detail: this method dosn't allocate. That means, if
    /// `offset > 0`, the inner vec initialized in an empty state, i.e. without
    /// the the leading zeros.
    pub fn new_for_offset(file_offset: u64, align: usize) -> Self {
        assert!(align.is_power_of_two(), "Alignment must be a power of two");
        Self {
            inner: AVec::new(align),
            offset: (file_offset & (align as u64 - 1)) as usize,
        }
    }

    /// Same as [`AlignedBuf::new_for_offset`], but reserves `capacity`.
    pub fn with_capacity(file_offset: u64, align: usize, capacity: usize) -> Self {
        assert!(align.is_power_of_two(), "Alignment must be a power of two");
        let offset = (file_offset & (align as u64 - 1)) as usize;
        let mut inner = AVec::with_capacity(align, offset + capacity);
        inner.extend_from_slice(&ZEROS[..offset]);
        Self { inner, offset }
    }

    /// Just like [`Vec::extend_from_slice`].
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        if self.inner.is_empty() && self.offset > 0 {
            // Recover from the empty state by filling leading zeros.
            self.inner.reserve(self.offset + data.len());
            self.inner.extend_from_slice(&ZEROS[..self.offset]);
        }
        self.inner.extend_from_slice(data);
    }

    /// Just like [`Vec::reserve`].
    pub fn reserve(&mut self, additional: usize) {
        let new_inner_capacity = self.offset
            + self.len() // Logical length. Not self.inner.len()!
            + additional;
        self.inner.reserve(new_inner_capacity - self.inner.len());
        if self.inner.is_empty() {
            // Recover from the empty state by filling leading zeros.
            self.inner.extend_from_slice(&ZEROS[..self.offset]);
        }
    }

    /// Just like [`Vec::capacity`].
    pub fn capacity(&self) -> usize {
        self.inner.capacity() - self.data_offset()
    }

    /// Just like [`Vec::set_len`].
    ///
    /// # Safety
    ///
    /// The elements at `old_len..new_len` must be initialized.
    pub unsafe fn set_len(&mut self, byte_length: usize) {
        let new_inner_len = self
            .offset
            .checked_add(byte_length)
            .expect("Length overflow");
        assert!(new_inner_len <= self.inner.capacity());
        unsafe { self.inner.set_len(new_inner_len) }
    }

    /// Drop the first `count` bytes from the front, like `vec.drain(..count)`.
    ///
    /// Preserves alignment:
    /// `&buf[i]` was aligned before => `&buf[i - count]` will be aligned after.
    pub fn remove_prefix(&mut self, range: RangeTo<usize>) {
        let RangeTo { end: count } = range;
        self.offset += count;
        let to_drop = self.offset & !(self.inner.alignment() - 1);
        self.offset &= self.inner.alignment() - 1;
        if to_drop > 0 {
            self.inner.copy_within(to_drop.., 0);
            self.inner.truncate(self.inner.len() - to_drop);
        }
    }

    /// Try to transmute an the buffer into `Vec<T>`.
    ///
    /// Size/alignment requirements are checked by this method.
    ///
    /// # Safety
    ///
    /// Bytes in the buffer must be valid `T` values.
    pub unsafe fn try_transmute<T: Copy>(self) -> Result<Vec<T>, CastError> {
        if self.offset == 0 {
            unsafe { transmute_avec(self.inner) }
        } else {
            // Offset screws up the alignment.
            Err(CastError)
        }
    }

    pub fn try_cast_bytemuck<T: bytemuck::Pod>(self) -> Result<Vec<T>, CastError> {
        // Safety: T is bytemuck::Pod
        unsafe { self.try_transmute() }
    }

    /// The offset where logical data starts, i.e. first index past the leading
    /// zeros. Special case: in the empty state, return 0 because we might have
    /// not enough leading zeros.
    fn data_offset(&self) -> usize {
        if self.inner.is_empty() {
            0
        } else {
            self.offset
        }
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.inner[self.data_offset()..]
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        let offset = self.data_offset();
        &mut self.inner[offset..]
    }
}

pub enum AlignedCow<'a> {
    Borrowed(&'a [u8]),
    Owned(AlignedBuf),
}

impl<'a> AlignedCow<'a> {
    pub fn try_cast_bytemuck<T: bytemuck::Pod>(self) -> Result<Cow<'a, [T]>, CastError> {
        Ok(match self {
            AlignedCow::Borrowed(bytes) => {
                Cow::Borrowed(bytemuck::try_cast_slice(bytes).map_err(|_| CastError)?)
            }
            AlignedCow::Owned(buf) => Cow::Owned(buf.try_cast_bytemuck()?),
        })
    }
}

impl Deref for AlignedCow<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            AlignedCow::Borrowed(bytes) => bytes,
            AlignedCow::Owned(buf) => buf,
        }
    }
}

/// Try to transmute an `AVec<u8, RuntimeAlign>` into `Vec<T>`.
///
/// # Safety
///
/// Bytes in the buffer must be valid `T` values.
unsafe fn transmute_avec<T: Copy>(avec: AVec<u8, RuntimeAlign>) -> Result<Vec<T>, CastError> {
    if size_of::<T>() == 0 {
        // We don't support ZST for simplicity.
        return Err(CastError);
    }
    if avec.alignment() != align_of::<T>() {
        // The pointer passed into Vec::from_raw_parts should be
        // allocated with _the same_ alignment as T. Emphasis: _the same_,
        // not just compatible.
        //
        // Why: `Vec::<T>::drop()` will call `dealloc(ptr, layout)`, and
        // `layout.align()` must be the same as the one used for allocation.
        // What if it isn't? Honestly, not a big deal, but the documentation
        // says it's an UB.
        return Err(CastError);
    }
    if !avec.len().is_multiple_of(size_of::<T>()) || !avec.capacity().is_multiple_of(size_of::<T>())
    {
        // Vec<T> counts length and capacity in elements, not bytes,
        // so these should be evenly divisible.
        return Err(CastError);
    }

    let (ptr, alignment_bytes, length_bytes, capacity_bytes) = avec.into_raw_parts();
    // Same checks again, just to be sure.
    assert_eq!(alignment_bytes, align_of::<T>());
    assert!(capacity_bytes.is_multiple_of(size_of::<T>()));
    assert!(length_bytes.is_multiple_of(size_of::<T>()));

    let length_elems = length_bytes / size_of::<T>();
    let capacity_elems = capacity_bytes / size_of::<T>();
    Ok(unsafe { Vec::from_raw_parts(ptr.cast(), length_elems, capacity_elems) })
}

#[derive(Debug)]
pub struct CastError;

impl From<CastError> for std::io::Error {
    fn from(_: CastError) -> Self {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Failed to cast AlignedBuf into Vec<T>",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buf() {
        let digits = (0..123).collect::<Vec<u8>>();
        let ptr_of = |x: &u8| x as *const _ as usize;

        let mut buf = AlignedBuf::new_for_offset(0xABC3, align_of::<u128>());
        assert_eq!(
            buf.inner.capacity(),
            0,
            "The constructor shouldn't allocate"
        );
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
