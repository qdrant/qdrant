use std::borrow::Cow;
use std::ops::Deref;

use aligned_vec::Alignment;
pub use aligned_vec::{AVec, RuntimeAlign};
use bytemuck::PodCastError;

#[derive(Debug)]
pub enum ACow<'a> {
    Borrowed(&'a [u8]),
    Owned(AVec<u8, RuntimeAlign>),
}

impl<'a> ACow<'a> {
    pub fn try_cast_bytemuck<T: bytemuck::Pod>(self) -> Result<Cow<'a, [T]>, PodCastError> {
        Ok(match self {
            ACow::Borrowed(slice) => Cow::Borrowed(bytemuck::try_cast_slice(slice)?),
            ACow::Owned(vec) => Cow::Owned(
                cast_avec(vec, bytemuck::try_cast_slice, PodCastError::SizeMismatch)
                    .map_err(|(_, e)| e)?,
            ),
        })
    }
}

impl Deref for ACow<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            ACow::Borrowed(slice) => slice,
            ACow::Owned(vec) => vec,
        }
    }
}

/// Try to transmute an [`AVec<T1>`] into [`Vec<T2>`].
fn cast_avec<T1, T2, E, A: Alignment>(
    avec: AVec<T1, A>,
    cast_slice: impl FnOnce(&[T1]) -> Result<&[T2], E>,
    err: E,
) -> Result<Vec<T2>, (AVec<T1, A>, E)> {
    let slice_t1 = avec.as_slice();
    let slice_t1_start = slice_t1.as_ptr_range().start.cast::<u8>();
    let slice_t1_end = slice_t1.as_ptr_range().end.cast::<u8>();

    let slice_t2 = match cast_slice(slice_t1) {
        Ok(slice_t2) => slice_t2,
        Err(e) => return Err((avec, e)),
    };
    let slice_t2_start = slice_t2.as_ptr_range().start.cast::<u8>();
    let slice_t2_end = slice_t2.as_ptr_range().end.cast::<u8>();

    if slice_t1_start != slice_t2_start || slice_t1_end != slice_t2_end {
        return Err((avec, err));
    }

    if avec.alignment() != align_of::<T2>() {
        // The pointer passed into Vec::from_raw_parts should be
        // allocated with _the same_ alignment as T2 (not just compatible).
        //
        // Why: `Vec::<T2>::drop()` will call `dealloc(ptr, layout)`, and
        // `layout.align()` must be the same as the one used for allocation.
        // What if it isn't? Honestly, not a big deal, but the documentation
        // says it's an UB.
        return Err((avec, err));
    }
    let (Some(len_t2), Some(cap_t2)) = (
        cast_len::<T1, T2>(avec.len()),
        cast_len::<T1, T2>(avec.capacity()),
    ) else {
        return Err((avec, err));
    };
    assert_eq!(slice_t2.len(), len_t2);

    let (ptr, _alignment, _len, _cap) = avec.into_raw_parts();
    // SAFETY: We made excessive checks above:
    // - ZST are not supported for simplicity (`cast_len` rejects them).
    // - Alignment requirements are met.
    // - Size/capacity are correctly converted by `cast_len`.
    // - Bytes are valid values of T2 (checked by `cast_slice`).
    unsafe { Ok(Vec::<T2>::from_raw_parts(ptr.cast(), len_t2, cap_t2)) }
}

/// Checked version of `len_t1 * size_of::<T1>() / size_of::<T2>()`.
fn cast_len<T1, T2>(len_t1: usize) -> Option<usize> {
    if size_of::<T1>() == 0 || size_of::<T2>() == 0 {
        return None;
    }
    let len_bytes = len_t1.checked_mul(size_of::<T1>())?;
    if !len_bytes.is_multiple_of(size_of::<T2>()) {
        return None;
    }
    len_bytes.checked_div(size_of::<T2>())
}
