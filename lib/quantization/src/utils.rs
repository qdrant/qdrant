use std::mem;
use std::mem::size_of;

pub fn transmute_to_u8_slice<T>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<u8>(), mem::size_of_val(v)) }
}

pub fn transmute_from_u8_to_slice<T>(data: &[u8]) -> &[T] {
    debug_assert_eq!(data.len() % size_of::<T>(), 0);

    assert_eq!(
        data.as_ptr().align_offset(mem::align_of::<T>()),
        0,
        "transmuting byte slice 0x{:p} into slice of {}: \
         required alignment is {} bytes, \
         byte slice misaligned by {} bytes",
        data.as_ptr(),
        std::any::type_name::<T>(),
        mem::align_of::<T>(),
        data.as_ptr().align_offset(mem::align_of::<T>()),
    );

    let len = data.len() / size_of::<T>();
    let ptr = data.as_ptr().cast::<T>();
    unsafe { std::slice::from_raw_parts(ptr, len) }
}
