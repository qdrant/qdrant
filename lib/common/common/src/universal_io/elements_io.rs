use crate::mmap;
use crate::universal_io::UniversalIoError;
use std::borrow::Cow;

pub type ElementsOffset = u64;

#[derive(Copy, Clone, Debug)]
pub struct ElementsRange {
    pub start: ElementsOffset,
    pub length: u64,
}

pub trait UniversalElementRead<T: Copy + 'static>: crate::universal_io::UniversalRead {
    /// Prefer [`read_batch`] if you need high performance.
    fn read_elements<const SEQUENTIAL: bool>(
        &self,
        range: ElementsRange,
    ) -> crate::universal_io::Result<Cow<'_, [T]>> {
        let byte_offset = range.start * size_of::<T>() as u64;
        let byte_length = range.length * size_of::<T>() as u64;

        let byte_range = crate::universal_io::BytesRange {
            start: byte_offset,
            length: byte_length,
        };

        let byte_data = self.read::<SEQUENTIAL>(byte_range)?;

        #[expect(deprecated, reason = "fuck off")]
        let res = match byte_data {
            Cow::Borrowed(bytes) => {
                Cow::Borrowed(unsafe { mmap::transmute_from_u8_to_slice(bytes) })
            }
            Cow::Owned(bytes) => Cow::Owned(unsafe { mmap::transmute_from_bytes_to_vec(bytes) }),
        };

        Ok(res)
    }

    fn read_elements_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = ElementsRange>,
        mut callback: impl FnMut(usize, &[T]) -> Result<(), UniversalIoError>,
    ) -> crate::universal_io::Result<()> {
        let byte_ranges = ranges
            .into_iter()
            .map(|range| crate::universal_io::BytesRange {
                start: range.start * size_of::<T>() as u64,
                length: range.length * size_of::<T>() as u64,
            });

        self.read_batch::<SEQUENTIAL>(byte_ranges, |idx, byte_data| {
            #[expect(deprecated, reason = "fuck off")]
            let data_slice = unsafe { mmap::transmute_from_u8_to_slice(byte_data) };
            callback(idx, data_slice)
        })
    }
}

pub trait UniversalElementWrite<T: Copy + 'static>: crate::universal_io::UniversalWrite {
    fn write_elements(
        &mut self,
        offset: ElementsOffset,
        data: &[T],
    ) -> crate::universal_io::Result<()>;

    fn write_elements_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ElementsOffset, &'a [T])>,
    ) -> crate::universal_io::Result<()>;
}
