use std::borrow::Cow;
use std::io;
use std::mem::size_of;

use zerocopy::FromBytes;

type BucketOffset = u64;

/// Parsed view over the raw bucket-offsets array.
///
/// Wraps the bytes read from the buckets region and provides indexed access
/// to individual bucket offset values.
pub(super) struct BucketOffsets<'a>(Cow<'a, [u8]>);

impl<'a> BucketOffsets<'a> {
    pub fn new(bytes: Cow<'a, [u8]>) -> Self {
        Self(bytes)
    }

    pub fn len(&self) -> usize {
        self.0.len() / size_of::<BucketOffset>()
    }

    pub fn get(&self, index: usize) -> io::Result<u64> {
        let start = index * size_of::<BucketOffset>();
        let (offset, _) = BucketOffset::read_from_prefix(self.0.get(start..).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Bucket index out of bounds")
        })?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Can't read bucket offset"))?;
        Ok(offset)
    }

    pub fn to_sorted_vec(&self) -> Vec<u64> {
        let mut offsets: Vec<u64> = (0..self.len())
            .map(|i| self.get(i).expect("bucket offset out of range"))
            .collect();
        offsets.sort_unstable();
        offsets
    }
}
