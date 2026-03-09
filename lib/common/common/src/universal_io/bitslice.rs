//! Storage-agnostic bitslice backed by any [`UniversalRead<u64>`] /
//! [`UniversalWrite<u64>`] backend.
//!
//! Provides [`BitSliceStorage`], a wrapper that interprets the underlying
//! `u64`-element storage as a sequence of bits, supporting both read and write
//! operations at the bit level.

use std::borrow::Cow;
use std::mem::size_of;
use std::path::Path;

use bitvec::mem::BitRegister;
use bitvec::order::Lsb0;
use bitvec::slice::BitSlice;
use bitvec::vec::BitVec;

use crate::mmap::create_and_ensure_length;
use crate::universal_io::{
    ElementsRange, Flusher, OpenOptions, Result, UniversalIoError, UniversalRead, UniversalWrite,
};

/// Number of bits per `u64` element.
const BITS_PER_ELEMENT: u64 = u64::BITS as u64;

/// Convenience alias for a bitslice backed by a memory-mapped file.
pub type MmapBitSliceStorage = BitSliceStorage<crate::universal_io::mmap::MmapUniversal<u64>>;

/// A storage-agnostic bitslice that supports both reading and writing bits.
///
/// Wraps any [`UniversalRead<u64>`] / [`UniversalWrite<u64>`] backend and
/// interprets the underlying `u64` elements as a sequence of bits.
/// Bit-level operations are translated to element-level reads and writes
/// on the backend.
#[derive(Debug)]
pub struct BitSliceStorage<S> {
    storage: S,
    /// Total number of `u64` elements in the underlying storage.
    element_len: u64,
}

impl<S: UniversalRead<u64>> BitSliceStorage<S> {
    /// Open a bitslice storage from the given path using backend `S`.
    pub fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let storage = S::open(path, options)?;
        let element_len = storage.len()?;
        Ok(Self {
            storage,
            element_len,
        })
    }

    /// Create a bitslice storage from an already-opened storage backend.
    pub fn from_storage(storage: S) -> Result<Self> {
        let element_len = storage.len()?;
        Ok(Self {
            storage,
            element_len,
        })
    }

    /// Create a new bitslice storage file with the given number of bits, all
    /// initialized to `false`.
    ///
    /// Creates the file at `path`, sizes it to hold at least `num_bits` bits
    /// (u64-aligned), and opens it.
    pub fn create(path: impl AsRef<Path>, num_bits: usize, options: OpenOptions) -> Result<Self> {
        let byte_len = num_bits
            .div_ceil(u8::BITS as usize)
            .next_multiple_of(size_of::<u64>());
        create_and_ensure_length(path.as_ref(), byte_len)?;
        Self::open(path, options)
    }

    /// Total number of bits available.
    pub fn bit_len(&self) -> u64 {
        self.element_len * BITS_PER_ELEMENT
    }

    /// Total number of `u64` elements in the underlying storage.
    pub fn element_len(&self) -> u64 {
        self.element_len
    }

    /// Read the entire storage and return it as a [`BitSlice`].
    ///
    /// Returns `Cow::Borrowed` when the backend supports zero-copy reads
    /// (e.g., mmap), otherwise returns `Cow::Owned`.
    pub fn read_all(&self) -> Result<Cow<'_, BitSlice<u64, Lsb0>>> {
        let elements = self.storage.read_whole()?;
        match elements {
            Cow::Borrowed(slice) => Ok(Cow::Borrowed(BitSlice::from_slice(slice))),
            Cow::Owned(vec) => Ok(Cow::Owned(BitVec::from_vec(vec))),
        }
    }

    /// Count the number of set bits in the entire storage.
    pub fn count_ones(&self) -> Result<usize> {
        Ok(self.read_all()?.count_ones())
    }

    /// Get a single bit at the given bit index.
    ///
    /// Fetches the containing `u64` element from the backend and extracts the
    /// target bit.
    ///
    /// Returns `None` if `bit_index` is out of bounds.
    pub fn get_bit(&self, bit_index: u64) -> Result<Option<bool>> {

		let element_index = bit_index >> <u64 as BitRegister>::INDX;
		let bit_within_element = bit_index as u8 & <u64 as BitRegister>::MASK;

        if element_index >= self.element_len {
            return Ok(None);
        }

        let element = self.storage.read::<false>(ElementsRange {
            start: element_index,
            length: 1,
        })?[0];

        let bitslice = BitSlice::<u64, Lsb0>::from_element(&element);

        Ok(bitslice.get(bit_within_element as usize).as_deref().copied())
    }

    /// Validate a bit range and return the corresponding element range.
    ///
    /// Returns `(element_start, element_count, offset_in_first_element)`.
    fn validate_bit_range(&self, bit_start: u64, bit_count: u64) -> Result<(u64, u64, usize)> {
        let bit_end = bit_start.checked_add(bit_count).ok_or_else(|| {
            UniversalIoError::OutOfBounds {
                start: bit_start,
                end: u64::MAX,
                elements: self.bit_len() as usize,
            }
        })?;
        let total_bits = self.bit_len();

        if bit_end > total_bits {
            return Err(UniversalIoError::OutOfBounds {
                start: bit_start,
                end: bit_end,
                elements: total_bits as usize,
            });
        }

        let element_start = bit_start / BITS_PER_ELEMENT;
        let element_end = bit_end.div_ceil(BITS_PER_ELEMENT);
        let element_count = element_end - element_start;
        let offset_in_first = (bit_start % BITS_PER_ELEMENT) as usize;

        Ok((element_start, element_count, offset_in_first))
    }

    /// Read a range of bits, returning them as a [`Cow`] bitslice.
    ///
    /// Returns `Cow::Borrowed` when the backend supports zero-copy reads
    /// (e.g., mmap), otherwise returns `Cow::Owned`.
    ///
    /// Translates the bit range to element-aligned reads so that only the
    /// minimal number of `u64` elements is fetched from the backend.
    pub fn read_bit_range(
        &self,
        bit_start: u64,
        bit_count: u64,
    ) -> Result<Cow<'_, BitSlice<u64, Lsb0>>> {
        if bit_count == 0 {
            return Ok(Cow::Owned(BitVec::new()));
        }

        let (element_start, element_count, offset_in_first) =
            self.validate_bit_range(bit_start, bit_count)?;

        let elements = self.storage.read::<false>(ElementsRange {
            start: element_start,
            length: element_count,
        })?;

        let end_within = offset_in_first + bit_count as usize;

        match elements {
            Cow::Borrowed(slice) => {
                let all_bits = BitSlice::<u64, Lsb0>::from_slice(slice);
                Ok(Cow::Borrowed(&all_bits[offset_in_first..end_within]))
            }
            Cow::Owned(vec) => {
                let all_bits = BitSlice::<u64, Lsb0>::from_slice(&vec);
                Ok(Cow::Owned(all_bits[offset_in_first..end_within].to_bitvec()))
            }
        }
    }

    /// Populate the underlying storage's RAM cache.
    pub fn populate(&self) -> Result<()> {
        self.storage.populate()
    }

    /// Evict the underlying storage's data from RAM cache.
    pub fn clear_ram_cache(&self) -> Result<()> {
        self.storage.clear_ram_cache()
    }
}

impl<S: UniversalWrite<u64>> BitSliceStorage<S> {
    /// Read-modify-write a single bit. Returns the previous value.
    ///
    /// Only writes to the backend if the element actually changed.
    fn modify_bit(&mut self, bit_index: u64, value: bool) -> Result<bool> {
        let element_index = bit_index / BITS_PER_ELEMENT;
        let bit_within_element = bit_index % BITS_PER_ELEMENT;

        if element_index >= self.element_len {
            return Err(UniversalIoError::OutOfBounds {
                start: bit_index,
                end: bit_index + 1,
                elements: self.bit_len() as usize,
            });
        }

        let elements = self.storage.read::<false>(ElementsRange {
            start: element_index,
            length: 1,
        })?;

        let old_element = elements[0];
        let old_bit = (old_element >> bit_within_element) & 1 != 0;

        let new_element = if value {
            old_element | (1u64 << bit_within_element)
        } else {
            old_element & !(1u64 << bit_within_element)
        };

        if old_element != new_element {
            self.storage.write(element_index, &[new_element])?;
        }

        Ok(old_bit)
    }

    /// Set a single bit at the given bit index.
    ///
    /// Only writes to the backend if the bit value actually changes.
    ///
    /// Returns `Err` if `bit_index` is out of bounds.
    pub fn set_bit(&mut self, bit_index: u64, value: bool) -> Result<()> {
        self.modify_bit(bit_index, value)?;
        Ok(())
    }

    /// Set a single bit and return its previous value.
    ///
    /// This is the equivalent of [`BitSlice::replace`] for universal IO.
    pub fn replace_bit(&mut self, bit_index: u64, value: bool) -> Result<bool> {
        self.modify_bit(bit_index, value)
    }

    /// Write a range of bits from a [`BitSlice`] into the storage.
    ///
    /// Writes `source.len()` bits starting at `bit_start` in the storage.
    /// The bit range is translated to element-aligned reads and writes;
    /// partial elements at the edges are read-modify-written.
    pub fn write_bit_range(
        &mut self,
        bit_start: u64,
        source: &BitSlice<u64, Lsb0>,
    ) -> Result<()> {
        let bit_count = source.len() as u64;
        if bit_count == 0 {
            return Ok(());
        }

        let (element_start, element_count, offset_in_first) =
            self.validate_bit_range(bit_start, bit_count)?;

        // Read the affected elements, modify the bits, write back
        let existing = self.storage.read::<false>(ElementsRange {
            start: element_start,
            length: element_count,
        })?;

        let mut buf = existing.into_owned();
        let buf_bits = BitSlice::<u64, Lsb0>::from_slice_mut(&mut buf);
        let end_within = offset_in_first + bit_count as usize;
        buf_bits[offset_in_first..end_within].copy_from_bitslice(source);

        self.storage.write(element_start, &buf)
    }

    /// Fill a range of bits with a single value.
    ///
    /// Sets all bits in `[bit_start, bit_start + bit_count)` to `value`.
    pub fn fill_bit_range(&mut self, bit_start: u64, bit_count: u64, value: bool) -> Result<()> {
        if bit_count == 0 {
            return Ok(());
        }

        let (element_start, element_count, offset_in_first) =
            self.validate_bit_range(bit_start, bit_count)?;

        // Read the affected elements, modify the bits, write back
        let existing = self.storage.read::<false>(ElementsRange {
            start: element_start,
            length: element_count,
        })?;

        let mut buf = existing.into_owned();
        let buf_bits = BitSlice::<u64, Lsb0>::from_slice_mut(&mut buf);
        let end_within = offset_in_first + bit_count as usize;
        buf_bits[offset_in_first..end_within].fill(value);

        self.storage.write(element_start, &buf)
    }

    /// Set multiple individual bits in a batch.
    ///
    /// Each `(bit_index, value)` pair sets a single bit. Bits within the same
    /// `u64` element are coalesced into a single read-modify-write.
    pub fn set_bits_batch(
        &mut self,
        updates: impl IntoIterator<Item = (u64, bool)>,
    ) -> Result<()> {
        // Group updates by element index for efficient read-modify-write
        let mut by_element: std::collections::BTreeMap<u64, Vec<(u64, bool)>> =
            std::collections::BTreeMap::new();

        for (bit_index, value) in updates {
            let element_index = bit_index / BITS_PER_ELEMENT;
            let bit_within_element = bit_index % BITS_PER_ELEMENT;
            by_element
                .entry(element_index)
                .or_default()
                .push((bit_within_element, value));
        }

        for (element_index, bit_updates) in by_element {
            if element_index >= self.element_len {
                // Report the first offending bit index for a useful error message
                let first_bit = element_index * BITS_PER_ELEMENT + bit_updates[0].0;
                return Err(UniversalIoError::OutOfBounds {
                    start: first_bit,
                    end: first_bit + 1,
                    elements: self.bit_len() as usize,
                });
            }

            let elements = self.storage.read::<false>(ElementsRange {
                start: element_index,
                length: 1,
            })?;

            let old_element = elements[0];
            let mut new_element = old_element;
            for (bit_within_element, value) in bit_updates {
                if value {
                    new_element |= 1u64 << bit_within_element;
                } else {
                    new_element &= !(1u64 << bit_within_element);
                }
            }

            if old_element != new_element {
                self.storage.write(element_index, &[new_element])?;
            }
        }

        Ok(())
    }

    /// Get a flusher for the underlying storage.
    pub fn flusher(&self) -> Flusher {
        self.storage.flusher()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::universal_io::mmap::MmapUniversal;

    type MmapBitSliceStorage = BitSliceStorage<MmapUniversal<u64>>;

    fn create_temp_file(data: &[u8]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        // Ensure the data length is a multiple of u64 for mmap alignment
        let aligned_len = if data.is_empty() {
            0
        } else {
            data.len()
                .next_multiple_of(std::mem::size_of::<u64>())
                .max(std::mem::size_of::<u64>())
        };
        let mut buf = vec![0u8; aligned_len];
        buf[..data.len()].copy_from_slice(data);
        f.write_all(&buf).unwrap();
        f.flush().unwrap();
        f
    }

    // ---- Read tests ----

    #[test]
    fn test_read_whole_bitslice() {
        // 0xB2 = 0b10110010
        let data = [0xB2u8];
        let f = create_temp_file(&data);

        let storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        let bs = storage.read_all().unwrap();

        // Lsb0: bit 0 is LSB of first byte
        // 0xB2 = 0b10110010
        // Lsb0 bits: [0,1,0,0,1,1,0,1]
        assert!(!bs[0]); // bit 0 = 0
        assert!(bs[1]); // bit 1 = 1
        assert!(!bs[2]); // bit 2 = 0
        assert!(!bs[3]); // bit 3 = 0
        assert!(bs[4]); // bit 4 = 1
        assert!(bs[5]); // bit 5 = 1
        assert!(!bs[6]); // bit 6 = 0
        assert!(bs[7]); // bit 7 = 1
    }

    #[test]
    fn test_get_single_bit() {
        let data = [0xB2u8]; // 0b10110010
        let f = create_temp_file(&data);

        let storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        // Lsb0: 0xB2 = bits [0,1,0,0,1,1,0,1]
        assert_eq!(storage.get_bit(0).unwrap(), Some(false));
        assert_eq!(storage.get_bit(1).unwrap(), Some(true));
        assert_eq!(storage.get_bit(4).unwrap(), Some(true));
        assert_eq!(storage.get_bit(7).unwrap(), Some(true));

        // Out of bounds (file is 8 bytes = 1 u64 = 64 bits)
        assert_eq!(storage.get_bit(64).unwrap(), None);
    }

    #[test]
    fn test_read_bit_range() {
        // 0xB2 = 0b10110010, Lsb0 bits: [0,1,0,0,1,1,0,1]
        let data = [0xB2u8];
        let f = create_temp_file(&data);

        let storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        // Read bits 1..5 => [1,0,0,1]
        let bits = storage.read_bit_range(1, 4).unwrap();
        assert_eq!(bits.len(), 4);
        assert!(bits[0]); // bit 1
        assert!(!bits[1]); // bit 2
        assert!(!bits[2]); // bit 3
        assert!(bits[3]); // bit 4
    }

    #[test]
    fn test_read_bit_range_cross_element() {
        // Create 16 bytes (2 u64 elements)
        // First u64: bits 0..63, Second u64: bits 64..127
        let mut data = [0u8; 16];
        // Set bytes 7 and 8 to 0xFF so bits around the element boundary are set
        data[7] = 0xFF; // bits 56..63 of first element
        data[8] = 0xFF; // bits 64..71 (bits 0..7 of second element)
        let f = create_temp_file(&data);

        let storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        // Read bits 56..72 (crossing the u64 element boundary)
        let bits = storage.read_bit_range(56, 16).unwrap();
        assert_eq!(bits.len(), 16);
        for i in 0..16 {
            assert!(bits[i], "bit {i} should be set");
        }
    }

    // ---- Write tests ----

    #[test]
    fn test_set_bit() {
        let f = create_temp_file(&[0x00; 8]);

        let mut storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        // Set bit 3
        storage.set_bit(3, true).unwrap();
        assert_eq!(storage.get_bit(3).unwrap(), Some(true));
        assert_eq!(storage.get_bit(0).unwrap(), Some(false));

        // Clear bit 3
        storage.set_bit(3, false).unwrap();
        assert_eq!(storage.get_bit(3).unwrap(), Some(false));
    }

    #[test]
    fn test_set_bit_out_of_bounds() {
        let f = create_temp_file(&[0x00; 8]);

        let mut storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        assert!(storage.set_bit(storage.bit_len(), true).is_err());
    }

    #[test]
    fn test_replace_bit() {
        let f = create_temp_file(&[0xFF; 8]); // all bits set
        let mut storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        // Replace bit 2 (was true) with false
        let old = storage.replace_bit(2, false).unwrap();
        assert!(old);
        assert_eq!(storage.get_bit(2).unwrap(), Some(false));

        // Replace bit 2 (now false) with true
        let old = storage.replace_bit(2, true).unwrap();
        assert!(!old);
        assert_eq!(storage.get_bit(2).unwrap(), Some(true));
    }

    #[test]
    fn test_write_bit_range() {
        let f = create_temp_file(&[0x00; 8]);

        let mut storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        // Write bits [1,0,1,1] starting at bit 2
        let source = bitvec::bitvec![u64, Lsb0; 1, 0, 1, 1];
        storage.write_bit_range(2, &source).unwrap();

        assert_eq!(storage.get_bit(0).unwrap(), Some(false));
        assert_eq!(storage.get_bit(1).unwrap(), Some(false));
        assert_eq!(storage.get_bit(2).unwrap(), Some(true)); // 1
        assert_eq!(storage.get_bit(3).unwrap(), Some(false)); // 0
        assert_eq!(storage.get_bit(4).unwrap(), Some(true)); // 1
        assert_eq!(storage.get_bit(5).unwrap(), Some(true)); // 1
        assert_eq!(storage.get_bit(6).unwrap(), Some(false));
    }

    #[test]
    fn test_write_bit_range_element_aligned() {
        let f = create_temp_file(&[0x00; 16]); // 2 u64 elements

        let mut storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        // Write a full u64 element at element boundary (bit 0, 64 bits)
        let mut source = BitVec::<u64, Lsb0>::repeat(false, 64);
        source.set(0, true);
        source.set(2, true);
        source.set(4, true);
        source.set(6, true);
        storage.write_bit_range(0, &source).unwrap();

        let all = storage.read_all().unwrap();
        assert_eq!(all[..64], source[..]);
    }

    #[test]
    fn test_fill_bit_range() {
        let f = create_temp_file(&[0x00; 8]);

        let mut storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        // Fill bits 3..11 with true
        storage.fill_bit_range(3, 8, true).unwrap();

        for i in 0..16 {
            let expected = (3..11).contains(&i);
            assert_eq!(
                storage.get_bit(i).unwrap(),
                Some(expected),
                "bit {i}: expected {expected}"
            );
        }

        // Fill bits 5..9 with false
        storage.fill_bit_range(5, 4, false).unwrap();

        for i in 3..11u64 {
            let expected = !(5..9).contains(&i);
            assert_eq!(
                storage.get_bit(i).unwrap(),
                Some(expected),
                "bit {i}: expected {expected}"
            );
        }
    }

    #[test]
    fn test_set_bits_batch() {
        let f = create_temp_file(&[0x00; 8]);

        let mut storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        // Set several bits
        storage
            .set_bits_batch([(0, true), (3, true), (7, true), (8, true), (15, true)])
            .unwrap();

        assert_eq!(storage.get_bit(0).unwrap(), Some(true));
        assert_eq!(storage.get_bit(1).unwrap(), Some(false));
        assert_eq!(storage.get_bit(3).unwrap(), Some(true));
        assert_eq!(storage.get_bit(7).unwrap(), Some(true));
        assert_eq!(storage.get_bit(8).unwrap(), Some(true));
        assert_eq!(storage.get_bit(15).unwrap(), Some(true));
    }

    #[test]
    fn test_flusher() {
        let f = create_temp_file(&[0x00; 8]);

        let mut storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        storage.set_bit(0, true).unwrap();
        storage.flusher()().unwrap();

        // Reopen and verify persistence
        let storage2: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();
        assert_eq!(storage2.get_bit(0).unwrap(), Some(true));
    }

    #[test]
    fn test_bit_len() {
        let f = create_temp_file(&[0u8; 16]); // 2 u64 elements

        let storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        assert_eq!(storage.element_len(), 2);
        assert_eq!(storage.bit_len(), 128);
    }

    #[test]
    fn test_read_bit_range_out_of_bounds() {
        let f = create_temp_file(&[0xFF; 8]);

        let storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        let result = storage.read_bit_range(0, storage.bit_len() + 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_all_as_bitslice() {
        let data = [0xAB, 0xCD, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let f = create_temp_file(&data);

        let storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        let bs = storage.read_all().unwrap();
        assert_eq!(bs.len(), storage.bit_len() as usize);
        // With mmap backend, read_all returns Cow::Borrowed (zero-copy)
        assert!(matches!(bs, Cow::Borrowed(_)));
    }

    #[test]
    fn test_read_bit_range_zero_copy() {
        let data = [0xAB, 0xCD, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let f = create_temp_file(&data);

        let storage: MmapBitSliceStorage =
            BitSliceStorage::open(f.path(), OpenOptions::default()).unwrap();

        // With mmap backend, read_bit_range returns Cow::Borrowed (zero-copy)
        let bits = storage.read_bit_range(0, 16).unwrap();
        assert!(matches!(bits, Cow::Borrowed(_)));
        assert_eq!(bits.len(), 16);
    }
}
