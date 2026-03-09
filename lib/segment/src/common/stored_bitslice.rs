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
use bitvec::vec::BitVec;
use common::mmap::create_and_ensure_length;
use common::universal_io::{
    ElementsRange, Flusher, OpenOptions, Result, UniversalIoError, UniversalRead, UniversalWrite,
};
use itertools::Itertools;

/// Number of bits per `u64` element.
const BITS_PER_ELEMENT: u64 = u64::BITS as u64;

type BitSlice = bitvec::slice::BitSlice<u64, Lsb0>;

/// Convenience alias for a bitslice backed by a memory-mapped file.
pub type MmapBitSlice = StoredBitSlice<common::universal_io::mmap::MmapUniversal<u64>>;

/// A storage-agnostic bitslice that supports both reading and writing bits.
///
/// Wraps any [`UniversalRead<u64>`] / [`UniversalWrite<u64>`] backend and
/// interprets the underlying `u64` elements as a sequence of bits.
/// Bit-level operations are translated to element-level reads and writes
/// on the backend.
#[derive(Debug)]
pub struct StoredBitSlice<S> {
    storage: S,
    /// Total number of `u64` elements in the underlying storage.
    element_len: u64,
}

impl<S: UniversalRead<u64>> StoredBitSlice<S> {
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
        // TODO: Replace with generic FileOps
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
    pub fn read_all(&self) -> Result<Cow<'_, BitSlice>> {
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

        let bitslice = BitSlice::from_element(&element);

        Ok(bitslice
            .get(bit_within_element as usize)
            .as_deref()
            .copied())
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

impl<S: UniversalWrite<u64>> StoredBitSlice<S> {
    /// Set multiple individual bits in a batch.
    ///
    /// Each `(bit_index, value)` pair sets a single bit. If contiguous, bits within the same
    /// `u64` element are coalesced into a single read-modify-write.
    pub fn set_bits_batch(&mut self, updates: impl IntoIterator<Item = (u64, bool)>) -> Result<()> {
        // group by bits on the same element
        let chunks = updates
            .into_iter()
            .chunk_by(|(bit_index, _)| bit_index >> <u64 as BitRegister>::INDX);

        for (element_idx, chunk) in &chunks {
            if element_idx >= self.element_len {
                return Err(UniversalIoError::OutOfBounds {
                    start: element_idx,
                    end: element_idx.saturating_add(1),
                    elements: self.element_len as usize,
                });
            }

            let mut element = self.storage.read::<false>(ElementsRange {
                start: element_idx,
                length: 1,
            })?[0];
            let old_element = element;
            let new_element = &mut element;

            let bitslice = BitSlice::from_element_mut(new_element);

            // Do all modifications to this element
            for (bit_idx, value) in chunk {
                let bit_within_element = bit_idx as u8 & <u64 as BitRegister>::MASK;
                bitslice.set(bit_within_element as usize, value);
            }

            // Write if it changed
            if old_element != *new_element {
                self.storage.write(element_idx, &[*new_element])?;
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

    use common::universal_io::mmap::MmapUniversal;
    use tempfile::NamedTempFile;

    use super::*;

    impl StoredBitSlice<MmapUniversal<u64>> {
        /// Read-modify-write a single bit. Returns the previous value.
        ///
        /// Only writes to the backend if the element actually changed.
        pub fn replace_bit(&mut self, bit_index: u64, value: bool) -> Result<bool> {
            let element_index = bit_index >> <u64 as BitRegister>::INDX;
            let bit_within_element = bit_index as u8 & <u64 as BitRegister>::MASK;

            if element_index >= self.element_len {
                return Err(UniversalIoError::OutOfBounds {
                    start: bit_index,
                    end: bit_index + 1,
                    elements: self.bit_len() as usize,
                });
            }

            let mut element = self.storage.read::<false>(ElementsRange {
                start: element_index,
                length: 1,
            })?[0];

            let element = &mut element;

            let bitslice = BitSlice::from_element_mut(element);

            let old_bit = bitslice.replace(bit_within_element as usize, value);

            if old_bit != value {
                self.storage.write(element_index, &[*element])?;
            }

            Ok(old_bit)
        }
    }

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
        // Two u64 elements (16 bytes):
        let data = [
            // element 0:
            0b10110010, 0b01001111, 0x00, 0x00, 0x00, 0x00, 0x00, 0b10000000,
            // element 1:
            0b00000001, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0b11111111,
        ];
        let f = create_temp_file(&data);

        let storage: MmapBitSlice = StoredBitSlice::open(f.path(), OpenOptions::default()).unwrap();

        assert_eq!(storage.element_len(), 2);
        assert_eq!(storage.bit_len(), 128);

        let bs = storage.read_all().unwrap();

        // Element 0, byte 0: 0xB2 = 0b10110010
        // Lsb0 bits: [0,1,0,0,1,1,0,1]
        assert!(!bs[0]); // bit 0 = 0
        assert!(bs[1]); // bit 1 = 1
        assert!(!bs[2]); // bit 2 = 0
        assert!(!bs[3]); // bit 3 = 0
        assert!(bs[4]); // bit 4 = 1
        assert!(bs[5]); // bit 5 = 1
        assert!(!bs[6]); // bit 6 = 0
        assert!(bs[7]); // bit 7 = 1

        // Element 0, byte 1: 0x4F = 0b01001111
        // Lsb0 bits: [1,1,1,1,0,0,1,0]
        assert!(bs[8]); // bit 8 = 1
        assert!(bs[9]); // bit 9 = 1
        assert!(bs[10]); // bit 10 = 1
        assert!(bs[11]); // bit 11 = 1
        assert!(!bs[12]); // bit 12 = 0
        assert!(!bs[13]); // bit 13 = 0
        assert!(bs[14]); // bit 14 = 1
        assert!(!bs[15]); // bit 15 = 0

        // Element 0, byte 7: 0x80 = 0b10000000 -> bit 63 is set
        assert!(!bs[56]); // bit 56 = 0
        assert!(bs[63]); // bit 63 = 1 (MSB of element 0)

        // Element 1, byte 0: 0x01 -> bit 64 (LSB of element 1) is set
        assert!(bs[64]); // bit 64 = 1
        assert!(!bs[65]); // bit 65 = 0

        // Element 1, byte 7: 0xFF → bits 120..=127 are all set
        for i in 120..=127 {
            assert!(bs[i], "bit {i} should be set");
        }

        // Spot-check some zeros in the middle of element 1
        for i in 72..120 {
            assert!(!bs[i], "bit {i} should be clear");
        }
    }

    #[test]
    fn test_get_single_bit() {
        let data = [0xB2u8]; // 0b10110010
        let f = create_temp_file(&data);

        let storage: MmapBitSlice = StoredBitSlice::open(f.path(), OpenOptions::default()).unwrap();

        // Lsb0: 0xB2 = bits [0,1,0,0,1,1,0,1]
        assert_eq!(storage.get_bit(0).unwrap(), Some(false));
        assert_eq!(storage.get_bit(1).unwrap(), Some(true));
        assert_eq!(storage.get_bit(4).unwrap(), Some(true));
        assert_eq!(storage.get_bit(7).unwrap(), Some(true));

        // Out of bounds (file is 8 bytes = 1 u64 = 64 bits)
        assert_eq!(storage.get_bit(64).unwrap(), None);
    }

    // ---- Write tests ----

    #[test]
    fn test_set_bit() {
        let f = create_temp_file(&[0x00; 8]);

        let mut storage: MmapBitSlice =
            StoredBitSlice::open(f.path(), OpenOptions::default()).unwrap();

        // Set bit 3
        storage.replace_bit(3, true).unwrap();
        assert_eq!(storage.get_bit(3).unwrap(), Some(true));
        assert_eq!(storage.get_bit(0).unwrap(), Some(false));

        // Clear bit 3
        storage.replace_bit(3, false).unwrap();
        assert_eq!(storage.get_bit(3).unwrap(), Some(false));
    }

    #[test]
    fn test_set_bit_out_of_bounds() {
        let f = create_temp_file(&[0x00; 8]);

        let mut storage: MmapBitSlice =
            StoredBitSlice::open(f.path(), OpenOptions::default()).unwrap();

        assert!(storage.replace_bit(storage.bit_len(), true).is_err());
    }

    #[test]
    fn test_replace_bit() {
        let f = create_temp_file(&[0xFF; 8]); // all bits set
        let mut storage: MmapBitSlice =
            StoredBitSlice::open(f.path(), OpenOptions::default()).unwrap();

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
    fn test_set_bits_batch() {
        let f = create_temp_file(&[0x00; 8]);

        let mut storage: MmapBitSlice =
            StoredBitSlice::open(f.path(), OpenOptions::default()).unwrap();

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

        let mut storage: MmapBitSlice =
            StoredBitSlice::open(f.path(), OpenOptions::default()).unwrap();

        storage.replace_bit(0, true).unwrap();
        storage.flusher()().unwrap();

        // Reopen and verify persistence
        let storage2: MmapBitSlice =
            StoredBitSlice::open(f.path(), OpenOptions::default()).unwrap();
        assert_eq!(storage2.get_bit(0).unwrap(), Some(true));
    }

    #[test]
    fn test_bit_len() {
        let f = create_temp_file(&[0u8; 16]); // 2 u64 elements

        let storage: MmapBitSlice = StoredBitSlice::open(f.path(), OpenOptions::default()).unwrap();

        assert_eq!(storage.element_len(), 2);
        assert_eq!(storage.bit_len(), 128);
    }

    #[test]
    fn test_read_all_as_bitslice() {
        let data = [0xAB, 0xCD, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let f = create_temp_file(&data);

        let storage: MmapBitSlice = StoredBitSlice::open(f.path(), OpenOptions::default()).unwrap();

        let bs = storage.read_all().unwrap();
        assert_eq!(bs.len(), storage.bit_len() as usize);
        // With mmap backend, read_all returns Cow::Borrowed (zero-copy)
        assert!(matches!(bs, Cow::Borrowed(_)));
    }
}
