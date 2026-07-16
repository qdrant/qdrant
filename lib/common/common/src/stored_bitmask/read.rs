use std::borrow::Cow;
use std::io;
use std::path::Path;

use roaring::RoaringBitmap;

use super::format::{BitmaskContent, BitmaskHeader, Encoding, HEADER_SIZE, MAGIC, VERSION};
use crate::bitvec::{BitSlice, BitVec};
use crate::generic_consts::Sequential;
use crate::universal_io::{
    OpenOptions, ReadRange, Result, TypedStorage, UniversalIoError, UniversalRead, UniversalReadFs,
};

/// Read handle over a bitmask persisted by [`save_bitmask`].
///
/// [`Self::open`] reads and validates only the fixed-size header; the payload
/// is not touched until [`Self::read`].
///
/// [`save_bitmask`]: super::save_bitmask
#[derive(Debug)]
pub struct StoredBitmask<S> {
    storage: TypedStorage<S, u8>,
    logical_len: u64,
    pub(super) encoding: Encoding,
    pub(super) payload_len: u64,
}

/// Copy little-endian payload bytes into an owned [`BitVec`] of `len` bits.
fn dense_to_bitvec(bytes: &[u8], len: usize) -> BitVec {
    let mut words = vec![0u64; bytes.len().div_ceil(size_of::<u64>())];
    bytemuck::cast_slice_mut::<u64, u8>(&mut words)[..bytes.len()].copy_from_slice(bytes);
    let mut bits = BitVec::from_vec(words);
    bits.truncate(len);
    bits
}

fn invalid_data(path: &Path, message: impl std::fmt::Display) -> UniversalIoError {
    UniversalIoError::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{}: {message}", path.display()),
    ))
}
impl<S: UniversalRead> StoredBitmask<S> {
    /// Open a persisted bitmask, reading and validating its header.
    pub fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Fs::OpenExtra,
    ) -> Result<Self> {
        let path = path.as_ref();
        let storage = TypedStorage::open(fs, path, options, extra)?;

        let file_len = storage.len()?;
        if file_len < HEADER_SIZE as u64 {
            return Err(invalid_data(
                path,
                format_args!("file of {file_len} bytes is too short to be a stored bitmask"),
            ));
        }

        let header_bytes = storage.read::<Sequential>(ReadRange {
            byte_offset: 0,
            length: HEADER_SIZE as u64,
        })?;
        let header: BitmaskHeader = bytemuck::pod_read_unaligned(&header_bytes);

        if header.magic != MAGIC {
            return Err(invalid_data(path, "not a stored bitmask file (bad magic)"));
        }
        if header.version != VERSION {
            return Err(invalid_data(
                path,
                format_args!(
                    "unsupported stored bitmask version {} (supported: {VERSION})",
                    header.version,
                ),
            ));
        }
        let Some(encoding) = Encoding::from_u32(header.encoding) else {
            return Err(invalid_data(
                path,
                format_args!("unknown stored bitmask encoding {}", header.encoding),
            ));
        };
        // `file_len >= HEADER_SIZE` was checked above, so the subtraction
        // cannot underflow; phrasing it this way avoids overflowing on a
        // corrupted `payload_len` near `u64::MAX`.
        if header.payload_len > file_len - HEADER_SIZE as u64 {
            return Err(invalid_data(
                path,
                format_args!(
                    "payload of {} bytes exceeds file of {file_len} bytes",
                    header.payload_len,
                ),
            ));
        }
        if encoding == Encoding::Dense
            && header.payload_len < header.logical_len.div_ceil(u64::from(u8::BITS))
        {
            return Err(invalid_data(
                path,
                format_args!(
                    "dense payload of {} bytes is too short for {} bits",
                    header.payload_len, header.logical_len,
                ),
            ));
        }

        Ok(Self {
            storage,
            logical_len: header.logical_len,
            encoding,
            payload_len: header.payload_len,
        })
    }

    /// Number of logical flags (bits) in the mask.
    pub fn bit_len(&self) -> u64 {
        self.logical_len
    }

    /// Read and decode the payload, in the stored polarity.
    pub fn read(&self) -> Result<BitmaskContent<'_>> {
        let payload = self.storage.read::<Sequential>(ReadRange {
            byte_offset: HEADER_SIZE as u64,
            length: self.payload_len,
        })?;

        match self.encoding {
            Encoding::Dense => {
                let len = self.logical_len as usize;
                let bits = match payload {
                    // Zero-copy when the backend exposes its bytes and they
                    // cast cleanly to whole u64 words (the payload starts at
                    // a u64-aligned offset and the writer pads it to words).
                    Cow::Borrowed(bytes) => match bytemuck::try_cast_slice::<u8, u64>(bytes) {
                        Ok(words) => Cow::Borrowed(&BitSlice::from_slice(words)[..len]),
                        Err(_) => Cow::Owned(dense_to_bitvec(bytes, len)),
                    },
                    Cow::Owned(bytes) => Cow::Owned(dense_to_bitvec(&bytes, len)),
                };
                Ok(BitmaskContent::Dense(bits))
            }
            Encoding::RoaringOnes => Ok(BitmaskContent::Ones(self.decode_roaring(&payload)?)),
            Encoding::RoaringZeros => Ok(BitmaskContent::Zeros(self.decode_roaring(&payload)?)),
        }
    }

    /// Deserialize a roaring payload, rejecting positions outside
    /// `0..logical_len` so [`BitmaskContent`]'s range contract holds even for
    /// corrupted files.
    fn decode_roaring(&self, payload: &[u8]) -> Result<RoaringBitmap> {
        let bitmap = RoaringBitmap::deserialize_from(payload)?;
        if let Some(max) = bitmap.max()
            && u64::from(max) >= self.logical_len
        {
            return Err(UniversalIoError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "stored bitmask position {max} out of {} bits",
                    self.logical_len,
                ),
            )));
        }
        Ok(bitmap)
    }
}
