use std::collections::HashMap;
use std::io::{self, BufRead as _, BufReader, Lines};
use std::path::Path;

use common::mmap::{Advice, AdviceSetting, open_read_mmap};
use fs_err::File;
use memmap2::Mmap;
use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::common::sparse_vector::SparseVector;

/// Compressed Sparse Row matrix, backed by memory-mapped file.
///
/// The layout of the memory-mapped file is as follows:
///
/// | name    | type          | size       | start               |
/// |---------|---------------|------------|---------------------|
/// | nrow    | `u64`         | 8          | 0                   |
/// | ncol    | `u64`         | 8          | 8                   |
/// | nnz     | `u64`         | 8          | 16                  |
/// | indptr  | `u64[nrow+1]` | 8*(nrow+1) | 24                  |
/// | indices | `u32[nnz]`    | 4*nnz      | 24+8*(nrow+1)       |
/// | data    | `u32[nnz]`    | 4*nnz      | 24+8*(nrow+1)+4*nnz |
pub struct Csr {
    mmap: Mmap,
    nrow: usize,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, FromBytes, Immutable, KnownLayout)]
pub struct CsrHeader {
    nrow: u64,
    ncol: u64,
    nnz: u64,
}

impl Csr {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mmap = open_read_mmap(path.as_ref(), AdviceSetting::from(Advice::Normal), false)?;
        let (header, _) = CsrHeader::ref_from_prefix(mmap.as_ref()).map_err(csr_error)?;
        let nrow = header.nrow as usize;
        Ok(Self { mmap, nrow })
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.nrow
    }

    pub fn iter(&self) -> io::Result<impl ExactSizeIterator<Item = SparseVector> + '_> {
        let (header, bytes) = CsrHeader::ref_from_prefix(self.mmap.as_ref()).map_err(csr_error)?;
        let nrow = header.nrow as usize;
        let nnz = header.nnz as usize;
        let (indptr, bytes) =
            <[u64]>::ref_from_prefix_with_elems(bytes, nrow + 1).map_err(csr_error)?;
        let (indices, bytes) =
            <[u32]>::ref_from_prefix_with_elems(bytes, nnz).map_err(csr_error)?;
        let (data, _) = <[f32]>::ref_from_prefix_with_elems(bytes, nnz).map_err(csr_error)?;
        Ok(indptr.array_windows().map(move |&[start, end]| {
            let (start, end) = (start as usize, end as usize);
            SparseVector::new_unchecked(indices[start..end].to_vec(), data[start..end].to_vec())
        }))
    }
}

fn csr_error(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid CSR format: {e}"),
    )
}

/// Stream of sparse vectors in JSON format.
pub struct JsonReader(Lines<BufReader<File>>);

impl JsonReader {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        Ok(JsonReader(BufReader::new(File::open(path)?).lines()))
    }
}

impl Iterator for JsonReader {
    type Item = Result<SparseVector, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|line| {
            line.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
                .and_then(|line| {
                    let data: HashMap<String, f32> = serde_json::from_str(&line)?;
                    SparseVector::new(
                        data.keys()
                            .map(|k| k.parse())
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                        data.values().copied().collect(),
                    )
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
                })
        })
    }
}
