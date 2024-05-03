use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead as _, BufReader, Lines};
use std::path::Path;

use memmap2::Mmap;
use memory::mmap_ops::{open_read_mmap, transmute_from_u8, transmute_from_u8_to_slice};
use validator::ValidationErrors;

use crate::common::sparse_vector::SparseVector;

/// Compressed Sparse Row matrix, baked by memory-mapped file.
///
/// The layout of the memory-mapped file is as follows:
///
/// =======  ===========  ==========  ===================
/// name     type         size        start
/// =======  ===========  ==========  ===================
/// nrow     u64          8           0
/// ncol     u64          8           8
/// nnz      u64          8           16
/// indptr   u64[nrow+1]  8*(nrow+1)  24
/// indices  u32[nnz]     4*nnz       24+8*(nrow+1)
/// data     u32[nnz]     4*nnz       24+8*(nrow+1)+4*nnz
pub struct Csr {
    mmap: Mmap,

    nrow: usize,
    nnz: usize,
    intptr: Vec<u64>,
}

impl Csr {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::from_mmap(open_read_mmap(path.as_ref())?)
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.nrow
    }

    pub fn iter(&self) -> CsrIter<'_> {
        CsrIter { csr: self, row: 0 }
    }

    fn from_mmap(mmap: Mmap) -> io::Result<Self> {
        let (nrow, ncol, nnz) = transmute_from_u8::<(u64, u64, u64)>(&mmap.as_ref()[..24]);
        let (nrow, _ncol, nnz) = (*nrow as usize, *ncol as usize, *nnz as usize);

        let indptr = Vec::from(transmute_from_u8_to_slice::<u64>(
            &mmap.as_ref()[24..24 + 8 * (nrow + 1)],
        ));
        if !indptr.windows(2).all(|w| w[0] <= w[1]) || indptr.last() != Some(&(nnz as u64)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid indptr array",
            ));
        }

        Ok(Self {
            mmap,
            nrow,
            nnz,
            intptr: indptr,
        })
    }

    #[inline]
    unsafe fn vec(&self, row: usize) -> Result<SparseVector, ValidationErrors> {
        let start = *self.intptr.get_unchecked(row) as usize;
        let end = *self.intptr.get_unchecked(row + 1) as usize;

        let mut pos = 24 + 8 * (self.nrow + 1);

        let indices = transmute_from_u8_to_slice::<u32>(
            self.mmap
                .as_ref()
                .get_unchecked(pos + 4 * start..pos + 4 * end),
        );

        pos += 4 * self.nnz;
        let data = transmute_from_u8_to_slice::<f32>(
            self.mmap
                .as_ref()
                .get_unchecked(pos + 4 * start..pos + 4 * end),
        );

        SparseVector::new(indices.to_vec(), data.to_vec())
    }
}

/// Iterator over the rows of a CSR matrix.
pub struct CsrIter<'a> {
    csr: &'a Csr,
    row: usize,
}

impl<'a> Iterator for CsrIter<'a> {
    type Item = Result<SparseVector, ValidationErrors>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.row < self.csr.nrow).then(|| {
            let vec = unsafe { self.csr.vec(self.row) };
            self.row += 1;
            vec
        })
    }
}

impl<'a> ExactSizeIterator for CsrIter<'a> {
    fn len(&self) -> usize {
        self.csr.nrow - self.row
    }
}

pub fn load_csr_vecs(path: impl AsRef<Path>) -> io::Result<Vec<SparseVector>> {
    Csr::open(path)?
        .iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Stream of sparse vectors in JSON format.
pub struct JsonReader(Lines<BufReader<File>>);

impl JsonReader {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
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
