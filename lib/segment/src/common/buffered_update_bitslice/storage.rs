//! Persisted side of a [`BufferedUpdateBitSlice`], in either of the two
//! on-disk bitmask formats.
//!
//! [`BufferedUpdateBitSlice`]: super::BufferedUpdateBitSlice

use std::path::{Path, PathBuf};

use ahash::AHashMap;
use common::bitvec::BitVec;
use common::stored_bitmask::{StoredBitmask, save_bitmask};
use common::stored_bitslice::StoredBitSlice;
use common::universal_io::{OkNotFound, OpenOptions, UniversalWrite, UniversalWriteFileOps};
use itertools::Itertools;
use roaring::RoaringBitmap;

use crate::common::operation_error::{OperationError, OperationResult};

/// On-disk format of a persisted bitmask.
///
/// Both formats are always readable — [`BufferedUpdateBitSlice::open`] picks
/// whichever one is present. The choice only applies to writing a new file
/// with [`BufferedUpdateBitSlice::create`].
///
/// [`BufferedUpdateBitSlice::open`]: super::BufferedUpdateBitSlice::open
/// [`BufferedUpdateBitSlice::create`]: super::BufferedUpdateBitSlice::create
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitmaskFormat {
    /// Raw dense bits, one per flag, mutated in place: a flush rewrites just
    /// the `u64` words that changed. Sized by the number of flags.
    Raw,
    /// Compact bitmask, held in RAM as its set positions: a flush re-encodes
    /// every flag and replaces the whole file atomically. Sized by the entropy
    /// of the flags, at the cost of rewriting all of them per flush.
    Compact,
}

/// Where the two formats of one bitmask live.
///
/// Each format has its own file name, so which one is persisted is a matter of
/// which file exists — never of guessing a format from a file's bytes. Only
/// one of the two ever exists at a time: [`BufferedUpdateBitSlice::create`]
/// removes the other.
///
/// [`BufferedUpdateBitSlice::create`]: super::BufferedUpdateBitSlice::create
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmaskPaths {
    /// File holding the flags in [`BitmaskFormat::Raw`].
    pub raw: PathBuf,
    /// File holding the flags in [`BitmaskFormat::Compact`].
    pub compact: PathBuf,
}

impl BitmaskPaths {
    pub fn new(raw: impl Into<PathBuf>, compact: impl Into<PathBuf>) -> Self {
        Self {
            raw: raw.into(),
            compact: compact.into(),
        }
    }

    /// The file `format` is stored in.
    pub fn of(&self, format: BitmaskFormat) -> &Path {
        match format {
            BitmaskFormat::Raw => &self.raw,
            BitmaskFormat::Compact => &self.compact,
        }
    }
}

/// The persisted flags, plus whatever is needed to write them back.
#[derive(Debug)]
pub(super) enum BitmaskStorage<S: UniversalWrite> {
    Raw {
        bitslice: StoredBitSlice<S>,
        path: PathBuf,
    },
    Compact(CompactStorage<S>),
}

impl<S: UniversalWrite> BitmaskStorage<S> {
    /// Open whichever of `paths` exists.
    ///
    /// Prefers the compact file when — against the invariant [`Self::create`]
    /// maintains — both are present, since that is the format a writer would
    /// have chosen last.
    pub(super) fn open(
        fs: &S::Fs,
        paths: &BitmaskPaths,
        options: OpenOptions,
    ) -> OperationResult<Self> {
        let compact = StoredBitmask::<S>::open(fs, &paths.compact, options, Default::default())
            .ok_not_found()?;

        match compact {
            Some(compact) => Ok(Self::Compact(CompactStorage::read_from(
                fs,
                paths.compact.clone(),
                compact,
            )?)),
            None => Ok(Self::Raw {
                bitslice: StoredBitSlice::open(fs, &paths.raw, options, Default::default())?,
                path: paths.raw.clone(),
            }),
        }
    }

    /// Persist `bit_len` flags with `ones` set, in the requested `format`,
    /// replacing whatever was at either of `paths`, and open them.
    ///
    /// `ones` must be ascending and within `0..bit_len`.
    pub(super) fn create(
        fs: &S::Fs,
        paths: &BitmaskPaths,
        options: OpenOptions,
        format: BitmaskFormat,
        bit_len: usize,
        ones: impl IntoIterator<Item = u64>,
    ) -> OperationResult<Self> {
        let storage = match format {
            BitmaskFormat::Raw => {
                // A whole number of `u64` words, so that writing `bits` covers
                // every byte of the file: no stale bits survive in the padding
                // when an existing file is being overwritten.
                let mut bits = BitVec::repeat(false, bit_len.next_multiple_of(u64::BITS as usize));
                for one in ones {
                    let index = usize::try_from(one).ok().filter(|index| *index < bit_len);
                    let Some(index) = index else {
                        return Err(OperationError::service_error(format!(
                            "bitmask position {one} is beyond its {bit_len} flags"
                        )));
                    };
                    bits.set(index, true);
                }

                fs.create(&paths.raw, bits.len() / u8::BITS as usize)?;
                let mut bitslice =
                    StoredBitSlice::open(fs, &paths.raw, options, Default::default())?;
                bitslice.write_bitslice(&bits)?;
                bitslice.flusher()()?;
                Self::Raw {
                    bitslice,
                    path: paths.raw.clone(),
                }
            }
            BitmaskFormat::Compact => {
                let bit_len = bit_len as u64;
                let ones = RoaringBitmap::from_sorted_iter(ones.into_iter().map(|one| one as u32))
                    .map_err(|err| {
                        OperationError::service_error(format!(
                            "bitmask positions are not ascending: {err}"
                        ))
                    })?;
                // Rejects positions beyond `bit_len`, and `bit_len` itself
                // beyond the `u32` position space the casts above assume.
                save_bitmask(fs, &paths.compact, bit_len, ones.clone())?;
                Self::Compact(CompactStorage {
                    fs: fs.clone(),
                    path: paths.compact.clone(),
                    bit_len,
                    ones,
                })
            }
        };

        // Drop the file left behind by an earlier create in the other format,
        // so a later open can't resurrect its flags.
        let stale = match format {
            BitmaskFormat::Raw => &paths.compact,
            BitmaskFormat::Compact => &paths.raw,
        };
        fs.remove(stale).ok_not_found()?;

        Ok(storage)
    }

    pub(super) fn format(&self) -> BitmaskFormat {
        match self {
            Self::Raw {
                bitslice: _,
                path: _,
            } => BitmaskFormat::Raw,
            Self::Compact(_) => BitmaskFormat::Compact,
        }
    }

    /// Path of the file the flags are actually stored in.
    pub(super) fn path(&self) -> &Path {
        match self {
            Self::Raw { bitslice: _, path } => path,
            Self::Compact(compact) => &compact.path,
        }
    }

    /// Number of addressable flags.
    ///
    /// The compact format records the exact count; the raw one only knows its
    /// file length, so it rounds up to a whole `u64` word.
    pub(super) fn bit_len(&self) -> u64 {
        match self {
            Self::Raw { bitslice, path: _ } => bitslice.bit_len(),
            Self::Compact(compact) => compact.bit_len,
        }
    }

    /// Read a single flag. Only reachable from
    /// [`BufferedUpdateBitSlice::get`](super::BufferedUpdateBitSlice::get),
    /// which is test-only.
    #[cfg(any(test, feature = "testing"))]
    pub(super) fn get_bit(&self, index: usize) -> OperationResult<Option<bool>> {
        match self {
            Self::Raw { bitslice, path: _ } => Ok(bitslice.get_bit(index as u64)?),
            Self::Compact(compact) => Ok(compact.get_bit(index)),
        }
    }

    /// Read every flag into an owned bitvec of [`Self::bit_len`] bits.
    pub(super) fn read_all(&self) -> OperationResult<BitVec> {
        match self {
            Self::Raw { bitslice, path: _ } => Ok(bitslice.read_all()?.into_owned()),
            Self::Compact(compact) => Ok(compact.to_bitvec()),
        }
    }

    /// Apply `updates` to the persisted flags and make them durable.
    ///
    /// On failure the flags this handle serves may already carry the updates
    /// while the file does not — the caller keeps them pending, so the next
    /// flush re-applies and re-persists them.
    pub(super) fn persist_updates(
        &mut self,
        updates: &AHashMap<usize, bool>,
    ) -> OperationResult<()> {
        match self {
            Self::Raw { bitslice, path: _ } => {
                bitslice.set_ascending_bits_batch(
                    updates
                        .iter()
                        .map(|(index, value)| (*index as u64, *value))
                        .sorted_unstable_by_key(|(index, _)| *index),
                )?;
                bitslice.flusher()()?;
                Ok(())
            }
            Self::Compact(compact) => compact.persist_updates(updates),
        }
    }

    /// Hint to the OS that pages backing the flags can be reclaimed.
    pub(super) fn clear_ram_cache(&self) -> OperationResult<()> {
        match self {
            Self::Raw { bitslice, path: _ } => Ok(bitslice.clear_ram_cache()?),
            // Nothing is mapped: the flags are held in RAM, and the file is
            // only ever touched to replace it wholesale.
            Self::Compact(_) => Ok(()),
        }
    }
}

/// Compact-format backing: the flags live in RAM as their set positions, and
/// each flush re-encodes all of them into a fresh file.
///
/// No file handle is kept: [`save_bitmask`] replaces the path, which leaves
/// any handle opened before it pointing at the previous, unlinked file.
///
/// Holding the flags is what makes a flush a pure write. The alternative is to
/// re-read and decode the file on every flush, since a whole-file format
/// cannot apply a delta; the copy costs the compressed size of the mask, which
/// is by construction no larger than the file.
#[derive(Debug)]
pub(super) struct CompactStorage<S: UniversalWrite> {
    fs: S::Fs,
    path: PathBuf,
    /// Number of flags, i.e. the exclusive upper bound of [`Self::ones`].
    bit_len: u64,
    /// Positions of the set flags — the authoritative copy of the mask.
    ///
    /// Equal to the file's contents, except from the moment
    /// [`Self::persist_updates`] folds updates in until its save returns: a
    /// save that fails leaves this ahead of the file, and those updates stay
    /// pending so the next flush re-saves them.
    ones: RoaringBitmap,
}

impl<S: UniversalWrite> CompactStorage<S> {
    /// Decode `stored` into RAM and drop its handle.
    fn read_from(fs: &S::Fs, path: PathBuf, stored: StoredBitmask<S>) -> OperationResult<Self> {
        Ok(Self {
            fs: fs.clone(),
            path,
            bit_len: stored.bit_len(),
            ones: stored.read_ones()?,
        })
    }

    #[cfg(any(test, feature = "testing"))]
    fn get_bit(&self, index: usize) -> Option<bool> {
        ((index as u64) < self.bit_len).then(|| self.ones.contains(index as u32))
    }

    fn to_bitvec(&self) -> BitVec {
        let mut bits = BitVec::repeat(false, self.bit_len as usize);
        for one in &self.ones {
            bits.set(one as usize, true);
        }
        bits
    }

    fn persist_updates(&mut self, updates: &AHashMap<usize, bool>) -> OperationResult<()> {
        let Self {
            fs,
            path,
            bit_len,
            ones,
        } = self;

        for (index, value) in updates {
            // In range because `BufferedUpdateBitSlice::set` bounds-checks
            // against the same `bit_len`; `save_bitmask` rejects it otherwise.
            debug_assert!((*index as u64) < *bit_len);
            if *value {
                ones.insert(*index as u32);
            } else {
                ones.remove(*index as u32);
            }
        }

        save_bitmask(fs, path, *bit_len, ones.clone())?;
        Ok(())
    }
}
