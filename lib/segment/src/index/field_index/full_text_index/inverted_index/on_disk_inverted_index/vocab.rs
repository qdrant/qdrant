use std::path::Path;

use common::persisted_hashmap::UniversalHashMap;
use common::universal_io::{
    MmapFile, OkNotFound, OneshotFile, OpenOptions, UniversalRead, UniversalReadFs,
};

use super::compact_vocab::{self, CompactVocab};
use super::vocab_format::VocabFormat;
use super::{COMPACT_VOCAB_FILE, VOCAB_FILE, unwrap_token};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::full_text_index::inverted_index::TokenId;

/// The vocabulary of an on-disk index, in either [`VocabFormat`].
pub enum Vocab<S: UniversalRead = MmapFile> {
    Hashmap(UniversalHashMap<str, TokenId, S>),
    Compact(CompactVocab),
}

impl<S: UniversalRead> Vocab<S> {
    pub fn format(&self) -> VocabFormat {
        match self {
            Self::Hashmap(_) => VocabFormat::Hashmap,
            Self::Compact(_) => VocabFormat::Compact,
        }
    }

    pub fn keys_count(&self) -> usize {
        match self {
            Self::Hashmap(vocab) => vocab.keys_count(),
            Self::Compact(vocab) => vocab.keys_count(),
        }
    }

    pub fn for_each_entry(
        &self,
        mut f: impl FnMut(&str, TokenId) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            Self::Hashmap(vocab) => vocab.for_each_entry(|k, v| f(k, unwrap_token(v))),
            Self::Compact(vocab) => vocab.for_each_entry(f),
        }
    }

    /// Heap held by the vocabulary: none for the hash map, which is read in place.
    pub(crate) fn ram_usage_bytes(&self) -> usize {
        match self {
            Self::Hashmap(_) => 0,
            Self::Compact(vocab) => vocab.ram_usage_bytes(),
        }
    }

    pub(super) fn populate(&self) -> OperationResult<()> {
        match self {
            Self::Hashmap(vocab) => vocab.populate()?,
            Self::Compact(_) => {}
        }
        Ok(())
    }

    pub(super) fn clear_cache(&self) -> OperationResult<()> {
        match self {
            Self::Hashmap(vocab) => vocab.clear_ram_cache()?,
            // In RAM, and its file left the page cache when it was read.
            Self::Compact(_) => {}
        }
        Ok(())
    }
}

/// The vocabulary in whichever [`VocabFormat`] is on disk.
///
/// The compact one is read whole, in one request, and leaves the page cache;
/// `options` only apply to the hash map, which is read in place.
pub(in super::super) fn open_vocab<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    path: &Path,
    options: OpenOptions,
) -> OperationResult<Vocab<S>> {
    let compact_path = path.join(COMPACT_VOCAB_FILE);
    let hashmap_path = path.join(VOCAB_FILE);

    let Some(file) = OneshotFile::open(fs, &compact_path).ok_not_found()? else {
        return Ok(Vocab::Hashmap(UniversalHashMap::open(
            fs,
            &hashmap_path,
            options,
            Default::default(),
        )?));
    };
    // `write_files` leaves only one format behind, so both on disk means a
    // file set no build produced, and neither can be trusted to be the one
    // the postings were written with.
    if fs.exists(&hashmap_path)? {
        return Err(OperationError::service_error(format!(
            "text index in {} has vocabularies of both formats",
            path.display(),
        )));
    }
    let bytes = file.read_whole::<u8>()?;
    let vocab = compact_vocab::read(&bytes).map_err(|reason| {
        OperationError::service_error(format!(
            "invalid compact vocabulary {}: {reason}",
            compact_path.display(),
        ))
    })?;
    Ok(Vocab::Compact(vocab))
}
