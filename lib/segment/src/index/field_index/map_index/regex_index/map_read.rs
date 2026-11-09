//! Regex surface of the string-keyed map index: every variant that carries
//! a regex index selects the keys a regex matches through this trait.
//! Variants without one (built without the `regex` option) return `None`;
//! the caller then checks every key instead.

use std::sync::atomic::AtomicBool;

use blobstore::Blob;
use common::universal_io::UniversalRead;
use ecow::EcoString;

use super::matcher::RegexMatcher;
use super::query::TrigramQuery;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::map_index::MapIndex;
use crate::index::field_index::map_index::immutable_map_index::ImmutableMapIndex;
use crate::index::field_index::map_index::key::MapIndexKey;
use crate::index::field_index::map_index::mutable_map_index::MutableMapIndex;
use crate::index::field_index::map_index::mutable_map_index::in_memory::InMemoryMapIndex;
use crate::index::field_index::map_index::mutable_map_index::read_only::ReadOnlyAppendableMapIndex;
use crate::index::field_index::map_index::on_disk_map_index::OnDiskMapIndex;
use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;

/// Upper bounds over the candidate keys of a trigram query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RegexCandidateStats {
    /// Number of candidate keys.
    pub keys: usize,
    /// Sum of their postings counts.
    pub postings: usize,
}

/// Regex key selection over the keys of a string-keyed map index.
///
/// All methods return `Ok(None)` when the index instance has no regex index.
pub trait StrMapIndexRegexRead {
    /// Keys `matcher` matches, in no particular order.
    ///
    /// Checking keys against the regex is the expensive part of a regex
    /// condition, so it gives up with
    /// [`OperationError::Cancelled`](crate::common::operation_error::OperationError::Cancelled)
    /// once `is_stopped` is set.
    fn regex_keys(
        &self,
        matcher: &RegexMatcher,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Option<Vec<EcoString>>>;

    /// Upper bounds over the keys `query` admits, without verifying them
    /// against the regex. Also `Ok(None)` when the query admits every key.
    fn regex_candidate_stats(
        &self,
        query: &TrigramQuery,
    ) -> OperationResult<Option<RegexCandidateStats>>;
}

impl StrMapIndexRegexRead for InMemoryMapIndex<str> {
    fn regex_keys(
        &self,
        matcher: &RegexMatcher,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Option<Vec<EcoString>>> {
        let Some(regex_index) = &self.regex_index else {
            return Ok(None);
        };
        let keys = regex_index.matching_keys(matcher, is_stopped)?;
        Ok(Some(keys.into_iter().cloned().collect()))
    }

    fn regex_candidate_stats(
        &self,
        query: &TrigramQuery,
    ) -> OperationResult<Option<RegexCandidateStats>> {
        let Some(regex_index) = &self.regex_index else {
            return Ok(None);
        };
        let Some(keys) = regex_index.candidate_keys(query) else {
            return Ok(None);
        };
        let mut stats = RegexCandidateStats::default();
        for key in keys {
            stats.keys += 1;
            stats.postings += self.map.get(key).map_or(0, |ids| ids.len() as usize);
        }
        Ok(Some(stats))
    }
}

impl StrMapIndexRegexRead for MutableMapIndex<str> {
    fn regex_keys(
        &self,
        matcher: &RegexMatcher,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Option<Vec<EcoString>>> {
        self.in_memory_index.regex_keys(matcher, is_stopped)
    }

    fn regex_candidate_stats(
        &self,
        query: &TrigramQuery,
    ) -> OperationResult<Option<RegexCandidateStats>> {
        self.in_memory_index.regex_candidate_stats(query)
    }
}

impl<S: UniversalRead> StrMapIndexRegexRead for ReadOnlyAppendableMapIndex<str, S> {
    fn regex_keys(
        &self,
        matcher: &RegexMatcher,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Option<Vec<EcoString>>> {
        self.in_memory_index.regex_keys(matcher, is_stopped)
    }

    fn regex_candidate_stats(
        &self,
        query: &TrigramQuery,
    ) -> OperationResult<Option<RegexCandidateStats>> {
        self.in_memory_index.regex_candidate_stats(query)
    }
}

/// Served by the regex index file of the backing storage, kept populated.
impl<S: UniversalRead> StrMapIndexRegexRead for ImmutableMapIndex<str, S> {
    fn regex_keys(
        &self,
        matcher: &RegexMatcher,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Option<Vec<EcoString>>> {
        self.storage.regex_keys(matcher, is_stopped)
    }

    fn regex_candidate_stats(
        &self,
        query: &TrigramQuery,
    ) -> OperationResult<Option<RegexCandidateStats>> {
        self.storage.regex_candidate_stats(query)
    }
}

/// Postings counts are recorded at build time, so the stats include points
/// deleted since; the keys themselves are always verified with the regex.
impl<S: UniversalRead> StrMapIndexRegexRead for OnDiskMapIndex<str, S> {
    fn regex_keys(
        &self,
        matcher: &RegexMatcher,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Option<Vec<EcoString>>> {
        self.regex_index()
            .map(|regex_index| regex_index.matching_keys(matcher, is_stopped))
            .transpose()
    }

    fn regex_candidate_stats(
        &self,
        query: &TrigramQuery,
    ) -> OperationResult<Option<RegexCandidateStats>> {
        Ok(match self.regex_index() {
            Some(regex_index) => regex_index.candidate_stats(query)?,
            None => None,
        })
    }
}

impl StrMapIndexRegexRead for MapIndex<str> {
    fn regex_keys(
        &self,
        matcher: &RegexMatcher,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Option<Vec<EcoString>>> {
        match self {
            MapIndex::Mutable(index) => index.regex_keys(matcher, is_stopped),
            MapIndex::Immutable(index) => index.regex_keys(matcher, is_stopped),
            MapIndex::OnDisk(index) => index.regex_keys(matcher, is_stopped),
        }
    }

    fn regex_candidate_stats(
        &self,
        query: &TrigramQuery,
    ) -> OperationResult<Option<RegexCandidateStats>> {
        match self {
            MapIndex::Mutable(index) => index.regex_candidate_stats(query),
            MapIndex::Immutable(index) => index.regex_candidate_stats(query),
            MapIndex::OnDisk(index) => index.regex_candidate_stats(query),
        }
    }
}

impl<S: UniversalRead> StrMapIndexRegexRead for ReadOnlyMapIndex<str, S>
where
    Vec<<str as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    fn regex_keys(
        &self,
        matcher: &RegexMatcher,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Option<Vec<EcoString>>> {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.regex_keys(matcher, is_stopped),
            ReadOnlyMapIndex::Immutable(index) => index.regex_keys(matcher, is_stopped),
            ReadOnlyMapIndex::OnDisk(index) => index.regex_keys(matcher, is_stopped),
        }
    }

    fn regex_candidate_stats(
        &self,
        query: &TrigramQuery,
    ) -> OperationResult<Option<RegexCandidateStats>> {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.regex_candidate_stats(query),
            ReadOnlyMapIndex::Immutable(index) => index.regex_candidate_stats(query),
            ReadOnlyMapIndex::OnDisk(index) => index.regex_candidate_stats(query),
        }
    }
}
