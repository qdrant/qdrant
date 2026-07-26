//! Prefix enumeration surface of the string-keyed map index.
//!
//! Counterpart of [`MapIndexRead`][1] for the [`Match::Prefix`][2] condition:
//! every variant that carries a prefix structure (the mutable `BTreeSet`, the
//! immutable sorted key vector, or the on-disk [`PrefixIndex`][3]) enumerates
//! keys by byte-prefix through this trait. Variants without the structure
//! (built without the `prefix` option, or loaded from legacy files) return
//! `None`, which makes the caller fall back to the generic slow paths.
//!
//! [1]: super::super::read_ops::MapIndexRead
//! [2]: crate::types::Match::Prefix
//! [3]: super::reader::PrefixIndex

use std::ops::Bound;

use blobstore::Blob;
use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::UniversalRead;
use ecow::EcoString;

use super::super::MapIndex;
use super::super::immutable_map_index::ImmutableMapIndex;
use super::super::key::MapIndexKey;
use super::super::mutable_map_index::MutableMapIndex;
use super::super::mutable_map_index::in_memory::InMemoryMapIndex;
use super::super::on_disk_map_index::OnDiskMapIndex;
use super::super::read_only::ReadOnlyMapIndex;
use super::super::read_ops::MapIndexRead;
use super::reader::PrefixIndexStats;
use crate::common::operation_error::{OperationError, OperationResult};

/// Prefix range scans over the keys of a string-keyed map index.
///
/// All methods return `Ok(None)` when the index instance has no prefix
/// structure; policy (whether that should have been possible) is decided
/// upstream, per the payload schema.
pub trait StrMapIndexPrefixRead {
    /// Keys starting with `prefix` in ascending byte order, each with its
    /// postings count.
    ///
    /// Counts are exact for in-RAM variants; the on-disk variant reports
    /// build-time counts, which ignore points deleted since the index was
    /// built. The actual postings iteration (via
    /// [`MapIndexRead::get_iterator`]) always filters deleted points, so
    /// this only affects estimates.
    fn prefix_keys_with_counts(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Vec<(EcoString, usize)>>>;

    /// Aggregate `(distinct keys, postings sum)` over keys starting with
    /// `prefix`. Same count semantics as
    /// [`Self::prefix_keys_with_counts`]; the on-disk variant computes this
    /// from per-block aggregates, decoding only boundary blocks.
    fn prefix_stats(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<PrefixIndexStats>>;
}

impl StrMapIndexPrefixRead for InMemoryMapIndex<str> {
    fn prefix_keys_with_counts(
        &self,
        prefix: &str,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Vec<(EcoString, usize)>>> {
        let Some(sorted_keys) = &self.sorted_keys else {
            return Ok(None);
        };
        let keys = sorted_keys
            .range::<str, _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(|key| key.starts_with(prefix))
            .map(|key| {
                let count = self
                    .map
                    .get(key.as_str())
                    .map_or(0, |ids| ids.len() as usize);
                (key.clone(), count)
            })
            .collect();
        Ok(Some(keys))
    }

    fn prefix_stats(
        &self,
        prefix: &str,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<PrefixIndexStats>> {
        let Some(sorted_keys) = &self.sorted_keys else {
            return Ok(None);
        };
        let mut stats = PrefixIndexStats::default();
        for key in sorted_keys
            .range::<str, _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(|key| key.starts_with(prefix))
        {
            stats.keys += 1;
            stats.postings += self
                .map
                .get(key.as_str())
                .map_or(0, |ids| ids.len() as usize);
        }
        Ok(Some(stats))
    }
}

impl StrMapIndexPrefixRead for MutableMapIndex<str> {
    fn prefix_keys_with_counts(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Vec<(EcoString, usize)>>> {
        self.in_memory_index
            .prefix_keys_with_counts(prefix, hw_counter)
    }

    fn prefix_stats(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<PrefixIndexStats>> {
        self.in_memory_index.prefix_stats(prefix, hw_counter)
    }
}

impl<S: UniversalRead> StrMapIndexPrefixRead for ImmutableMapIndex<str, S> {
    fn prefix_keys_with_counts(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Vec<(EcoString, usize)>>> {
        let Some(sorted_keys) = &self.sorted_keys else {
            return Ok(None);
        };
        let start = sorted_keys.partition_point(|key| key.as_str() < prefix);
        let keys = sorted_keys[start..]
            .iter()
            .take_while(|key| key.starts_with(prefix))
            .map(|key| {
                // Live count; `None` means every posting of the key has been
                // deleted since load.
                let count = self
                    .get_count_for_value(key.as_str(), hw_counter)
                    .unwrap_or(0);
                (key.clone(), count)
            })
            .collect();
        Ok(Some(keys))
    }

    fn prefix_stats(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<PrefixIndexStats>> {
        let Some(sorted_keys) = &self.sorted_keys else {
            return Ok(None);
        };
        let start = sorted_keys.partition_point(|key| key.as_str() < prefix);
        let mut stats = PrefixIndexStats::default();
        for key in sorted_keys[start..]
            .iter()
            .take_while(|key| key.starts_with(prefix))
        {
            stats.keys += 1;
            stats.postings += self
                .get_count_for_value(key.as_str(), hw_counter)
                .unwrap_or(0);
        }
        Ok(Some(stats))
    }
}

impl<S: UniversalRead> StrMapIndexPrefixRead for OnDiskMapIndex<str, S> {
    fn prefix_keys_with_counts(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Vec<(EcoString, usize)>>> {
        let Some(prefix_index) = &self.storage.prefix_index else {
            return Ok(None);
        };
        let mut keys = Vec::new();
        prefix_index.for_each_key_with_prefix(
            prefix.as_bytes(),
            hw_counter,
            &mut |key, count| {
                let key = std::str::from_utf8(key).map_err(|_| {
                    OperationError::service_error("Prefix index contains non-UTF-8 key")
                })?;
                keys.push((EcoString::from(key), count));
                Ok(())
            },
        )?;
        Ok(Some(keys))
    }

    fn prefix_stats(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<PrefixIndexStats>> {
        let Some(prefix_index) = &self.storage.prefix_index else {
            return Ok(None);
        };
        Ok(Some(
            prefix_index.prefix_stats(prefix.as_bytes(), hw_counter)?,
        ))
    }
}

impl StrMapIndexPrefixRead for MapIndex<str> {
    fn prefix_keys_with_counts(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Vec<(EcoString, usize)>>> {
        match self {
            MapIndex::Mutable(index) => index.prefix_keys_with_counts(prefix, hw_counter),
            MapIndex::Immutable(index) => index.prefix_keys_with_counts(prefix, hw_counter),
            MapIndex::OnDisk(index) => index.prefix_keys_with_counts(prefix, hw_counter),
        }
    }

    fn prefix_stats(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<PrefixIndexStats>> {
        match self {
            MapIndex::Mutable(index) => index.prefix_stats(prefix, hw_counter),
            MapIndex::Immutable(index) => index.prefix_stats(prefix, hw_counter),
            MapIndex::OnDisk(index) => index.prefix_stats(prefix, hw_counter),
        }
    }
}

impl<S: UniversalRead> StrMapIndexPrefixRead for ReadOnlyMapIndex<str, S>
where
    Vec<<str as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    fn prefix_keys_with_counts(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Vec<(EcoString, usize)>>> {
        match self {
            // Prefix support is not wired for the read-only appendable
            // variant; see `ReadOnlyAppendableMapIndex::open`.
            ReadOnlyMapIndex::Appendable(_) => Ok(None),
            ReadOnlyMapIndex::Immutable(index) => index.prefix_keys_with_counts(prefix, hw_counter),
            ReadOnlyMapIndex::OnDisk(index) => index.prefix_keys_with_counts(prefix, hw_counter),
        }
    }

    fn prefix_stats(
        &self,
        prefix: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<PrefixIndexStats>> {
        match self {
            ReadOnlyMapIndex::Appendable(_) => Ok(None),
            ReadOnlyMapIndex::Immutable(index) => index.prefix_stats(prefix, hw_counter),
            ReadOnlyMapIndex::OnDisk(index) => index.prefix_stats(prefix, hw_counter),
        }
    }
}
