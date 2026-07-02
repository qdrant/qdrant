//! IDF statistics for sparse vector scoring with the `idf` modifier.
//!
//! Statistics — the document count `N` and per-term document frequencies
//! `df(term)` — are computed either globally (over all indexed vectors) or
//! over an *IDF corpus*: the points matching a caller-supplied filter.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::bitvec::{BitSliceExt as _, BitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{DeferredBehavior, PointOffsetType};
use sparse::common::types::DimId;
use sparse::index::inverted_index::InvertedIndex;
use sparse::index::posting_list_common::PostingListIter as _;

use super::SparseVectorIndexReadView;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::types::Filter;
use crate::vector_storage::VectorStorageRead;

/// Membership representation of a corpus within one segment.
///
/// The representation drives how document frequencies are counted:
///
/// - A small corpus is kept as a sorted id list, and df is counted by
///   skipping through the query terms' posting lists — cost scales with the
///   corpus, not with posting lengths or segment size.
/// - A large corpus is kept as a dense membership mask (an id list for it
///   would cost more than the mask), and df is counted by walking the
///   posting lists against the mask.
enum CorpusPoints {
    SortedIds(Vec<PointOffsetType>),
    Mask(BitVec),
}

impl<I, V, P, TInvertedIndex> SparseVectorIndexReadView<'_, I, V, P, TInvertedIndex>
where
    I: IdTrackerRead,
    V: VectorStorageRead,
    P: PayloadIndexRead,
    TInvertedIndex: InvertedIndex,
{
    /// Update statistics for idf-dot similarity over the given corpus.
    ///
    /// Returns the number of documents contributing to the statistics:
    /// vectors of the points matching the corpus filter, or all indexed
    /// vectors when `corpus` is `None` (global statistics).
    pub fn fill_idf_statistics(
        &self,
        idf: &mut HashMap<DimId, usize>,
        corpus: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        match corpus {
            None => self.fill_global_idf_statistics(idf, hw_counter),
            Some(corpus) => self.fill_corpus_idf_statistics(idf, corpus, is_stopped, hw_counter),
        }
    }

    /// Global statistics: df(term) is the whole posting list length.
    fn fill_global_idf_statistics(
        &self,
        idf: &mut HashMap<DimId, usize>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        let iter = idf.iter_mut().filter_map(|(dim_id, count)| {
            let offset = self.indices_tracker.remap_index(*dim_id)?;
            Some((count, offset))
        });
        self.inverted_index
            .posting_list_len_batch(iter, hw_counter, |count, len| {
                *count += len;
                Ok(())
            })?;
        Ok(self.inverted_index.vector_count())
    }

    /// Corpus-scoped statistics: df(term) counts only the posting entries of
    /// points matching the corpus filter.
    fn fill_corpus_idf_statistics(
        &self,
        idf: &mut HashMap<DimId, usize>,
        corpus: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        let (document_count, corpus_points) =
            self.collect_corpus_points(corpus, is_stopped, hw_counter)?;

        let arena = blink_alloc::Blink::new();
        let ids = idf.iter_mut().filter_map(|(dim_id, count)| {
            let offset = self.indices_tracker.remap_index(*dim_id)?;
            Some((count, offset))
        });
        match corpus_points {
            CorpusPoints::Mask(mask) => {
                self.inverted_index
                    .get_batch(ids, &arena, hw_counter, |count, posting_list_iter| {
                        for element in posting_list_iter.into_std_iter() {
                            if mask.get_bit(element.record_id as usize).unwrap_or(false) {
                                *count += 1;
                            }
                        }
                        Ok(())
                    })?;
            }
            CorpusPoints::SortedIds(corpus_ids) => {
                self.inverted_index.get_batch(
                    ids,
                    &arena,
                    hw_counter,
                    |count, mut posting_list_iter| {
                        let Some(last_id) = posting_list_iter.last_id() else {
                            return Ok(());
                        };
                        for &point_offset in &corpus_ids {
                            if point_offset > last_id {
                                break;
                            }
                            if posting_list_iter.skip_to(point_offset).is_some() {
                                *count += 1;
                            }
                        }
                        Ok(())
                    },
                )?;
            }
        }
        check_process_stopped(is_stopped)?;

        Ok(document_count)
    }

    /// Resolve the corpus filter once for this segment into the set of points
    /// that match the corpus AND have this sparse vector. Points without the
    /// vector are not documents of this corpus.
    ///
    /// The cardinality estimate picks the initial representation; since it is
    /// only an estimate, the id list degrades into the mask if the actual
    /// corpus outgrows the threshold.
    fn collect_corpus_points(
        &self,
        corpus: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<(usize, CorpusPoints)> {
        let total_points = self.id_tracker.total_point_count();
        // At 1/32 of the segment the sorted id list (4 bytes/id) starts to
        // outweigh the dense mask (1 bit/point).
        let id_list_threshold = (total_points / 32).max(128);

        let cardinality = self
            .payload_index
            .estimate_cardinality(corpus, hw_counter)?;

        let deleted_vectors = self.vector_storage.deleted_vector_bitslice();

        let mut document_count: usize = 0;
        let mut corpus_ids: Vec<PointOffsetType> = Vec::new();
        let mut corpus_mask: Option<BitVec> = (cardinality.exp > id_list_threshold)
            .then(|| BitVec::repeat(false, total_points));

        let points_iter = self.payload_index.iter_filtered_points(
            corpus,
            &cardinality,
            hw_counter,
            is_stopped,
            DeferredBehavior::VisibleOnly,
        )?;
        for point_offset in points_iter {
            // The deleted-vector bitvec grows lazily; an out-of-bounds bit
            // means the vector was never marked deleted.
            let has_vector = !deleted_vectors
                .get_bit(point_offset as usize)
                .unwrap_or(false);
            if !has_vector {
                continue;
            }
            document_count += 1;

            match &mut corpus_mask {
                Some(mask) => mask.set(point_offset as usize, true),
                None => {
                    corpus_ids.push(point_offset);
                    if corpus_ids.len() > id_list_threshold {
                        // The estimate undershot; degrade to the dense mask.
                        let mut mask = BitVec::repeat(false, total_points);
                        for &id in &corpus_ids {
                            mask.set(id as usize, true);
                        }
                        corpus_ids = Vec::new();
                        corpus_mask = Some(mask);
                    }
                }
            }
        }
        check_process_stopped(is_stopped)?;

        let corpus_points = match corpus_mask {
            Some(mask) => CorpusPoints::Mask(mask),
            None => {
                // `skip_to` only moves forward, so probe in ascending order.
                corpus_ids.sort_unstable();
                CorpusPoints::SortedIds(corpus_ids)
            }
        };
        Ok((document_count, corpus_points))
    }
}
