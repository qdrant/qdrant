#![allow(dead_code)]

use std::path::PathBuf;

use bitvec::vec::BitVec;
use common::mmap_hashmap::MmapHashMap;
use memory::mmap_type::{MmapBitSlice, MmapSlice};
use mmap_postings::MmapPostings;

use super::inverted_index::ImmutableInvertedIndex;
use crate::common::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper;
use crate::common::operation_error::OperationResult;

mod mmap_postings;

const POSTINGS_FILE: &str = "postings.dat";
const VOCAB_FILE: &str = "vocab.dat";
const POINT_TO_TOKENS_COUNT_FILE: &str = "point_to_tokens_count.dat";
const DELETED_POINTS_FILE: &str = "deleted_points.dat";

pub struct MmapInvertedIndex {
    path: PathBuf,
    postings: MmapPostings,
    vocab: MmapHashMap<str>,
    point_to_tokens_count: MmapSlice<usize>,
    deleted_points: MmapBitSliceBufferedUpdateWrapper,
    points_count: usize,
}

impl MmapInvertedIndex {
    pub fn create(path: PathBuf, inverted_index: ImmutableInvertedIndex) -> OperationResult<()> {
        let ImmutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens_count,
            points_count: _,
        } = inverted_index;

        MmapPostings::create(path.clone().join(POSTINGS_FILE), &postings)?;

        // TODO(luis): Currently MmapHashMap maps str -> [u32], but we only need to map str -> u32.
        // Consider making another mmap structure for this case.
        MmapHashMap::<str>::create(
            path.clone().join(VOCAB_FILE).as_path(),
            vocab.iter().map(|(k, v)| (k.as_str(), std::iter::once(*v))),
        )?;

        // Save point_to_tokens_count, separated into a bitslice for None values and a slice for actual values
        //
        // None values are represented as deleted in the bitslice
        let deleted_bitslice: BitVec = point_to_tokens_count
            .iter()
            .map(|count| count.is_none())
            .collect();
        MmapBitSlice::create(&path.clone().join(DELETED_POINTS_FILE), &deleted_bitslice)?;

        // The actual values go in the slice
        let point_to_tokens_count_iter = point_to_tokens_count
            .into_iter()
            .map(|count| count.unwrap_or(0));

        MmapSlice::create(
            &path.clone().join(POINT_TO_TOKENS_COUNT_FILE),
            point_to_tokens_count_iter,
        )?;

        // TODO(luis): save points_count to a file?
        Ok(())
    }
}
