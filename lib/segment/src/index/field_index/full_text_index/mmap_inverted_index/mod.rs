#![allow(dead_code)]

use std::path::PathBuf;

use common::mmap_hashmap::MmapHashMap;
use mmap_postings::MmapPostings;

use super::inverted_index::ImmutableInvertedIndex;
use crate::common::mmap_slice_buffered_update_wrapper::MmapSliceBufferedUpdateWrapper;
use crate::common::operation_error::OperationResult;

mod mmap_postings;

const POSTINGS_FILE: &str = "postings.dat";
const VOCAB_FILE: &str = "vocab.dat";
const POINT_TO_TOKENS_COUNT_FILE: &str = "point_to_tokens_count.dat";

pub struct MmapInvertedIndex {
    path: PathBuf,
    postings: MmapPostings,
    vocab: MmapHashMap<str>,
    point_to_tokens_count: MmapSliceBufferedUpdateWrapper<Option<usize>>,
    points_count: usize,
}

impl MmapInvertedIndex {
    pub fn create_from(
        path: PathBuf,
        inverted_index: ImmutableInvertedIndex,
    ) -> OperationResult<()> {
        let ImmutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens_count: _,
            points_count: _,
        } = inverted_index;

        MmapPostings::create_from(path.clone().join(POSTINGS_FILE), &postings)?;

        // TODO(luis): Currently MmapHashMap maps str -> [u32], but we only need to map str -> u32.
        // Consider making another mmap structure for this case.
        MmapHashMap::<str>::create(
            path.clone().join(VOCAB_FILE).as_path(),
            vocab.iter().map(|(k, v)| (k.as_str(), std::iter::once(*v))),
        )?;

        // TODO(luis): create point_to_tokens_count mmap file

        // TODO(luis): save points_count to a file?

        Ok(())
    }
}
