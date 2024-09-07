#![allow(dead_code)]

use std::path::PathBuf;

use bitvec::vec::BitVec;
use common::mmap_hashmap::MmapHashMap;
use memory::madvise::AdviceSetting;
use memory::mmap_ops;
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

        let postings_path = path.join(POSTINGS_FILE);
        let vocab_path = path.join(VOCAB_FILE);
        let point_to_tokens_count_path = path.join(POINT_TO_TOKENS_COUNT_FILE);
        let deleted_points_path = path.join(DELETED_POINTS_FILE);

        MmapPostings::create(postings_path, &postings)?;

        // TODO(luis): Currently MmapHashMap maps str -> [u32], but we only need to map str -> u32.
        // Consider making another mmap structure for this case.
        MmapHashMap::<str>::create(
            &vocab_path,
            vocab.iter().map(|(k, v)| (k.as_str(), std::iter::once(*v))),
        )?;

        // Save point_to_tokens_count, separated into a bitslice for None values and a slice for actual values
        //
        // None values are represented as deleted in the bitslice
        let deleted_bitslice: BitVec = point_to_tokens_count
            .iter()
            .map(|count| count.is_none())
            .collect();
        MmapBitSlice::create(&deleted_points_path, &deleted_bitslice)?;

        // The actual values go in the slice
        let point_to_tokens_count_iter = point_to_tokens_count
            .into_iter()
            .map(|count| count.unwrap_or(0));

        MmapSlice::create(&point_to_tokens_count_path, point_to_tokens_count_iter)?;

        // TODO(luis): save points_count to a file?
        Ok(())
    }

    pub fn open(path: PathBuf) -> OperationResult<Self> {
        let postings_path = path.clone().join(POSTINGS_FILE);
        let vocab_path = path.clone().join(VOCAB_FILE);
        let point_to_tokens_count_path = path.clone().join(POINT_TO_TOKENS_COUNT_FILE);
        let deleted_points_path = path.clone().join(DELETED_POINTS_FILE);

        let postings = MmapPostings::open(&postings_path)?;
        let vocab = MmapHashMap::<str>::open(&vocab_path)?;

        let point_to_tokens_count = unsafe {
            MmapSlice::try_from(mmap_ops::open_write_mmap(
                &point_to_tokens_count_path,
                AdviceSetting::Global,
            )?)?
        };

        let deleted = mmap_ops::open_write_mmap(&deleted_points_path, AdviceSetting::Global)?;
        let deleted = MmapBitSlice::from(deleted, 0);
        let deleted_points = MmapBitSliceBufferedUpdateWrapper::new(deleted);

        let points_count = point_to_tokens_count.len();

        Ok(Self {
            path,
            postings,
            vocab,
            point_to_tokens_count,
            deleted_points,
            points_count,
        })
    }
}
