use std::path::{Path, PathBuf};

use bitvec::slice::BitSlice;
use common::types::PointOffsetType;
use itertools::Itertools;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    PrimaryCondition, ValueIndexer,
};
use crate::types::{FieldCondition, Match, MatchValue, PayloadKeyType, ValueVariants};
use crate::vector_storage::common::PAGE_SIZE_BYTES;
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;

const BASE_DIR_PREFIX: &str = "bool-";
const TRUES_DIRNAME: &str = "trues";
const FALSES_DIRNAME: &str = "falses";

/// Payload index for boolean values, stored in memory-mapped files.
pub struct MmapBoolIndex {
    base_dir: PathBuf,
    indexed_count: usize,
    trues_slice: DynamicMmapFlags,
    falses_slice: DynamicMmapFlags,
}

impl MmapBoolIndex {
    /// Returns the directory name for the given field name, where the files should live.
    fn dirname(field_name: &str) -> String {
        format!("{BASE_DIR_PREFIX}{field_name}")
    }

    /// Creates a new boolean index at the given path. If it already exists, loads the index.
    ///
    /// # Arguments
    /// - `parent_path`: The path to the directory containing the rest of the indexes.
    pub fn open_or_create(indexes_path: &Path, field_name: String) -> OperationResult<Self> {
        let path = indexes_path.join(Self::dirname(&field_name));

        let falses_dir = path.join(FALSES_DIRNAME);
        if falses_dir.is_dir() {
            Self::open(&path)
        } else {
            std::fs::create_dir_all(&path).map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to create mmap bool index directory: {err}"
                ))
            })?;
            Self::open(&path)
        }
    }

    fn open(path: &Path) -> OperationResult<Self> {
        if !path.is_dir() {
            return Err(OperationError::service_error("Path is not a directory"));
        }

        // Trues bitslice
        let trues_path = path.join(TRUES_DIRNAME);
        let trues_slice = DynamicMmapFlags::open(&trues_path)?;

        // Falses bitslice
        let falses_path = path.join(FALSES_DIRNAME);
        let falses_slice = DynamicMmapFlags::open(&falses_path)?;

        Ok(Self {
            base_dir: path.to_path_buf(),
            trues_slice,
            falses_slice,
            // loading is done after opening during `PayloadFieldIndex::load()`
            indexed_count: 0,
        })
    }

    fn set_or_insert(&mut self, id: u32, has_true: bool, has_false: bool) -> OperationResult<()> {
        let prev_true = set_or_insert_flag(&mut self.trues_slice, id as usize, has_true)?;
        let prev_false = set_or_insert_flag(&mut self.falses_slice, id as usize, has_false)?;

        let was_indexed = prev_true || prev_false;
        let gets_indexed = has_true || has_false;

        match (was_indexed, gets_indexed) {
            (false, true) => {
                self.indexed_count += 1;
            }
            (true, false) => {
                self.indexed_count = self.indexed_count.saturating_sub(1);
            }
            _ => {}
        }

        Ok(())
    }

    fn get_slice_for(&self, value: bool) -> &BitSlice {
        if value {
            self.trues_slice.get_bitslice()
        } else {
            self.falses_slice.get_bitslice()
        }
    }

    fn bitslice_usize_view(bitslice: &BitSlice) -> &[usize] {
        let (head, body, tail) = bitslice
            .domain()
            .region()
            .expect("BitSlice should be bigger than one usize");

        debug_assert!(head.is_none(), "BitSlice should be aligned to usize");
        debug_assert!(tail.is_none(), "BitSlice should be aligned to usize");

        body
    }

    /// Calculates the count of true values of the union of both slices.
    fn calculate_indexed_count(&self) -> u32 {
        // indexed_count would be trues_bitslice.union_count(falses_bitslice), but there is no such operation yet.
        // So we do it manually.
        let trues = Self::bitslice_usize_view(self.get_slice_for(true));
        let falses = Self::bitslice_usize_view(self.get_slice_for(false));

        let mut indexed_count = 0;
        for elem in trues.iter().zip_longest(falses) {
            match elem {
                itertools::EitherOrBoth::Both(trues_elem, falses_elem) => {
                    let union = trues_elem | falses_elem;
                    indexed_count += union.count_ones();
                }
                itertools::EitherOrBoth::Left(trues_elem) => {
                    indexed_count += trues_elem.count_ones();
                }
                itertools::EitherOrBoth::Right(falses_elem) => {
                    indexed_count += falses_elem.count_ones();
                }
            }
        }

        indexed_count
    }
}

/// Set or insert a flag in the given flags. Returns previous value.
///
/// Resizes if needed to set to true.
fn set_or_insert_flag(
    flags: &mut DynamicMmapFlags,
    key: usize,
    value: bool,
) -> OperationResult<bool> {
    // Set to true
    if value {
        // Make sure the key fits
        if key >= flags.len() {
            let new_len = (key + 1).next_multiple_of(PAGE_SIZE_BYTES);
            flags.set_len(new_len)?;
        }
        return Ok(flags.set(key, value));
    }

    // Set to false
    if key < flags.len() {
        return Ok(flags.set(key, value));
    }

    Ok(false)
}

pub struct BoolIndexBuilder(MmapBoolIndex);

impl FieldIndexBuilderTrait for BoolIndexBuilder {
    type FieldIndexType = MmapBoolIndex;

    fn init(&mut self) -> OperationResult<()> {
        // After Self is created, it is already initialized
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&serde_json::Value],
    ) -> OperationResult<()> {
        self.0.add_point(id, payload)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(self.0)
    }
}

impl ValueIndexer for MmapBoolIndex {
    type ValueType = bool;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
    ) -> OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        let has_true = values.iter().any(|v| *v);
        let has_false = values.iter().any(|v| !*v);

        self.set_or_insert(id, has_true, has_false)?;

        Ok(())
    }

    fn get_value(value: &serde_json::Value) -> Option<Self::ValueType> {
        value.as_bool()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.set_or_insert(id, false, false)?;
        Ok(())
    }
}
impl PayloadFieldIndex for MmapBoolIndex {
    fn count_indexed_points(&self) -> usize {
        self.indexed_count
    }

    fn load(&mut self) -> OperationResult<bool> {
        let calculated_indexed_count = self.calculate_indexed_count();

        // Destructure to not forget any fields
        let Self {
            base_dir: _,
            indexed_count,
            trues_slice: _,
            falses_slice: _,
        } = self;

        *indexed_count = calculated_indexed_count as usize;

        Ok(true)
    }

    fn cleanup(self) -> OperationResult<()> {
        std::fs::remove_dir_all(self.base_dir)?;
        Ok(())
    }

    fn flusher(&self) -> crate::common::Flusher {
        let Self {
            base_dir: _,
            indexed_count: _,
            trues_slice,
            falses_slice,
        } = self;

        let trues_flusher = trues_slice.flusher();
        let falses_flusher = falses_slice.flusher();

        Box::new(move || {
            trues_flusher()?;
            falses_flusher()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        let mut files = self.trues_slice.files();
        files.extend(self.falses_slice.files());

        files
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Bool(value),
            })) => Some(Box::new(
                self.get_slice_for(*value)
                    .iter_ones()
                    .map(|x| x as PointOffsetType),
            )),
            _ => None,
        }
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Bool(value),
            })) => {
                let count = self.get_slice_for(*value).count_ones();

                let estimation = CardinalityEstimation::exact(count)
                    .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone())));

                Some(estimation)
            }
            _ => None,
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        let make_block = |count, value, key: PayloadKeyType| {
            if count > threshold {
                Some(PayloadBlockCondition {
                    condition: FieldCondition::new_match(
                        key,
                        Match::Value(MatchValue {
                            value: ValueVariants::Bool(value),
                        }),
                    ),
                    cardinality: count,
                })
            } else {
                None
            }
        };

        // just two possible blocks: true and false
        let iter = [
            make_block(self.trues_slice.count_flags(), true, key.clone()),
            make_block(self.falses_slice.count_flags(), false, key),
        ]
        .into_iter()
        .flatten();

        Box::new(iter)
    }
}
