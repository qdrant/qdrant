use std::collections::HashMap;

use common::types::PointOffsetType;
use smallvec::SmallVec;

pub type ElementIndices = SmallVec<[u32; 2]>;

/// Per-field index of array element positions, used for nested filter same-element checks.
#[derive(Debug, Default)]
pub struct NestedElementIndex {
    // point_id → type-prefixed value (e.g. "s:foo", "n:42", "b:true") → element indices
    positions: Vec<HashMap<String, ElementIndices>>,
}

impl NestedElementIndex {
    pub fn new() -> Self {
        Self {
            positions: Vec::new(),
        }
    }

    pub fn add_entry(&mut self, point_id: PointOffsetType, value: String, element_idx: u32) {
        let idx = point_id as usize;
        if self.positions.len() <= idx {
            self.positions.resize_with(idx + 1, HashMap::new);
        }
        self.positions[idx]
            .entry(value)
            .or_default()
            .push(element_idx);
    }

    pub fn get_indices(&self, point_id: PointOffsetType, value: &str) -> Option<&ElementIndices> {
        self.positions.get(point_id as usize)?.get(value)
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }
}

/// Intersect element indices and return the matching indices.
pub fn intersect_to_vec(sets: &[&ElementIndices]) -> SmallVec<[u32; 4]> {
    if sets.is_empty() {
        return SmallVec::new();
    }
    let (smallest_idx, _) = sets
        .iter()
        .enumerate()
        .min_by_key(|(_, s)| s.len())
        .unwrap();

    let mut result = SmallVec::new();
    'outer: for &candidate in sets[smallest_idx].iter() {
        for (i, set) in sets.iter().enumerate() {
            if i == smallest_idx {
                continue;
            }
            if !set.contains(&candidate) {
                continue 'outer;
            }
        }
        result.push(candidate);
    }
    result
}

/// Returns true if any element index is present in ALL provided sets.
pub fn intersect_element_indices(sets: &[&ElementIndices]) -> bool {
    if sets.is_empty() {
        return false;
    }
    let (smallest_idx, _) = sets
        .iter()
        .enumerate()
        .min_by_key(|(_, s)| s.len())
        .unwrap();

    'outer: for &candidate in sets[smallest_idx].iter() {
        for (i, set) in sets.iter().enumerate() {
            if i == smallest_idx {
                continue;
            }
            if !set.contains(&candidate) {
                continue 'outer;
            }
        }
        return true;
    }
    false
}
