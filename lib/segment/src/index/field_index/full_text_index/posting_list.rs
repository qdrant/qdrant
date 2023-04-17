use bitvec::prelude::*;

use crate::types::PointOffsetType;

#[derive(Clone, Debug)]
pub enum PostingList {
    FullVec(Vec<PointOffsetType>),
    BitVec(BitVec),
}

impl Default for PostingList {
    fn default() -> Self {
        Self::FullVec(vec![])
    }
}

impl PostingList {
    const LOWER_THRESHOLD: f64 = 0.03;
    const UPPER_THRESHOLD: f64 = 0.032;

    pub fn new(idx: PointOffsetType) -> Self {
        Self::FullVec(vec![idx])
    }

    pub fn insert(&mut self, idx: PointOffsetType, doc_count: usize) {
        match self {
            Self::FullVec(vec) => {
                if let Err(insertion_idx) = vec.binary_search(&idx) {
                    vec.insert(insertion_idx, idx);
                }
                if vec.len() as f64 > Self::UPPER_THRESHOLD * doc_count as f64 {
                    // dynamically change it to bitvec
                    self.convert_to_bitvec();
                }
            }
            Self::BitVec(bitvec) => {
                bitvec.resize(doc_count, false);
                bitvec.set(idx as usize, true);
            }
        }
    }

    pub fn remove(&mut self, idx: PointOffsetType, doc_count: usize) {
        match self {
            Self::FullVec(vec) => {
                if let Ok(removal_idx) = vec.binary_search(&idx) {
                    vec.remove(removal_idx);
                }
            }
            Self::BitVec(bitvec) => {
                bitvec.set(idx as usize, false);
                if (bitvec.count_ones() as f64) < Self::LOWER_THRESHOLD * doc_count as f64 {
                    self.convert_to_vec();
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::FullVec(v) => v.len(),
            Self::BitVec(v) => v.count_ones(),
        }
    }

    pub fn contains(&self, val: &PointOffsetType) -> bool {
        match self {
            Self::FullVec(v) => v.binary_search(val).is_ok(),
            Self::BitVec(v) => {
                if let Some(res) = v.get((*val) as usize).as_deref() {
                    res == &true
                } else {
                    false
                }
            }
        }
    }

    fn convert_to_bitvec(&mut self) {
        *self = match std::mem::replace(self, Self::BitVec(bitvec![])) {
            Self::FullVec(vec) => {
                let mut bit_vec = bitvec![];
                for idx in vec {
                    if bit_vec.len() <= idx as usize {
                        bit_vec.resize(idx as usize + 1, Default::default());
                    }
                    bit_vec.set(idx as usize, true);
                }
                Self::BitVec(bit_vec)
            }
            v => v,
        }
    }

    fn convert_to_vec(&mut self) {
        *self = match std::mem::replace(self, Self::FullVec(vec![])) {
            Self::BitVec(vec) => Self::FullVec(
                vec.iter()
                    .by_vals()
                    .enumerate()
                    .filter(|(_idx, present_doc)| *present_doc)
                    .map(|(doc_idx, _)| doc_idx as PointOffsetType)
                    .collect(),
            ),
            v => v,
        }
    }
}

impl<'a> IntoIterator for &'a PostingList {
    type Item = PointOffsetType;

    type IntoIter = PostingListIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        PostingListIter {
            postings: self,
            index: 0,
        }
    }
}

pub struct PostingListIter<'a> {
    postings: &'a PostingList,
    index: usize,
}

impl Iterator for PostingListIter<'_> {
    type Item = PointOffsetType;

    fn next(&mut self) -> Option<Self::Item> {
        match self.postings {
            PostingList::FullVec(vec) => {
                if self.index < self.postings.len() {
                    let ret = Some(vec[self.index]);
                    self.index += 1;
                    ret
                } else {
                    None
                }
            }
            PostingList::BitVec(vec) => {
                while self.index < vec.len() {
                    match vec.get(self.index).as_deref() {
                        Some(&true) => {
                            let ret = Some(self.index as u32);
                            self.index += 1;
                            return ret;
                        }
                        Some(&false) => {
                            self.index += 1;
                        }
                        None => {
                            return None;
                        }
                    }
                }
                None
            }
        }
    }
}
