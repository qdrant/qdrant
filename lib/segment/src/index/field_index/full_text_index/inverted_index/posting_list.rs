use common::types::PointOffsetType;
use roaring::RoaringBitmap;

#[derive(Clone, Debug, Default)]
pub struct PostingList {
    list: RoaringBitmap,
}

impl PostingList {
    pub fn insert(&mut self, idx: PointOffsetType) {
        self.list.insert(idx);
    }

    pub fn remove(&mut self, idx: PointOffsetType) {
        self.list.remove(idx);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.list.len() as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    #[inline]
    pub fn contains(&self, val: PointOffsetType) -> bool {
        self.list.contains(val)
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.list.iter()
    }
}
