use std::ops::Range;

use common::generic_consts::AccessPattern;
use common::universal_io::UserData;

use super::CompactedTracker;
use crate::Result;
use crate::tracker::{PointOffset, PointerItem, TrackerRead, ValuePointer};

impl CompactedTracker {
    fn item(&self, point_offset: PointOffset) -> PointerItem {
        match self.pointers.get(point_offset as usize) {
            Some(pointer) => PointerItem::from(*pointer),
            None => PointerItem::OutOfRange,
        }
    }
}

impl TrackerRead for CompactedTracker {
    fn max_point_offset(&self) -> Result<PointOffset> {
        Ok(self.pointer_count())
    }

    fn get<P: AccessPattern>(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        Ok(self.pointers.get(point_offset as usize).copied().flatten())
    }

    fn get_range<P: AccessPattern>(
        &self,
        point_offsets: Range<PointOffset>,
    ) -> Result<Vec<Option<ValuePointer>>> {
        let start = (point_offsets.start as usize).min(self.pointers.len());
        let end = (point_offsets.end as usize).min(self.pointers.len());
        let mut pointers = self.pointers[start..end].to_vec();
        pointers.resize(point_offsets.len(), None);
        Ok(pointers)
    }

    fn iter<U, I>(&self, point_offsets: I) -> Result<impl Iterator<Item = Result<(U, PointerItem)>>>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffset)>,
    {
        Ok(point_offsets.map(|(user_data, point_offset)| Ok((user_data, self.item(point_offset)))))
    }
}
