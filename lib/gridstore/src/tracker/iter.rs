use std::mem::{align_of, size_of};

use ahash::AHashMap;
use common::generic_consts::Random;
use common::universal_io::{ReadPipeline, UniversalRead, UserData};

use crate::Result;
use crate::tracker::{OptionalPointer, PointOffset, PointerUpdates, TrackerHeader, ValuePointer};

pub struct Iter<'a, U, I, S>
where
    U: UserData,
    I: Iterator<Item = (U, PointOffset)>,
    S: UniversalRead,
{
    point_offsets: I,
    storage: &'a S,
    storage_len: u64,
    pending_updates: &'a AHashMap<PointOffset, PointerUpdates>,
    pipeline: S::ReadPipeline<'a, U>,
}

impl<'a, U, I, S> Iter<'a, U, I, S>
where
    U: UserData,
    I: Iterator<Item = (U, PointOffset)>,
    S: UniversalRead,
{
    pub fn new(
        point_offsets: I,
        storage: &'a S,
        pending_updates: &'a AHashMap<PointOffset, PointerUpdates>,
    ) -> Result<Self> {
        let iter = Self {
            point_offsets,
            storage,
            storage_len: storage.len::<u8>()?,
            pending_updates,
            pipeline: S::ReadPipeline::new()?,
        };

        Ok(iter)
    }
}

impl<'a, U, I, S> Iterator for Iter<'a, U, I, S>
where
    U: UserData,
    I: Iterator<Item = (U, PointOffset)>,
    S: UniversalRead,
{
    type Item = Result<(U, PointerItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pipeline.can_schedule()
            && let Some(point_offset) = self.point_offsets.next()
        {
            let (user_data, point_offset) = point_offset;

            if let Some(pending) = self.pending_updates.get(&point_offset) {
                debug_assert!(!pending.is_empty(), "pending updates must not be empty");

                // Apply pending updates before reading from storage.
                //
                // A pending `None` is a pending *unset*, so falling back to disk
                // would revive a stale pointer.
                //
                // See `get`, which returns `pending.current` directly.
                let item = match pending.current {
                    Some(pointer) => PointerItem::Valid(pointer),
                    None => PointerItem::Empty,
                };

                return Some(Ok((user_data, item)));
            }

            let header_size = size_of::<TrackerHeader>();
            let item_size = size_of::<OptionalPointer>();

            let start = header_size + item_size * point_offset as usize;
            let end = start + item_size;

            if self.storage_len < end as u64 {
                let item = PointerItem::OutOfRange;
                return Some(Ok((user_data, item)));
            }

            let range = start as u64..end as u64;
            let result = self.pipeline.schedule::<Random>(
                user_data,
                self.storage,
                range,
                align_of::<OptionalPointer>(),
            );

            if let Err(err) = result {
                return Some(Err(err.into()));
            }
        }

        let result = self.pipeline.wait_bytemuck::<OptionalPointer>();

        let (user_data, pointer) = match result {
            Ok(pointer) => pointer?,
            Err(err) => return Some(Err(err.into())),
        };

        let &[pointer] = pointer.as_ref() else {
            unreachable!();
        };

        let pointer = match pointer.to_option() {
            Some(pointer) => PointerItem::Valid(pointer),
            None => PointerItem::Empty,
        };

        Some(Ok((user_data, pointer)))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PointerItem {
    Valid(ValuePointer),
    Empty,
    OutOfRange,
}
