use serde::{Deserialize, Serialize};

pub type EntryId = u64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct EntryApplyProgressQueue(Option<(EntryId, EntryId)>);

impl EntryApplyProgressQueue {
    pub fn new(first: EntryId, last: EntryId) -> Self {
        Self(Some((first, last)))
    }

    /// Return oldest un-applied entry id if any
    pub fn current(&self) -> Option<EntryId> {
        match self.0 {
            Some((current_index, last_index)) => {
                if current_index > last_index {
                    None
                } else {
                    Some(current_index)
                }
            }
            None => None,
        }
    }

    pub fn applied(&mut self) {
        match &mut self.0 {
            Some((current_index, _)) => {
                *current_index += 1;
            }
            None => (),
        }
    }

    pub fn get_last_applied(&self) -> Option<EntryId> {
        match &self.0 {
            Some((0, _)) => None,
            Some((current, _)) => Some(current - 1),
            None => None,
        }
    }

    pub fn set(&mut self, first_index: EntryId, last_index: EntryId) {
        let first_index = match self.0 {
            Some((first_index, _)) => first_index,
            None => first_index,
        };

        self.0 = Some((first_index, last_index));
    }

    pub fn set_from_snapshot(&mut self, snapshot_at_commit: EntryId) {
        self.0 = Some((snapshot_at_commit + 1, snapshot_at_commit))
    }

    pub fn len(&self) -> usize {
        match self.0 {
            None => 0,
            Some((current, last)) => (last as isize - current as isize + 1) as usize,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
