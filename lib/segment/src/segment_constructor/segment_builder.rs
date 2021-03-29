use crate::segment::Segment;
use crate::entry::entry_point::{OperationResult, SegmentEntry, OperationError};
use core::cmp;
use crate::types::PayloadKeyType;
use std::collections::HashSet;
use std::convert::TryInto;

/// Structure for constructing segment out of several other segments
pub struct SegmentBuilder {
    pub segment: Segment,
    pub indexed_fields: HashSet<PayloadKeyType>
}

impl SegmentBuilder {

    pub fn new(segment: Segment) -> Self {
        SegmentBuilder {
            segment,
            indexed_fields: Default::default()
        }
    }

    /// Update current segment builder with all (not deleted) vectors and payload form `other` segment
    /// Perform index building at the end of update
    pub fn update_from(&mut self, other: &Segment) -> OperationResult<()> {
        self.segment.version = cmp::max(self.segment.version, other.version());

        let other_id_mapper = other.id_mapper.borrow();
        let other_vector_storage = other.vector_storage.borrow();
        let other_payload_storage = other.payload_storage.borrow();

        let new_internal_range = self.segment.vector_storage.borrow_mut().update_from(&*other_vector_storage)?;

        let mut id_mapper = self.segment.id_mapper.borrow_mut();
        let mut payload_storage = self.segment.payload_storage.borrow_mut();

        for (new_internal_id, old_internal_id) in new_internal_range.zip(other.vector_storage.borrow().iter_ids()) {
            let other_external_id = other_id_mapper.external_id(old_internal_id).unwrap();
            id_mapper.set_link(other_external_id, new_internal_id)?;
            payload_storage.assign_all(new_internal_id, other_payload_storage.payload(old_internal_id))?;
        }

        for field in other.payload_index.borrow().indexed_fields().into_iter() {
            self.indexed_fields.insert(field);
        }

        Ok(())
    }
}

impl TryInto<Segment> for SegmentBuilder {
    type Error = OperationError;

    fn try_into(mut self) -> Result<Segment, Self::Error> {
        for field in self.indexed_fields.iter() {
            self.segment.create_field_index(self.segment.version, field)?;
        }
        self.segment.query_planner.borrow_mut().build_index()?;
        Ok(self.segment)
    }
}