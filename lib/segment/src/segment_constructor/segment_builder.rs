use crate::segment::Segment;
use crate::entry::entry_point::{OperationResult, SegmentEntry, OperationError};
use core::cmp;
use crate::types::{PayloadKeyType, SegmentConfig};
use std::collections::HashSet;
use std::convert::TryInto;
use crate::segment_constructor::segment_constructor::{build_segment, load_segment};
use std::path::{Path, PathBuf};
use std::fs;
use crate::common::error_logging::LogError;

/// Structure for constructing segment out of several other segments
pub struct SegmentBuilder {
    pub segment: Option<Segment>,
    pub destination_path: PathBuf,
    pub temp_path: PathBuf,
    pub indexed_fields: HashSet<PayloadKeyType>,
}

impl SegmentBuilder {
    pub fn new(segment_path: &Path, temp_dir: &Path, segment_config: &SegmentConfig) -> OperationResult<Self> {
        let segment = build_segment(temp_dir, segment_config)?;
        let temp_path = segment.current_path.clone();

        let destination_path = segment_path.join(temp_path.file_name().unwrap());

        Ok(SegmentBuilder {
            segment: Some(segment),
            destination_path,
            temp_path,
            indexed_fields: Default::default(),
        })
    }

    /// Update current segment builder with all (not deleted) vectors and payload form `other` segment
    /// Perform index building at the end of update
    pub fn update_from(&mut self, other: &Segment) -> OperationResult<()> {
        match &mut self.segment {
            None => Err(OperationError::ServiceError {
                description: "Segment building error: created segment not found".to_owned()
            }),
            Some(self_segment) => {
                self_segment.version = cmp::max(self_segment.version, other.version());

                let other_id_mapper = other.id_mapper.borrow();
                let other_vector_storage = other.vector_storage.borrow();
                let other_payload_storage = other.payload_storage.borrow();

                let new_internal_range = self_segment.vector_storage.borrow_mut().update_from(&*other_vector_storage)?;

                let mut id_mapper = self_segment.id_mapper.borrow_mut();
                let mut payload_storage = self_segment.payload_storage.borrow_mut();

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
    }
}

impl TryInto<Segment> for SegmentBuilder {
    type Error = OperationError;

    fn try_into(mut self) -> Result<Segment, Self::Error> {
        {
            let mut segment = self.segment.ok_or(OperationError::ServiceError {
                description: "Segment building error: created segment not found".to_owned()
            })?;
            self.segment = None;

            for field in self.indexed_fields.iter() {
                segment.create_field_index(segment.version, field)?;
            }

            segment.query_planner.borrow_mut().build_index()?;

            segment.flush()?;
            // Now segment is going to be evicted from RAM
        }

        // Move fully constructed segment into collection directory and load back to RAM
        fs::rename(&self.temp_path, &self.destination_path).describe("Moving segment data after optimization")?;

        load_segment(&self.destination_path)
    }
}