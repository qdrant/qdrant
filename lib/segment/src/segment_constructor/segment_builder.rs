use core::cmp;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::common::error_logging::LogError;
use crate::entry::entry_point::{OperationError, OperationResult, SegmentEntry};
use crate::index::PayloadIndex;
use crate::segment::Segment;
use crate::segment_constructor::{build_segment, load_segment};
use crate::types::{PayloadFieldSchema, PayloadKeyType, SegmentConfig};

/// Structure for constructing segment out of several other segments
pub struct SegmentBuilder {
    pub segment: Option<Segment>,
    pub destination_path: PathBuf,
    pub temp_path: PathBuf,
    pub indexed_fields: HashMap<PayloadKeyType, PayloadFieldSchema>,
}

impl SegmentBuilder {
    pub fn new(
        segment_path: &Path,
        temp_dir: &Path,
        segment_config: &SegmentConfig,
    ) -> OperationResult<Self> {
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
    ///
    /// # Arguments
    ///
    /// * `other` - segment to add into construction
    ///
    /// # Result
    ///
    /// * `bool` - if `true` - data successfully added, if `false` - process was interrupted
    ///
    pub fn update_from(&mut self, other: &Segment, stopped: &AtomicBool) -> OperationResult<bool> {
        match &mut self.segment {
            None => Err(OperationError::service_error(
                "Segment building error: created segment not found",
            )),
            Some(self_segment) => {
                self_segment.version = cmp::max(self_segment.version(), other.version());

                let other_id_tracker = other.id_tracker.borrow();
                let other_vector_storages: HashMap<_, _> = other
                    .vector_data
                    .iter()
                    .map(|(vector_name, vector_data)| {
                        (vector_name.to_owned(), vector_data.vector_storage.borrow())
                    })
                    .collect();
                let other_payload_index = other.payload_index.borrow();

                let mut id_tracker = self_segment.id_tracker.borrow_mut();
                let mut vector_storages: HashMap<_, _> = self_segment
                    .vector_data
                    .iter()
                    .map(|(vector_name, vector_data)| {
                        (
                            vector_name.to_owned(),
                            vector_data.vector_storage.borrow_mut(),
                        )
                    })
                    .collect();
                let mut payload_index = self_segment.payload_index.borrow_mut();

                if vector_storages.len() != other_vector_storages.len() {
                    return Err(OperationError::ServiceError {
                        description: format!("Self and other segments have different vector names count. Self count: {}, other count: {}", vector_storages.len(), other_vector_storages.len()),
                    });
                }

                let mut internal_id_iter = None;
                for (vector_name, vector_storage) in &mut vector_storages {
                    let other_vector_storage = other_vector_storages.get(vector_name);
                    if other_vector_storage.is_none() {
                        return Err(OperationError::ServiceError {
                            description: format!("Cannot update from other segment because if missing vector name {}", vector_name),
                        });
                    }
                    let other_vector_storage = other_vector_storage.unwrap();
                    let new_internal_range = vector_storage.update_from(&**other_vector_storage)?;
                    internal_id_iter =
                        Some(new_internal_range.zip(other_vector_storage.iter_ids()));
                }
                if internal_id_iter.is_none() {
                    return Err(OperationError::ServiceError {
                        description:
                            "Empty intersection between self segment names and other segment names"
                                .to_owned(),
                    });
                }

                for (new_internal_id, old_internal_id) in internal_id_iter.unwrap() {
                    if stopped.load(Ordering::Relaxed) {
                        return Err(OperationError::Cancelled {
                            description: "Cancelled by external thread".to_string(),
                        });
                    }
                    let external_id =
                        if let Some(external_id) = other_id_tracker.external_id(old_internal_id) {
                            external_id
                        } else {
                            log::warn!(
                                "Cannot find external id for internal id {}, skipping",
                                old_internal_id
                            );
                            for vector_storage in vector_storages.values_mut() {
                                vector_storage.delete(new_internal_id)?;
                            }
                            continue;
                        };
                    let other_version = other_id_tracker.internal_version(old_internal_id).unwrap();

                    match id_tracker.internal_id(external_id) {
                        None => {
                            // New point, just insert
                            id_tracker.set_link(external_id, new_internal_id)?;
                            id_tracker.set_internal_version(new_internal_id, other_version)?;
                            payload_index.assign(
                                new_internal_id,
                                &other_payload_index.payload(old_internal_id)?,
                            )?;
                        }
                        Some(existing_internal_id) => {
                            // Point exists in both: newly constructed and old segments, so we need to merge them
                            // Based on version
                            let existing_version =
                                id_tracker.internal_version(existing_internal_id).unwrap();
                            let remove_id = if existing_version < other_version {
                                // Other version is the newest, remove the existing one and replace
                                id_tracker.drop(external_id)?;
                                id_tracker.set_link(external_id, new_internal_id)?;
                                id_tracker.set_internal_version(new_internal_id, other_version)?;
                                payload_index.drop(existing_internal_id)?;
                                payload_index.assign(
                                    new_internal_id,
                                    &other_payload_index.payload(old_internal_id)?,
                                )?;
                                existing_internal_id
                            } else {
                                // Old version is still good, do not move anything else
                                // Mark newly added vector as removed
                                new_internal_id
                            };
                            for vector_storage in vector_storages.values_mut() {
                                vector_storage.delete(remove_id)?;
                            }
                        }
                    }
                }

                for (field, payload_schema) in other.payload_index.borrow().indexed_fields() {
                    self.indexed_fields.insert(field, payload_schema);
                }

                Ok(true)
            }
        }
    }

    pub fn build(mut self, stopped: &AtomicBool) -> Result<Segment, OperationError> {
        {
            let mut segment = self.segment.ok_or_else(|| {
                OperationError::service_error("Segment building error: created segment not found")
            })?;
            self.segment = None;

            for (field, payload_schema) in &self.indexed_fields {
                segment.create_field_index(segment.version(), field, Some(payload_schema))?;
                if stopped.load(Ordering::Relaxed) {
                    return Err(OperationError::Cancelled {
                        description: "Cancelled by external thread".to_string(),
                    });
                }
            }

            for vector_data in segment.vector_data.values_mut() {
                vector_data.vector_index.borrow_mut().build_index(stopped)?;
            }

            segment.flush(true)?;
            // Now segment is going to be evicted from RAM
        }

        // Move fully constructed segment into collection directory and load back to RAM
        fs::rename(&self.temp_path, &self.destination_path)
            .describe("Moving segment data after optimization")?;

        load_segment(&self.destination_path)?.ok_or_else(|| {
            OperationError::service_error(&format!(
                "Segment loading error: {}",
                self.destination_path.display()
            ))
        })
    }
}
