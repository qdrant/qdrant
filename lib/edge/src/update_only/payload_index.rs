//! Readers load an index from `payload_index/config.json`, so it is written last on create and
//! first on delete; a segment already in the requested state is skipped, so a call resumes.

use std::path::{Path, PathBuf};

use common::ambient;
use common::reason::Reason;
use common::universal_io::{OkNotFound as _, UniversalAppendFs, UniversalWriteFsAsync};
use segment::common::operation_error::OperationResult;
use segment::data_types::update_metering::indexed_value_bytes;
use segment::index::payload_config::{
    FullPayloadIndexType, PayloadConfig, PayloadFieldSchemaWithIndexType,
};
use segment::index::struct_payload_index::{StorageType, build_detached_field_indexes};
use segment::json_path::JsonPath;
use segment::segment::update_only::UpdateOnlySegmentEnum;
use segment::segment_constructor::get_payload_index_path;
use segment::types::PayloadFieldSchema;
use uuid::Uuid;

use super::UpdateOnlyEdgeShard;
use super::lifecycle::copy_dir;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PayloadIndexOutcome {
    pub changed: usize,
    /// Segments left unvisited because `should_yield` asked to stop.
    pub remaining: usize,
    pub indexed_bytes: u64,
}

struct SegmentRef {
    uuid: Uuid,
    path: PathBuf,
    appendable: bool,
}

impl<Fs> UpdateOnlyEdgeShard<Fs>
where
    Fs: UniversalAppendFs + UniversalWriteFsAsync,
{
    /// Replaces an index of another schema. `temp_path` is local scratch for the build; an error
    /// loses the writer.
    pub fn create_payload_index(
        mut self,
        field: &JsonPath,
        schema: &PayloadFieldSchema,
        temp_path: &Path,
        should_yield: impl Fn() -> bool,
    ) -> OperationResult<(Self, PayloadIndexOutcome)> {
        let _scope = ambient::unmeasured_guard(Reason::EDGE_UNMEASURED);
        fs_err::create_dir_all(temp_path)?;
        let mut outcome = PayloadIndexOutcome::default();
        let segments = self.segment_refs();
        for (visited, segment) in segments.iter().enumerate() {
            if should_yield() {
                outcome.remaining = segments.len() - visited;
                break;
            }
            let config_path = config_path(&segment.path);
            let mut config = self.load_config(&config_path)?;
            if config
                .indices
                .get(field)
                .is_some_and(|indexed| indexed.schema == *schema && !indexed.types.is_empty())
            {
                continue;
            }

            self.close_writer(segment);
            if config.indices.remove(field).is_some() {
                self.save_config(&config_path, &config)?;
            }
            self.remove_field_files(&segment.path, field)?;
            let (types, bytes) = self.build_and_upload(segment, field, schema, temp_path)?;
            config.indices.insert(
                field.clone(),
                PayloadFieldSchemaWithIndexType::new(schema.clone(), types),
            );
            self.save_config(&config_path, &config)?;
            self.reopen_writer(segment)?;

            outcome.changed += 1;
            outcome.indexed_bytes = outcome.indexed_bytes.saturating_add(bytes);
        }
        Ok((self, outcome))
    }

    /// Also removes files a crashed call left behind; an error loses the writer.
    pub fn delete_payload_index(
        mut self,
        field: &JsonPath,
        should_yield: impl Fn() -> bool,
    ) -> OperationResult<(Self, PayloadIndexOutcome)> {
        let _scope = ambient::unmeasured_guard(Reason::EDGE_UNMEASURED);
        let mut outcome = PayloadIndexOutcome::default();
        let segments = self.segment_refs();
        for (visited, segment) in segments.iter().enumerate() {
            if should_yield() {
                outcome.remaining = segments.len() - visited;
                break;
            }
            let config_path = config_path(&segment.path);
            let mut config = self.load_config(&config_path)?;
            let indexed = config.indices.remove(field).is_some();
            let files = self.field_files(&segment.path, field)?;
            if !indexed && files.is_empty() {
                continue;
            }

            self.close_writer(segment);
            if indexed {
                self.save_config(&config_path, &config)?;
            }
            for file in files {
                self.fs.remove(&file)?;
            }
            self.reopen_writer(segment)?;
            outcome.changed += 1;
        }
        Ok((self, outcome))
    }

    fn segment_refs(&self) -> Vec<SegmentRef> {
        let mut segments: Vec<SegmentRef> = self
            .segments
            .read()
            .iter()
            .map(|(uuid, segment)| {
                let segment = segment.read();
                SegmentRef {
                    uuid,
                    path: segment.segment_path.clone(),
                    appendable: segment.appendable,
                }
            })
            .collect();
        segments.sort_by_key(|segment| segment.uuid);
        segments
    }

    fn load_config(&self, path: &Path) -> OperationResult<PayloadConfig> {
        Ok(PayloadConfig::load_universal(&self.fs, path)?.unwrap_or_default())
    }

    fn save_config(&self, path: &Path, config: &PayloadConfig) -> OperationResult<()> {
        let bytes = serde_json::to_vec(config)?;
        Ok(self.fs.atomic_save(path, &bytes)?)
    }

    fn field_files(&self, segment_path: &Path, field: &JsonPath) -> OperationResult<Vec<PathBuf>> {
        let prefix = get_payload_index_path(segment_path).join(format!("{}-", field.filename()));
        let listed = self.fs.list_files(&prefix).ok_not_found()?;
        Ok(listed
            .unwrap_or_default()
            .into_iter()
            .map(|file| file.path)
            .collect())
    }

    /// Leftovers of an earlier build would be read as part of a new one.
    fn remove_field_files(&self, segment_path: &Path, field: &JsonPath) -> OperationResult<()> {
        for file in self.field_files(segment_path, field)? {
            self.fs.remove(&file)?;
        }
        Ok(())
    }

    fn build_and_upload(
        &self,
        segment: &SegmentRef,
        field: &JsonPath,
        schema: &PayloadFieldSchema,
        temp_path: &Path,
    ) -> OperationResult<(Vec<FullPayloadIndexType>, u64)> {
        let scratch = tempfile::Builder::new()
            .prefix("payload-index-")
            .tempdir_in(temp_path)?;
        let mut bytes = 0u64;
        let types = {
            let segments = self.segments.read();
            let lookup = segments.get(segment.uuid)?.read();
            let payload_storage = lookup.payload_storage.borrow();
            let id_tracker = lookup.id_tracker.borrow();
            build_detached_field_indexes(
                scratch.path(),
                StorageType::from_appendable(segment.appendable),
                field,
                schema,
                &*payload_storage,
                &*id_tracker,
                |payload| {
                    bytes = bytes.saturating_add(indexed_value_bytes(payload, field)?);
                    Ok(())
                },
            )?
        };
        let remote = get_payload_index_path(&segment.path);
        futures::executor::block_on(copy_dir(&self.fs, scratch.path(), &remote))?;
        Ok((types, bytes))
    }

    /// An appendable writer keeps its field indexes open after its first store.
    fn close_writer(&mut self, segment: &SegmentRef) {
        if segment.appendable {
            self.writers.remove(&segment.uuid);
        }
    }

    fn reopen_writer(&mut self, segment: &SegmentRef) -> OperationResult<()> {
        if !segment.appendable {
            return Ok(());
        }
        let writer = {
            let segments = self.segments.read();
            let lookup = segments.get(segment.uuid)?.read();
            UpdateOnlySegmentEnum::open(
                self.fs.clone(),
                &lookup.segment_path,
                &lookup.segment_config,
                lookup.writer_state(),
            )?
        };
        self.writers.insert(segment.uuid, writer);
        Ok(())
    }
}

fn config_path(segment_path: &Path) -> PathBuf {
    PayloadConfig::get_config_path(&get_payload_index_path(segment_path))
}
