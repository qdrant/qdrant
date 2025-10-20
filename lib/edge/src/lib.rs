pub mod retrieve;
pub mod scroll;
pub mod search;
pub mod update;

use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::save_on_disk::SaveOnDisk;
use fs_err as fs;
use parking_lot::Mutex;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::entry::SegmentEntry;
use segment::segment_constructor::load_segment;
use segment::types::SegmentConfig;
use shard::operations::CollectionUpdateOperations;
use shard::segment_holder::{LockedSegmentHolder, SegmentHolder};
use shard::wal::SerdeWal;
use wal::WalOptions;

#[derive(Debug)]
pub struct Shard {
    _path: PathBuf,
    config: SegmentConfig,
    wal: Mutex<SerdeWal<CollectionUpdateOperations>>,
    segments: LockedSegmentHolder,
}

const WAL_PATH: &str = "wal";
const SEGMENTS_PATH: &str = "segments";

impl Shard {
    pub fn load(path: &Path, mut config: Option<SegmentConfig>) -> OperationResult<Self> {
        let wal_path = path.join(WAL_PATH);

        if !wal_path.exists() {
            fs::create_dir(&wal_path).map_err(|err| {
                OperationError::service_error(format!("failed to create WAL directory: {err}"))
            })?;
        }

        let wal: SerdeWal<CollectionUpdateOperations> =
            SerdeWal::new(&wal_path, default_wal_options()).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open WAL {}: {err}",
                    wal_path.display(),
                ))
            })?;

        let segments_path = path.join(SEGMENTS_PATH);

        if !segments_path.exists() {
            fs::create_dir(&segments_path).map_err(|err| {
                OperationError::service_error(format!("failed to create segments directory: {err}"))
            })?;
        }

        let segments_dir = fs::read_dir(&segments_path).map_err(|err| {
            OperationError::service_error(format!("failed to read segments directory: {err}"))
        })?;

        let mut segments = SegmentHolder::default();

        for entry in segments_dir {
            let entry = entry.map_err(|err| {
                OperationError::service_error(format!(
                    "failed to read entry in segments directory: {err}",
                ))
            })?;

            let segment_path = entry.path();

            if !segment_path.is_dir() {
                log::warn!(
                    "Skipping non-directory segment entry {}",
                    segment_path.display(),
                );

                continue;
            }

            if let Some(name) = segment_path.file_name()
                && let Some(name) = name.to_str()
                && name.starts_with(".")
            {
                log::warn!(
                    "Skipping hidden segment directory {}",
                    segment_path.display(),
                );
                continue;
            }

            let segment = load_segment(&segment_path, &AtomicBool::new(false)).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to load segment {}: {err}",
                    segment_path.display(),
                ))
            })?;

            let Some(mut segment) = segment else {
                fs::remove_dir_all(&segment_path).map_err(|err| {
                    OperationError::service_error(format!(
                        "failed to remove leftover segment: {err}",
                    ))
                })?;

                continue;
            };

            if let Some(config) = &config {
                if !config.is_compatible(segment.config()) {
                    return Err(OperationError::service_error(format!(
                        "segment {} is incompatible with provided config or previously loaded segments: \
                         expected {:?}, but received {:?}",
                        segment_path.display(),
                        config,
                        segment.config(),
                    )));
                }
            } else {
                config = Some(segment.config().clone());
            }

            segment.check_consistency_and_repair().map_err(|err| {
                OperationError::service_error(format!(
                    "failed to repair segment {}: {err}",
                    segment_path.display(),
                ))
            })?;

            segments.add_new(segment);
        }

        if !segments.has_appendable_segment() {
            let Some(config) = &config else {
                return Err(OperationError::service_error(
                    "segment config is not provided and no segments were loaded",
                ));
            };

            let payload_index_schema_path = path.join("payload_index.json");
            let payload_index_schema = SaveOnDisk::load_or_init_default(&payload_index_schema_path)
                .map_err(|err| {
                    OperationError::service_error(format!(
                        "failed to initialize temporary payload index schema file {}: {err}",
                        payload_index_schema_path.display(),
                    ))
                })?;

            segments.create_appendable_segment(
                &segments_path,
                config.clone(),
                Arc::new(payload_index_schema),
            )?;

            debug_assert!(segments.has_appendable_segment());
        }

        let shard = Self {
            _path: path.into(),
            config: config.expect("config was provided or at least one segment was loaded"),
            wal: parking_lot::Mutex::new(wal),
            segments: Arc::new(parking_lot::RwLock::new(segments)),
        };

        Ok(shard)
    }

    pub fn config(&self) -> &SegmentConfig {
        &self.config
    }
}

fn default_wal_options() -> WalOptions {
    WalOptions {
        segment_capacity: 32 * 1024 * 1024,
        segment_queue_len: 0,
        retain_closed: NonZero::new(1).unwrap(),
    }
}
