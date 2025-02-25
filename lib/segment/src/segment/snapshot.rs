use std::io::{Seek, Write};
use std::path::Path;
use std::{fs, thread};

use common::tar_ext;
use io::storage_version::VERSION_FILE;
use uuid::Uuid;

use crate::common::operation_error::OperationResult;
use crate::index::{PayloadIndex, VectorIndex};
use crate::payload_storage::PayloadStorage;
use crate::segment::{
    DB_BACKUP_PATH, PAYLOAD_DB_BACKUP_PATH, SEGMENT_STATE_FILE, SNAPSHOT_FILES_PATH, Segment,
};
use crate::utils::path::strip_prefix;
use crate::vector_storage::VectorStorage;

pub const ROCKS_DB_VIRT_FILE: &str = "::ROCKS_DB";
pub const PAYLOAD_INDEX_ROCKS_DB_VIRT_FILE: &str = "::PAYLOAD_INDEX_ROCKS_DB";

pub fn snapshot_files(
    segment: &Segment,
    temp_path: &Path,
    tar: &tar_ext::BuilderExt<impl Write + Seek>,
    include_if: impl Fn(&Path) -> bool,
) -> OperationResult<()> {
    // use temp_path for intermediary files
    let temp_path = temp_path.join(format!("segment-{}", Uuid::new_v4()));

    // TODO: Version RocksDB!? 🤯

    if include_if(ROCKS_DB_VIRT_FILE.as_ref()) {
        let db_backup_path = temp_path.join(DB_BACKUP_PATH);

        let db = segment.database.read();
        crate::rocksdb_backup::create(&db, &db_backup_path)?;
    }

    if include_if(PAYLOAD_INDEX_ROCKS_DB_VIRT_FILE.as_ref()) {
        let payload_index_db_backup_path = temp_path.join(PAYLOAD_DB_BACKUP_PATH);

        segment
            .payload_index
            .borrow()
            .take_database_snapshot(&payload_index_db_backup_path)?;
    }

    tar.blocking_append_dir_all(&temp_path, Path::new(""))?;

    // remove tmp directory in background
    let _ = thread::spawn(move || {
        let res = fs::remove_dir_all(&temp_path);
        if let Err(err) = res {
            log::error!(
                "Failed to remove tmp directory at {}: {err:?}",
                temp_path.display(),
            );
        }
    });

    let tar = tar.descend(Path::new(SNAPSHOT_FILES_PATH))?;

    for vector_data in segment.vector_data.values() {
        for file in vector_data.vector_index.borrow().files() {
            let stripped_path = strip_prefix(&file, &segment.current_path)?;

            if include_if(stripped_path) {
                tar.blocking_append_file(&file, stripped_path)?;
            }
        }

        for file in vector_data.vector_storage.borrow().files() {
            let stripped_path = strip_prefix(&file, &segment.current_path)?;

            if include_if(stripped_path) {
                tar.blocking_append_file(&file, stripped_path)?;
            }
        }

        if let Some(quantized_vectors) = vector_data.quantized_vectors.borrow().as_ref() {
            for file in quantized_vectors.files() {
                let stripped_path = strip_prefix(&file, &segment.current_path)?;

                if include_if(stripped_path) {
                    tar.blocking_append_file(&file, stripped_path)?;
                }
            }
        }
    }

    for file in segment.payload_index.borrow().files() {
        let stripped_path = strip_prefix(&file, &segment.current_path)?;

        if include_if(stripped_path) {
            tar.blocking_append_file(&file, stripped_path)?;
        }
    }

    for file in segment.payload_storage.borrow().files() {
        let stripped_path = strip_prefix(&file, &segment.current_path)?;

        if include_if(stripped_path) {
            tar.blocking_append_file(&file, stripped_path)?;
        }
    }

    for file in segment.id_tracker.borrow().files() {
        let stripped_path = strip_prefix(&file, &segment.current_path)?;

        if include_if(stripped_path) {
            tar.blocking_append_file(&file, stripped_path)?;
        }
    }

    tar.blocking_append_file(
        &segment.current_path.join(SEGMENT_STATE_FILE),
        Path::new(SEGMENT_STATE_FILE),
    )?;

    tar.blocking_append_file(
        &segment.current_path.join(VERSION_FILE),
        Path::new(VERSION_FILE),
    )?;

    Ok(())
}
