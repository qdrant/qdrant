use std::io::{self, Read as _};
use std::path::{self, Path};

use segment::common::validate_snapshot_archive::open_snapshot_archive;
use segment::segment::SEGMENT_STATE_FILE;
use segment::types::{SegmentConfig, SegmentState};

use crate::operations::types::{CollectionError, CollectionResult};

pub fn extract_segment_config(snapshot_path: &Path) -> CollectionResult<SegmentConfig> {
    let mut ar = open_snapshot_archive(snapshot_path).map_err(|err| {
        CollectionError::service_error(format!("failed to open snapshot archive: {err}"))
    })?;

    let entries = ar.entries_with_seek().map_err(|err| {
        CollectionError::service_error(format!("failed to read snapshot archive: {err}"))
    })?;

    let segment_config = extract_segment_config_impl(entries, ArchiveType::Shard)?;
    Ok(segment_config)
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ArchiveType {
    Shard,
    Segment,
}

fn extract_segment_config_impl<R>(
    entries: tar::Entries<'_, R>,
    archive_type: ArchiveType,
) -> CollectionResult<SegmentConfig>
where
    R: io::Read,
{
    for entry in entries {
        let mut entry = entry.map_err(|err| {
            CollectionError::service_error(format!("failed to read next entry: {err}"))
        })?;

        if !entry.header().entry_type().is_file() {
            continue;
        }

        let entry_path = entry
            .path()
            .map_err(|err| {
                CollectionError::service_error(format!("failed to read next entry path: {err}"))
            })?
            .into_owned();

        let Some(entry_path_components) = path_components(&entry_path, 4) else {
            continue;
        };

        match (archive_type, entry_path_components.as_slice()) {
            // Partial or streaming shard snapshot
            (ArchiveType::Shard, ["segments", _segment, "files", SEGMENT_STATE_FILE]) => {
                let segment_state = read_segment_state(&mut entry, &entry_path)?;
                return Ok(segment_state.config);
            }

            // Regular shard snapshot
            (ArchiveType::Shard, ["segments", segment]) if segment.ends_with(".tar") => {
                let mut ar = tar::Archive::new(&mut entry as &mut dyn io::Read);
                let entries = ar.entries().map_err(CollectionError::from)?;

                let segment_config = extract_segment_config_impl(entries, ArchiveType::Segment).map_err(|err| {
                    CollectionError::service_error(format!(
                        "failed to extract segment config from inner segment snapshot archive {}: {err}",
                        entry_path.display(),
                    ))
                })?;

                return Ok(segment_config);
            }

            // Segment snapshot
            (ArchiveType::Segment, ["snapshot", "files", SEGMENT_STATE_FILE]) => {
                let segment_state = read_segment_state(&mut entry, &entry_path)?;
                return Ok(segment_state.config);
            }

            _ => (),
        }
    }

    Err(CollectionError::not_found("segment config not found"))
}

fn path_components(path: &Path, max_length: usize) -> Option<Vec<&str>> {
    let mut components = Vec::with_capacity(max_length);

    for component in path.components() {
        if components.len() >= max_length {
            return None;
        }

        if component == path::Component::RootDir {
            continue;
        }

        // We expect all paths inside snapshot archive to be valid UTF-8
        let component = component.as_os_str().to_str()?;
        components.push(component);
    }

    Some(components)
}

fn read_segment_state(
    entry: &mut tar::Entry<'_, impl io::Read>,
    path: &Path,
) -> CollectionResult<SegmentState> {
    let mut segment_json = Vec::with_capacity(entry.size() as _);

    entry.read_to_end(&mut segment_json).map_err(|err| {
        CollectionError::service_error(format!(
            "failed to read segment state file {}: {err}",
            path.display(),
        ))
    })?;

    let segment_state = serde_json::from_slice(&segment_json).map_err(|err| {
        CollectionError::service_error(format!(
            "failed to deserialize segment state file {}: {err}",
            path.display(),
        ))
    })?;

    Ok(segment_state)
}
