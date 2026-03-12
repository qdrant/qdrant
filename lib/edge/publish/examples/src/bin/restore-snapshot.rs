// See lib/edge/python/examples/restore-snapshot.py for the equivalent Python example.

use std::error::Error;
use std::io::Write;
use std::path::{Path, PathBuf};

use examples::DATA_DIR;
use qdrant_edge::external::serde_json;
use qdrant_edge::{EdgeShard, PointId, WithPayloadInterface, WithVector};

const SNAPSHOT_URL: &str =
    "https://storage.googleapis.com/qdrant-benchmark-snapshots/test-shard.snapshot";

// Obtained via download_partial.py and manually inserting a new point with ID 100500
const PARTIAL_SNAPSHOT_URL: &str =
    "https://storage.googleapis.com/qdrant-benchmark-snapshots/partial.snapshot";

fn main() -> Result<(), Box<dyn Error>> {
    let snapshot_path = download_snapshot(SNAPSHOT_URL, DATA_DIR)?;
    println!("Snapshot downloaded to: {}", snapshot_path.display());

    let recovered_path = Path::new(DATA_DIR).join("restored_shard");
    println!(
        "Restoring shard from snapshot to: {}",
        recovered_path.display()
    );
    if recovered_path.exists() {
        println!("Removing existing recovered shard directory...");
        fs_err::remove_dir_all(&recovered_path)?;
    }

    EdgeShard::unpack_snapshot(&snapshot_path, &recovered_path)?;

    let shard = EdgeShard::load(&recovered_path, None)?;

    let points = shard.retrieve(
        &[PointId::NumId(1), PointId::NumId(2), PointId::NumId(3)],
        Some(WithPayloadInterface::Bool(true)),
        Some(WithVector::Bool(false)),
    )?;

    for point in &points {
        println!("{point:?}");
    }

    let manifest = shard.snapshot_manifest()?;
    println!(
        "Manifest of restored shard: {}",
        serde_json::to_string_pretty(&manifest)?
    );

    let partial_snapshot_path = download_snapshot(PARTIAL_SNAPSHOT_URL, DATA_DIR)?;

    update_from_snapshot(shard, &recovered_path, &partial_snapshot_path)?;

    let shard = EdgeShard::load(&recovered_path, None)?;

    let points = shard.retrieve(
        &[PointId::NumId(100500)],
        Some(WithPayloadInterface::Bool(true)),
        Some(WithVector::Bool(false)),
    )?;

    for point in &points {
        println!("{point:?}");
    }

    let info = shard.info();

    println!("{info:?}");

    Ok(())
}

// Download the snapshot file into data directory
fn download_snapshot(url: &str, dest_folder: &str) -> Result<PathBuf, Box<dyn Error>> {
    fs_err::create_dir_all(dest_folder)?;

    let filename = url.split('/').next_back().unwrap();
    let local_filename = Path::new(dest_folder).join(filename);
    if local_filename.exists() {
        println!("Snapshot already exists at: {}", local_filename.display());
        return Ok(local_filename);
    }

    let response = ureq::get(url).call()?;
    let mut file = fs_err::File::create(&local_filename)?;
    let mut reader = response.into_body().into_reader();
    std::io::copy(&mut reader, &mut file)?;
    file.flush()?;

    Ok(local_filename)
}

// NOTE: This function is copy-pasted from PyEdgeShard::update_from_snapshot
// because it's not available in EdgeShard yet.
//
// Later we either port it to EdgeShard; or, more likely, we'll replace it with
// different mechanism and remove it from all examples.
fn update_from_snapshot(
    shard: EdgeShard,
    shard_path: &Path,
    snapshot_path: &Path,
) -> Result<(), Box<dyn Error>> {
    use qdrant_edge::internal::{SnapshotManifest, clear_data, move_data};

    let tmp_dir = snapshot_path.parent().unwrap_or(Path::new("."));

    // A place where we can temporarily unpack the snapshot
    let unpack_dir = tempfile::Builder::new().tempdir_in(tmp_dir)?;
    EdgeShard::unpack_snapshot(snapshot_path, unpack_dir.path())?;

    let snapshot_manifest = SnapshotManifest::load_from_snapshot(unpack_dir.path(), None)?;

    // Assume full snapshot recovery in case of empty manifest
    let full_recovery = snapshot_manifest.is_empty();

    if full_recovery {
        drop(shard);
        clear_data(shard_path)?;
        move_data(unpack_dir.path(), shard_path)?;
        return Ok(());
    }

    let current_manifest = shard.snapshot_manifest()?;

    drop(shard);

    EdgeShard::recover_partial_snapshot(
        shard_path,
        &current_manifest,
        unpack_dir.path(),
        &snapshot_manifest,
    )?;

    Ok(())
}
