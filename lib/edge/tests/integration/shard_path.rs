use edge::{Distance, EdgeConfig, EdgeShard, EdgeVectorParams};

fn config() -> EdgeConfig {
    EdgeConfig::builder()
        .vector("v", EdgeVectorParams::builder(4, Distance::Dot).build())
        .build()
}

#[test]
fn create_makes_missing_directories() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested").join("shard");

    let shard = EdgeShard::new(&path, config()).expect("create into a missing directory");
    drop(shard);

    assert!(path.is_dir());
    EdgeShard::load(&path, None).expect("load the created shard");
}

#[test]
fn load_of_missing_directory_names_the_directory() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing");

    let err = EdgeShard::load(&path, Some(config()))
        .unwrap_err()
        .to_string();

    // Before the fix this failed while opening the WAL, naming `missing/wal`
    assert!(!err.contains("WAL"), "{err}");
    assert!(err.contains("does not exist"), "{err}");
    assert!(err.contains(&path.display().to_string()), "{err}");
    assert!(!path.exists(), "load must not create the directory");
}

/// `load(path, None)` of a directory that holds no shard is how a host asks "is there a shard
/// here?", so the answer must leave the directory as it was: no `segments/`, no `wal/`.
#[test]
fn load_without_config_of_a_directory_without_a_shard_leaves_it_untouched() {
    let dir = tempfile::tempdir().unwrap();
    fs_err::write(dir.path().join("notes.txt"), "not a shard").unwrap();

    let err = EdgeShard::load(dir.path(), None).unwrap_err().to_string();

    assert!(err.contains("no segments were loaded"), "{err}");
    let mut entries: Vec<_> = fs_err::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    entries.sort();
    assert_eq!(entries, ["notes.txt"], "load(None) changed the directory");
}
