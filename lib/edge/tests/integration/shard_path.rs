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
