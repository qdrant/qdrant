use std::error::Error;

use qdrant_edge::{Distance, EdgeConfig, EdgeShard, EdgeVectorParams};

fn main() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;

    let config = EdgeConfig::builder()
        .vector("v", EdgeVectorParams::builder(4, Distance::Dot).build())
        .build();

    // The first shard initializes default feature flags and checks multi-mmap support.
    let shard = EdgeShard::new(storage.path(), config)?;
    drop(shard);

    let shard = EdgeShard::load(storage.path(), None)?;
    assert_eq!(shard.info()?.points_count, 0);
    println!("Created and reloaded a shard with automatic process-wide initialization");
    drop(shard);
    Ok(())
}
