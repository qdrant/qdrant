use std::env;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let args: Vec<_> = env::args().skip(1).take(2).collect();

    let [edge_shard_path] = args
        .try_into()
        .map_err(|args| anyhow::format_err!("unexpected arguments {args:?}"))?;

    let _edge_shard = qdrant_edge::EdgeShard::load(Path::new(&edge_shard_path), None)?;
    Ok(())
}
