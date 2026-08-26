use std::collections::HashMap;

use anyhow::{Context, Result};
use edge::{EdgeShard, PointId, PointInsertOperations, PointOperations, UpdateOperation};
use rand::SeedableRng as _;
use rand::rngs::StdRng;

use crate::args::UpsertArgs;
use crate::points::{Schema, random_point};

pub fn run(args: UpsertArgs) -> Result<()> {
    let shard = EdgeShard::load(&args.path, None)
        .with_context(|| format!("failed to open collection at {}", args.path.display()))?;

    let info = shard.info().context("failed to read collection info")?;
    let start_id = args.start_id.unwrap_or(info.points_count as u64);

    let config = shard.config();
    let dense: HashMap<String, usize> = config
        .vectors
        .iter()
        .map(|(name, params)| (name.clone(), params.size))
        .collect();
    let sparse = config.sparse_vectors.keys().cloned();
    let schema = Schema::new(dense, sparse, &info.payload_schema);
    drop(config);

    if schema.dense.is_empty() && schema.sparse.is_empty() {
        anyhow::bail!(
            "collection at {} has no vectors configured",
            args.path.display()
        );
    }
    log::info!(
        "upserting {} point(s) (ids {start_id}..{}), {} dense vector(s), {} sparse vector(s), \
         {} payload field(s)",
        args.num,
        start_id + args.num as u64,
        schema.dense.len(),
        schema.sparse.len(),
        schema.payload.len(),
    );

    let mut rng = StdRng::seed_from_u64(args.seed);
    let batch_size = args.batch_size.max(1);
    let mut written = 0usize;
    while written < args.num {
        let batch = batch_size.min(args.num - written);
        let points = (0..batch as u64)
            .map(|offset| {
                random_point(
                    PointId::NumId(start_id + written as u64 + offset),
                    &schema,
                    &mut rng,
                )
            })
            .collect::<Vec<_>>();

        shard
            .update(UpdateOperation::PointOperation(
                PointOperations::UpsertPoints(PointInsertOperations::PointsList(points)),
            ))
            .context("failed to upsert points")?;

        written += batch;
        if written < args.num {
            log::debug!("upserted {written}/{} point(s)", args.num);
        }
    }

    shard.flush().context("failed to flush the collection")?;
    log::info!(
        "upserted {} point(s); collection now has ~{} point(s)",
        args.num,
        shard
            .info()
            .context("failed to read collection info")?
            .points_count,
    );
    Ok(())
}
