use std::collections::HashSet;

use anyhow::{Context, Result, anyhow, bail};
use edge::{
    CreateIndex, Distance, EdgeConfigBuilder, EdgeOptimizersConfig, EdgeShard,
    EdgeSparseVectorParams, EdgeVectorParams, FieldIndexOperations, JsonPath, PayloadFieldSchema,
    PayloadSchemaType, UpdateOperation,
};

use crate::args::CreateArgs;

/// Default name for a single, unnamed `--dense` vector.
const DEFAULT_DENSE_NAME: &str = "dense";
/// Default name for a single, unnamed `--sparse` vector.
const DEFAULT_SPARSE_NAME: &str = "sparse";

pub fn run(args: CreateArgs) -> Result<()> {
    let dense = parse_dense_specs(&args.dense)?;
    let sparse = parse_sparse_specs(&args.sparse);
    check_no_duplicate_names(&dense, &sparse)?;
    if dense.is_empty() && sparse.is_empty() {
        bail!("collection needs at least one vector: pass --dense <SIZE> and/or --sparse");
    }
    let payload_index = parse_payload_index_specs(&args.payload_index)?;

    let distance: Distance = args.distance.into();
    let mut builder = EdgeConfigBuilder::new().on_disk_payload(args.on_disk_payload);
    for (name, size) in &dense {
        builder = builder.vector(
            name.clone(),
            EdgeVectorParams {
                size: *size,
                distance,
                on_disk: None,
                multivector_config: None,
                datatype: None,
                quantization_config: None,
                hnsw_config: None,
            },
        );
    }
    for name in &sparse {
        builder = builder.sparse_vector(name.clone(), EdgeSparseVectorParams::default());
    }
    if let Some(preset) = args.quantization {
        builder = builder.quantization_config(preset.to_config());
    }
    if let Some(indexing_threshold) = args.indexing_threshold_kb {
        builder = builder.optimizers(EdgeOptimizersConfig {
            indexing_threshold: Some(indexing_threshold),
            ..Default::default()
        });
    }

    fs_err::create_dir_all(&args.path)
        .with_context(|| format!("failed to create directory {}", args.path.display()))?;
    let shard = EdgeShard::new(&args.path, builder.build()).with_context(|| {
        format!(
            "failed to create edge collection at {}",
            args.path.display()
        )
    })?;
    log::info!(
        "created edge collection at {}: {} dense vector(s) {:?}, {} sparse vector(s) {:?}",
        args.path.display(),
        dense.len(),
        dense
            .iter()
            .map(|(name, size)| format!("{name:?}({size})"))
            .collect::<Vec<_>>(),
        sparse.len(),
        sparse,
    );

    for (name, schema_type) in &payload_index {
        let field_name: JsonPath = name
            .parse()
            .map_err(|()| anyhow!("invalid payload field name: {name:?}"))?;
        shard
            .update(UpdateOperation::FieldIndexOperation(
                FieldIndexOperations::CreateIndex(CreateIndex {
                    field_name,
                    field_schema: Some(PayloadFieldSchema::FieldType(*schema_type)),
                }),
            ))
            .with_context(|| format!("failed to create payload index on {name:?}"))?;
        log::info!("created payload index: {name} ({schema_type:?})");
    }

    shard
        .flush()
        .context("failed to flush the new collection")?;
    log::info!("collection ready at {}", args.path.display());
    Ok(())
}

/// Parse `--dense` specs. A single bare `SIZE` names the vector
/// [`DEFAULT_DENSE_NAME`]; with more than one `--dense`, every one must be
/// `NAME:SIZE`.
fn parse_dense_specs(specs: &[String]) -> Result<Vec<(String, usize)>> {
    if let [spec] = specs
        && !spec.contains(':')
    {
        let size: usize = spec
            .trim()
            .parse()
            .with_context(|| format!("invalid --dense size: {spec:?}"))?;
        check_dense_size(size, spec)?;
        return Ok(vec![(DEFAULT_DENSE_NAME.to_string(), size)]);
    }

    specs
        .iter()
        .map(|spec| {
            let (name, size) = spec.split_once(':').ok_or_else(|| {
                anyhow!(
                    "multiple dense vectors require explicit names: use `--dense NAME:SIZE` \
                     (got {spec:?})"
                )
            })?;
            let size: usize = size
                .trim()
                .parse()
                .with_context(|| format!("invalid dense vector size in {spec:?}"))?;
            check_dense_size(size, spec)?;
            Ok((name.trim().to_string(), size))
        })
        .collect()
}

fn check_dense_size(size: usize, spec: &str) -> Result<()> {
    if size == 0 {
        bail!("dense vector size must be greater than zero (got {spec:?})");
    }
    Ok(())
}

/// Parse `--sparse` specs: `default_missing_value = ""` turns a bare
/// `--sparse` into [`DEFAULT_SPARSE_NAME`]; `--sparse=NAME` names it.
fn parse_sparse_specs(specs: &[String]) -> Vec<String> {
    specs
        .iter()
        .map(|name| {
            if name.is_empty() {
                DEFAULT_SPARSE_NAME.to_string()
            } else {
                name.trim().to_string()
            }
        })
        .collect()
}

fn check_no_duplicate_names(dense: &[(String, usize)], sparse: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for (name, _) in dense {
        if !seen.insert(name.as_str()) {
            bail!("duplicate vector name: {name:?}");
        }
    }
    for name in sparse {
        if !seen.insert(name.as_str()) {
            bail!("duplicate vector name: {name:?}");
        }
    }
    Ok(())
}

/// Parse `--payload-index` specs (`NAME:TYPE`), restricted to the types this
/// tool knows how to generate random values for.
fn parse_payload_index_specs(specs: &[String]) -> Result<Vec<(String, PayloadSchemaType)>> {
    specs
        .iter()
        .map(|spec| {
            let (name, kind) = spec.split_once(':').ok_or_else(|| {
                anyhow!("--payload-index entries must be `NAME:TYPE` (got {spec:?})")
            })?;
            let schema_type = match kind.trim().to_ascii_lowercase().as_str() {
                "keyword" => PayloadSchemaType::Keyword,
                "integer" => PayloadSchemaType::Integer,
                "float" => PayloadSchemaType::Float,
                "text" => PayloadSchemaType::Text,
                "geo" => PayloadSchemaType::Geo,
                other => bail!(
                    "unsupported payload index type {other:?} in {spec:?}; expected one of: \
                     keyword, integer, float, text, geo"
                ),
            };
            Ok((name.trim().to_string(), schema_type))
        })
        .collect()
}
