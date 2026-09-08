//! Experimental binary that opens an [`UpdateOnlyEdgeShard`] over a local
//! shard directory — or directly over object storage (AWS S3 / S3-compatible,
//! or Google Cloud Storage) — and runs a batch of random upserts against it.
//!
//! The counterpart of `edge-shard-query` for the write path. Point ids to
//! overwrite are taken from the command line; the *shape* of each generated
//! point is derived from the shard's own schema — every dense/sparse vector
//! named in the appendable segment's config, and one payload value per field
//! in its payload-index schema — so the batch is exactly what a real client
//! could have written.
//!
//! By default the tool dry-runs: it runs [`preview_batch`], which shares the
//! decision pipeline with the real apply, and logs what *would* happen —
//! which segments hold each point today, in which slots and at what versions,
//! which points would be stored/skipped, and which slots would be tombstoned.
//! Nothing is written.
//!
//! With `--apply` it calls [`apply_batch`] instead and writes for real:
//! appends to the write target and tombstones in every segment holding an
//! older copy — over object storage through the appendable
//! [`CachedBlobFile`], so the mutations are direct appends or server-side
//! rewrites per the store's capability, durable once the run reports `Ok`.
//! Adding `--interactive` keeps the session open: after each batch the tool
//! prompts on stdin for the next round's ids and applies them through the
//! writer `apply_batch` handed back — no shard re-open — with the op-num
//! incremented per round.
//!
//! [`apply_batch`]: UpdateOnlyEdgeShard::apply_batch
//!
//! Example — local shard directory:
//!
//! ```sh
//! cargo run -p edge-shard-update -- \
//!     --path  ./qdrant_storage/collections/benchmark/0 \
//!     --ids   1,2,42 \
//!     --op-num 20000
//! ```
//!
//! Example — shard on S3-compatible object storage (here: GCS via its S3
//! interoperability endpoint). Segments are discovered from the leader's
//! segment manifest, and reads go through a local disk cache, exactly like
//! `edge-shard-query`:
//!
//! ```sh
//! cargo run -p edge-shard-update -- \
//!     --backend aws \
//!     --bucket   qdrant-benchmark-snapshots \
//!     --endpoint https://storage.googleapis.com \
//!     --region   auto \
//!     --access-key xxxx \
//!     --secret-key xxxx \
//!     --prefix   serverless/shard-100k \
//!     --ids      1,2,42 \
//!     --op-num   20000
//! ```
//!
//! [`preview_batch`]: UpdateOnlyEdgeShard::preview_batch

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use clap::{Args as ClapArgs, Parser, ValueEnum};
use common::universal_io::{
    DiskCacheConfig, MmapFs, OkNotFound as _, UniversalAppendFs, UniversalReadFileOps,
    UniversalReadFs, read_json_via,
};
use edge::external::uuid::Uuid;
use edge::{
    FullyQualifiedPoint, ManifestSegmentEnumerator, PAYLOAD_INDEX_CONFIG_FILE, Payload,
    PayloadConfig, PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType, PointAction,
    PointApplyKind, PointApplyRecord, PointId, PointOperations, PointStructPersisted,
    SegmentConfig, SegmentConfigInfo, SparseVector, UpdateOnlyEdgeShard, UpdateOperation,
    VectorPersisted, VectorStructPersisted, get_payload_index_path,
};
use io_bridge_object_store::backends::aws::{AwsConfig, AwsCredentials};
use io_bridge_object_store::backends::gcp::{GcsConfig, GcsCredentials};
use io_bridge_object_store::{AsyncAppend, CachedBlobFs, CachedBlobFsContext, ObjectStoreSource};
use object_store::aws::AmazonS3;
use object_store::gcp::GoogleCloudStorage;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};

/// Storage backend to read the shard from.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Backend {
    /// A local shard directory (`--path`), read via memory-mapped files and
    /// discovered by scanning `segments/`.
    Local,
    /// AWS S3 or an S3-compatible store (MinIO, RustFS, GCS interop, ...).
    Aws,
    /// Google Cloud Storage.
    Gcs,
}

#[derive(Parser, Debug)]
#[command(
    about = "Open an UpdateOnlyEdgeShard over a local directory or S3/GCS object storage and \
             run a batch of random upserts against it — a dry run by default, written for real \
             with --apply"
)]
struct Cli {
    #[command(flatten)]
    connection: ConnectionArgs,

    /// Apply the batch for real: append to the write target and tombstone
    /// older copies, durable on the backend once the run reports Ok. Without
    /// this flag the batch is only resolved and logged; nothing is written.
    #[arg(long)]
    apply: bool,

    /// After a batch is applied, prompt on stdin for the next round's ids
    /// (comma-separated; an empty line or EOF quits) and apply them through
    /// the same writer — no shard re-open — with --op-num incremented by one
    /// per round.
    #[arg(long, requires = "apply")]
    interactive: bool,

    /// Point ids to overwrite with random points: comma-separated integers or
    /// UUIDs. Ids no segment holds are created rather than overwritten.
    #[arg(long, value_delimiter = ',', required = true)]
    ids: Vec<String>,

    /// Operation number recorded as the new points' version. A point whose
    /// stored version is at or beyond this is reported as skipped — the
    /// writer's replay semantics — so pick a value above the versions the log
    /// reports to see the points overwritten.
    #[arg(long, default_value_t = 1)]
    op_num: u64,

    /// RNG seed for the generated points, so a run is reproducible.
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

/// How to reach the shard: a local path, or an object-storage location.
#[derive(ClapArgs, Debug)]
struct ConnectionArgs {
    /// Which backend to read the shard from.
    #[arg(long, value_enum, default_value = "local")]
    backend: Backend,

    /// [local] Path to the shard root directory (the one containing
    /// `segments/`).
    #[arg(long)]
    path: Option<PathBuf>,

    /// [AWS/GCS] Bucket name (without any scheme prefix).
    #[arg(long, env = "BLOB_BUCKET")]
    bucket: Option<String>,

    /// [AWS] Custom S3 endpoint URL (MinIO / RustFS / GCS interop; omit for real AWS).
    #[arg(long, env = "S3_ENDPOINT")]
    endpoint: Option<String>,

    /// [AWS] Region (e.g. `us-east-1`). Required for real AWS; optional otherwise.
    #[arg(long, env = "S3_REGION")]
    region: Option<String>,

    /// [AWS] Access key id. If omitted, the AWS default credential chain is used.
    #[arg(long, env = "S3_ACCESS_KEY")]
    access_key: Option<String>,

    /// [AWS] Secret access key. Required when `--access-key` is given.
    #[arg(long, env = "S3_SECRET_KEY")]
    secret_key: Option<String>,

    /// [AWS] Optional session token for short-lived credentials.
    #[arg(long, env = "S3_SESSION_TOKEN")]
    session_token: Option<String>,

    /// [AWS] Use S3 Express One Zone (directory buckets, named `*--x-s3`).
    #[arg(long, env = "S3_EXPRESS")]
    s3_express: bool,

    /// [AWS] The store honors native write-offset appends without being S3
    /// Express — MinIO AiStor, RustFS. Implied by --s3-express; leave off
    /// for plain S3, where appends go through server-side rewrites.
    #[arg(long, env = "S3_NATIVE_APPEND")]
    native_append: bool,

    /// [GCS] Path to a service-account JSON key file. Takes precedence over
    /// `--gcs-service-account-key`; if neither is set, application default
    /// credentials (ADC) are used.
    #[arg(long, env = "GCS_SERVICE_ACCOUNT_PATH")]
    gcs_service_account_path: Option<String>,

    /// [GCS] Inline service-account JSON key contents (instead of a path).
    #[arg(long, env = "GCS_SERVICE_ACCOUNT_KEY")]
    gcs_service_account_key: Option<String>,

    /// [AWS/GCS] Key prefix inside the bucket pointing at the edge-shard root
    /// (the directory containing `segments/` and the segment manifest). Empty
    /// means the bucket root.
    #[arg(long, default_value = "")]
    prefix: String,

    /// [AWS/GCS] Local directory for the segment disk cache. Remote blocks are
    /// fetched once and mirrored here; later reads hit this directory instead.
    /// Defaults to a stable subdirectory of the system temp dir.
    #[arg(long)]
    cache_dir: Option<PathBuf>,
}

/// The shard's write-facing schema, read off the appendable segment: the
/// segment config names the vectors a point must carry, the payload-index
/// config names the payload fields worth generating.
struct ShardSchema {
    config: SegmentConfig,
    payload_fields: Vec<(String, PayloadSchemaType)>,
}

/// The write-target schema off the opened shard: its segment config (already
/// parsed during the open — no extra reads) plus the payload-index schema,
/// which is the one file the writer deliberately never opens, so the tool
/// reads it through `fs` itself.
fn read_schema<Fs: UniversalAppendFs, F: UniversalReadFs>(
    shard: &UpdateOnlyEdgeShard<Fs>,
    fs: &F,
    shard_path: &Path,
) -> Result<ShardSchema> {
    let mut target = None;

    for info in shard.segment_configs() {
        let SegmentConfigInfo {
            uuid,
            is_write_target,
            config,
        } = info;
        log::info!(
            "segment {uuid}: appendable={is_write_target}, {} dense vector(s) {:?}, \
             {} sparse vector(s) {:?}",
            config.vector_data.len(),
            config.vector_data.keys().collect::<Vec<_>>(),
            config.sparse_vector_data.len(),
            config.sparse_vector_data.keys().collect::<Vec<_>>(),
        );
        if is_write_target {
            target = Some((uuid, config));
        }
    }

    let (uuid, config) = target
        .ok_or_else(|| anyhow!("no appendable segment found — the shard has no write target"))?;
    log::info!("write target: segment {uuid}");

    let segment_path = shard_path.join("segments").join(uuid.to_string());
    let payload_config_path = get_payload_index_path(&segment_path).join(PAYLOAD_INDEX_CONFIG_FILE);
    let payload_config: Option<PayloadConfig> =
        match read_json_via(fs, &payload_config_path).ok_not_found() {
            Ok(Some(payload_config)) => Some(payload_config),
            Ok(None) => {
                log::info!(
                    "no payload index config at {} — generating without payload",
                    payload_config_path.display(),
                );
                None
            }
            // A dry-run generator should not die on an unreadable auxiliary
            // file, but the reason must stay visible — it may be a
            // credentials problem.
            Err(err) => {
                log::warn!(
                    "could not read payload index config at {}: {err} — generating without payload",
                    payload_config_path.display(),
                );
                None
            }
        };
    let payload_fields = match payload_config {
        Some(payload_config) => {
            let mut fields: Vec<(String, PayloadSchemaType)> = payload_config
                .indices
                .to_schemas()
                .into_iter()
                .map(|(key, schema)| (key.to_string(), base_schema_type(&schema)))
                .collect();
            fields.sort_by(|(a, _), (b, _)| a.cmp(b));
            fields
        }
        None => Vec::new(),
    };

    for (field, schema_type) in &payload_fields {
        log::info!("payload schema: {field:?} -> {schema_type:?}");
    }

    Ok(ShardSchema {
        config,
        payload_fields,
    })
}

/// The base value type behind a payload field schema, with or without index
/// params.
fn base_schema_type(schema: &PayloadFieldSchema) -> PayloadSchemaType {
    match schema {
        PayloadFieldSchema::FieldType(schema_type) => *schema_type,
        PayloadFieldSchema::FieldParams(params) => match params {
            PayloadSchemaParams::Keyword(_) => PayloadSchemaType::Keyword,
            PayloadSchemaParams::Integer(_) => PayloadSchemaType::Integer,
            PayloadSchemaParams::Float(_) => PayloadSchemaType::Float,
            PayloadSchemaParams::Geo(_) => PayloadSchemaType::Geo,
            PayloadSchemaParams::Text(_) => PayloadSchemaType::Text,
            PayloadSchemaParams::Bool(_) => PayloadSchemaType::Bool,
            PayloadSchemaParams::Datetime(_) => PayloadSchemaType::Datetime,
            PayloadSchemaParams::Uuid(_) => PayloadSchemaType::Uuid,
        },
    }
}

const WORDS: &[&str] = &[
    "amber", "basalt", "cobalt", "dune", "ember", "fjord", "garnet", "harbor",
];

/// One random point in the shape the schema prescribes: every named vector at
/// its configured dimensionality, one payload value per indexed field.
fn random_point(id: PointId, schema: &ShardSchema, rng: &mut StdRng) -> PointStructPersisted {
    let mut vectors = HashMap::new();

    for (name, vector_config) in &schema.config.vector_data {
        let dense = |rng: &mut StdRng| {
            (0..vector_config.size)
                .map(|_| rng.random_range(-1.0..1.0))
                .collect::<Vec<f32>>()
        };
        let vector = if vector_config.multivector_config.is_some() {
            VectorPersisted::MultiDense(vec![dense(rng), dense(rng)])
        } else {
            VectorPersisted::Dense(dense(rng))
        };
        vectors.insert(name.clone(), vector);
    }

    for name in schema.config.sparse_vector_data.keys() {
        // Cumulative random gaps: sorted, unique indices without a sampler.
        let mut index = 0u32;
        let mut indices = Vec::new();
        let mut values = Vec::new();
        for _ in 0..8 {
            index += rng.random_range(1..1000);
            indices.push(index);
            values.push(rng.random_range(0.0..1.0));
        }
        vectors.insert(
            name.clone(),
            VectorPersisted::Sparse(SparseVector { indices, values }),
        );
    }

    let mut payload = serde_json::Map::new();
    for (field, schema_type) in &schema.payload_fields {
        payload.insert(field.clone(), random_payload_value(*schema_type, rng));
    }

    PointStructPersisted {
        id,
        vector: VectorStructPersisted::Named(vectors),
        payload: Some(
            serde_json::from_value::<Payload>(serde_json::Value::Object(payload))
                .expect("a JSON object is always a valid payload"),
        ),
    }
}

fn random_payload_value(schema_type: PayloadSchemaType, rng: &mut StdRng) -> serde_json::Value {
    let word = |rng: &mut StdRng| WORDS[rng.random_range(0..WORDS.len())].to_string();
    match schema_type {
        PayloadSchemaType::Keyword => word(rng).into(),
        PayloadSchemaType::Integer => rng.random_range(0..1000).into(),
        PayloadSchemaType::Float => rng.random_range(0.0..100.0).into(),
        PayloadSchemaType::Bool => rng.random::<bool>().into(),
        PayloadSchemaType::Geo => serde_json::json!({
            "lon": rng.random_range(-180.0..180.0),
            "lat": rng.random_range(-85.0..85.0),
        }),
        PayloadSchemaType::Text => format!("{} {} {}", word(rng), word(rng), word(rng)).into(),
        PayloadSchemaType::Datetime => format!(
            "2026-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            rng.random_range(1..=12),
            rng.random_range(1..=28),
            rng.random_range(0..24),
            rng.random_range(0..60),
            rng.random_range(0..60),
        )
        .into(),
        PayloadSchemaType::Uuid => Uuid::from_u128(rng.random()).to_string().into(),
    }
}

/// Parse a point id from the command line: a bare integer id or a UUID string.
fn parse_point_id(raw: &str) -> Result<PointId> {
    // `PointId` deserializes a JSON number into a numeric id; a bare UUID is not
    // valid JSON, so quote it and retry as a JSON string.
    if let Ok(id) = serde_json::from_str::<PointId>(raw) {
        return Ok(id);
    }
    let quoted = serde_json::to_string(raw).expect("string is always serializable");
    serde_json::from_str::<PointId>(&quoted)
        .with_context(|| format!("invalid point id (not an integer or UUID): {raw:?}"))
}

fn log_preview_point(point: &edge::PointPreview) {
    let edge::PointPreview {
        id,
        current,
        slots,
        action,
    } = point;

    match current {
        Some(current) => log::info!(
            "point {id}: newest copy in segment {} slot {} at version {}",
            current.segment,
            current.internal_id,
            current.version,
        ),
        None => log::info!("point {id}: not stored in any segment (would be created)"),
    }
    for (segment, internal_id) in slots {
        log::info!("point {id}: occupies segment {segment} slot {internal_id}");
    }

    match action {
        PointAction::Store(resolved) => {
            let FullyQualifiedPoint {
                id: _,
                version,
                stored_vectors,
                updated_vectors,
                payload,
            } = resolved.as_ref();
            log::info!(
                "point {id}: would be stored at version {version}: \
                 {} vector(s) carried over as raw bytes {:?}, \
                 {} vector(s) supplied by the batch {:?}, payload {}",
                stored_vectors.len(),
                stored_vectors
                    .iter()
                    .map(|(name, bytes)| format!("{name}({} B)", bytes.len()))
                    .collect::<Vec<_>>(),
                updated_vectors.len(),
                updated_vectors.keys().collect::<Vec<_>>(),
                serde_json::to_string(payload).unwrap_or_else(|err| err.to_string()),
            );
            if !slots.is_empty() {
                log::info!(
                    "point {id}: its {} current slot(s) would be tombstoned",
                    slots.len()
                );
            }
        }
        PointAction::Delete => {
            log::info!(
                "point {id}: would be deleted, tombstoning {} slot(s)",
                slots.len()
            );
        }
        PointAction::Skip => log::info!(
            "point {id}: already at or beyond version — skipped (replay no-op); \
             re-run with a higher --op-num to overwrite",
        ),
        PointAction::Rejected => {
            log::info!("point {id}: already there — rejected by the upsert's update mode");
        }
        PointAction::Missing => {
            log::info!("point {id}: names a point no segment holds — nothing to do");
        }
    }
}

/// Generate the random upsert batch off the shard's schema, logging each
/// point's shape.
fn generate_batch(schema: &ShardSchema, ids: &[PointId], seed: u64) -> UpdateOperation {
    let mut rng = StdRng::seed_from_u64(seed);
    let points: Vec<PointStructPersisted> = ids
        .iter()
        .map(|&id| {
            let point = random_point(id, schema, &mut rng);
            log::info!(
                "generated point {id}: vectors {:?}, payload {}",
                match &point.vector {
                    VectorStructPersisted::Single(v) => vec![format!("(default, {} dim)", v.len())],
                    VectorStructPersisted::MultiDense(m) =>
                        vec![format!("(default, {} multivector(s))", m.len())],
                    VectorStructPersisted::Named(named) => named
                        .iter()
                        .map(|(name, vector)| match vector {
                            VectorPersisted::Dense(v) => format!("{name}({} dim)", v.len()),
                            VectorPersisted::Sparse(s) =>
                                format!("{name}({} nnz)", s.indices.len()),
                            VectorPersisted::MultiDense(m) =>
                                format!("{name}({} multivector(s))", m.len()),
                        })
                        .collect(),
                },
                point
                    .payload
                    .as_ref()
                    .map(|payload| serde_json::to_string(payload)
                        .unwrap_or_else(|err| err.to_string()))
                    .unwrap_or_else(|| "<none>".to_string()),
            );
            point
        })
        .collect();

    UpdateOperation::PointOperation(PointOperations::UpsertPoints(points.into()))
}

/// Generate the random batch, resolve it against the open shard, and log what
/// it would do. The backend is behind `S`, so this is the whole dry run for
/// local and object-storage shards alike.
fn dry_run<Fs: UniversalAppendFs>(
    shard: &UpdateOnlyEdgeShard<Fs>,
    schema: &ShardSchema,
    ids: &[PointId],
    op_num: u64,
    seed: u64,
) -> Result<()> {
    let operation = generate_batch(schema, ids, seed);

    let preview = shard
        .preview_batch([(op_num, operation)])
        .context("failed to resolve the batch")?;

    let mut stored = 0usize;
    let mut skipped = 0usize;
    let mut rejected = 0usize;
    let mut tombstones: HashMap<Uuid, usize> = HashMap::new();
    for point in &preview.points {
        log_preview_point(point);
        match &point.action {
            PointAction::Store(_) => {
                stored += 1;
                for (segment, _) in &point.slots {
                    *tombstones.entry(*segment).or_default() += 1;
                }
            }
            PointAction::Delete => {
                for (segment, _) in &point.slots {
                    *tombstones.entry(*segment).or_default() += 1;
                }
            }
            PointAction::Skip => skipped += 1,
            PointAction::Rejected => rejected += 1,
            PointAction::Missing => {}
        }
    }

    log::info!(
        "summary: {stored} point(s) would be appended to the write target, \
         {skipped} skipped, {rejected} rejected by their update mode",
    );
    for (segment, count) in &tombstones {
        log::info!("summary: segment {segment} would receive {count} tombstone(s)");
    }
    log::info!("dry run: nothing written — pass --apply to write");

    Ok(())
}

/// Generate the random batch and apply it for real: appends to the write
/// target, tombstones in every segment holding an older copy — durable on
/// the backend once this returns `Ok`. With `interactive`, keeps prompting
/// for the next round's ids and applies them through the writer the previous
/// `apply_batch` handed back, one op-num (and seed) higher per round.
fn apply_run<Fs: UniversalAppendFs>(
    mut shard: UpdateOnlyEdgeShard<Fs>,
    schema: &ShardSchema,
    ids: &[PointId],
    mut op_num: u64,
    mut seed: u64,
    interactive: bool,
) -> Result<()> {
    let mut ids = ids.to_vec();
    loop {
        let operation = generate_batch(schema, &ids, seed);

        let (returned, outcome) = shard
            .apply_batch([(op_num, operation)])
            .context("failed to apply the batch")?;
        shard = returned;

        log::info!(
            "applied at op-num {op_num}: {} stored, {} deleted, {} skipped, \
             {} rejected, {} missing",
            outcome.stored,
            outcome.deleted,
            outcome.skipped,
            outcome.rejected,
            outcome.missing,
        );
        for record in &outcome.points {
            log_apply_record(record);
        }

        if !interactive {
            return Ok(());
        }
        match prompt_next_ids()? {
            Some(next_ids) => ids = next_ids,
            None => return Ok(()),
        }
        op_num += 1;
        seed = seed.wrapping_add(1);
    }
}

/// One line per applied point: whether it was created fresh or overwrote
/// existing copies, and from which segments (and slots) the old copies were
/// removed.
fn log_apply_record(record: &PointApplyRecord) {
    let PointApplyRecord {
        id,
        kind,
        tombstoned,
        superseded,
    } = record;

    let slot = |(segment, internal_id): &(Uuid, _)| format!("segment {segment} slot {internal_id}");
    let tombstoned_list = tombstoned.iter().map(slot).collect::<Vec<_>>();

    match kind {
        PointApplyKind::Stored => {
            let mut retired = tombstoned_list;
            if let Some(superseded) = superseded {
                retired.push(format!("{} (superseded in place)", slot(superseded)));
            }
            if retired.is_empty() {
                log::info!("point {id}: created — no previous copy in any segment");
            } else {
                log::info!(
                    "point {id}: overwritten — old copies deleted from {}",
                    retired.join(", "),
                );
            }
        }
        PointApplyKind::Deleted => {
            log::info!("point {id}: deleted from {}", tombstoned_list.join(", "));
        }
        PointApplyKind::Skipped => {
            log::info!("point {id}: skipped — already at or beyond this op-num");
        }
        PointApplyKind::Rejected => {
            log::info!("point {id}: already there — rejected by its update mode");
        }
        PointApplyKind::Missing => {
            log::info!("point {id}: no segment holds it — nothing to do");
        }
    }
}

/// Read the next round's ids from stdin: comma-separated on one line, with
/// unparsable input re-prompted; an empty line or EOF ends the session.
fn prompt_next_ids() -> Result<Option<Vec<PointId>>> {
    loop {
        eprint!("next ids (comma-separated, empty to quit): ");
        let mut line = String::new();
        if std::io::stdin()
            .read_line(&mut line)
            .context("failed to read ids from stdin")?
            == 0
        {
            return Ok(None);
        }
        let line = line.trim();
        if line.is_empty() {
            return Ok(None);
        }

        match line
            .split(',')
            .map(|raw| parse_point_id(raw.trim()))
            .collect::<Result<Vec<_>>>()
        {
            Ok(ids) => return Ok(Some(ids)),
            Err(err) => log::warn!("{err:#}"),
        }
    }
}

/// The bucket name, required by the object-storage backends.
fn require_bucket(conn: &ConnectionArgs) -> Result<String> {
    conn.bucket
        .clone()
        .ok_or_else(|| anyhow!("--bucket is required for the {:?} backend", conn.backend))
}

fn build_aws_config(conn: &ConnectionArgs) -> Result<AwsConfig> {
    let credentials = match (&conn.access_key, &conn.secret_key) {
        (Some(access_key_id), Some(secret_access_key)) => AwsCredentials::Static {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: conn.session_token.clone(),
        },
        (None, None) => AwsCredentials::Default,
        _ => {
            return Err(anyhow!(
                "--access-key and --secret-key must be provided together"
            ));
        }
    };

    Ok(AwsConfig {
        bucket: require_bucket(conn)?,
        region: conn.region.clone(),
        endpoint: conn.endpoint.clone(),
        s3_express: conn.s3_express,
        native_append: conn.native_append,
        credentials,
    })
}

fn build_gcs_config(conn: &ConnectionArgs) -> Result<GcsConfig> {
    let credentials = if let Some(key) = &conn.gcs_service_account_key {
        GcsCredentials::ServiceAccountKey(key.clone())
    } else if let Some(path) = &conn.gcs_service_account_path {
        GcsCredentials::ServiceAccountPath(path.clone())
    } else {
        GcsCredentials::Default
    };

    Ok(GcsConfig {
        bucket: require_bucket(conn)?,
        credentials,
    })
}

/// Build the combined cached-blob filesystem: reads are served through a
/// local disk-cache mirror (each remote block fetched once), appends go
/// straight to the remote.
fn build_cached_fs<A>(
    remote_config: A::Config,
    remote_prefix: &Path,
    cache_dir: &Path,
) -> Result<CachedBlobFs<A>>
where
    A: AsyncAppend + Clone,
    A::Config: Clone,
{
    fs_err::create_dir_all(cache_dir)
        .with_context(|| format!("failed to create cache dir {}", cache_dir.display()))?;

    let config = DiskCacheConfig::new(remote_prefix.to_path_buf(), cache_dir.to_path_buf())
        .context("failed to build disk cache config")?;

    CachedBlobFs::<A>::from_context(CachedBlobFsContext {
        disk_cache: Arc::new(config),
        remote: remote_config,
    })
    .context("failed to build cached-blob filesystem")
}

/// Dry-run against a local shard directory: segments discovered by scanning
/// `segments/`, read over memory-mapped files.
fn run_local(cli: &Cli, ids: &[PointId]) -> Result<()> {
    let path = cli
        .connection
        .path
        .clone()
        .ok_or_else(|| anyhow!("--path is required for the local backend"))?;

    let shard = UpdateOnlyEdgeShard::<MmapFs>::open_mmap(&path)
        .context("failed to open update-only edge shard")?;
    log::info!(
        "opened update-only shard with {} segment(s)",
        shard.segments_count()
    );

    let schema = read_schema(&shard, &common::universal_io::MmapFs, &path)?;
    if cli.apply {
        apply_run(shard, &schema, ids, cli.op_num, cli.seed, cli.interactive)
    } else {
        dry_run(&shard, &schema, ids, cli.op_num, cli.seed)
    }
}

/// Run against object storage: segments discovered from the leader's
/// segment manifest, reads through the local disk cache, appends (with
/// `--apply`) straight to the remote.
fn run_remote<A>(cli: &Cli, ids: &[PointId], remote_config: A::Config) -> Result<()>
where
    A: AsyncAppend + Clone,
    A::Config: Clone,
{
    let prefix = PathBuf::from(&cli.connection.prefix);
    let cache_dir = cli
        .connection
        .cache_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("edge-shard-update-cache"));

    let cached_fs = build_cached_fs::<A>(remote_config, &prefix, &cache_dir)?;
    log::info!("caching segment reads under {}", cache_dir.display());

    let enumerator = ManifestSegmentEnumerator::new(cached_fs.clone(), &prefix);
    let shard =
        UpdateOnlyEdgeShard::<CachedBlobFs<A>>::open(cached_fs.clone(), &prefix, enumerator)
            .context("failed to open update-only edge shard over object storage")?;
    log::info!(
        "opened update-only shard with {} segment(s)",
        shard.segments_count()
    );

    let schema = read_schema(&shard, &cached_fs, &prefix)?;
    if cli.apply {
        apply_run(shard, &schema, ids, cli.op_num, cli.seed, cli.interactive)
    } else {
        dry_run(&shard, &schema, ids, cli.op_num, cli.seed)
    }
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let cli = Cli::parse();

    let ids = cli
        .ids
        .iter()
        .map(|raw| parse_point_id(raw))
        .collect::<Result<Vec<_>>>()?;
    log::info!("dry-run upsert of {} random point(s): {ids:?}", ids.len());

    let conn = &cli.connection;
    match conn.backend {
        Backend::Local => run_local(&cli, &ids),
        Backend::Aws => {
            run_remote::<ObjectStoreSource<AmazonS3>>(&cli, &ids, build_aws_config(conn)?)
        }
        Backend::Gcs => {
            run_remote::<ObjectStoreSource<GoogleCloudStorage>>(&cli, &ids, build_gcs_config(conn)?)
        }
    }
}
