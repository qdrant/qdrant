//! `load-beir`: load a BEIR corpus into an existing collection, one point per
//! document, so BM25 runs on real text instead of random words.
//!
//! Each document's title and text go to one payload field, and its BEIR `_id`
//! to `doc_id` so results can be checked against the qrels. Vectors and the
//! other indexed payload fields are generated as `upsert` generates them.

use std::io::BufRead as _;

use anyhow::{Context, Result, anyhow, bail};
use edge::{EdgeShard, JsonPath, PayloadSchemaParams, PointId, PointStructPersisted};
use rand::SeedableRng as _;
use rand::rngs::StdRng;

use crate::args::LoadBeirArgs;
use crate::points::{Schema, random_point, set_at_path};
use crate::upsert::upsert_points;

/// Payload key holding each document's BEIR `_id`.
const DOC_ID_KEY: &str = "doc_id";

pub fn run(args: LoadBeirArgs) -> Result<()> {
    let shard = EdgeShard::load(&args.path, None)
        .with_context(|| format!("failed to open collection at {}", args.path.display()))?;

    let field: JsonPath = args
        .field
        .parse()
        .map_err(|()| anyhow!("invalid --field path: {:?}", args.field))?;
    let doc_id_key: JsonPath = DOC_ID_KEY.parse().expect("valid payload key");
    if field.compatible(&doc_id_key) {
        bail!("--field {field} overlaps `{DOC_ID_KEY}`, where the BEIR id goes");
    }

    let info = shard.info().context("failed to read collection info")?;
    match info
        .payload_schema
        .get(&field)
        .and_then(|index| index.params.as_ref())
    {
        Some(PayloadSchemaParams::Text(params)) if params.scoring() => {}
        _ => log::warn!(
            "field {field} has no scoring text index: create the collection with \
             `--payload-index {field}:bm25` to rank it by BM25"
        ),
    }

    let mut schema = Schema::of_shard(&shard, &info)
        .with_context(|| format!("cannot load into {}", args.path.display()))?;
    // The document fills these two. Any other indexed path under or above
    // them would be overwritten by the document's values.
    schema
        .payload
        .retain(|(key, _)| *key != field && *key != doc_id_key);
    if let Some((key, _)) = schema
        .payload
        .iter()
        .find(|(key, _)| key.compatible(&field) || key.compatible(&doc_id_key))
    {
        bail!("indexed field {key} overlaps --field {field} or `{DOC_ID_KEY}`");
    }

    let corpus = fs_err::File::open(&args.corpus)?;
    let mut rng = StdRng::seed_from_u64(args.seed);
    let batch_size = args.batch_size.max(1);
    let mut batch = Vec::with_capacity(batch_size);
    let mut loaded = 0u64;
    let limit = args.limit.map_or(u64::MAX, |limit| limit as u64);
    for (line_number, line) in (1..).zip(std::io::BufReader::new(corpus).lines()) {
        if loaded == limit {
            break;
        }
        let line = line.with_context(|| format!("failed to read {}", args.corpus.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let id = args
            .start_id
            .checked_add(loaded)
            .context("point id past u64::MAX: lower --start-id")?;
        let id = PointId::NumId(id);
        let point = document_point(&line, id, &field, &doc_id_key, &schema, &mut rng)
            .with_context(|| format!("{}:{line_number}", args.corpus.display()))?;
        batch.push(point);
        loaded += 1;

        if batch.len() == batch_size {
            upsert_points(&shard, std::mem::take(&mut batch))?;
            log::debug!("loaded {loaded} document(s)");
        }
    }
    if !batch.is_empty() {
        upsert_points(&shard, batch)?;
    }

    shard.flush().context("failed to flush the collection")?;
    log::info!(
        "loaded {loaded} document(s) from {} into {field} (ids {}..{})",
        args.corpus.display(),
        args.start_id,
        args.start_id.saturating_add(loaded),
    );
    Ok(())
}

/// The point for one corpus line: generated vectors and payload, plus the
/// document's text in `field` and its BEIR id in `doc_id_key`.
fn document_point(
    line: &str,
    id: PointId,
    field: &JsonPath,
    doc_id_key: &JsonPath,
    schema: &Schema,
    rng: &mut StdRng,
) -> Result<PointStructPersisted> {
    let document: serde_json::Value = serde_json::from_str(line).context("not a JSON document")?;
    let doc_id = match &document["_id"] {
        serde_json::Value::String(doc_id) => doc_id.clone(),
        serde_json::Value::Number(doc_id) => doc_id.to_string(),
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Array(_)
        | serde_json::Value::Object(_) => bail!("document has no `_id`"),
    };
    let text = document["text"]
        .as_str()
        .ok_or_else(|| anyhow!("document {doc_id} has no `text`"))?;
    let text = match document["title"].as_str() {
        Some(title) if !title.is_empty() => format!("{title}\n{text}"),
        _ => text.to_string(),
    };

    let mut point = random_point(id, schema, rng);
    let mut payload = point.payload.take().unwrap_or_default().0;
    set_at_path(&mut payload, field, text.into());
    set_at_path(&mut payload, doc_id_key, doc_id.into());
    point.payload = Some(payload.into());
    Ok(point)
}
