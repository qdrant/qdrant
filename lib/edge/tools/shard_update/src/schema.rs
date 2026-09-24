//! The write-target schema read off the opened shard.

use std::path::Path;

use anyhow::{Result, anyhow};
use common::universal_io::{OkNotFound as _, UniversalAppendFs, UniversalReadFs, read_json_via};
use edge::{
    PAYLOAD_INDEX_CONFIG_FILE, PayloadConfig, PayloadFieldSchema, PayloadSchemaParams,
    PayloadSchemaType, SegmentConfig, SegmentConfigInfo, UpdateOnlyEdgeShard,
    get_payload_index_path,
};

/// The shard's write-facing schema, read off the appendable segment: the
/// segment config names the vectors a point must carry, the payload-index
/// config names the payload fields worth generating.
pub struct ShardSchema {
    pub config: SegmentConfig,
    pub payload_fields: Vec<(String, PayloadSchemaType)>,
}

/// The write-target schema off the opened shard: its segment config (already
/// parsed during the open — no extra reads) plus the payload-index schema,
/// which is the one file the writer deliberately never opens, so the tool
/// reads it through `fs` itself.
pub fn read_schema<Fs: UniversalAppendFs, F: UniversalReadFs>(
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
