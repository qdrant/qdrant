//! Parsing of command-line values: data arguments, filters, ids and vectors.

use std::io::Read as _;

use anyhow::{Context, Result, anyhow};
use edge::{
    Condition, FieldCondition, Filter, JsonPath, Match, PointId, SparseVector, ValueVariants,
};

/// Read a curl `--data`-style argument: a literal value, `@path` to read from a
/// file, or `@-` to read from stdin.
pub fn read_data_arg(raw: &str) -> Result<String> {
    match raw.strip_prefix('@') {
        Some("-") => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .context("failed to read from stdin")?;
            Ok(buf)
        }
        Some(path) => {
            fs_err::read_to_string(path).with_context(|| format!("failed to read file {path:?}"))
        }
        None => Ok(raw.to_string()),
    }
}

/// Build a single "field equals value" filter from the `--filter-key`/
/// `--filter-value` shortcut. Returns `None` when neither is set. The value is
/// parsed as an integer or boolean when it looks like one, otherwise as a string.
pub fn build_kv_filter(key: Option<&str>, value: Option<&str>) -> Result<Option<Filter>> {
    let (Some(key), Some(value)) = (key, value) else {
        return Ok(None);
    };

    let path: JsonPath = key
        .parse()
        .map_err(|()| anyhow!("invalid --filter-key path: {key:?}"))?;

    let value = if let Ok(int) = value.parse::<i64>() {
        ValueVariants::Integer(int)
    } else if let Ok(flag) = value.parse::<bool>() {
        ValueVariants::Bool(flag)
    } else {
        ValueVariants::String(value.to_string())
    };

    let condition = FieldCondition::new_match(path, Match::from(value));
    Ok(Some(Filter::new_must(Condition::Field(condition))))
}

/// Parse a point id from the command line: a bare integer id or a UUID string.
pub fn parse_point_id(raw: &str) -> Result<PointId> {
    // `PointId` deserializes a JSON number into a numeric id; a bare UUID is not
    // valid JSON, so quote it and retry as a JSON string.
    if let Ok(id) = serde_json::from_str::<PointId>(raw) {
        return Ok(id);
    }
    let quoted = serde_json::to_string(raw).expect("string is always serializable");
    serde_json::from_str::<PointId>(&quoted)
        .with_context(|| format!("invalid point id (not an integer or UUID): {raw:?}"))
}

/// Parse a query vector from a JSON array or a comma-separated list of floats.
pub fn parse_vector(raw: &str) -> Result<Vec<f32>> {
    let data = read_data_arg(raw)?;
    let trimmed = data.trim();
    if trimmed.starts_with('[') {
        serde_json::from_str(trimmed)
            .with_context(|| format!("failed to parse --vector as a JSON float array: {trimmed}"))
    } else {
        trimmed
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<f32>()
                    .with_context(|| format!("invalid float in --vector: {s:?}"))
            })
            .collect()
    }
}

/// Parse a sparse query vector from a JSON object (`{"indices": [...],
/// "values": [...]}`) or a comma-separated list of `index:value` pairs
/// (`12:0.4,700:0.9`). The result is sorted by indices and validated
/// (equal lengths, unique indices).
pub fn parse_sparse_vector(raw: &str) -> Result<SparseVector> {
    let data = read_data_arg(raw)?;
    let trimmed = data.trim();

    let mut vector: SparseVector = if trimmed.starts_with('{') {
        serde_json::from_str(trimmed).with_context(|| {
            format!("failed to parse --vector as a JSON sparse vector: {trimmed}")
        })?
    } else {
        let mut indices = Vec::new();
        let mut values = Vec::new();
        for pair in trimmed.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (index, value) = pair
                .split_once(':')
                .with_context(|| format!("expected `index:value` in --vector, got {pair:?}"))?;
            indices.push(
                index
                    .trim()
                    .parse::<u32>()
                    .with_context(|| format!("invalid index in --vector: {index:?}"))?,
            );
            values.push(
                value
                    .trim()
                    .parse::<f32>()
                    .with_context(|| format!("invalid value in --vector: {value:?}"))?,
            );
        }
        SparseVector { indices, values }
    };

    vector.sort_by_indices();
    let SparseVector { indices, values } = vector;
    SparseVector::new(indices, values).map_err(|err| anyhow!("invalid sparse vector: {err}"))
}
