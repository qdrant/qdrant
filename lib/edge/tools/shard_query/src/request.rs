//! The prepared read request: built once from the CLI, run on every (re)load, rendered as rows.

use anyhow::{Context, Result, anyhow};
use edge::{
    DEFAULT_VECTOR_NAME, EdgeShardRead, JsonPath, LoadProfile, NamedQuery, OrderByInterface,
    PointId, QueryEnum, Record, ScoredPoint, ScrollRequest, SearchParams, SearchRequest,
    VectorInternal, WithPayloadInterface,
};

use crate::cli::Command;
use crate::parse::{parse_point_id, parse_sparse_vector, parse_vector};

/// Render a payload + (optional) vector as a single JSON line.
///
/// `VectorStructInternal` is not `Serialize`, so the vector is rendered via
/// `Debug` instead.
fn record_json(
    id: serde_json::Value,
    payload: serde_json::Value,
    vector: Option<impl std::fmt::Debug>,
    extra: serde_json::Value,
) -> serde_json::Value {
    let mut json = serde_json::json!({
        "id": id,
        "payload": payload,
        "vector": vector.map(|v| format!("{v:?}")),
    });
    if let (Some(obj), serde_json::Value::Object(extra)) = (json.as_object_mut(), extra) {
        obj.extend(extra);
    }
    json
}

/// One result row in a comparable form: the point id (as its JSON encoding,
/// used as the diff key) plus the rendered JSON line.
pub type Row = (String, serde_json::Value);

/// A read request parsed once from the CLI args, so every live-reload
/// iteration re-runs exactly the same request.
pub enum PreparedRequest {
    Scroll(ScrollRequest),
    Search(SearchRequest),
}

impl PreparedRequest {
    pub fn build(command: &Command) -> Result<Self> {
        match command {
            Command::Scroll(args) => {
                let filter = args.common.resolve_filter()?;
                match &filter {
                    Some(filter) => {
                        log::info!("scrolling with filter: {}", serde_json::to_string(filter)?)
                    }
                    None => log::info!("scrolling with no filter (all points)"),
                }

                let offset = args.offset.as_deref().map(parse_point_id).transpose()?;
                let order_by = args
                    .order_by
                    .as_deref()
                    .map(|key| {
                        key.parse::<JsonPath>()
                            .map(OrderByInterface::Key)
                            .map_err(|()| anyhow!("invalid --order-by path: {key:?}"))
                    })
                    .transpose()?;

                Ok(Self::Scroll(ScrollRequest {
                    offset,
                    limit: Some(args.common.limit),
                    filter,
                    with_payload: Some(WithPayloadInterface::Bool(true)),
                    with_vector: args.common.with_vectors.into(),
                    order_by,
                }))
            }
            Command::Search(args) => {
                let filter = args.common.resolve_filter()?;
                match &filter {
                    Some(filter) => {
                        log::info!("searching with filter: {}", serde_json::to_string(filter)?)
                    }
                    None => log::info!("searching with no filter"),
                }

                // No `--vector` leaves the placeholder empty; it is replaced by a
                // random vector once the shard is open and its config known (see
                // `fill_random_vector`) — the load profile only needs the name.
                let vector = match &args.vector {
                    Some(raw) => {
                        let vector = parse_vector(raw)?;
                        log::info!("query vector has {} dimension(s)", vector.len());
                        vector
                    }
                    None => Vec::new(),
                };

                let query = QueryEnum::Nearest(NamedQuery {
                    query: VectorInternal::Dense(vector),
                    using: args.using.clone(),
                });

                let params = (args.hnsw_ef.is_some() || args.exact).then(|| SearchParams {
                    hnsw_ef: args.hnsw_ef,
                    exact: args.exact,
                    ..Default::default()
                });

                Ok(Self::Search(SearchRequest {
                    query,
                    filter,
                    params,
                    limit: args.common.limit,
                    offset: args.offset,
                    with_payload: Some(WithPayloadInterface::Bool(true)),
                    with_vector: Some(args.common.with_vectors.into()),
                    score_threshold: args.score_threshold,
                }))
            }
            Command::SearchSparse(args) => {
                let filter = args.common.resolve_filter()?;
                match &filter {
                    Some(filter) => {
                        log::info!("searching with filter: {}", serde_json::to_string(filter)?)
                    }
                    None => log::info!("searching with no filter"),
                }

                let vector = parse_sparse_vector(&args.vector)?;
                log::info!(
                    "sparse query vector has {} non-zero dimension(s)",
                    vector.indices.len()
                );

                let query = QueryEnum::Nearest(NamedQuery {
                    query: VectorInternal::Sparse(vector),
                    using: args.using.clone(),
                });

                let params = args.exact.then(|| SearchParams {
                    exact: args.exact,
                    ..Default::default()
                });

                Ok(Self::Search(SearchRequest {
                    query,
                    filter,
                    params,
                    limit: args.common.limit,
                    offset: args.offset,
                    with_payload: Some(WithPayloadInterface::Bool(true)),
                    with_vector: Some(args.common.with_vectors.into()),
                    score_threshold: args.score_threshold,
                }))
            }
        }
    }

    /// The request's [`LoadProfile`], deciding which segment components the
    /// shard open warms. Derived once, before the open.
    pub fn load_profile(&self) -> LoadProfile {
        match self {
            Self::Scroll(request) => request.load_profile(),
            Self::Search(request) => request.load_profile(),
        }
    }

    /// Replace an omitted `--vector` (the empty placeholder from
    /// [`build`](Self::build)) with a random one, its dimension read from the
    /// now-open shard's config. No-op for scroll and explicit vectors.
    pub fn fill_random_vector<S: EdgeShardRead>(&mut self, shard: &S) -> Result<()> {
        let Self::Search(request) = self else {
            return Ok(());
        };
        let QueryEnum::Nearest(named) = &mut request.query else {
            return Ok(());
        };
        let VectorInternal::Dense(vector) = &mut named.query else {
            return Ok(());
        };
        if !vector.is_empty() {
            return Ok(());
        }

        let config = shard.config_snapshot();
        let name = named.using.as_deref().unwrap_or(DEFAULT_VECTOR_NAME);
        let dim = config
            .vectors
            .get(name)
            .with_context(|| format!("vector {name:?} is not present in the shard config"))?
            .size;

        use rand::RngExt as _;
        let mut rng = rand::rng();
        *vector = (0..dim).map(|_| rng.random::<f32>()).collect();
        log::info!("searching with a random {dim}-dimensional vector");
        Ok(())
    }

    /// Run the request against the shard's current state. Returns the result
    /// rows and, for scroll, the next-page offset of this run.
    pub fn run<S: EdgeShardRead>(&self, shard: &S) -> Result<(Vec<Row>, Option<PointId>)> {
        match self {
            Self::Scroll(request) => {
                let (records, next_offset) = shard
                    .scroll(request.clone())
                    .context("scroll request failed")?;
                let rows = records.iter().map(record_row).collect::<Result<_>>()?;
                Ok((rows, next_offset))
            }
            Self::Search(request) => {
                let points = shard
                    .search(request.clone())
                    .context("search request failed")?;
                let rows = points.iter().map(scored_point_row).collect::<Result<_>>()?;
                Ok((rows, None))
            }
        }
    }

    /// Print a full result set (the first answer; later runs print diffs).
    pub fn print_full(&self, rows: &[Row], next_offset: Option<&PointId>) -> Result<()> {
        match self {
            Self::Scroll(_) => println!("scroll returned {} record(s)", rows.len()),
            Self::Search(_) => println!("search returned {} result(s)", rows.len()),
        }
        for (_, row) in rows {
            println!("{}", serde_json::to_string(row)?);
        }
        if let Self::Scroll(_) = self {
            match next_offset {
                Some(offset) => println!("next_page_offset: {}", serde_json::to_string(offset)?),
                None => println!("next_page_offset: <none>"),
            }
        }
        Ok(())
    }
}

fn record_row(record: &Record) -> Result<Row> {
    let json = record_json(
        serde_json::to_value(record.id)?,
        serde_json::to_value(&record.payload)?,
        record.vector.as_ref(),
        serde_json::Value::Null,
    );
    Ok((serde_json::to_string(&record.id)?, json))
}

fn scored_point_row(point: &ScoredPoint) -> Result<Row> {
    let json = record_json(
        serde_json::to_value(point.id)?,
        serde_json::to_value(&point.payload)?,
        point.vector.as_ref(),
        serde_json::json!({ "score": point.score, "version": point.version }),
    );
    Ok((serde_json::to_string(&point.id)?, json))
}
