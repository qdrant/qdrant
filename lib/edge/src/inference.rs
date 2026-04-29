//! Document → vector resolution at the edge boundary.
//!
//! [`Inference`] holds a registry of [`ModelResolver`]s keyed by model name.
//! When the user submits a [`Document`](crate::Document), the edge layer looks
//! up the corresponding resolver and produces a concrete [`Vector`] before any
//! shard- or segment-level code runs.
//!
//! Local resolvers (in-process embedding models) and remote resolvers
//! (HTTP-based inference services) share the same trait, so callers don't
//! distinguish between them — only the registered backend differs.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::vectors::VectorInternal;
use shard::operations::point_ops::{PointStructPersisted, VectorPersisted, VectorStructPersisted};

use crate::Vector;
use crate::bm25_embed::{EdgeBm25, EdgeBm25Config};
use crate::types::{
    Document, DocumentOptions, EdgePoint, NamedVectorInput, PointVectorsInput,
};

/// Backend that turns a [`Document`] into a concrete [`Vector`].
///
/// Both local in-process models and remote inference services implement this
/// trait so the edge layer can dispatch on model name without caring where
/// the model runs. The query/document split lets BM25-style scorings apply
/// the right weighting (TF for documents, unit for queries).
pub trait ModelResolver: Send + Sync + fmt::Debug {
    /// Embed for indexed-document use (e.g. upsert): TF-weighted for BM25.
    fn embed_document(&self, doc: &Document) -> OperationResult<Vector>;
    /// Embed for query use: unit-weighted for BM25.
    fn embed_query(&self, doc: &Document) -> OperationResult<Vector>;
}

/// Registry of [`ModelResolver`]s keyed by model name.
#[derive(Default, Clone, Debug)]
pub struct Inference {
    resolvers: HashMap<String, Arc<dyn ModelResolver>>,
}

impl Inference {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a resolver under the given model name. Replaces any previous
    /// resolver registered with the same name.
    pub fn register(&mut self, name: impl Into<String>, resolver: Arc<dyn ModelResolver>) {
        self.resolvers.insert(name.into(), resolver);
    }

    /// Whether any resolver is registered. Used as a fast path to skip
    /// document-walking when no inference is configured.
    pub fn is_empty(&self) -> bool {
        self.resolvers.is_empty()
    }

    /// Resolve a document for query use (unit-weighted for BM25).
    pub fn resolve_query(&self, doc: &Document) -> OperationResult<Vector> {
        self.lookup(&doc.model)?.embed_query(doc)
    }

    /// Resolve a document for indexed-document use (TF-weighted for BM25).
    pub fn resolve_document(&self, doc: &Document) -> OperationResult<Vector> {
        self.lookup(&doc.model)?.embed_document(doc)
    }

    fn lookup(&self, model: &str) -> OperationResult<&Arc<dyn ModelResolver>> {
        self.resolvers.get(model).ok_or_else(|| {
            OperationError::validation_error(format!(
                "no inference model registered for '{model}'",
            ))
        })
    }

    /// Resolve an [`EdgePoint`] (which may carry [`Document`] inputs) into the
    /// already-embedded form expected by the shard layer.
    pub fn resolve_point(&self, point: EdgePoint) -> OperationResult<PointStructPersisted> {
        let EdgePoint {
            id,
            vectors,
            payload,
        } = point;

        let vector = match vectors {
            PointVectorsInput::SingleDense(v) => VectorStructPersisted::Single(v),
            PointVectorsInput::MultiDense(v) => VectorStructPersisted::MultiDense(v),
            PointVectorsInput::Named(named) => {
                let mut out = HashMap::with_capacity(named.len());
                for (name, input) in named {
                    let v = match input {
                        NamedVectorInput::Vector(v) => vector_internal_to_persisted(v),
                        NamedVectorInput::Document(doc) => {
                            let resolved = self.resolve_document(&doc)?;
                            vector_internal_to_persisted(resolved.0)
                        }
                    };
                    out.insert(name, v);
                }
                VectorStructPersisted::Named(out)
            }
        };

        Ok(PointStructPersisted {
            id,
            vector,
            payload,
        })
    }
}

fn vector_internal_to_persisted(v: VectorInternal) -> VectorPersisted {
    match v {
        VectorInternal::Dense(d) => VectorPersisted::Dense(d),
        VectorInternal::Sparse(s) => VectorPersisted::Sparse(s),
        VectorInternal::MultiDense(m) => VectorPersisted::MultiDense(m.into_multi_vectors()),
    }
}

/// Local BM25 resolver.
#[derive(Debug)]
pub struct Bm25Resolver {
    model: EdgeBm25,
}

impl Bm25Resolver {
    pub fn new(config: EdgeBm25Config) -> Self {
        Self {
            model: EdgeBm25::new(config),
        }
    }
}

impl Bm25Resolver {
    /// Per-call BM25 overrides via `DocumentOptions::Bm25` would require
    /// building a fresh model — out of scope for the registered fast path.
    /// Reject explicitly so callers don't silently get the registered
    /// model's params instead of their override.
    fn check_options(doc: &Document) -> OperationResult<()> {
        if let Some(DocumentOptions::Bm25(_)) = doc.options.as_ref() {
            return Err(OperationError::validation_error(
                "per-document BM25 option overrides are not yet supported; \
                 register a separate model under a different name instead",
            ));
        }
        Ok(())
    }
}

impl ModelResolver for Bm25Resolver {
    fn embed_document(&self, doc: &Document) -> OperationResult<Vector> {
        Self::check_options(doc)?;
        let bm25_doc = bm25::Bm25Document::new(&doc.text);
        Ok(Vector::from(self.model.embed_document(&bm25_doc)))
    }

    fn embed_query(&self, doc: &Document) -> OperationResult<Vector> {
        Self::check_options(doc)?;
        let bm25_doc = bm25::Bm25Document::new(&doc.text);
        Ok(Vector::from(self.model.embed_query(&bm25_doc)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EdgePoint, NamedVectorInput, PointVectorsInput};

    fn registry() -> Inference {
        let mut inf = Inference::new();
        inf.register(
            "qdrant/bm25",
            Arc::new(Bm25Resolver::new(EdgeBm25Config::default())),
        );
        inf
    }

    #[test]
    fn resolve_point_with_document() {
        let inf = registry();
        let mut named = HashMap::new();
        named.insert(
            "text".to_string(),
            NamedVectorInput::Document(Document::new("hello world", "qdrant/bm25")),
        );
        let p = EdgePoint::new(1u64, PointVectorsInput::Named(named), None);

        let resolved = inf.resolve_point(p).expect("resolution succeeds");
        match resolved.vector {
            VectorStructPersisted::Named(map) => {
                let v = map.get("text").expect("text slot present");
                match v {
                    VectorPersisted::Sparse(sv) => assert!(!sv.indices.is_empty()),
                    other => panic!("expected sparse, got {other:?}"),
                }
            }
            other => panic!("expected named, got {other:?}"),
        }
    }

    #[test]
    fn unknown_model_errors() {
        let inf = registry();
        let mut named = HashMap::new();
        named.insert(
            "text".to_string(),
            NamedVectorInput::Document(Document::new("hi", "no-such-model")),
        );
        let p = EdgePoint::new(1u64, PointVectorsInput::Named(named), None);
        let err = inf.resolve_point(p).unwrap_err();
        assert!(err.to_string().contains("no inference model"));
    }
}
