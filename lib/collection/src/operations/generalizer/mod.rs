mod filter;
mod placeholders;
mod query;
mod count;
mod facet;
mod points;
mod matrix;
mod query_group;
mod update;

#[derive(Debug, Clone, Copy)]
pub enum GeneralizationLevel {
    /// In this mode only vector-specific details are stripped from the request.
    /// For example, in search request the actual vector values are removed, but other details like
    /// filter conditions, top, etc are preserved.
    OnlyVector,

    /// In this mode all request-specific details are stripped, leaving only the operation type
    /// and general structure.
    /// For example, exact values of filters, top, etc are replaced with generalized placeholders.
    VectorAndValues,
}

/// This trait provides an interface for generalizing Requests to Qdrant into a more abstract representation.
/// For example, search request should be stripped of all vector-specific details and represented as a generic "search" operation.
/// Desired level of detalization is up to each Request type implementation.
///
/// This generalized representation is supposed to be used for logging and performance analysis purposes.
pub trait Generalizer {
    fn generalize(&self, level: GeneralizationLevel) -> serde_json::Value;
}
