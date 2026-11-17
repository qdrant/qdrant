//! Conversions from the edge request types into the internal request types the read path
//! executes, grouped by request type. Every conversion destructures both sides exhaustively:
//! a field added to an internal request type (or to an edge request type) fails compilation
//! here instead of being silently dropped.
//!
//! [`RetrieveRequest`](crate::RetrieveRequest), [`SearchMatrixRequest`](crate::SearchMatrixRequest)
//! and [`GroupRequest`](crate::GroupRequest) have no internal counterpart struct — the read path
//! consumes them directly — so they have no conversion module.

mod count;
mod facet;
mod query;
mod scroll;
mod search;
