mod count;
mod facet;
mod matrix;
mod points;
mod query;
mod update_persisted;

/// A trait, that provides an interface for removing vectors and payloads from a structure.
/// This is useful for generalizing requests by stripping out vector-specific and payload-specific details,
/// and essentially making the structure much lighter.
///
/// It does create copy of the structure for all other fields except vectors.
/// Vectors are replaces with length indications, payloads are replaced with keys and length indications.
pub trait Generalizer {
    fn remove_details(&self) -> Self;
}
