//! Compare the shadow state against the state `TableOfContent` holds

use std::mem;

use crate::content_manager::consensus_state_machine::{ApplyOutcome, ClusterState};
use crate::content_manager::errors::StorageResult;

/// How `shadow` differs from `actual`, `None` when the two match.
///
/// Both sides are printed whole. The derived `PartialEq` covers a field added to
/// [`ClusterState`] or to the collection state under it without anyone having to remember it.
/// Naming the fields that differ, rather than printing everything, is worth doing once a field
/// turns up that consensus does not decide and the compare has to skip.
pub fn cluster(shadow: &ClusterState, actual: &ClusterState) -> Option<String> {
    if shadow == actual {
        return None;
    }

    Some(format!("shadow {shadow:#?} against applied {actual:#?}"))
}

/// How the machine's decision differs from what the apply path answered.
///
/// Only the class of the answer is compared. A rejection message reaches the client, so the
/// machine reproduces the wording of the handler it replaces, but a difference in wording is
/// for the soak to collect rather than for this to report.
pub fn outcome(shadow: &ApplyOutcome, actual: &StorageResult<bool>) -> Option<String> {
    match (shadow, actual) {
        (ApplyOutcome::Accepted(_), Ok(_)) => None,

        (ApplyOutcome::Rejected(shadow), Err(actual))
            if mem::discriminant(shadow) == mem::discriminant(actual) =>
        {
            None
        }

        (ApplyOutcome::Accepted(_), Err(actual)) => {
            Some(format!("machine accepted, apply rejected it: {actual}"))
        }

        (ApplyOutcome::Rejected(shadow), Ok(_)) => Some(format!(
            "machine rejected it with `{shadow}`, apply accepted"
        )),

        (ApplyOutcome::Rejected(shadow), Err(actual)) => Some(format!(
            "machine and apply rejected it differently: `{shadow}` against `{actual}`"
        )),

        // Caller invalidates the machine rather than comparing
        (ApplyOutcome::NotCovered, Ok(_) | Err(_)) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use segment::types::PayloadSchemaType;

    use super::*;
    use crate::content_manager::consensus_state_machine::tests::collection_state;

    const COLLECTION: &str = "books";

    #[test]
    fn cluster_match() {
        let state = cluster_state();

        assert_eq!(cluster(&state, &state), None);
    }

    /// A collection both sides hold, differing inside. The compare reads collection state, not
    /// just the names.
    #[test]
    fn cluster_collection_differs() {
        let field = "city".parse().expect("valid field name");

        let mut actual = cluster_state();
        actual
            .collections
            .get_mut(COLLECTION)
            .expect("collection exists")
            .payload_index_schema
            .schema
            .insert(field, PayloadSchemaType::Keyword.into());

        assert!(cluster(&cluster_state(), &actual).is_some());
    }

    fn cluster_state() -> ClusterState {
        ClusterState {
            collections: HashMap::from([(COLLECTION.to_string(), collection_state(Vec::new()))]),
            ..Default::default()
        }
    }
}
