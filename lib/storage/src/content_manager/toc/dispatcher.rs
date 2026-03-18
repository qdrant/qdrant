use std::sync::Weak;

use super::TableOfContent;
use crate::content_manager::consensus_manager::ConsensusStateRef;

#[derive(Clone)]
pub struct TocDispatcher {
    /// Reference to table of contents
    ///
    /// This dispatcher is stored inside the table of contents after construction. It therefore
    /// uses a weak reference to avoid a reference cycle which would prevent dropping the table of
    /// contents on exit.
    pub(super) toc: Weak<TableOfContent>,
    pub(super) consensus_state: ConsensusStateRef,
}

impl TocDispatcher {
    pub fn new(toc: Weak<TableOfContent>, consensus_state: ConsensusStateRef) -> Self {
        Self {
            toc,
            consensus_state,
        }
    }

    pub fn consensus_state(&self) -> &ConsensusStateRef {
        &self.consensus_state
    }
}
