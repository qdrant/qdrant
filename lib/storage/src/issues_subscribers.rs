use std::sync::Arc;

use collection::events::{CollectionDeletedEvent, IndexCreatedEvent, SlowQueryEvent};
use issues::broker::Subscriber;

use crate::content_manager::toc::TableOfContent;
use crate::dispatcher::Dispatcher;
use crate::rbac::Access;

#[allow(dead_code)] // TODO: remove
#[derive(Clone)]
pub struct UnindexedFieldSubscriber {
    toc: Arc<TableOfContent>,
    access: Access,
}

impl UnindexedFieldSubscriber {
    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        let access = Access::full("For Issues API");
        let toc = dispatcher.toc(&access).clone();
        Self { toc, access }
    }
}

impl Subscriber<SlowQueryEvent> for UnindexedFieldSubscriber {
    fn notify(&self, _event: Arc<SlowQueryEvent>) {
        // TODO: process filters and compare against the schema taken from collection info
    }
}

impl Subscriber<CollectionDeletedEvent> for UnindexedFieldSubscriber {
    fn notify(&self, _event: Arc<CollectionDeletedEvent>) {
        // TODO: solve all unindexed field issues related to the deleted collection
    }
}

impl Subscriber<IndexCreatedEvent> for UnindexedFieldSubscriber {
    fn notify(&self, _event: Arc<IndexCreatedEvent>) {
        // TODO: solve the missing index issue
    }
}
