use std::sync::Arc;

use collection::events::{CollectionDeletedEvent, IndexCreatedEvent, SlowQueryEvent};
use issues::broker::Subscriber;
use segment::problems::UnindexedField;

use crate::{
    content_manager::toc::TableOfContent,
    dispatcher::Dispatcher,
    rbac::{Access, AccessRequirements},
};

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
    fn notify(&self, event: Arc<SlowQueryEvent>) {
        if event.filters.is_empty() {
            return;
        }

        let toc = self.toc.clone();
        let access = self.access.clone();

        tokio::spawn(async move {
            let collection_name = event.collection_id.clone();
            let collection_pass = access
                .check_collection_access(&collection_name, AccessRequirements::new().manage())
                .unwrap(); // It is always full access

            let Ok(collection) = toc.get_collection(&collection_pass).await else {
                // Don't keep processing
                return;
            };

            let payload_schema = collection.payload_index_schema();

            for filter in &event.filters {
                segment::problems::UnindexedField::submit_possible_suspects(
                    filter,
                    &payload_schema.schema,
                    event.collection_id.clone(),
                )
            }
        });
    }
}

impl Subscriber<CollectionDeletedEvent> for UnindexedFieldSubscriber {
    fn notify(&self, event: Arc<CollectionDeletedEvent>) {
        issues::solve_by_filter::<UnindexedField, _>(|code| {
            UnindexedField::get_collection_name_from_code(code) == event.collection_id
        });
    }
}

impl Subscriber<IndexCreatedEvent> for UnindexedFieldSubscriber {
    fn notify(&self, event: Arc<IndexCreatedEvent>) {
        issues::solve(UnindexedField::get_code(
            &event.collection_id,
            &event.field_name,
        ));
    }
}
