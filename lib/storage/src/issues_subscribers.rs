use std::sync::Arc;

use collection::events::{CollectionDeletedEvent, IndexCreatedEvent, SlowQueryEvent};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use issues::broker::Subscriber;
use issues::Code;
use segment::problems::UnindexedField;
use segment::types::PayloadFieldSchema;

use crate::content_manager::toc::TableOfContent;
use crate::dispatcher::Dispatcher;
use crate::rbac::{Access, AccessRequirements};

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

            let Ok(collection_info) = collection.info(&ShardSelectorInternal::All).await else {
                // Don't keep processing
                return;
            };

            // Extract payload schema from collection info
            let payload_schema = collection_info
                .payload_schema
                .into_iter()
                .filter_map(|(field_name, schema)| {
                    PayloadFieldSchema::try_from(schema)
                        .ok()
                        .map(|schema| (field_name, schema))
                })
                .collect();

            for filter in &event.filters {
                segment::problems::UnindexedField::submit_possible_suspects(
                    filter,
                    &payload_schema,
                    event.collection_id.clone(),
                )
            }
        });
    }
}

impl Subscriber<CollectionDeletedEvent> for UnindexedFieldSubscriber {
    fn notify(&self, event: Arc<CollectionDeletedEvent>) {
        issues::solve_by_filter::<UnindexedField, _>(|code| {
            UnindexedField::get_collection_name(code) == event.collection_id
        });
    }
}

impl Subscriber<IndexCreatedEvent> for UnindexedFieldSubscriber {
    fn notify(&self, event: Arc<IndexCreatedEvent>) {
        issues::solve(Code::new::<UnindexedField>(
            UnindexedField::get_instance_id(&event.collection_id, &event.field_name),
        ));
    }
}
