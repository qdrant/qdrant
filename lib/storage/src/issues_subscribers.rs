use std::sync::Arc;

use collection::events::{
    CollectionCreatedEvent, CollectionDeletedEvent, IndexCreatedEvent, SlowQueryEvent,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::CountRequestInternal;
use collection::problems::TooManyCollections;
use issues::broker::Subscriber;
use issues::Code;
use segment::problems::UnindexedField;

use crate::content_manager::toc::TableOfContent;
use crate::dispatcher::Dispatcher;
use crate::rbac::Access;

#[derive(Clone, Copy)]
pub struct UnindexedFieldSubscriber;

impl Subscriber<SlowQueryEvent> for UnindexedFieldSubscriber {
    fn notify(&self, event: Arc<SlowQueryEvent>) {
        if event.filters.is_empty() {
            return;
        }

        for filter in &event.filters {
            segment::problems::UnindexedField::submit_possible_suspects(
                filter,
                &event.schema,
                event.collection_id.clone(),
            )
        }
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

#[derive(Clone)]
pub struct TooManyCollectionsSubscriber {
    toc: Arc<TableOfContent>,
}

impl TooManyCollectionsSubscriber {
    pub const FULL_ACCESS: Access = Access::full("For checking collections counts");

    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            toc: dispatcher.toc(&Self::FULL_ACCESS).clone(),
        }
    }

    async fn collection_sizes(toc: Arc<TableOfContent>) -> Vec<usize> {
        let collection_passes = toc.all_collections(&Self::FULL_ACCESS).await;

        let mut collection_sizes = Vec::with_capacity(collection_passes.len());
        for collection in collection_passes {
            let Ok(collection) = toc.get_collection(&collection).await else {
                continue;
            };

            let count_request = CountRequestInternal {
                filter: None,
                exact: false,
            };
            let Ok(result) = collection
                .count(count_request, None, &ShardSelectorInternal::All)
                .await
            else {
                continue;
            };

            collection_sizes.push(result.count);
        }

        collection_sizes
    }
}

impl Subscriber<CollectionDeletedEvent> for TooManyCollectionsSubscriber {
    fn notify(&self, _event: Arc<CollectionDeletedEvent>) {
        let toc = Arc::clone(&self.toc);
        tokio::spawn(async move {
            let collection_sizes = Self::collection_sizes(toc).await;

            TooManyCollections::solve_if_sane_amount_of_collections(collection_sizes).await
        });
    }
}

impl Subscriber<CollectionCreatedEvent> for TooManyCollectionsSubscriber {
    fn notify(&self, _event: Arc<CollectionCreatedEvent>) {
        let toc = Arc::clone(&self.toc);
        tokio::spawn(async move {
            let collection_sizes = Self::collection_sizes(toc).await;

            TooManyCollections::submit_if_too_many_collections(collection_sizes).await
        });
    }
}
