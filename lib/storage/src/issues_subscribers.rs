use std::sync::Arc;

use collection::events::{CollectionDeletedEvent, IndexCreatedEvent, SlowQueryEvent};
use issues::broker::Subscriber;
use issues::Code;
use segment::problems::UnindexedField;

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
