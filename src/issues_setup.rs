use collection::events::{CollectionDeletedEvent, IndexCreatedEvent, SlowQueryEvent};
use storage::issues_subscribers::UnindexedFieldSubscriber;

pub fn setup_subscribers() {
    let unindexed_subscriber = UnindexedFieldSubscriber;

    issues::broker::add_subscriber::<SlowQueryEvent>(Box::new(unindexed_subscriber));
    issues::broker::add_subscriber::<IndexCreatedEvent>(Box::new(unindexed_subscriber));
    issues::broker::add_subscriber::<CollectionDeletedEvent>(Box::new(unindexed_subscriber));
}
