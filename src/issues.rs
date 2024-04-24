use std::sync::Arc;

use collection::events::{CollectionDeletedEvent, IndexCreatedEvent, SlowQueryEvent};
use storage::{dispatcher::Dispatcher, issues_subscribers::UnindexedFieldSubscriber};

pub fn setup_subscribers(dispatcher: Arc<Dispatcher>) {
    let unindexed_subscriber = UnindexedFieldSubscriber::new(dispatcher);

    issues::broker::add_subscriber::<SlowQueryEvent>(Box::new(unindexed_subscriber.clone()));
    issues::broker::add_subscriber::<IndexCreatedEvent>(Box::new(unindexed_subscriber.clone()));
    issues::broker::add_subscriber::<CollectionDeletedEvent>(Box::new(unindexed_subscriber));
}