use std::sync::Arc;
use std::time::Duration;

use collection::events::{
    CollectionCreatedEvent, CollectionDeletedEvent, IndexCreatedEvent, SlowQueryEvent,
};
use segment::problems::unindexed_field;
use storage::dispatcher::Dispatcher;
use storage::issues_subscribers::{TooManyCollectionsSubscriber, UnindexedFieldSubscriber};

use crate::settings::Settings;

pub fn setup_subscribers(settings: &Settings, dispatcher: Arc<Dispatcher>) {
    settings
        .service
        .slow_query_secs
        .map(|secs| unindexed_field::SLOW_QUERY_THRESHOLD.set(Duration::from_secs_f32(secs)));

    let unindexed_subscriber = UnindexedFieldSubscriber;
    issues::broker::add_subscriber::<SlowQueryEvent>(Box::new(unindexed_subscriber));
    issues::broker::add_subscriber::<IndexCreatedEvent>(Box::new(unindexed_subscriber));
    issues::broker::add_subscriber::<CollectionDeletedEvent>(Box::new(unindexed_subscriber));

    let too_many_collections_subscriber = TooManyCollectionsSubscriber::new(dispatcher);
    issues::broker::add_subscriber::<CollectionCreatedEvent>(Box::new(
        too_many_collections_subscriber.clone(),
    ));
    issues::broker::add_subscriber::<CollectionDeletedEvent>(Box::new(
        too_many_collections_subscriber,
    ));
}
