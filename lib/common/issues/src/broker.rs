use std::sync::Arc;

use tokio::task;

use crate::typemap::TypeMap;

pub trait Subscriber<E> {
    fn notify(&self, event: Arc<E>);
}

struct SubscriberMap(TypeMap);

type DynSubscriber<E> = Box<dyn Subscriber<E> + Send + Sync>;
type SubscriVec<E> = Vec<Arc<DynSubscriber<E>>>;

impl SubscriberMap {
    fn new() -> Self {
        Self(TypeMap::new())
    }
    fn push<E: 'static>(&mut self, subscriber: DynSubscriber<E>) {
        if !self.0.has::<SubscriVec<E>>() {
            self.0.insert(SubscriVec::<E>::new());
        }
        let sub = Arc::new(subscriber);

        self.0.get_mut::<SubscriVec<E>>().unwrap().push(sub);
    }

    fn get<E: 'static>(&self) -> Option<&SubscriVec<E>> {
        self.0.get()
    }
}

pub struct EventBroker {
    subscriptions: SubscriberMap,
}

impl EventBroker {
    pub fn new() -> Self {
        Self {
            subscriptions: SubscriberMap::new(),
        }
    }
    pub fn add_subscriber<E: 'static>(&mut self, subscriber: DynSubscriber<E>) {
        self.subscriptions.push(subscriber);
    }

    /// Notify all subscribers of the event. Spawns one tokio task per subscriber, so that it can be completed in the background.
    pub fn notify_async<E: Send + Sync + 'static>(&self, event: E) {
        if let Some(subscribers) = self.subscriptions.get::<E>() {
            let event = Arc::new(event);
            for sub in subscribers {
                let event = event.clone();
                let sub = sub.clone();
                task::spawn(async move { sub.notify(event) });
            }
        }
    }

    /// Notify all subscribers of the event. This method will block until all subscribers have handled the event.
    pub fn notify<E: 'static>(&self, event: E) {
        if let Some(subscribers) = self.subscriptions.get::<E>() {
            let event = Arc::new(event);
            for sub in subscribers {
                sub.notify(event.clone());
            }
        }
    }
}

impl Default for EventBroker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::issue::DummyIssue;

    struct DummySubscriber;

    struct DummyEvent {
        pub collection_id: String,
    }

    impl Subscriber<DummyEvent> for DummySubscriber {
        fn notify(&self, event: Arc<DummyEvent>) {
            let issue = DummyIssue::new(event.collection_id.clone());
            crate::submit(issue);
        }
    }

    struct CollectionDeletedEvent {
        collection_id: String,
    }

    impl Subscriber<CollectionDeletedEvent> for DummySubscriber {
        fn notify(&self, event: Arc<CollectionDeletedEvent>) {
            crate::solve_by_filter::<DummyIssue, _>(|code| code.distinctive == event.collection_id);
        }
    }

    #[test]
    fn test_basic_use() {
        let mut broker = EventBroker::new();

        broker.add_subscriber::<DummyEvent>(Box::new(DummySubscriber));
        broker.add_subscriber::<CollectionDeletedEvent>(Box::new(DummySubscriber));

        broker.notify(DummyEvent {
            collection_id: "dummy".to_string(),
        });

        assert!(crate::all_issues()
            .iter()
            .any(|issue| issue.id == "DUMMY/dummy"));

        broker.notify(CollectionDeletedEvent {
            collection_id: "dummy".to_string(),
        });

        assert!(
            crate::all_issues()
                .iter()
                .all(|issue| issue.id != "DUMMY/dummy"),
            "{:?}",
            crate::all_issues()
        );
    }
}
