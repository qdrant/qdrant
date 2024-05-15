use std::sync::{Arc, OnceLock, RwLock};

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

    /// Notify all subscribers of the event. This method will block until all subscribers have handled the event, subscribers can choose if they want to handle the event in the background (non-blocking).
    pub fn publish<E: 'static>(&self, event: E) {
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

fn broker() -> Arc<RwLock<EventBroker>> {
    static BROKER: OnceLock<Arc<RwLock<EventBroker>>> = OnceLock::new();
    BROKER
        .get_or_init(|| Arc::new(RwLock::new(EventBroker::default())))
        .clone()
}

pub fn publish<E: 'static>(event: E) {
    // This will only read if the lock is not poisoned
    if let Ok(guard) = broker().read() {
        guard.publish(event)
    }
}

pub fn add_subscriber<E: 'static>(subscriber: Box<dyn Subscriber<E> + Send + Sync>) {
    // This will only write if the lock is not poisoned
    if let Ok(mut guard) = broker().write() {
        guard.add_subscriber(subscriber);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::Dashboard;
    use crate::issue::DummyIssue;

    #[derive(Clone)]
    struct DummySubscriber {
        dashboard: Arc<Dashboard>,
    }

    struct DummyEvent {
        pub collection_id: String,
    }

    impl Subscriber<DummyEvent> for DummySubscriber {
        fn notify(&self, event: Arc<DummyEvent>) {
            let issue = DummyIssue::new(event.collection_id.clone());
            self.dashboard.add_issue(issue);
        }
    }

    struct ClearAllIssuesEvent;

    impl Subscriber<ClearAllIssuesEvent> for DummySubscriber {
        fn notify(&self, _event: Arc<ClearAllIssuesEvent>) {
            self.dashboard.issues.clear();
        }
    }

    #[test]
    fn test_basic_use() {
        let mut broker = EventBroker::new();

        let test_dashboard = Arc::new(Dashboard::default());

        let subscriber = DummySubscriber {
            dashboard: test_dashboard.clone(),
        };

        broker.add_subscriber::<DummyEvent>(Box::new(subscriber.clone()));
        broker.add_subscriber::<ClearAllIssuesEvent>(Box::new(subscriber));

        broker.publish(DummyEvent {
            collection_id: "dummy".to_string(),
        });

        assert!(test_dashboard
            .get_all_issues()
            .iter()
            .any(|issue| issue.id == "DUMMY/dummy"));

        broker.publish(ClearAllIssuesEvent);

        assert!(
            test_dashboard
                .get_all_issues()
                .iter()
                .all(|issue| issue.id != "DUMMY/dummy"),
            "{:?}",
            test_dashboard.get_all_issues()
        );
    }
}
