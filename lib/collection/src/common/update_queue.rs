use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use shard::operations::UpdateType;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::mpsc::{self, Permit, Receiver, Sender};

use crate::shards::telemetry::UpdateQueueTelemetry;

/// A sender for the update queue. Keeps track of enqueued items, separated by update type.
pub struct UpdateQueueSender<T> {
    sender: Sender<T>,
    counter: Arc<AtomicUpdateQueueCounter>,
}

/// A receiver for the update queue. Keeps track of enqueued items, separated by update type.
pub struct UpdateQueueReceiver<T> {
    receiver: Receiver<T>,
    counter: Arc<AtomicUpdateQueueCounter>,
}

/// A mpsc::Permit wrapper for the update queue. Keeps track of enqueued items, separated by update type.
pub struct UpdateQueuePermit<'a, T> {
    permit: Option<Permit<'a, T>>,
    counter: &'a AtomicUsize,
}

/// Atomic counter for the update-queue's items, separated by update type.
#[derive(Default, Debug)]
pub struct AtomicUpdateQueueCounter {
    pub point_updates: AtomicUsize,
    pub vector_updates: AtomicUsize,
    pub index_updates: AtomicUsize,
    pub payload_updates: AtomicUsize,
    pub other: AtomicUsize, // Counter for e.g. signals
}

impl AtomicUpdateQueueCounter {
    /// Returns the corresponding atomic counter for the passed update type. A `None` update-type will return the special counter "other".
    pub fn get_counter(&self, update_type: Option<UpdateType>) -> &AtomicUsize {
        match update_type {
            Some(t) => match t {
                UpdateType::Point => &self.point_updates,
                UpdateType::Vector => &self.vector_updates,
                UpdateType::FieldIndex => &self.index_updates,
                UpdateType::Payload => &self.payload_updates,
            },
            None => &self.other,
        }
    }

    /// Converts the atomic queue counters to the corresponding telemetry type.
    pub fn to_telemetry(&self) -> UpdateQueueTelemetry {
        UpdateQueueTelemetry {
            point_updates: self.point_updates.load(Ordering::Relaxed),
            vector_updates: self.vector_updates.load(Ordering::Relaxed),
            index_updates: self.index_updates.load(Ordering::Relaxed),
            payload_updates: self.payload_updates.load(Ordering::Relaxed),
            other: self.other.load(Ordering::Relaxed),
        }
    }
}

/// Creates a new UpdateQueue channel, returning the sender and receiver similar to `mpsc::channel(..)`.
/// The atomic counters can be accessed via `UpdateQueueReceiver::counter()`.
pub fn new_update_queue_channel<T>(
    buffer: usize,
) -> (UpdateQueueSender<T>, UpdateQueueReceiver<T>) {
    let (tx, rx) = mpsc::channel(buffer);
    let counter = Arc::new(AtomicUpdateQueueCounter::default());
    (
        UpdateQueueSender {
            sender: tx,
            counter: counter.clone(),
        },
        UpdateQueueReceiver {
            receiver: rx,
            counter,
        },
    )
}

const COUNTER_STEP: usize = 1;

impl<T> UpdateQueueSender<T> {
    pub async fn send(
        &self,
        value: T,
        update_type: Option<UpdateType>,
    ) -> Result<(), SendError<T>> {
        self.sender.send(value).await?;
        self.increment_counter(update_type);
        Ok(())
    }

    pub fn blocking_send(
        &self,
        value: T,
        update_type: Option<UpdateType>,
    ) -> Result<(), SendError<T>> {
        self.sender.blocking_send(value)?;
        self.increment_counter(update_type);
        Ok(())
    }

    pub async fn reserve(
        &self,
        update_type: Option<UpdateType>,
    ) -> Result<UpdateQueuePermit<'_, T>, SendError<()>> {
        let permit = self.sender.reserve().await?;
        self.increment_counter(update_type);
        let counter = self.counter.get_counter(update_type);
        Ok(UpdateQueuePermit::new(permit, counter))
    }

    pub fn try_send(
        &self,
        value: T,
        update_type: Option<UpdateType>,
    ) -> Result<(), TrySendError<T>> {
        self.sender.try_send(value)?;
        self.increment_counter(update_type);
        Ok(())
    }

    fn increment_counter(&self, update_type: Option<UpdateType>) {
        self.counter
            .get_counter(update_type)
            .fetch_add(COUNTER_STEP, Ordering::Relaxed);
    }
}

impl<T> UpdateQueueReceiver<T> {
    pub async fn recv<F>(&mut self, to_update_type: F) -> Option<T>
    where
        F: Fn(&T) -> Option<UpdateType>,
    {
        let val = self.receiver.recv().await?;
        self.decrement_counter(to_update_type(&val));
        Some(val)
    }

    fn decrement_counter(&self, update_type: Option<UpdateType>) {
        self.counter
            .get_counter(update_type)
            .fetch_sub(COUNTER_STEP, Ordering::Relaxed);
    }

    /// Returns the atomic counters of the update queue.
    pub fn counter(&self) -> &Arc<AtomicUpdateQueueCounter> {
        &self.counter
    }
}

impl<'a, T> UpdateQueuePermit<'a, T> {
    fn new(permit: Permit<'a, T>, counter: &'a AtomicUsize) -> Self {
        Self {
            permit: Some(permit),
            counter,
        }
    }

    pub fn send(mut self, value: T) {
        self.permit.take().unwrap().send(value);
    }
}

impl<'a, T> Drop for UpdateQueuePermit<'a, T> {
    fn drop(&mut self) {
        // Decrement the counter if our permit wasn't used.
        if self.permit.is_some() {
            self.counter.fetch_sub(COUNTER_STEP, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod test {
    use tokio::task::spawn_blocking;

    use super::*;

    #[tokio::test]
    async fn test_update_queue() {
        let (send, mut rec) = new_update_queue_channel::<UpdateType>(1);

        let to_update_type = |i: &UpdateType| -> Option<UpdateType> { Some(*i) };

        let counter = rec.counter().clone();

        // Send
        send.send(UpdateType::FieldIndex, Some(UpdateType::FieldIndex))
            .await
            .unwrap();
        assert_eq!(counter.index_updates.load(Ordering::Relaxed), 1);

        let item = rec.recv(to_update_type).await.unwrap();
        assert_eq!(item, UpdateType::FieldIndex);
        assert_eq!(counter.index_updates.load(Ordering::Relaxed), 0);

        // Try Send
        send.try_send(UpdateType::Vector, Some(UpdateType::Vector))
            .unwrap();
        assert_eq!(counter.vector_updates.load(Ordering::Relaxed), 1);

        let item = rec.recv(to_update_type).await.unwrap();
        assert_eq!(item, UpdateType::Vector);
        assert_eq!(counter.vector_updates.load(Ordering::Relaxed), 0);

        // Permit
        let permit = send.reserve(Some(UpdateType::Point)).await.unwrap();
        assert_eq!(counter.point_updates.load(Ordering::Relaxed), 1);
        drop(permit);
        assert_eq!(counter.point_updates.load(Ordering::Relaxed), 0);

        let permit = send.reserve(Some(UpdateType::Point)).await.unwrap();
        assert_eq!(counter.point_updates.load(Ordering::Relaxed), 1);
        permit.send(UpdateType::Point);
        assert_eq!(counter.point_updates.load(Ordering::Relaxed), 1);

        let item = rec.recv(to_update_type).await.unwrap();
        assert_eq!(item, UpdateType::Point);
        assert_eq!(counter.point_updates.load(Ordering::Relaxed), 0);

        // Blocking send
        spawn_blocking(move || {
            send.blocking_send(UpdateType::FieldIndex, Some(UpdateType::FieldIndex))
                .unwrap();
        })
        .await
        .unwrap();
        assert_eq!(counter.index_updates.load(Ordering::Relaxed), 1);

        let item = rec.recv(to_update_type).await.unwrap();
        assert_eq!(item, UpdateType::FieldIndex);
        assert_eq!(counter.index_updates.load(Ordering::Relaxed), 0);
    }
}
