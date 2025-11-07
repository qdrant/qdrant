use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use shard::operations::UpdateType;
use tokio::sync::mpsc::{
    self, Permit, Receiver, Sender,
    error::{SendError, TrySendError},
};

use crate::shards::telemetry::UpdateQueueCounter;

pub struct UpdateQueueSender<T> {
    sender: Sender<T>,
    counter: Arc<AtomicUpdateQueueCounter>,
}

pub struct UpdateQueueReceiver<T> {
    receiver: Receiver<T>,
    counter: Arc<AtomicUpdateQueueCounter>,
}

pub struct UpdateQueuePermit<'a, T> {
    permit: Option<Permit<'a, T>>,
    counter: &'a AtomicUsize,
}

#[derive(Default, Debug)]
pub struct AtomicUpdateQueueCounter {
    pub point_updates: AtomicUsize,
    pub vector_updates: AtomicUsize,
    pub index_updates: AtomicUsize,
    pub payload_updates: AtomicUsize,
    pub other: AtomicUsize,
}

impl AtomicUpdateQueueCounter {
    pub fn get_counter(&self, update_type: Option<UpdateType>) -> &AtomicUsize {
        match update_type {
            Some(t) => match t {
                UpdateType::Point => &self.point_updates,
                UpdateType::Vector => &self.vector_updates,
                UpdateType::Index => &self.index_updates,
                UpdateType::Payload => &self.payload_updates,
            },
            None => &self.other,
        }
    }

    pub fn to_telemetry(&self) -> UpdateQueueCounter {
        UpdateQueueCounter {
            point_updates: self.point_updates.load(Ordering::Relaxed),
            vector_updates: self.vector_updates.load(Ordering::Relaxed),
            index_updates: self.index_updates.load(Ordering::Relaxed),
            payload_updates: self.payload_updates.load(Ordering::Relaxed),
            other: self.other.load(Ordering::Relaxed),
        }
    }
}

// #[derive(Default, Debug)]
// pub struct UpdateQueueCounter {
//     pub point_updates: usize,
//     pub vector_updates: usize,
//     pub index_updates: usize,
//     pub payload_updates: usize,
//     pub other: usize,
// }

// impl UpdateQueueCounter {
//     pub fn total(&self) -> usize {
//         let UpdateQueueCounter {
//             point_updates,
//             vector_updates,
//             index_updates,
//             payload_updates,
//             other,
//         } = self;
//         point_updates + vector_updates + index_updates + payload_updates + other
//     }
// }

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

impl<T> UpdateQueueSender<T> {
    pub async fn send(
        &self,
        value: T,
        update_type: Option<UpdateType>,
    ) -> Result<(), SendError<T>> {
        self.sender.send(value).await?;
        self.counter
            .get_counter(update_type)
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn blocking_send(
        &self,
        value: T,
        update_type: Option<UpdateType>,
    ) -> Result<(), SendError<T>> {
        self.sender.blocking_send(value)?;
        self.counter
            .get_counter(update_type)
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn reserve(
        &self,
        update_type: Option<UpdateType>,
    ) -> Result<UpdateQueuePermit<'_, T>, SendError<()>> {
        let permit = self.sender.reserve().await?;
        let counter = self.counter.get_counter(update_type);
        counter.fetch_add(1, Ordering::Relaxed);
        Ok(UpdateQueuePermit::new(permit, counter))
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(value)
    }
}

impl<T> UpdateQueueReceiver<T> {
    pub async fn recv<F>(&mut self, to_update_type: F) -> Option<T>
    where
        F: Fn(&T) -> Option<UpdateType>,
    {
        let val = self.receiver.recv().await?;
        let update_type = to_update_type(&val);
        self.counter
            .get_counter(update_type)
            .fetch_sub(1, Ordering::Relaxed);
        Some(val)
    }

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
        if self.permit.is_some() {
            self.counter.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
