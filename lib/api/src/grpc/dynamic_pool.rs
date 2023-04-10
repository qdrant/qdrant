use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;

#[derive(Debug)]
struct ItemWithStats<T: Clone> {
    pub item: T,
    pub usage: AtomicUsize,
    pub last_success: AtomicUsize,
}

impl<T: Clone> ItemWithStats<T> {
    fn new(item: T, last_used_since: usize) -> Self {
        Self {
            item,
            usage: AtomicUsize::new(0),
            last_success: AtomicUsize::new(last_used_since),
        }
    }
}

pub struct DynamicPool<T: Clone> {
    items: HashMap<u64, Arc<ItemWithStats<T>>>,
    /// How many times one item can be used
    max_usage_per_item: usize,
    /// Minimal number of items in the pool
    min_items: usize,
    /// Instant when the pool was created
    init_at: Instant,
}

pub struct CountedItem<T: Clone> {
    item: Arc<ItemWithStats<T>>,
    item_id: u64,
    init_at: Instant,
}

impl<T: Clone> CountedItem<T> {
    fn new(item_id: u64, item: Arc<ItemWithStats<T>>, init_at: Instant) -> Self {
        item.usage.fetch_add(1, Ordering::Relaxed);
        Self {
            item,
            item_id,
            init_at,
        }
    }

    pub fn item(&self) -> &T {
        &self.item.item
    }

    pub fn report_success(&self) {
        let time_since_init = Instant::now().duration_since(self.init_at).as_millis() as usize;
        self.item
            .last_success
            .store(time_since_init, Ordering::Relaxed);
    }

    pub fn last_success_age(&self) -> Duration {
        let time_since_init = Instant::now().duration_since(self.init_at).as_millis() as usize;
        let time_since_last_success = self.item.last_success.load(Ordering::Relaxed);
        Duration::from_millis((time_since_init - time_since_last_success) as u64)
    }
}

impl<T: Clone> Drop for CountedItem<T> {
    fn drop(&mut self) {
        self.item.usage.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<T: Clone> DynamicPool<T> {
    fn random_idx() -> u64 {
        rand::thread_rng().gen()
    }

    pub fn new(items: Vec<T>, max_usage_per_item: usize, min_items: usize) -> Self {
        debug_assert!(max_usage_per_item > 0);
        debug_assert!(items.len() >= min_items);
        let init_at = Instant::now();
        let last_success_since = Instant::now().duration_since(init_at).as_millis() as usize;
        let items = items
            .into_iter()
            .map(|item| {
                let item = Arc::new(ItemWithStats::new(item, last_success_since));
                (Self::random_idx(), item)
            })
            .collect();
        Self {
            items,
            max_usage_per_item,
            min_items,
            init_at,
        }
    }

    pub fn drop_item(&mut self, item: CountedItem<T>) {
        let item_id = item.item_id;
        self.items.remove(&item_id);
    }

    pub fn add(&mut self, item: T) -> CountedItem<T> {
        let item_with_stats = Arc::new(ItemWithStats::new(
            item,
            Instant::now().duration_since(self.init_at).as_millis() as usize,
        ));
        let item_id = Self::random_idx();
        self.items.insert(item_id, item_with_stats.clone());
        CountedItem::new(item_id, item_with_stats, self.init_at)
    }

    // Returns None if current capacity is not enough
    pub fn choose(&mut self) -> Option<CountedItem<T>> {
        if self.items.len() < self.min_items {
            return None;
        }

        let mut total_usage = 0;
        let mut min_usage = std::usize::MAX;
        let mut min_usage_idx = 0;
        for (idx, item) in self.items.iter() {
            let usage = item.usage.load(Ordering::Relaxed);
            total_usage += usage;
            if usage < min_usage {
                min_usage = usage;
                min_usage_idx = *idx;
            }
        }
        if min_usage >= self.max_usage_per_item {
            // All items are used too much, we can not use any of them.
            // Return None to indicate that we need to add a new item
            return None;
        }

        let current_usage_capacity = self.items.len().saturating_mul(self.max_usage_per_item);

        if current_usage_capacity.saturating_sub(total_usage)
            > self.max_usage_per_item.saturating_mul(2)
            && self.items.len() > self.min_items
        {
            // We have too many items, and we have enough capacity to remove some of them
            let item = self
                .items
                .remove(&min_usage_idx)
                .expect("Item must exist, as we just found it");
            return Some(CountedItem::new(min_usage_idx, item, self.init_at));
        }

        Some(CountedItem::new(
            min_usage_idx,
            self.items
                .get(&min_usage_idx)
                .expect("Item must exist, as we just found it")
                .clone(),
            self.init_at,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    async fn use_item(item: CountedItem<Arc<AtomicUsize>>) {
        item.item().fetch_add(1, Ordering::SeqCst);
        // Sleep for 1-100 ms
        tokio::time::sleep(std::time::Duration::from_millis(
            rand::random::<u64>() % 100 + 1,
        ))
        .await;
        item.item().fetch_sub(1, Ordering::SeqCst);
        item.report_success();
        drop(item);
    }

    #[test]
    fn test_dynamic_pool() {
        let items = vec![Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];

        let mut pool = DynamicPool::new(items, 5, 2);

        let mut items = vec![];

        for _ in 0..17 {
            let item = match pool.choose() {
                None => pool.add(Arc::new(AtomicUsize::new(0))),
                Some(it) => it,
            };

            item.item().fetch_add(1, Ordering::SeqCst);
            items.push(item);
        }

        assert_eq!(pool.items.len(), 4);

        for _ in 0..10 {
            items.pop();
        }

        for (idx, item) in pool.items.iter() {
            println!("{} -> {:?}", idx, item);
        }

        assert!(pool.choose().is_some());

        assert_eq!(pool.items.len(), 3);
    }

    #[test]
    fn test_dynamic_pool_with_runtime() {
        let items = vec![Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];

        let runtime = tokio::runtime::Runtime::new().unwrap();

        let mut pool = DynamicPool::new(items, 5, 2);

        let mut handles = vec![];
        for _ in 0..1000 {
            let item = match pool.choose() {
                None => pool.add(Arc::new(AtomicUsize::new(0))),
                Some(it) => it,
            };

            let handle = runtime.spawn(async move { use_item(item).await });

            handles.push(handle);

            // Sleep for 3 ms with std
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        runtime.block_on(async move {
            for handle in handles {
                handle.await.unwrap();
            }
        });

        pool.items.iter().for_each(|(_, item)| {
            assert_eq!(item.item.load(Ordering::SeqCst), 0);
            assert_eq!(item.usage.load(Ordering::SeqCst), 0);
        });

        assert!(pool.items.len() < 50);
    }
}
