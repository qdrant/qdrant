use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub struct DynamicPool<T: Clone> {
    items: Vec<T>,
    // How many items are currently in use
    usage: Vec<Arc<AtomicUsize>>,
    // How many times one item can be used
    max_usage_per_item: usize,
    // Minimal number of items in the pool
    min_items: usize,
}

pub struct CountedItem<T: Clone> {
    item: T,
    counter: Arc<AtomicUsize>,
}

impl<T: Clone> CountedItem<T> {
    fn new(item: T, counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self { item, counter }
    }

    pub fn item(&self) -> &T {
        &self.item
    }

    pub fn item_mut(&mut self) -> &mut T {
        &mut self.item
    }
}

impl<T: Clone> Drop for CountedItem<T> {
    fn drop(&mut self) {
        self.counter
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl<T: Clone> DynamicPool<T> {
    pub fn new(items: Vec<T>, max_usage_per_item: usize, min_items: usize) -> Self {
        debug_assert!(max_usage_per_item > 0);
        debug_assert!(items.len() >= min_items);
        let usages = items
            .iter()
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();
        Self {
            items,
            usage: usages,
            max_usage_per_item,
            min_items,
        }
    }

    pub fn add(&mut self, item: T) -> CountedItem<T> {
        self.items.push(item.clone());
        let usage = Arc::new(AtomicUsize::new(0));
        self.usage.push(usage.clone());
        CountedItem::new(item, usage)
    }

    // Returns None if current capacity is not enough
    pub fn choose(&mut self) -> Option<CountedItem<T>> {
        let mut total_usage = 0;
        let mut min_usage = std::usize::MAX;
        let mut min_usage_idx = 0;
        for (idx, usage) in self
            .usage
            .iter()
            .enumerate()
            .map(|(id, usage)| (id, usage.load(std::sync::atomic::Ordering::SeqCst)))
        {
            total_usage += usage;
            if usage < min_usage {
                min_usage = usage;
                min_usage_idx = idx;
            }
        }

        if min_usage >= self.max_usage_per_item {
            // All items are used too much, we can not use any of them.
            // Return None to indicate that we need to add a new item
            return None;
        }

        let current_usage_capacity = self.items.len() * self.max_usage_per_item;

        if current_usage_capacity.saturating_sub(total_usage) > self.max_usage_per_item * 2
            && self.items.len() > self.min_items
        {
            // We have too many items, and we have enough capacity to remove some of them
            let item = self.items.remove(min_usage_idx);
            let usage = self.usage.remove(min_usage_idx);
            return Some(CountedItem::new(item, usage));
        }

        Some(CountedItem::new(
            self.items[min_usage_idx].clone(),
            self.usage[min_usage_idx].clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn use_item(item: CountedItem<Arc<AtomicUsize>>) {
        item.item()
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Sleep for 1-100 ms
        tokio::time::sleep(std::time::Duration::from_millis(
            rand::random::<u64>() % 100 + 1,
        ))
        .await;
        item.item()
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
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

            item.item()
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            items.push(item);
        }

        assert_eq!(pool.items.len(), 4);

        for _ in 0..10 {
            items.pop();
        }

        for usage in pool.usage.iter() {
            println!("{}", usage.load(std::sync::atomic::Ordering::SeqCst));
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

        pool.items.iter().for_each(|item| {
            assert_eq!(item.load(std::sync::atomic::Ordering::SeqCst), 0);
        });

        pool.usage.iter().for_each(|usage| {
            assert_eq!(usage.load(std::sync::atomic::Ordering::SeqCst), 0);
        });

        assert!(pool.items.len() < 50);
    }
}
