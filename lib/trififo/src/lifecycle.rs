pub trait Lifecycle<K, V> {
    fn on_evict(&self, key: K, value: V);
}

#[derive(Default)]
pub struct NoLifecycle;

impl<K, V> Lifecycle<K, V> for NoLifecycle {
    fn on_evict(&self, _key: K, _value: V) {}
}
