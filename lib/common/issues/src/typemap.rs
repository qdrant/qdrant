use std::any::{Any, TypeId};
use std::collections::HashMap;

pub struct TypeMap(HashMap<TypeId, Box<dyn Any + Send + Sync>>);
impl TypeMap {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn has<T: 'static>(&self) -> bool {
        self.0.contains_key(&TypeId::of::<T>())
    }

    pub fn insert<T: 'static + Send + Sync>(&mut self, value: T) {
        self.0.insert(TypeId::of::<T>(), Box::new(value));
    }

    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.0
            .get(&TypeId::of::<T>())
            .map(|value| value.downcast_ref().unwrap())
    }

    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.0
            .get_mut(&TypeId::of::<T>())
            .map(|value| value.downcast_mut().unwrap())
    }
}
