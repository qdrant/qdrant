use std::collections::TryReserveError;

pub trait TrySetCapacity {
    fn try_set_capacity(&mut self, capacity: usize) -> Result<(), TryReserveError>;
}

pub trait TrySetCapacityExact {
    fn try_set_capacity_exact(&mut self, capacity: usize) -> Result<(), TryReserveError>;
}

impl<T> TrySetCapacity for Vec<T> {
    fn try_set_capacity(&mut self, capacity: usize) -> Result<(), TryReserveError> {
        let current_capacity = self.capacity();
        let additional_capacity = capacity.saturating_sub(current_capacity);
        self.try_reserve(additional_capacity)
    }
}

impl<T> TrySetCapacityExact for Vec<T> {
    fn try_set_capacity_exact(&mut self, capacity: usize) -> Result<(), TryReserveError> {
        let current_capacity = self.capacity();
        let additional_capacity = capacity.saturating_sub(current_capacity);
        self.try_reserve_exact(additional_capacity)
    }
}
