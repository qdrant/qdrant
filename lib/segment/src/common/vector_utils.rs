use std::collections::TryReserveError;

pub trait TrySetCapacity {
    fn try_set_capacity(&mut self, capacity: usize) -> Result<(), TryReserveError>;
}

pub trait TrySetCapacityExact {
    fn try_set_capacity_exact(&mut self, capacity: usize) -> Result<(), TryReserveError>;
}

impl<T> TrySetCapacity for Vec<T> {
    fn try_set_capacity(&mut self, capacity: usize) -> Result<(), TryReserveError> {
        let additional = capacity.saturating_sub(self.len());
        self.try_reserve(additional)
    }
}

impl<T> TrySetCapacityExact for Vec<T> {
    fn try_set_capacity_exact(&mut self, capacity: usize) -> Result<(), TryReserveError> {
        let additional = capacity.saturating_sub(self.len());
        self.try_reserve_exact(additional)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_set_capacity() {
        // Constructing uses exact capacity
        let mut v = vec![1, 2, 3];
        assert_eq!(v.capacity(), 3);

        // Set capacity to 5, but it will double to 6
        v.try_set_capacity(5).unwrap();
        assert_eq!(v.capacity(), 6);

        // Setting capacity again does nothing
        v.try_set_capacity(5).unwrap();
        assert_eq!(v.capacity(), 6);

        // Fill up to capacity
        v.extend([4, 5, 6]);
        assert_eq!(v.capacity(), 6);

        // Push over capacity will double it
        v.push(7);
        assert_eq!(v.capacity(), 12);

        // Set capacity to 100
        v.try_set_capacity(100).unwrap();
        assert_eq!(v.capacity(), 100);

        // Capacity will never shrink
        v.try_set_capacity(5).unwrap();
        assert_eq!(v.capacity(), 100);
    }

    #[test]
    fn test_try_set_capacity_exact() {
        // Constructing uses exact capacity
        let mut v = vec![1, 2, 3];
        assert_eq!(v.capacity(), 3);

        // Set capacity to exactly 5
        v.try_set_capacity_exact(5).unwrap();
        assert_eq!(v.capacity(), 5);

        // Setting capacity again does nothing
        v.try_set_capacity_exact(5).unwrap();
        assert_eq!(v.capacity(), 5);

        // Fill up to capacity
        v.extend([4, 5]);
        assert_eq!(v.capacity(), 5);

        // Push over capacity will double it
        v.push(6);
        assert_eq!(v.capacity(), 10);

        // Set capacity to exactly 100
        v.try_set_capacity_exact(100).unwrap();
        assert_eq!(v.capacity(), 100);

        // Capacity will never shrink
        v.try_set_capacity_exact(5).unwrap();
        assert_eq!(v.capacity(), 100);
    }
}
