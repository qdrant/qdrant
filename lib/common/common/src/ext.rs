pub mod aligned_vec;

pub trait OptionExt {
    /// `replace` if the given `value` is `Some`
    fn replace_if_some(&mut self, value: Self);
}

impl<T> OptionExt for Option<T> {
    #[inline]
    fn replace_if_some(&mut self, value: Self) {
        if let Some(value) = value {
            self.replace(value);
        }
    }
}

pub trait ResultOptionExt<T, E> {
    fn map_some<U, F>(self, f: F) -> Result<Option<U>, E>
    where
        F: FnOnce(T) -> U;
}

impl<T, E> ResultOptionExt<T, E> for Result<Option<T>, E> {
    fn map_some<U, F>(self, f: F) -> Result<Option<U>, E>
    where
        F: FnOnce(T) -> U,
    {
        Ok(self?.map(f))
    }
}

pub trait VecExt<T> {
    /// Same as `self.into_iter().map(f).collect()`, but with some assertions
    /// to make sure that the compiler can use iterate-and-collect optimization.
    fn transform_in_place<U, F: Fn(T) -> U>(self, f: F) -> Vec<U>;
}

impl<T> VecExt<T> for Vec<T> {
    fn transform_in_place<U, F: Fn(T) -> U>(self, f: F) -> Vec<U> {
        const {
            assert!(size_of::<T>() == size_of::<U>());
            assert!(align_of::<T>() == align_of::<U>());
        }
        self.into_iter().map(f).collect()
    }
}
