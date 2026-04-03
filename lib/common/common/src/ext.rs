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
