use log::debug;

pub trait LogError {
    fn describe(self, msg: &str) -> Self;
}

impl<T, E> LogError for Result<T, E> {
    fn describe(self, msg: &str) -> Self {
        if self.is_err() {
            debug!("Error while: {}", msg);
        }
        self
    }
}
