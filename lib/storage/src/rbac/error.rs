pub struct AccessDeniedError {
    message: String,
}

impl AccessDeniedError {
    pub fn new(message: &str) -> Self {
        AccessDeniedError {
            message: message.to_string(),
        }
    }
}
