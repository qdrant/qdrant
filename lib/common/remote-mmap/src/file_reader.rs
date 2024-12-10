use std::io;

#[derive(Debug, thiserror::Error)]
pub enum FileError {
    #[error("{0}")]
    Io(#[from] io::Error),

    #[error("File read error: {0}")]
    FileReadError(String),
}

pub trait FileReader {
    fn read(&self, path: &str, offset: u64, size: usize) -> Result<Vec<u8>, FileError>;
}
