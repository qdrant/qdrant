use std::io::{self, Write};

/// A writer that counts the number of bytes written to it without actually storing them.
#[derive(Default)]
pub struct CountingWrite {
    bytes: u64,
}

impl CountingWrite {
    /// Returns the total number of bytes that have been "written" to this writer.
    pub fn bytes_written(&self) -> u64 {
        self.bytes
    }
}

impl Write for CountingWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes += buf.len() as u64;
        Ok(buf.len()) // pretend we wrote everything
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
