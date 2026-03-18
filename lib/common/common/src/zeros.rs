use std::io::{Result, Write};

static ZEROS: [u8; 8096] = [0u8; 8096];

pub trait WriteZerosExt {
    /// Write `len` zeros to the writer.
    fn write_zeros(&mut self, len: usize) -> Result<()>;
}

impl<W: Write> WriteZerosExt for W {
    fn write_zeros(&mut self, mut len: usize) -> Result<()> {
        while len > 0 {
            len -= self.write(&ZEROS[..ZEROS.len().min(len)])?;
        }
        Ok(())
    }
}
