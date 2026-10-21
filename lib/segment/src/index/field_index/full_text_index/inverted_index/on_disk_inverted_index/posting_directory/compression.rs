/// How the entries after the header are stored. Ids are never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub(super) enum Compression {
    /// The entries in one zstd frame with a checksum.
    Zstd = 1,
}

impl Compression {
    pub(super) fn from_id(id: u32) -> Option<Self> {
        match id {
            1 => Some(Self::Zstd),
            _ => None,
        }
    }
}
