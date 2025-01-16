#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct U24([u8; 3]);

impl U24 {
    pub const MAX: u32 = 0xFFFFFF;

    #[inline]
    pub fn new_wrapped(value: u32) -> Self {
        let arr = value.to_le_bytes();
        Self([arr[0], arr[1], arr[2]])
    }

    #[inline]
    pub fn get(self) -> u32 {
        u32::from_ne_bytes([self.0[0], self.0[1], self.0[2], 0])
    }
}

impl TryFrom<u32> for U24 {
    type Error = ();

    #[inline]
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        let arr = value.to_le_bytes();
        if arr[3] != 0 {
            return Err(());
        }
        Ok(Self([arr[0], arr[1], arr[2]]))
    }
}

impl From<U24> for u32 {
    #[inline]
    fn from(value: U24) -> Self {
        value.get()
    }
}
