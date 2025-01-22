#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct U24([u8; 3]);

impl U24 {
    pub const MAX: u32 = 0xFFFFFF;

    #[inline]
    pub const fn new_wrapped(value: u32) -> Self {
        let arr = value.to_le_bytes();
        Self([arr[0], arr[1], arr[2]])
    }

    #[inline]
    pub const fn get(self) -> u32 {
        u32::from_le_bytes([self.0[0], self.0[1], self.0[2], 0])
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

#[cfg(test)]
mod tests {
    pub use super::*;

    #[test]
    fn test_u24() {
        assert_eq!(U24::new_wrapped(0x13_57_9A_BC).get(), 0x57_9A_BC);
        assert!(U24::try_from(0x13_57_9A_BC).is_err());
        assert_eq!(U24::try_from(0x57_9A_BC).map(U24::get), Ok(0x57_9A_BC));
    }
}
