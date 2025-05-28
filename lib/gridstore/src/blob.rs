pub trait Blob {
    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(bytes: &[u8]) -> Self;
}

impl Blob for i64 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn from_bytes(data: &[u8]) -> Self {
        i64::from_le_bytes(
            data.try_into()
                .expect("Slice with incorrect length for i64"),
        )
    }
}

impl Blob for u128 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn from_bytes(data: &[u8]) -> Self {
        u128::from_le_bytes(
            data.try_into()
                .expect("Slice with incorrect length for u128"),
        )
    }
}

impl Blob for f64 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn from_bytes(data: &[u8]) -> Self {
        f64::from_le_bytes(
            data.try_into()
                .expect("Slice with incorrect length for f64"),
        )
    }
}

impl<T> Blob for Vec<T>
where
    T: Blob + Sized,
{
    fn to_bytes(&self) -> Vec<u8> {
        self.iter().flat_map(|v| v.to_bytes()).collect()
    }

    fn from_bytes(data: &[u8]) -> Self {
        data.chunks(size_of::<T>())
            .map(|v| T::from_bytes(v))
            .collect()
    }
}
