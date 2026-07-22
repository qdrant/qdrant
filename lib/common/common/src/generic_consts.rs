pub trait AccessPattern: Copy + Default {
    const IS_SEQUENTIAL: bool;
}

#[derive(Copy, Clone, Default)]
pub struct Random;

#[derive(Copy, Clone, Default)]
pub struct Sequential;

impl AccessPattern for Random {
    const IS_SEQUENTIAL: bool = false;
}

impl AccessPattern for Sequential {
    const IS_SEQUENTIAL: bool = true;
}
