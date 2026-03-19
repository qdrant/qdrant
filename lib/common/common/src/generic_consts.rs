pub trait AccessPattern: Copy {
    const IS_SEQUENTIAL: bool;
}

#[derive(Copy, Clone)]
pub struct Random;

#[derive(Copy, Clone)]
pub struct Sequential;

impl AccessPattern for Random {
    const IS_SEQUENTIAL: bool = false;
}

impl AccessPattern for Sequential {
    const IS_SEQUENTIAL: bool = true;
}
