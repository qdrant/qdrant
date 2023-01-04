#![allow(deprecated)]

pub mod common;
pub mod entry;
pub mod fixtures;
mod id_tracker;
pub mod index;
pub mod payload_storage;
pub mod segment;
pub mod segment_constructor;
pub mod spaces;
pub mod telemetry;

pub mod data_types;
#[allow(deprecated)]
pub mod types;
pub mod vector_storage;

#[macro_use]
extern crate num_derive;
extern crate core;

static MMAP_ADVISE: parking_lot::RwLock<memmap2::Advice> =
    parking_lot::RwLock::new(memmap2::Advice::Normal);

pub fn get_mmap_advise() -> memmap2::Advice {
    *MMAP_ADVISE.read()
}

pub fn set_mmap_advice(advice: memmap2::Advice) {
    *MMAP_ADVISE.write() = advice;
}

pub fn set_mmap_advice_random() {
    set_mmap_advice(memmap2::Advice::Random);
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
