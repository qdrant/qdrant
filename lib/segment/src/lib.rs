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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
