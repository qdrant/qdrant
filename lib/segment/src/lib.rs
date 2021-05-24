mod id_mapper;
pub mod payload_storage;
pub mod index;
pub mod vector_storage;
pub mod segment;
pub mod spaces;
pub mod segment_constructor;
pub mod entry;
pub mod types;
pub mod fixtures;
mod common;


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
