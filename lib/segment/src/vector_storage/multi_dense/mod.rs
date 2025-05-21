pub mod appendable_mmap_multi_dense_vector_storage;
#[cfg(feature = "rocksdb")]
pub mod simple_multi_dense_vector_storage;
#[cfg(test)]
pub mod volatile_multi_dense_vector_storage;
