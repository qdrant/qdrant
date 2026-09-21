//! Page-major attention read path, vendored from kv-search.
//! The on-disk format and integer SIMD kernels are unchanged.
pub mod arr;
pub mod index;
pub mod kernel;
pub mod pagekernel;
pub mod pages;
pub mod pagesearch;
pub mod rescore;
pub mod tq4;
