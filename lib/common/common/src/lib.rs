pub mod cpu;
pub mod defaults;
pub mod disk;
pub mod fixed_length_priority_queue;
pub mod iterator_ext;
pub mod math;
pub mod mmap_hashmap;
pub mod panic;
pub mod top_k;
pub mod types;
pub mod validation;

pub fn clone_from_opt<T: Clone>(this: &mut Option<T>, other: Option<&T>) {
    match (this, other) {
        (Some(this), Some(other)) => this.clone_from(other),
        (this, other) => *this = other.cloned(),
    }
}
