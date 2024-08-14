use mmap_rs::{Error, MmapOptions};

fn main() -> Result<(), Error> {
    // Allocate a single page of anonymous memory that is private and mutable.
    let mut mapping = MmapOptions::new(MmapOptions::page_size())?.map_mut()?;

    mapping[0..4].copy_from_slice(b"test");

    let mapping = match mapping.make_read_only() {
        Ok(mapping) => mapping,
        Err((_, e)) => return Err(e),
    };

    println!("mapping: {:x?}", &mapping[0..4]);

    Ok(())
}
