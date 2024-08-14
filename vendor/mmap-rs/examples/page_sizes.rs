use mmap_rs::{Error, MmapOptions};

fn main() -> Result<(), Error> {
    println!("available page sizes: {:?}", MmapOptions::page_sizes()?);

    Ok(())
}
