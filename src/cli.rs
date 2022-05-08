mod common;

use std::path::Path;
use clap::Parser;
use tokio::runtime::Runtime;
use collection::Collection;


#[derive(Parser, Debug)]
#[clap(version, about)]
struct Args {

    /// Path to the collection
    #[clap(short, long)]
    pub collection: String,
}


fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "DEBUG");
    env_logger::init();
    let args: Args = Args::parse();

    let rt = Runtime::new().expect("create runtime");

    let collection_path = Path::new(&args.collection);

    let collection_name = collection_path
        .file_name()
        .expect("Can't resolve a filename of one of the collection files")
        .to_str()
        .expect("A filename of one of the collection files is not a valid UTF-8")
        .to_string();

    eprintln!("collection_name = {:#?}", collection_name);

    let mut collection = rt
        .block_on(Collection::load(collection_name.clone(), &collection_path));

    eprintln!("collection.info() = {:#?}", rt.block_on(collection.info(None)));

    rt.block_on(collection.before_drop());
    Ok(())
}