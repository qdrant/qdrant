#[macro_use]
extern crate log;

mod common;
mod settings;

use crate::common::helpers::create_search_runtime;
use storage::content_manager::toc::TableOfContent;

fn main() {
    let settings = settings::Settings::new().expect("Can't read config.");
    std::env::set_var("RUST_LOG", settings.log_level);
    env_logger::init();

    let runtime = create_search_runtime(settings.storage.performance.max_search_threads).unwrap();
    let handle = runtime.handle().clone();
    let toc = TableOfContent::new(&settings.storage, handle);

    toc.visit_all_collection_names(|keys| {
        for collection in keys {
            info!("loaded collection: {}", collection);
        }
    });
}
