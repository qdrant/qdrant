#[macro_use]
extern crate log;

#[cfg(feature = "web")]
mod actix;
mod common;
mod settings;
#[cfg(feature = "grpc")]
mod tonic;

use std::io::Error;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use storage::content_manager::toc::TableOfContent;

use crate::common::helpers::create_search_runtime;
use crate::settings::Settings;

fn main() -> std::io::Result<()> {
    let settings = Settings::new().expect("Can't read config.");
    std::env::set_var("RUST_LOG", &settings.log_level);
    env_logger::init();

    // Create and own search runtime out of the scope of async context to ensure correct
    // destruction of it
    let runtime = create_search_runtime(settings.storage.performance.max_search_threads)
        .expect("Can't create runtime.");
    let search_runtime_handle = runtime.handle().clone();

    let toc = TableOfContent::new(&settings.storage, search_runtime_handle);
    for collection in toc.all_collections() {
        info!("loaded collection: {}", collection);
    }
    let toc_arc = Arc::new(toc);

    let mut handles: Vec<JoinHandle<Result<(), Error>>> = vec![];

    #[cfg(feature = "web")]
    {
        let toc_arc = toc_arc.clone();
        let settings = settings.clone();
        let handle = thread::spawn(move || actix::init(toc_arc, settings));
        handles.push(handle);
    }
    #[cfg(feature = "grpc")]
    {
        let toc_arc = toc_arc.clone();
        let settings = settings.clone();
        let handle = thread::spawn(move || tonic::init(toc_arc, settings));
        handles.push(handle);
    }

    for handle in handles.into_iter() {
        handle.join().expect("Couldn't join on the thread")?;
    }
    drop(toc_arc);
    drop(settings);
    Ok(())
}
