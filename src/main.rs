#[macro_use] extern crate log;

mod settings;

mod common;
mod collections_api;

use actix_web::middleware::Logger;

use actix_web::{web, App, HttpServer};

use env_logger;
use storage::content_manager::toc::TableOfContent;
use crate::collections_api::api::collections;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let settings = settings::Settings::new().expect("Can't read config.");
    std::env::set_var("RUST_LOG",  settings.log_level);
    env_logger::init();

    let toc = TableOfContent::new(&settings.storage);

    for collection in toc.all_collections() {
        info!("loaded collection: {}", collection);
    }

    let toc_data = web::Data::new(toc);

    HttpServer::new(move || {

        let app = App::new()
            .wrap(Logger::default())
            .app_data(toc_data.clone())
            .data(web::JsonConfig::default().limit(33554432)) // 32 Mb
            .service(collections);

        app
    })
    // .workers(1)
    .bind(format!("{}:{}", settings.service.host, settings.service.port))?
    .run()
    .await
}
