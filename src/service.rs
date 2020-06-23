mod settings;
mod storage;

mod common;
mod operations;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};

use env_logger;


async fn index() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

async fn index2() -> impl Responder {
    HttpResponse::Ok().body("Hello world again!")
}


#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let settings = settings::Settings::new().expect("Can't read config.");
    std::env::set_var("RUST_LOG",  settings.log_level);
    env_logger::init();
    
    HttpServer::new(|| {
        App::new()
            .wrap(Logger::default())
            .route("/", web::get().to(index))
            .route("/again", web::get().to(index2))
    })
    .workers(1)
    .bind(format!("{}:{}", settings.service.host, settings.service.port))?
    .run()
    .await
}
