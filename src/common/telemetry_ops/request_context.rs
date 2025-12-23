use std::cell::RefCell;

use tokio::task_local;

task_local! {
    pub static COLLECTION_CONTEXT: RefCell<Option<String>>;
}

pub fn set_collection_context(name: &str) {
    let _ = COLLECTION_CONTEXT.try_with(|ctx| {
        *ctx.borrow_mut() = Some(name.to_string());
    });
}
