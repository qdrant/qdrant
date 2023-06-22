use signal_hook::{
    consts::{SIGINT, SIGTERM},
    flag::register,
};
use std::sync::{atomic::AtomicBool, Arc};

pub fn register_signal_context(context: Arc<AtomicBool>) -> Result<(), std::io::Error> {
    register(SIGTERM, context.clone())?;
    register(SIGINT, context)?;
    Ok(())
}
