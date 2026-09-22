use std::error::Error;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 7 {
        return Err(
            "usage: page-attention-prepare INPUT_DIR OUTPUT_DIR SESSION_ID LAYER HEAD DIM THREADS"
                .into(),
        );
    }
    let threads: usize = args[6].parse()?;
    if threads == 0 {
        return Err("THREADS must be positive".into());
    }
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()?;
    page_attention::builder::prepare_head(
        Path::new(&args[0]),
        Path::new(&args[1]),
        &args[2],
        args[3].parse()?,
        args[4].parse()?,
        args[5].parse()?,
    )?;
    Ok(())
}
