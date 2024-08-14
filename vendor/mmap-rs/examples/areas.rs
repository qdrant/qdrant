use mmap_rs::{Error, MemoryAreas, Protection, ShareMode};

fn main() -> Result<(), Error> {
    let maps = MemoryAreas::open(None)?;

    for area in maps {
        let area = area?;

        println!(
            "{:x}-{:x} {}{}{}{}{}",
            area.start(),
            area.end(),
            if area.protection().contains(Protection::READ) {
                "r"
            } else {
                "-"
            },
            if area.protection().contains(Protection::WRITE) {
                "w"
            } else {
                "-"
            },
            if area.protection().contains(Protection::EXECUTE) {
                "x"
            } else {
                "-"
            },
            if area.share_mode() == ShareMode::Shared {
                "s"
            } else {
                "p"
            },
            format!(
                " {:x} {}",
                area.file_offset().unwrap_or(0),
                area.path()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default(),
            ),
        );
    }

    Ok(())
}
