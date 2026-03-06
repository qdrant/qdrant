/// Convert number of bytes into human-readable format
///
/// # Examples
/// - 123 -> "123 B"
/// - 1024 -> "1.00 KiB"
/// - 1000000 -> "976.56 KiB"
/// - 1048576 -> "1.00 MiB"
pub fn bytes_to_human(bytes: usize) -> String {
    const UNITS: [&str; 9] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];

    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{size:.2} {}", UNITS[unit_index])
}
