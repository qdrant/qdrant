use api::grpc::models::VersionInfo;
use atty::Stream;
use colored::Colorize;
use std::env;

/// Prints welcome message
#[rustfmt::skip]
pub fn welcome() {
    if !atty::is(Stream::Stdout) {
        colored::control::set_override(false);
    }

    match env::var("COLORTERM") {
        Ok(val) => if val != "24bit" && val != "truecolor" {
            colored::control::set_override(false);
        },
        Err(_) => colored::control::set_override(false),
    }

    println!("{}", r#"           _                 _    "#.bold().truecolor(184, 20, 56));
    println!("{}", r#"  __ _  __| |_ __ __ _ _ __ | |_  "#.bold().truecolor(184, 20, 56));
    println!("{}", r#" / _` |/ _` | '__/ _` | '_ \| __| "#.bold().truecolor(184, 20, 56));
    println!("{}", r#"| (_| | (_| | | | (_| | | | | |_  "#.bold().truecolor(184, 20, 56));
    println!("{}", r#" \__, |\__,_|_|  \__,_|_| |_|\__| "#.bold().truecolor(184, 20, 56));
    println!("{}", r#"    |_|                           "#.bold().truecolor(184, 20, 56));
    println!();
    let ui_link = format!("https://ui.qdrant.tech/?v=v{}", VersionInfo::default().version);

    println!("{} {}",
             "Access web UI at".truecolor(134, 186, 144),
             ui_link.bold().underline().truecolor(82, 139, 183));
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_welcome() {
        welcome()
    }
}
