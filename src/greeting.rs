use std::env;
use std::io::{stdout, IsTerminal};

use colored::{Color, ColoredString, Colorize};

use crate::settings::Settings;

fn paint(text: &str, true_color: bool) -> ColoredString {
    if true_color {
        text.bold().truecolor(184, 20, 56)
    } else {
        text.bold().color(Color::Red)
    }
}

/// Prints welcome message
#[rustfmt::skip]
#[allow(clippy::needless_raw_string_hashes)]
pub fn welcome(settings: &Settings) {
    if !stdout().is_terminal()  {
        colored::control::set_override(false);
    }

    let mut true_color = true;

    match env::var("COLORTERM") {
        Ok(val) => if val != "24bit" && val != "truecolor" {
            true_color = false;
        },
        Err(_) => true_color = false,
    }

    println!("{}", paint(r#"           _                 _    "#, true_color));
    println!("{}", paint(r#"  __ _  __| |_ __ __ _ _ __ | |_  "#, true_color));
    println!("{}", paint(r#" / _` |/ _` | '__/ _` | '_ \| __| "#, true_color));
    println!("{}", paint(r#"| (_| | (_| | | | (_| | | | | |_  "#, true_color));
    println!("{}", paint(r#" \__, |\__,_|_|  \__,_|_| |_|\__| "#, true_color));
    println!("{}", paint(r#"    |_|                           "#, true_color));
    println!();
    let ui_link = format!(
        "http{}://{}:{}/dashboard",
        if settings.service.enable_tls { "s" } else { "" },
        settings.service.host,
        settings.service.http_port
    );

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
        welcome(&Settings::new(None).unwrap());
    }
}
