use std::cmp::min;
use std::env;
use std::io::{stdout, IsTerminal};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use api::rest::models::get_git_commit_id;
use colored::{Color, ColoredString, Colorize};

use crate::settings::Settings;

fn paint_red(text: &str, true_color: bool) -> ColoredString {
    if true_color {
        text.bold().truecolor(184, 20, 56)
    } else {
        text.bold().color(Color::Red)
    }
}

fn paint_green(text: &str, true_color: bool) -> ColoredString {
    if true_color {
        text.truecolor(134, 186, 144)
    } else {
        text.color(Color::Green)
    }
}

fn paint_blue(text: &str, true_color: bool) -> ColoredString {
    if true_color {
        text.bold().truecolor(82, 139, 183)
    } else {
        text.bold().color(Color::Blue)
    }
}

/// Check whether the given IP will be reachable from `localhost`
///
/// This is a static analysis based on (very) common defaults and doesn't probe the current
/// routing table.
fn is_localhost_ip(host: &str) -> bool {
    let Ok(ip) = host.parse::<IpAddr>() else {
        return false;
    };

    // Unspecified IPs bind to all interfaces, so `localhost` always points to it
    if ip == IpAddr::V4(Ipv4Addr::UNSPECIFIED) || ip == IpAddr::V6(Ipv6Addr::UNSPECIFIED) {
        return true;
    }

    // On all tested OSes IPv4 localhost points to `localhost`
    if ip == IpAddr::V4(Ipv4Addr::LOCALHOST) {
        return true;
    }

    // On macOS IPv6 localhost points to `localhost`, on Linux it is `ip6-localhost`
    if cfg!(target_os = "macos") && ip == IpAddr::V6(Ipv6Addr::LOCALHOST) {
        return true;
    }

    false
}

/// Prints welcome message
pub fn welcome(settings: &Settings) {
    if !stdout().is_terminal() {
        colored::control::set_override(false);
    }

    let mut true_color = true;

    match env::var("COLORTERM") {
        Ok(val) => {
            if val != "24bit" && val != "truecolor" {
                true_color = false;
            }
        }
        Err(_) => true_color = false,
    }

    let title = [
        r"           _                 _    ",
        r"  __ _  __| |_ __ __ _ _ __ | |_  ",
        r" / _` |/ _` | '__/ _` | '_ \| __| ",
        r"| (_| | (_| | | | (_| | | | | |_  ",
        r" \__, |\__,_|_|  \__,_|_| |_|\__| ",
        r"    |_|                           ",
    ];
    for line in title {
        println!("{}", paint_red(line, true_color));
    }
    println!();

    // Print current version and, if available, first 8 characters of the git commit hash
    let git_commit_info = get_git_commit_id()
        .map(|git_commit| {
            format!(
                ", {} {}",
                paint_green("build:", true_color),
                paint_blue(&git_commit[..min(8, git_commit.len())], true_color),
            )
        })
        .unwrap_or_default();

    println!(
        "{} {}{}",
        paint_green("Version:", true_color),
        paint_blue(env!("CARGO_PKG_VERSION"), true_color),
        git_commit_info
    );

    // Print link to web UI
    let ui_link = format!(
        "http{}://{}:{}/dashboard",
        if settings.service.enable_tls { "s" } else { "" },
        if is_localhost_ip(&settings.service.host) {
            "localhost"
        } else {
            &settings.service.host
        },
        settings.service.http_port
    );

    println!(
        "{} {}",
        paint_green("Access web UI at", true_color),
        paint_blue(&ui_link, true_color).underline()
    );
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
