use crate::printer::Formatter;
use crate::tree::{Event, Shared, Span, Tree};
use crate::Tag;
use std::fmt::{self, Write};

#[cfg(feature = "smallvec")]
type IndentVec = smallvec::SmallVec<[Indent; 32]>;
#[cfg(not(feature = "smallvec"))]
type IndentVec = Vec<Indent>;

#[cfg(feature = "ansi")]
use ansi_term::Color;
#[cfg(feature = "ansi")]
use tracing::Level;

/// Format logs for pretty printing.
///
/// # Interpreting span times
///
/// Spans have the following format:
/// ```txt
/// <NAME> [ <DURATION> | <BODY> / <ROOT> ]
/// ```
/// * `DURATION` represents the total time the span was entered for. If the span
/// was used to instrument a `Future` that sleeps, then that time won't be counted
/// since the `Future` won't be polled during that time, and so the span won't enter.
/// * `BODY` represents the percent time the span is entered relative to the root
/// span, *excluding* time that any child spans are entered.
/// * `ROOT` represents the percent time the span is entered relative to the root
/// span, *including* time that any child spans are entered.
///
/// As a mental model, look at `ROOT` to quickly narrow down which branches are
/// costly, and look at `BASE` to pinpoint exactly which spans are expensive.
///
/// Spans without any child spans would have the same `BASE` and `ROOT`, so the
/// redundency is omitted.
///
/// # Examples
///
/// An arbitrarily complex example:
/// ```log
/// INFO     try_from_entry_ro [ 324µs | 8.47% / 100.00% ]
/// INFO     ┝━ server::internal_search [ 296µs | 19.02% / 91.53% ]
/// INFO     │  ┝━ ｉ [filter.info]: Some filter info...
/// INFO     │  ┝━ server::search [ 226µs | 10.11% / 70.01% ]
/// INFO     │  │  ┝━ be::search [ 181µs | 6.94% / 55.85% ]
/// INFO     │  │  │  ┕━ be::search -> filter2idl [ 158µs | 19.65% / 48.91% ]
/// INFO     │  │  │     ┝━ be::idl_arc_sqlite::get_idl [ 20.4µs | 6.30% ]
/// INFO     │  │  │     │  ┕━ ｉ [filter.info]: Some filter info...
/// INFO     │  │  │     ┕━ be::idl_arc_sqlite::get_idl [ 74.3µs | 22.96% ]
/// ERROR    │  │  │        ┝━ 🚨 [admin.error]: On no, an admin error occurred :(
/// DEBUG    │  │  │        ┝━ 🐛 [debug]: An untagged debug log
/// INFO     │  │  │        ┕━ ｉ [admin.info]: there's been a big mistake | alive: false | status: "very sad"
/// INFO     │  │  ┕━ be::idl_arc_sqlite::get_identry [ 13.1µs | 4.04% ]
/// ERROR    │  │     ┝━ 🔐 [security.critical]: A security critical log
/// INFO     │  │     ┕━ 🔓 [security.access]: A security access log
/// INFO     │  ┕━ server::search<filter_resolve> [ 8.08µs | 2.50% ]
/// WARN     │     ┕━ 🚧 [filter.warn]: Some filter warning
/// TRACE    ┕━ 📍 [trace]: Finished!
/// ```
#[derive(Debug)]
pub struct Pretty;

impl Formatter for Pretty {
    type Error = fmt::Error;

    fn fmt(&self, tree: &Tree) -> Result<String, fmt::Error> {
        let mut writer = String::with_capacity(256);

        Pretty::format_tree(tree, None, &mut IndentVec::new(), &mut writer)?;

        Ok(writer)
    }
}

impl Pretty {
    fn format_tree(
        tree: &Tree,
        duration_root: Option<f64>,
        indent: &mut IndentVec,
        writer: &mut String,
    ) -> fmt::Result {
        match tree {
            Tree::Event(event) => {
                Pretty::format_shared(&event.shared, writer)?;
                Pretty::format_indent(indent, writer)?;
                Pretty::format_event(event, writer)
            }
            Tree::Span(span) => {
                Pretty::format_shared(&span.shared, writer)?;
                Pretty::format_indent(indent, writer)?;
                Pretty::format_span(span, duration_root, indent, writer)
            }
        }
    }

    fn format_shared(shared: &Shared, writer: &mut String) -> fmt::Result {
        #[cfg(feature = "uuid")]
        write!(writer, "{} ", shared.uuid)?;

        #[cfg(feature = "chrono")]
        write!(writer, "{:<36} ", shared.timestamp.to_rfc3339())?;

        #[cfg(feature = "ansi")]
        return write!(writer, "{:<8} ", ColorLevel(shared.level));

        #[cfg(not(feature = "ansi"))]
        return write!(writer, "{:<8} ", shared.level);
    }

    fn format_indent(indent: &[Indent], writer: &mut String) -> fmt::Result {
        for indent in indent {
            writer.write_str(indent.repr())?;
        }
        Ok(())
    }

    fn format_event(event: &Event, writer: &mut String) -> fmt::Result {
        let tag = event.tag().unwrap_or_else(|| Tag::from(event.level()));

        write!(writer, "{} [{}]: ", tag.icon(), tag)?;

        if let Some(message) = event.message() {
            writer.write_str(message)?;
        }

        for field in event.fields().iter() {
            write!(writer, " | {}: {}", field.key(), field.value())?;
        }

        writeln!(writer)
    }

    fn format_span(
        span: &Span,
        duration_root: Option<f64>,
        indent: &mut IndentVec,
        writer: &mut String,
    ) -> fmt::Result {
        let total_duration = span.total_duration().as_nanos() as f64;
        let inner_duration = span.inner_duration().as_nanos() as f64;
        let root_duration = duration_root.unwrap_or(total_duration);
        let percent_total_of_root_duration = 100.0 * total_duration / root_duration;

        write!(
            writer,
            "{} [ {} | ",
            span.name(),
            DurationDisplay(total_duration)
        )?;

        if inner_duration > 0.0 {
            let base_duration = span.base_duration().as_nanos() as f64;
            let percent_base_of_root_duration = 100.0 * base_duration / root_duration;
            write!(writer, "{:.2}% / ", percent_base_of_root_duration)?;
        }

        write!(writer, "{:.2}% ]", percent_total_of_root_duration)?;

        for (n, field) in span.shared.fields.iter().enumerate() {
            write!(
                writer,
                "{} {}: {}",
                if n == 0 { "" } else { " |" },
                field.key(),
                field.value()
            )?;
        }
        writeln!(writer)?;

        if let Some((last, remaining)) = span.nodes().split_last() {
            match indent.last_mut() {
                Some(edge @ Indent::Turn) => *edge = Indent::Null,
                Some(edge @ Indent::Fork) => *edge = Indent::Line,
                _ => {}
            }

            indent.push(Indent::Fork);

            for tree in remaining {
                if let Some(edge) = indent.last_mut() {
                    *edge = Indent::Fork;
                }
                Pretty::format_tree(tree, Some(root_duration), indent, writer)?;
            }

            if let Some(edge) = indent.last_mut() {
                *edge = Indent::Turn;
            }
            Pretty::format_tree(last, Some(root_duration), indent, writer)?;

            indent.pop();
        }

        Ok(())
    }
}

enum Indent {
    Null,
    Line,
    Fork,
    Turn,
}

impl Indent {
    fn repr(&self) -> &'static str {
        match self {
            Self::Null => "   ",
            Self::Line => "│  ",
            Self::Fork => "┝━ ",
            Self::Turn => "┕━ ",
        }
    }
}

struct DurationDisplay(f64);

// Taken from chrono
impl fmt::Display for DurationDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut t = self.0;
        for unit in ["ns", "µs", "ms", "s"] {
            if t < 10.0 {
                return write!(f, "{:.2}{}", t, unit);
            } else if t < 100.0 {
                return write!(f, "{:.1}{}", t, unit);
            } else if t < 1000.0 {
                return write!(f, "{:.0}{}", t, unit);
            }
            t /= 1000.0;
        }
        write!(f, "{:.0}s", t * 1000.0)
    }
}

// From tracing-tree
#[cfg(feature = "ansi")]
struct ColorLevel(Level);

#[cfg(feature = "ansi")]
impl fmt::Display for ColorLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let color = match self.0 {
            Level::TRACE => Color::Purple,
            Level::DEBUG => Color::Blue,
            Level::INFO => Color::Green,
            Level::WARN => Color::RGB(252, 234, 160), // orange
            Level::ERROR => Color::Red,
        };
        let style = color.bold();
        write!(f, "{}", style.prefix())?;
        f.pad(self.0.as_str())?;
        write!(f, "{}", style.suffix())
    }
}
