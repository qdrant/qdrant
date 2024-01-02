use std::fmt::{self, Write as _};
use std::time::Duration;

use crate::printer::Formatter;
use crate::tree::{Event, Field, Shared, Span, Tree};
use crate::Tag;

#[cfg(feature = "smallvec")]
type IndentVec = smallvec::SmallVec<[Indent; 32]>;
#[cfg(not(feature = "smallvec"))]
type IndentVec = Vec<Indent>;

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
pub struct Pretty {
    ansi: bool,
}

impl Pretty {
    #[allow(missing_docs)]
    pub const fn new(ansi: bool) -> Self {
        Self { ansi }
    }
}

impl Formatter for Pretty {
    type Error = fmt::Error;

    fn fmt(&self, tree: &Tree) -> Result<String, fmt::Error> {
        let mut writer = String::with_capacity(256);

        self.format_tree(tree, &mut IndentVec::new(), &mut writer)?;

        Ok(writer)
    }
}

impl Pretty {
    fn format_tree(&self, tree: &Tree, indent: &mut IndentVec, writer: &mut String) -> fmt::Result {
        match tree {
            Tree::Event(event) => {
                self.format_shared(&event.shared, writer)?;
                self.format_indent(indent, writer)?;
                self.format_event(event, writer)?;
            }

            Tree::Span(span) => {
                self.format_shared(&span.shared, writer)?;
                self.format_indent(indent, writer)?;
                self.format_span(span, indent, writer)?;
            }
        }

        Ok(())
    }

    fn format_shared(&self, shared: &Shared, writer: &mut String) -> fmt::Result {
        #[cfg(feature = "uuid")]
        write!(writer, "{} ", shared.uuid)?;

        #[cfg(feature = "chrono")]
        write!(
            writer,
            "{:<26} ",
            shared
                .timestamp
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        )?;

        if self.ansi {
            write!(writer, "{:<8} ", ColorLevel(shared.level))?;
        } else {
            write!(writer, "{:<8} ", shared.level)?;
        }

        Ok(())
    }

    fn format_indent(&self, indent: &[Indent], writer: &mut String) -> fmt::Result {
        for indent in indent {
            writer.write_str(indent.repr())?;
        }

        Ok(())
    }

    fn format_event(&self, event: &Event, writer: &mut String) -> fmt::Result {
        let tag = event.tag().unwrap_or_else(|| Tag::from(event.level()));

        write!(writer, "{} [{}]: ", tag.icon(), tag)?;

        if let Some(message) = event.message() {
            writer.write_str(message)?;
        }

        self.format_fields(event.fields(), writer)?;
        writeln!(writer)?;

        Ok(())
    }

    fn format_span(&self, span: &Span, indent: &mut IndentVec, writer: &mut String) -> fmt::Result {
        let total_duration = span.total_duration();
        let busy_duration = span.busy_duration();
        let idle_duration = span.idle_duration();

        // Don't print idle duration if it's insignificantly small
        if busy_duration > idle_duration && idle_duration < Duration::from_millis(10) {
            write!(
                writer,
                "{} [ time = {} ]",
                span.name(),
                DurationDisplay(total_duration as _),
            )?;
        } else {
            write!(
                writer,
                "{} [ time.busy = {}, time.idle = {} ]",
                span.name(),
                DurationDisplay(busy_duration as _),
                DurationDisplay(idle_duration as _),
            )?;
        }

        self.format_fields(span.fields(), writer)?;
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

                self.format_tree(tree, indent, writer)?;
            }

            if let Some(edge) = indent.last_mut() {
                *edge = Indent::Turn;
            }

            self.format_tree(last, indent, writer)?;

            indent.pop();
        }

        Ok(())
    }

    fn format_fields(&self, fields: &[Field], writer: &mut String) -> fmt::Result {
        let mut first_field_printed = false;

        for field in fields {
            // Skip `internal = true` field
            if field.key() == "internal" && field.value() == "true" {
                continue;
            }

            write!(
                writer,
                "{}{}: {}",
                if !first_field_printed { " { " } else { ", " },
                field.key(),
                field.value()
            )?;

            first_field_printed = true;
        }

        if first_field_printed {
            write!(writer, " }}")?;
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

struct DurationDisplay(Duration);

impl fmt::Display for DurationDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut t = self.0.as_nanos() as f64;

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

        write!(f, "{:.0}s", t * 1000.0)?;

        Ok(())
    }
}

struct ColorLevel(tracing::Level);

impl fmt::Display for ColorLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let color = match self.0 {
            tracing::Level::TRACE => ansi_term::Color::Purple,
            tracing::Level::DEBUG => ansi_term::Color::Blue,
            tracing::Level::INFO => ansi_term::Color::Green,
            tracing::Level::WARN => ansi_term::Color::Yellow,
            tracing::Level::ERROR => ansi_term::Color::Red,
        };

        let style = color.bold();

        write!(f, "{}", style.prefix())?;
        f.pad(self.0.as_str())?;
        write!(f, "{}", style.suffix())?;

        Ok(())
    }
}
