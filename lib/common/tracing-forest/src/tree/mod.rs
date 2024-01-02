//! The core tree structure of `tracing-forest`.
//!
//! This module provides methods used for log inspection when using [`capture`].
//! It consists of three types: [`Tree`], [`Span`], and [`Event`].
//!
//! [`capture`]: crate::runtime::capture
use crate::tag::Tag;
#[cfg(feature = "chrono")]
use chrono::{DateTime, Utc};
#[cfg(feature = "serde")]
use serde::Serialize;
use std::time::Duration;
use thiserror::Error;
use tracing::Level;
#[cfg(feature = "uuid")]
use uuid::Uuid;

mod field;
#[cfg(feature = "serde")]
mod ser;

pub use field::Field;
pub(crate) use field::FieldSet;

/// A node in the log tree, consisting of either a [`Span`] or an [`Event`].
///
/// The inner types can be extracted through a `match` statement. Alternatively,
/// the [`event`] and [`span`] methods provide a more ergonomic way to access the
/// inner types in unit tests when combined with the [`capture`] function.
///
/// [`event`]: Tree::event
/// [`span`]: Tree::span
/// [`capture`]: crate::runtime::capture
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[allow(clippy::large_enum_variant)] // https://github.com/rust-lang/rust-clippy/issues/9798
pub enum Tree {
    /// An [`Event`] leaf node.
    Event(Event),

    /// A [`Span`] inner node.
    Span(Span),
}

/// A leaf node in the log tree carrying information about a Tracing event.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Event {
    /// Shared fields between events and spans.
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub(crate) shared: Shared,

    /// The message associated with the event.
    pub(crate) message: Option<String>,

    /// The tag that the event was collected with.
    pub(crate) tag: Option<Tag>,
}

/// An internal node in the log tree carrying information about a Tracing span.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Span {
    /// Shared fields between events and spans.
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub(crate) shared: Shared,

    /// The name of the span.
    pub(crate) name: &'static str,

    /// The duration the span was alive for.
    #[cfg_attr(
        feature = "serde",
        serde(rename = "nanos_total", serialize_with = "ser::nanos")
    )]
    pub(crate) total_duration: Duration,

    /// The duration the span was open for.
    #[cfg_attr(
        feature = "serde",
        serde(rename = "nanos_busy", serialize_with = "ser::nanos")
    )]
    pub(crate) busy_duration: Duration,

    /// Events and spans collected while the span was open.
    pub(crate) nodes: Vec<Tree>,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub(crate) struct Shared {
    /// The ID of the event or span.
    #[cfg(feature = "uuid")]
    pub(crate) uuid: Uuid,

    /// When the event occurred or when the span opened.
    #[cfg(feature = "chrono")]
    #[cfg_attr(feature = "serde", serde(serialize_with = "ser::timestamp"))]
    pub(crate) timestamp: DateTime<Utc>,

    /// The level the event or span occurred at.
    #[cfg_attr(feature = "serde", serde(serialize_with = "ser::level"))]
    pub(crate) level: Level,

    /// Key-value data.
    #[cfg_attr(feature = "serde", serde(serialize_with = "ser::fields"))]
    pub(crate) fields: FieldSet,
}

/// Error returned by [`Tree::event`][event].
///
/// [event]: crate::tree::Tree::event
#[derive(Error, Debug)]
#[error("Expected an event, found a span")]
pub struct ExpectedEventError(());

/// Error returned by [`Tree::span`][span].
///
/// [span]: crate::tree::Tree::span
#[derive(Error, Debug)]
#[error("Expected a span, found an event")]
pub struct ExpectedSpanError(());

impl Tree {
    /// Returns a reference to the inner [`Event`] if the tree is an event.
    ///
    /// # Errors
    ///
    /// This function returns an error if the `Tree` contains the `Span` variant.
    ///
    /// # Examples
    ///
    /// Inspecting a `Tree` returned from [`capture`]:
    /// ```
    /// use tracing::{info, info_span};
    /// use tracing_forest::tree::{Tree, Event};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let logs: Vec<Tree> = tracing_forest::capture()
    ///         .build()
    ///         .on(async {
    ///             info!("some information");
    ///         })
    ///         .await;
    ///
    ///     assert!(logs.len() == 1);
    ///
    ///     let event: &Event = logs[0].event()?;
    ///     assert!(event.message() == Some("some information"));
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`capture`]: crate::runtime::capture
    pub fn event(&self) -> Result<&Event, ExpectedEventError> {
        match self {
            Tree::Event(event) => Ok(event),
            Tree::Span(_) => Err(ExpectedEventError(())),
        }
    }

    /// Returns a reference to the inner [`Span`] if the tree is a span.
    ///
    /// # Errors
    ///
    /// This function returns an error if the `Tree` contains the `Event` variant.
    ///
    /// # Examples
    ///
    /// Inspecting a `Tree` returned from [`capture`]:
    /// ```
    /// use tracing::{info, info_span};
    /// use tracing_forest::tree::{Tree, Span};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let logs: Vec<Tree> = tracing_forest::capture()
    ///         .build()
    ///         .on(async {
    ///             info_span!("my_span").in_scope(|| {
    ///                 info!("inside the span");
    ///             });
    ///         })
    ///         .await;
    ///
    ///     assert!(logs.len() == 1);
    ///
    ///     let my_span: &Span = logs[0].span()?;
    ///     assert!(my_span.name() == "my_span");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`capture`]: crate::runtime::capture
    pub fn span(&self) -> Result<&Span, ExpectedSpanError> {
        match self {
            Tree::Event(_) => Err(ExpectedSpanError(())),
            Tree::Span(span) => Ok(span),
        }
    }
}

impl Event {
    /// Returns the event's [`Uuid`].
    #[cfg(feature = "uuid")]
    pub fn uuid(&self) -> Uuid {
        self.shared.uuid
    }

    /// Returns the [`DateTime`] that the event occurred at.
    #[cfg(feature = "chrono")]
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.shared.timestamp
    }

    /// Returns the event's [`Level`].
    pub fn level(&self) -> Level {
        self.shared.level
    }

    /// Returns the event's message, if there is one.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Returns the event's [`Tag`], if there is one.
    pub fn tag(&self) -> Option<Tag> {
        self.tag
    }

    /// Returns the event's fields.
    pub fn fields(&self) -> &[Field] {
        &self.shared.fields
    }
}

impl Span {
    pub(crate) fn new(shared: Shared, name: &'static str) -> Self {
        Span {
            shared,
            name,
            total_duration: Duration::ZERO,
            busy_duration: Duration::ZERO,
            nodes: Vec::new(),
        }
    }

    /// Returns the span's [`Uuid`].
    #[cfg(feature = "uuid")]
    pub fn uuid(&self) -> Uuid {
        self.shared.uuid
    }

    /// Returns the [`DateTime`] that the span occurred at.
    #[cfg(feature = "chrono")]
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.shared.timestamp
    }

    /// Returns the span's [`Level`].
    pub fn level(&self) -> Level {
        self.shared.level
    }

    /// Returns the span's name.
    pub fn name(&self) -> &str {
        self.name
    }

    /// Returns the span's fields.
    pub fn fields(&self) -> &[Field] {
        &self.shared.fields
    }

    /// Returns the span's child trees.
    pub fn nodes(&self) -> &[Tree] {
        &self.nodes
    }

    /// Returns the total duration the span was alive for.
    pub fn total_duration(&self) -> Duration {
        self.total_duration
    }

    /// Returns the duration the span was open for.
    pub fn busy_duration(&self) -> Duration {
        self.busy_duration
    }

    /// Returns the duration this span was idle (i.e., alive but not open) for.
    pub fn idle_duration(&self) -> Duration {
        self.total_duration.saturating_sub(self.busy_duration)
    }
}
