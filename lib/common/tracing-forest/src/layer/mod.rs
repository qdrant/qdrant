use crate::fail;
use crate::printer::{PrettyPrinter, TestCapturePrinter};
use crate::processor::{Processor, Sink};
use crate::tag::{NoTag, Tag, TagParser};
use crate::tree::{self, FieldSet, Tree};
#[cfg(feature = "chrono")]
use chrono::Utc;
use std::fmt;
use std::io::{self, Write};
use std::time::Instant;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::{LookupSpan, Registry, SpanRef};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::util::TryInitError;
#[cfg(feature = "uuid")]
use uuid::Uuid;
#[cfg(feature = "uuid")]
pub(crate) mod id;

pub(crate) struct OpenedSpan {
    span: tree::Span,
    new: Instant,
    enter: Instant,
}

impl OpenedSpan {
    fn new<S>(attrs: &Attributes, _ctx: &Context<S>) -> Self
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let mut fields = FieldSet::default();
        #[cfg(feature = "uuid")]
        let mut maybe_uuid = None;

        attrs.record(&mut |field: &Field, value: &dyn fmt::Debug| {
            #[cfg(feature = "uuid")]
            if field.name() == "uuid" && maybe_uuid.is_none() {
                const LENGTH: usize = 45;
                let mut buf = [0u8; LENGTH];
                let mut remaining = &mut buf[..];

                if let Ok(()) = write!(remaining, "{:?}", value) {
                    let len = LENGTH - remaining.len();
                    if let Some(parsed) = id::try_parse(&buf[..len]) {
                        maybe_uuid = Some(parsed);
                    }
                }
                return;
            }

            let value = format!("{:?}", value);
            fields.push(tree::Field::new(field.name(), value));
        });

        let shared = tree::Shared {
            #[cfg(feature = "chrono")]
            timestamp: Utc::now(),
            level: *attrs.metadata().level(),
            fields,
            #[cfg(feature = "uuid")]
            uuid: maybe_uuid.unwrap_or_else(|| match _ctx.lookup_current() {
                Some(parent) => parent
                    .extensions()
                    .get::<OpenedSpan>()
                    .expect(fail::OPENED_SPAN_NOT_IN_EXTENSIONS)
                    .span
                    .uuid(),
                None => Uuid::new_v4(),
            }),
        };

        OpenedSpan {
            span: tree::Span::new(shared, attrs.metadata().name()),
            new: Instant::now(),
            enter: Instant::now(),
        }
    }

    fn enter(&mut self) {
        self.enter = Instant::now();
    }

    fn exit(&mut self) {
        self.span.busy_duration += self.enter.elapsed();
    }

    fn close(mut self) -> tree::Span {
        self.span.total_duration = self.new.elapsed();
        self.span
    }

    fn record_event(&mut self, event: tree::Event) {
        #[cfg(feature = "uuid")]
        let event = {
            let mut event = event;
            event.shared.uuid = self.span.uuid();
            event
        };

        self.span.nodes.push(Tree::Event(event));
    }

    fn record_span(&mut self, span: tree::Span) {
        self.span.nodes.push(Tree::Span(span));
    }

    #[cfg(feature = "uuid")]
    pub(crate) fn uuid(&self) -> Uuid {
        self.span.uuid()
    }
}

/// A [`Layer`] that collects and processes trace data while preserving
/// contextual coherence.
#[derive(Clone, Debug)]
pub struct ForestLayer<P, T> {
    processor: P,
    tag: T,
}

impl<P: Processor, T: TagParser> ForestLayer<P, T> {
    /// Create a new `ForestLayer` from a [`Processor`] and a [`TagParser`].
    pub fn new(processor: P, tag: T) -> Self {
        ForestLayer { processor, tag }
    }
}

impl<P: Processor> From<P> for ForestLayer<P, NoTag> {
    fn from(processor: P) -> Self {
        ForestLayer::new(processor, NoTag)
    }
}

impl ForestLayer<Sink, NoTag> {
    /// Create a new `ForestLayer` that does nothing with collected trace data.
    pub fn sink() -> Self {
        ForestLayer::from(Sink)
    }
}

impl Default for ForestLayer<PrettyPrinter, NoTag> {
    fn default() -> Self {
        ForestLayer {
            processor: PrettyPrinter::new(),
            tag: NoTag,
        }
    }
}

impl<P, T, S> Layer<S> for ForestLayer<P, T>
where
    P: Processor,
    T: TagParser,
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes, id: &Id, ctx: Context<S>) {
        let span = ctx.span(id).expect(fail::SPAN_NOT_IN_CONTEXT);
        let opened = OpenedSpan::new(attrs, &ctx);

        let mut extensions = span.extensions_mut();
        extensions.insert(opened);
    }

    fn on_event(&self, event: &Event, ctx: Context<S>) {
        struct Visitor {
            message: Option<String>,
            fields: FieldSet,
            immediate: bool,
        }

        impl Visit for Visitor {
            fn record_bool(&mut self, field: &Field, value: bool) {
                match field.name() {
                    "immediate" => self.immediate |= value,
                    _ => self.record_debug(field, &value),
                }
            }

            fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
                let value = format!("{:?}", value);
                match field.name() {
                    "message" if self.message.is_none() => self.message = Some(value),
                    key => self.fields.push(tree::Field::new(key, value)),
                }
            }
        }

        let mut visitor = Visitor {
            message: None,
            fields: FieldSet::default(),
            immediate: false,
        };

        event.record(&mut visitor);

        let shared = tree::Shared {
            #[cfg(feature = "uuid")]
            uuid: Uuid::nil(),
            #[cfg(feature = "chrono")]
            timestamp: Utc::now(),
            level: *event.metadata().level(),
            fields: visitor.fields,
        };

        let tree_event = tree::Event {
            shared,
            message: visitor.message,
            tag: self.tag.parse(event),
        };

        let current_span = ctx.event_span(event);

        if visitor.immediate {
            write_immediate(&tree_event, current_span.as_ref()).expect("writing urgent failed");
        }

        match current_span.as_ref() {
            Some(parent) => parent
                .extensions_mut()
                .get_mut::<OpenedSpan>()
                .expect(fail::OPENED_SPAN_NOT_IN_EXTENSIONS)
                .record_event(tree_event),
            None => self
                .processor
                .process(Tree::Event(tree_event))
                .expect(fail::PROCESSING_ERROR),
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<S>) {
        ctx.span(id)
            .expect(fail::SPAN_NOT_IN_CONTEXT)
            .extensions_mut()
            .get_mut::<OpenedSpan>()
            .expect(fail::OPENED_SPAN_NOT_IN_EXTENSIONS)
            .enter();
    }

    fn on_exit(&self, id: &Id, ctx: Context<S>) {
        ctx.span(id)
            .expect(fail::SPAN_NOT_IN_CONTEXT)
            .extensions_mut()
            .get_mut::<OpenedSpan>()
            .expect(fail::OPENED_SPAN_NOT_IN_EXTENSIONS)
            .exit();
    }

    fn on_close(&self, id: Id, ctx: Context<S>) {
        let span_ref = ctx.span(&id).expect(fail::SPAN_NOT_IN_CONTEXT);

        let mut span = span_ref
            .extensions_mut()
            .remove::<OpenedSpan>()
            .expect(fail::OPENED_SPAN_NOT_IN_EXTENSIONS)
            .close();

        // Ensure that the total duration is at least as much as the busy duration.
        if span.total_duration < span.busy_duration {
            span.total_duration = span.busy_duration;
        }

        match span_ref.parent() {
            Some(parent) => parent
                .extensions_mut()
                .get_mut::<OpenedSpan>()
                .expect(fail::OPENED_SPAN_NOT_IN_EXTENSIONS)
                .record_span(span),
            None => self
                .processor
                .process(Tree::Span(span))
                .expect(fail::PROCESSING_ERROR),
        }
    }
}

fn write_immediate<S>(event: &tree::Event, current: Option<&SpanRef<S>>) -> io::Result<()>
where
    S: for<'a> LookupSpan<'a>,
{
    // uuid timestamp LEVEL root > inner > leaf > my message here | key: val
    #[cfg(feature = "smallvec")]
    let mut writer = smallvec::SmallVec::<[u8; 256]>::new();
    #[cfg(not(feature = "smallvec"))]
    let mut writer = Vec::with_capacity(256);

    #[cfg(feature = "uuid")]
    if let Some(span) = current {
        let uuid = span
            .extensions()
            .get::<OpenedSpan>()
            .expect(fail::OPENED_SPAN_NOT_IN_EXTENSIONS)
            .span
            .uuid();
        write!(writer, "{} ", uuid)?;
    }

    #[cfg(feature = "chrono")]
    write!(writer, "{} ", event.timestamp().to_rfc3339())?;

    write!(writer, "{:<8} ", event.level())?;

    let tag = event.tag().unwrap_or_else(|| Tag::from(event.level()));

    write!(writer, "{icon} IMMEDIATE {icon} ", icon = tag.icon())?;

    if let Some(span) = current {
        for ancestor in span.scope().from_root() {
            write!(writer, "{} > ", ancestor.name())?;
        }
    }

    // we should just do pretty printing here.

    if let Some(message) = event.message() {
        write!(writer, "{}", message)?;
    }

    for field in event.fields().iter() {
        write!(writer, " | {}: {}", field.key(), field.value())?;
    }

    writeln!(writer)?;

    io::stderr().write_all(&writer)
}

/// Initializes a global subscriber with a [`ForestLayer`] using the default configuration.
///
/// This function is intended for quick initialization and processes log trees "inline",
/// meaning it doesn't take advantage of a worker task for formatting and writing.
/// To use a worker task, consider using the [`worker_task`] function. Alternatively,
/// configure a `Subscriber` manually using a `ForestLayer`.
///
/// [`worker_task`]: crate::runtime::worker_task
///
/// # Examples
/// ```
/// use tracing::{info, info_span};
///
/// tracing_forest::init();
///
/// info!("Hello, world!");
/// info_span!("my_span").in_scope(|| {
///     info!("Relevant information");
/// });
/// ```
/// Produces the the output:
/// ```log
/// INFO     ｉ [info]: Hello, world!
/// INFO     my_span [ 26.0µs | 100.000% ]
/// INFO     ┕━ ｉ [info]: Relevant information
/// ```
pub fn init() {
    Registry::default().with(ForestLayer::default()).init();
}

/// Initializes a global subscriber for cargo tests with a [`ForestLayer`] using the default
/// configuration.
///
/// This function is intended for test case initialization and processes log trees "inline",
/// meaning it doesn't take advantage of a worker task for formatting and writing.
///
/// [`worker_task`]: crate::runtime::worker_task
///
/// # Examples
/// ```
/// use tracing::{info, info_span};
///
/// let _ = tracing_forest::test_init();
///
/// info!("Hello, world!");
/// info_span!("my_span").in_scope(|| {
///     info!("Relevant information");
/// });
/// ```
pub fn test_init() -> Result<(), TryInitError> {
    Registry::default()
        .with(ForestLayer::new(TestCapturePrinter::new(), NoTag))
        .try_init()
}
