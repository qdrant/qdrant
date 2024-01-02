//! Trait for processing log trees on completion.
//!
//! See [`Processor`] for more details.
use crate::printer::{MakeStderr, MakeStdout, Pretty, Printer};
use crate::tree::Tree;
use std::error;
use std::sync::Arc;
use thiserror::Error;

/// Error type returned if a [`Processor`] fails.
#[derive(Error, Debug)]
#[error("{source}")]
pub struct Error {
    /// The recoverable [`Tree`] type that couldn't be processed.
    pub tree: Tree,

    source: Box<dyn error::Error + Send + Sync>,
}

/// Create an error for when a [`Processor`] fails to process a [`Tree`].
pub fn error(tree: Tree, source: Box<dyn error::Error + Send + Sync>) -> Error {
    Error { tree, source }
}

/// The result type of [`Processor::process`].
pub type Result = std::result::Result<(), Error>;

/// A trait for processing completed [`Tree`]s.
///
/// `Processor`s are responsible for both formatting and writing logs to their
/// intended destinations. This is typically implemented using
/// [`Formatter`], [`MakeWriter`], and [`io::Write`].
///
/// While this trait may be implemented on downstream types, [`from_fn`]
/// provides a convenient interface for creating `Processor`s without having to
/// explicitly define new types.
///
/// [trace trees]: crate::tree::Tree
/// [`Formatter`]: crate::printer::Formatter
/// [`MakeWriter`]: tracing_subscriber::fmt::MakeWriter
/// [`io::Write`]: std::io::Write
pub trait Processor: 'static + Sized {
    /// Process a [`Tree`]. This can mean many things, such as writing to
    /// stdout or a file, sending over a network, storing in memory, ignoring,
    /// or anything else.
    ///
    /// # Errors
    ///
    /// If the `Tree` cannot be processed, then it is returned along with a
    /// `Box<dyn Error + Send + Sync>`. If the processor is configured with a
    /// fallback processor from [`Processor::or`], then the `Tree` is deferred
    /// to that processor.
    fn process(&self, tree: Tree) -> Result;

    /// Returns a `Processor` that first attempts processing with `self`, and
    /// resorts to processing with `fallback` on failure.
    ///
    /// Note that [`or_stdout`], [`or_stderr`], and [`or_none`] can be used as
    /// shortcuts for pretty printing or dropping the `Tree` entirely.
    ///
    /// [`or_stdout`]: Processor::or_stdout
    /// [`or_stderr`]: Processor::or_stderr
    /// [`or_none`]: Processor::or_none
    fn or<P: Processor>(self, processor: P) -> WithFallback<Self, P> {
        WithFallback {
            primary: self,
            fallback: processor,
        }
    }

    /// Returns a `Processor` that first attempts processing with `self`, and
    /// resorts to pretty-printing to stdout on failure.
    fn or_stdout(self) -> WithFallback<Self, Printer<Pretty, MakeStdout>> {
        self.or(Printer::new().writer(MakeStdout))
    }

    /// Returns a `Processor` that first attempts processing with `self`, and
    /// resorts to pretty-printing to stderr on failure.
    fn or_stderr(self) -> WithFallback<Self, Printer<Pretty, MakeStderr>> {
        self.or(Printer::new().writer(MakeStderr))
    }

    /// Returns a `Processor` that first attempts processing with `self`, otherwise
    /// silently fails.
    fn or_none(self) -> WithFallback<Self, Sink> {
        self.or(Sink)
    }
}

/// A [`Processor`] composed of a primary and a fallback `Processor`.
///
/// This type is returned by [`Processor::or`].
#[derive(Debug)]
pub struct WithFallback<P, F> {
    primary: P,
    fallback: F,
}

/// A [`Processor`] that ignores any incoming logs.
///
/// This processor cannot fail.
#[derive(Debug)]
pub struct Sink;

/// A [`Processor`] that processes incoming logs via a function.
///
/// Instances of `FromFn` are returned by the [`from_fn`] function.
#[derive(Debug)]
pub struct FromFn<F>(F);

/// Create a processor that processes incoming logs via a function.
///
/// # Examples
///
/// Internally, [`worker_task`] uses `from_fn` to allow the subscriber to send
/// trace data across a channel to a processing task.
/// ```
/// use tokio::sync::mpsc;
/// use tracing_forest::processor;
///
/// let (tx, rx) = mpsc::unbounded_channel();
///
/// let sender_processor = processor::from_fn(move |tree| tx
///     .send(tree)
///     .map_err(|err| {
///         let msg = err.to_string().into();
///         processor::error(err.0, msg)
///     })
/// );
///
/// // -- snip --
/// ```
///
/// [`worker_task`]: crate::runtime::worker_task
pub fn from_fn<F>(f: F) -> FromFn<F>
where
    F: 'static + Fn(Tree) -> Result,
{
    FromFn(f)
}

impl<P, F> Processor for WithFallback<P, F>
where
    P: Processor,
    F: Processor,
{
    fn process(&self, tree: Tree) -> Result {
        self.primary.process(tree).or_else(|err| {
            eprintln!("{}, using fallback processor...", err);
            self.fallback.process(err.tree)
        })
    }
}

impl Processor for Sink {
    fn process(&self, _tree: Tree) -> Result {
        Ok(())
    }
}

impl<F> Processor for FromFn<F>
where
    F: 'static + Fn(Tree) -> Result,
{
    fn process(&self, tree: Tree) -> Result {
        (self.0)(tree)
    }
}

impl<P: Processor> Processor for Box<P> {
    fn process(&self, tree: Tree) -> Result {
        self.as_ref().process(tree)
    }
}

impl<P: Processor> Processor for Arc<P> {
    fn process(&self, tree: Tree) -> Result {
        self.as_ref().process(tree)
    }
}
