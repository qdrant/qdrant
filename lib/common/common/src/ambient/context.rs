use std::fmt;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::Arc;

use ecow::EcoString;

use super::hw::{HardwareData, HwSharedDrain};
use super::profile::ProfileHandle;
use super::trace::{self, Event, Sink, SpanId};
use crate::cpu_utilization::CpuUtilization;

/// A handle to a node of a request's span tree: the request itself, or a span within it.
/// Reference-counted: clones read and write the same counters and trace.
#[derive(Clone)]
pub struct AmbientContext(Arc<Node>);

pub(super) struct Node {
    root: Arc<Root>,
    span: Option<SpanId>,
    /// Kept alive, so it ends after its children.
    _parent: Option<AmbientContext>,
}

/// One per request.
struct Root {
    /// Always collected. Measures _only this_ request.
    hw: HwSharedDrain,
    /// When present, measurements are _also_ added to this drain.
    collection: Option<Arc<HwSharedDrain>>,
    cpu_utilization: CpuUtilization,
    sink: Option<Sink>,
    /// Present when the request is profiled, see [`super::profile`].
    profile: Option<ProfileHandle>,
    /// Profile forks only: measurements are _also_ added to this context, instead of
    /// `collection`, so the fork can be read on its own and still counts for the request.
    forward: Option<AmbientContext>,
}

impl AmbientContext {
    /// The root of a new request. `collection` also receives its hardware usage.
    pub fn root(name: &str, collection: Option<Arc<HwSharedDrain>>, sink: Option<Sink>) -> Self {
        Self::root_profiled(name, collection, sink, None)
    }

    /// [`Self::root`], profiled into `profile` when set.
    pub(super) fn root_profiled(
        name: &str,
        collection: Option<Arc<HwSharedDrain>>,
        sink: Option<Sink>,
        profile: Option<ProfileHandle>,
    ) -> Self {
        let root = Arc::new(Root {
            hw: HwSharedDrain::default(),
            collection,
            cpu_utilization: CpuUtilization::new(),
            sink: sink.filter(Sink::is_started),
            profile,
            forward: None,
        });
        Self::node(root, None, || EcoString::from(name))
    }

    /// A profile fork of this context: its own counters and profile frame, forwarding all
    /// measurements to `self`. Only created for profiled requests.
    pub(super) fn fork(&self, profile: ProfileHandle) -> Self {
        self.fork_with(Some(profile))
    }

    fn fork_with(&self, profile: Option<ProfileHandle>) -> Self {
        let root = Arc::new(Root {
            hw: HwSharedDrain::default(),
            collection: None,
            cpu_utilization: self.0.root.cpu_utilization.clone(),
            sink: self.0.root.sink.clone(),
            profile,
            forward: Some(self.clone()),
        });
        // Keeps tracing into the span of `self`, if any; the fork owns no span.
        Self(Arc::new(Node {
            root,
            span: None,
            _parent: Some(self.clone()),
        }))
    }

    /// A measuring fork of this context, for per-operation statistics: its own counters,
    /// forwarding all measurements to `self`, and the same profile as `self`, if any.
    pub fn measure_fork(&self) -> Self {
        self.fork_with(self.0.root.profile.clone())
    }

    /// The profile of this request, if profiled.
    pub(super) fn profile(&self) -> Option<&ProfileHandle> {
        self.0.root.profile.as_ref()
    }

    #[cfg(feature = "testing")]
    #[expect(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::root("", None, None)
    }

    /// A server request, traced as `name` into the global sink, see [`trace::install`].
    pub fn request(name: &str, collection: Arc<HwSharedDrain>) -> Self {
        Self::root(name, Some(collection), trace::global().cloned())
    }

    /// A child span, see [`super::trace::span!`].
    pub(super) fn child(&self, name: impl FnOnce() -> EcoString) -> Self {
        Self::node(Arc::clone(&self.0.root), Some(self), name)
    }

    pub fn cpu_utilization(&self) -> CpuUtilization {
        self.0.root.cpu_utilization.clone()
    }

    pub fn accumulate(&self, src: HardwareData) {
        let root = &self.0.root;
        root.hw.add(src);
        if let Some(collection) = &root.collection {
            collection.add(src);
        }
        if let Some(forward) = &root.forward {
            forward.accumulate(src);
        }
    }

    /// Accumulate usage values for request drain only.
    /// This is useful if we want to report usage, which happened on another machine
    /// So we don't want to accumulate the same usage on the current machine second time
    pub fn accumulate_request(&self, src: HardwareData) {
        self.0.root.hw.add(src);
        if let Some(forward) = &self.0.root.forward {
            forward.accumulate_request(src);
        }
    }

    /// Get hardware usage for this request.
    /// Not related to the collection drain.
    pub fn hw_data(&self) -> HardwareData {
        self.0.root.hw.load()
    }

    pub fn is_traced(&self) -> bool {
        self.traced().is_some()
    }

    /// The sink and the id of this node's span.
    pub(super) fn traced(&self) -> Option<(&Sink, SpanId)> {
        Some((self.0.root.sink.as_ref()?, self.0.span?))
    }

    fn node(root: Arc<Root>, parent: Option<&Self>, name: impl FnOnce() -> EcoString) -> Self {
        let span = root.sink.as_ref().map(|sink| {
            let id = trace::next_id();
            sink.send(Event::SpanStart {
                id,
                parent: parent.and_then(|parent| parent.0.span).unwrap_or(0),
                timestamp: trace::now(),
                name: name(),
            });
            id
        });
        Self(Arc::new(Node {
            root,
            span,
            _parent: parent.cloned(),
        }))
    }

    pub(super) fn as_ptr(&self) -> NonNull<Node> {
        NonNull::new(Arc::as_ptr(&self.0).cast_mut()).expect("Arc::as_ptr is never null")
    }

    /// # Safety
    /// `ptr` comes from [`Self::as_ptr`] of a context that outlives the result.
    pub(super) unsafe fn borrow_ptr(ptr: NonNull<Node>) -> ManuallyDrop<Self> {
        // SAFETY: see the function contract; `ManuallyDrop` keeps the refcount intact.
        ManuallyDrop::new(Self(unsafe { Arc::from_raw(ptr.as_ptr()) }))
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let (Some(sink), Some(id)) = (&self.root.sink, self.span) {
            sink.send(Event::SpanEnd {
                id,
                timestamp: trace::now(),
            });
        }
    }
}

impl fmt::Debug for AmbientContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmbientContext")
            .field("span", &self.0.span)
            .finish_non_exhaustive()
    }
}
