//! Lazy first-use initialization of a [`DiskCache`]'s [`State`].
//!
//! A freshly opened `DiskCache` has no [`State`] yet — only an [`InitSource`]
//! describing *how* the local mirror should be brought to life. The first
//! operation that actually needs the mirror ([`DiskCache::state`], or an
//! explicit prefill from [`super::reopen`]) takes `init_lock` and runs
//! [`DiskCache::init_state`] exactly once, consuming the `InitSource` and
//! publishing a populated `State` into the `OnceLock`.

use super::{DiskCache, State};
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::simple_disk_cache::{DiskCacheRemote, to_block_range};
use crate::universal_io::{OwnedPipeline, Result, UniversalRead};

/// Where a [`DiskCache`]'s [`State`] comes from the first time it is needed.
///
/// `FromScratch` is *lazy*: it needs only the remote's **length** and faults
/// blocks in on demand. `Prefiller` is *eager*: it holds an in-flight whole-object
/// read and fills the mirror from those **bytes** at once.
///
/// ```text
/// Populate::No | Auto    →  FromScratch          →  remote.len(); empty mmap; blocks faulted in on read
/// Populate::Blocking|Pref →  Prefiller(pipeline) →  pipeline.wait(); mmap sized to bytes; all written
/// ```
pub(crate) enum InitSource<R>
where
    R: UniversalRead + 'static,
{
    /// Lazy: build an empty local mmap (sized from the remote length) and let
    /// reads fill blocks on demand. Chosen for `Populate::No` / `Populate::Auto`.
    FromScratch,
    /// Eager: an in-flight whole-object read scheduled at open time; init waits
    /// on it and writes the whole mirror. For `Populate::Blocking` / `PreferBackground`.
    Prefiller(OwnedPipeline<R, ()>),
    /// The reopen-time counterpart of [`Prefiller`](Self::Prefiller): an in-flight
    /// read of just the appended tail (block-aligned old length → new EOF). Init
    /// resizes the mirror and writes only that suffix. See [`super::reopen`].
    PartialPrefiller {
        prefiller: OwnedPipeline<R, u64>,
        local_state: LocalState,
    },
}

impl<R> std::fmt::Debug for InitSource<R>
where
    R: UniversalRead,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InitSource::FromScratch => write!(f, "FromScratch"),
            InitSource::Prefiller(_) => write!(f, "Prefiller"),
            InitSource::PartialPrefiller { .. } => write!(f, "PartialPrefiller"),
        }
    }
}

impl<R> DiskCache<R>
where
    R: DiskCacheRemote,
{
    /// Return the cached [`State`], initializing it on first call.
    pub(crate) fn state(&self) -> Result<&State<R>> {
        if let Some(state) = self.state.get() {
            return Ok(state);
        }

        let mut init_guard = self.init_lock.lock();

        // Try again now that we have the lock, in case another thread initialized it first.
        if self.state.get().is_none() {
            self.init_state(&mut init_guard, true)?;
        }

        Ok(self.state.get().expect("just initialized"))
    }

    /// Initialize the local state depending on `InitSource`
    ///
    /// If `allow_from_scratch` is false, this method will avoid initializing if `InitSource::FromScratch` is set.
    /// This is helpful for [`Self::reopen`] scenario where we can avoid work if no reads have taken place.
    pub(crate) fn init_state(
        &self,
        init_guard: &mut InitSource<R>,
        allow_from_scratch: bool,
    ) -> Result<()> {
        let state = match std::mem::replace(init_guard, InitSource::FromScratch) {
            InitSource::FromScratch => {
                if !allow_from_scratch {
                    return Ok(());
                }
                self.new_state_from_scratch(self.open_remote()?)?
            }
            InitSource::Prefiller(mut prefiller) => {
                match prefiller.wait()? {
                    Some((_, bytes)) => {
                        let local = LocalState::new(
                            &self.local_path,
                            // bytes length is the length of the remote file
                            bytes.len() as u64,
                            self.open_options,
                        )?;
                        let blocks_range = to_block_range(0..bytes.len() as u64);
                        unsafe { local.write_mmap_bytes(&bytes, blocks_range) };
                        State {
                            local,
                            remote: prefiller.into_inner(),
                        }
                    }
                    None => {
                        debug_assert!(
                            false,
                            "Looks like the request for prefill bytes was incorrect"
                        );
                        if !allow_from_scratch {
                            return Ok(());
                        }
                        // init from scratch
                        self.new_state_from_scratch(prefiller.into_inner())?
                    }
                }
            }
            InitSource::PartialPrefiller {
                mut prefiller,
                mut local_state,
            } => {
                match prefiller.wait()? {
                    Some((start, bytes)) if !bytes.is_empty() => {
                        let end = start + bytes.len() as u64;

                        local_state.resize(&self.local_path, end)?;

                        let blocks_range = to_block_range(start..end);
                        unsafe { local_state.write_mmap_bytes(&bytes, blocks_range) }
                    }
                    // `None`: nothing was scheduled. `Some(_, empty)`: the
                    // open-ended tail read from our block-aligned offset came back
                    // empty, i.e. the remote didn't grow past it. Either way there
                    // is nothing to apply — and we must not `resize` down to the
                    // offset, which would truncate the local mirror.
                    // TODO: double check that the remote didn't shrink?
                    Some(_) | None => {}
                };

                let remote = prefiller.into_inner();

                State {
                    remote,
                    local: local_state,
                }
            }
        };

        self.state
            .set(state)
            .expect("OnceLock::set must succeed while holding init_lock");

        Ok(())
    }

    fn new_state_from_scratch(&self, remote: R) -> Result<State<R>> {
        let len = remote.len::<u8>()?;
        let local = LocalState::new(&self.local_path, len, self.open_options)?;
        Ok(State { remote, local })
    }

    /// Set up [`InitSource::Prefiller`] when the cache is cold
    /// (not initialized and `InitSource::FromScratch`)
    pub(super) fn prefill_if_uninit(&self) -> Result<()> {
        if self.state.get().is_none() {
            let mut init_guard = self.init_lock.lock();
            if self.state.get().is_none() {
                // Cold cache: use the remote's `schedule_whole` to prevent an
                // additional `len` call to remote.
                if matches!(&*init_guard, InitSource::FromScratch) {
                    let mut pipeline = OwnedPipeline::new(self.open_remote()?)?;
                    pipeline.schedule_whole((), 0)?;
                    *init_guard = InitSource::Prefiller(pipeline);
                }
            }
        }

        Ok(())
    }
}
