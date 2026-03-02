//! Minimal multi-source read implementation: [`VecMultiUniversalIo`].

use std::path::Path;

use crate::universal_io::multi_universal_read::{MultiUniversalRead, SourceId};
use crate::universal_io::{
    ElementOffset, ElementsRange, Flusher, MultiUniversalWrite, OpenOptions, Result,
    UniversalIoError, UniversalRead, UniversalWrite,
};

/// Minimal multi-source read implementation: a collection of [`UniversalRead`] backends
/// addressable by [`SourceId`] (index). Supports attaching more sources by path at runtime.
#[derive(Debug)]
pub struct VecMultiUniversalIo<T, S> {
    sources: Vec<S>,
    open_options: OpenOptions,
    _element: std::marker::PhantomData<T>,
}

impl<T: Copy + 'static, S: UniversalRead<T> + Send> MultiUniversalRead<T>
    for VecMultiUniversalIo<T, S>
{
    fn new(options: OpenOptions) -> Self {
        Self {
            sources: Vec::new(),
            open_options: options,
            _element: std::marker::PhantomData,
        }
    }

    fn len(&self) -> usize {
        self.sources.len()
    }

    fn attach(&mut self, path: &Path) -> Result<SourceId> {
        let source = S::open(path, self.open_options)?;
        let id = SourceId(self.sources.len());
        self.sources.push(source);
        Ok(id)
    }

    fn read_batch_multi<const SEQUENTIAL: bool>(
        &self,
        requests: impl IntoIterator<Item = (SourceId, ElementsRange)>,
        mut callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        for (idx, (source_id, range)) in requests.into_iter().enumerate() {
            let source =
                self.sources
                    .get(source_id.0)
                    .ok_or(UniversalIoError::InvalidSourceId {
                        source_id: source_id.0,
                        num_sources: self.sources.len(),
                    })?;
            let data = source.read::<SEQUENTIAL>(range)?;
            callback(idx, &data)?;
        }
        Ok(())
    }

    fn source_len(&self, source_id: SourceId) -> Result<u64> {
        let source = self
            .sources
            .get(source_id.0)
            .ok_or(UniversalIoError::InvalidSourceId {
                source_id: source_id.0,
                num_sources: self.sources.len(),
            })?;
        source.len()
    }

    fn populate(&self) -> Result<()> {
        for source in &self.sources {
            source.populate()?;
        }
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        for source in &self.sources {
            source.clear_ram_cache()?;
        }
        Ok(())
    }
}

impl<T: Copy + 'static, S: UniversalWrite<T> + Send> MultiUniversalWrite<T>
    for VecMultiUniversalIo<T, S>
{
    fn write_batch_multi<'a>(
        &mut self,
        requests: impl IntoIterator<Item = (SourceId, ElementOffset, &'a [T])>,
    ) -> Result<()> {
        let num_sources = self.sources.len();
        for (source_id, offset, data) in requests {
            let source =
                self.sources
                    .get_mut(source_id.0)
                    .ok_or(UniversalIoError::InvalidSourceId {
                        source_id: source_id.0,
                        num_sources,
                    })?;
            source.write(offset, data)?;
        }
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let flushers: Vec<Flusher> = self.sources.iter().map(|s| s.flusher()).collect();
        Box::new(move || {
            for f in flushers {
                f()?;
            }
            Ok(())
        })
    }
}
