//! Minimal multi-source write implementation: [`VecMultiUniversalWrite`].

use std::path::Path;

use super::MultiUniversalWrite;
use crate::universal_io::multi_universal_read::SourceId;
use crate::universal_io::{
    ElementOffset, Flusher, OpenOptions, Result, UniversalIoError, UniversalWrite,
};

/// Minimal multi-source write implementation: a collection of [`UniversalWrite`] backends
/// addressable by [`SourceId`] (index). Supports attaching more sources by path at runtime.
#[derive(Debug)]
pub struct VecMultiUniversalWrite<T, S> {
    sources: Vec<S>,
    _element: std::marker::PhantomData<T>,
}

impl<T: Copy + 'static, S: UniversalWrite<T> + Send> VecMultiUniversalWrite<T, S> {
    /// Create from an initial set of sources. Source ids will be 0, 1, 2, ...
    pub fn from_sources(sources: Vec<S>) -> Self {
        Self {
            sources,
            _element: std::marker::PhantomData,
        }
    }
}

impl<T: Copy + 'static, S: UniversalWrite<T> + Send> MultiUniversalWrite<T>
    for VecMultiUniversalWrite<T, S>
{
    fn new(options: OpenOptions) -> Self {
        let _ = options;
        Self {
            sources: Vec::new(),
            _element: std::marker::PhantomData,
        }
    }

    fn len(&self) -> usize {
        self.sources.len()
    }

    fn attach(&mut self, path: &Path, options: OpenOptions) -> Result<SourceId> {
        let source = S::open(path, options)?;
        let id = SourceId(self.sources.len());
        self.sources.push(source);
        Ok(id)
    }

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

    fn flusher(&self) -> Flusher {
        let flushers: Vec<Flusher> = self.sources.iter().map(|s| s.flusher()).collect();
        Box::new(move || {
            for f in flushers {
                f()?;
            }
            Ok(())
        })
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
