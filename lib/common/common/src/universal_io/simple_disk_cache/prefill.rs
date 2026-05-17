use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{LazyLock, mpsc};

use tokio::sync::oneshot;

use crate::generic_consts::Sequential;
use crate::universal_io::simple_disk_cache::BLOCK_SIZE;
use crate::universal_io::simple_disk_cache::file::LocalState;
use crate::universal_io::{
    OpenOptions, Populate, ReadRange, Result, UniversalRead, UniversalReadPipeline,
};

// TODO: choose appropriate default backend
type PrefillBackend = cfg_select! {
    target_os = "linux" => { crate::universal_io::IoUringFile }
    _ => { crate::universal_io::MmapFile }
};

pub static GLOBAL_PREFILL_THREAD: LazyLock<DiskPrefiller<PrefillBackend>> =
    LazyLock::new(DiskPrefiller::new);

pub struct DiskPrefiller<R> {
    sender: mpsc::Sender<PrefillReq>,
    _handle: std::thread::JoinHandle<Result<()>>,
    remote: PhantomData<R>,
}

struct PrefillProcess<R>
where
    R: UniversalRead + 'static,
{
    req_recv: mpsc::Receiver<PrefillReq>,
    pipeline: R::ReadPipeline<'static, u8, RemoteMeta<R>>,
}

impl<R> PrefillProcess<R>
where
    R: UniversalRead + 'static,
{
    fn schedule(&mut self, req: PrefillReq) -> Result<()> {
        let remote = R::open(
            &req.remote_path,
            OpenOptions {
                writeable: false,
                need_sequential: true,
                disk_parallel: None,
                populate: Populate::No,
                advice: None,
                prevent_caching: None,
            },
        )?;

        let remote = Box::new(remote);
        let remote = Box::leak::<'static>(remote);

        let remote_len = remote.len::<u8>()?;

        let range = ReadRange {
            byte_offset: 0,
            length: remote_len,
        };

        let local_state = LocalState::new(&req.local_path, remote_len, req.local_options)?;

        let meta = RemoteMeta {
            // SAFETY: Meta lives at least as long as the file ref in UniversalReadPipeline
            // For disk prefill, we don't need to care about lifetime after receiving
            // from `wait` because we write right after receiving, then drop the Box too.
            _remote: unsafe { Box::from_raw(remote) },
            local_state,
            finish: req.finish,
        };

        self.pipeline.schedule::<Sequential>(meta, remote, range)
    }
}

struct PrefillReq {
    remote_path: PathBuf,
    local_path: PathBuf,
    local_options: OpenOptions,
    finish: oneshot::Sender<LocalState>,
}

struct RemoteMeta<R> {
    _remote: Box<R>,
    local_state: LocalState,
    finish: oneshot::Sender<LocalState>,
}

impl<R> Default for DiskPrefiller<R>
where
    R: UniversalRead + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<R> DiskPrefiller<R>
where
    R: UniversalRead + 'static,
{
    pub fn new() -> Self {
        let (sender, recv) = mpsc::channel::<PrefillReq>();

        let process = move || -> Result<()> {
            let mut proc = PrefillProcess::<R> {
                req_recv: recv,
                pipeline: R::ReadPipeline::new()?,
            };

            loop {
                // Eagerly schedule as much as the pipeline will accept.
                while proc.pipeline.can_schedule()
                    && let Ok(req) = proc.req_recv.try_recv()
                {
                    proc.schedule(req)?;
                }

                // `wait()` blocks for a completion if anything is in flight,
                // and returns `None` immediately otherwise.
                match proc.pipeline.wait()? {
                    Some((meta, bytes)) => {
                        let RemoteMeta {
                            _remote,
                            local_state,
                            finish,
                        } = meta;
                        unsafe {
                            local_state.write_mmap_bytes(
                                &bytes,
                                0..bytes.len().div_ceil(BLOCK_SIZE) as u32,
                            );
                        };
                        finish.send(local_state).expect("todo: handle this");
                    }
                    None => {
                        // Nothing in flight and nothing pending. Let channel block for new work.
                        let Ok(req) = proc.req_recv.recv() else {
                            return Ok(());
                        };
                        proc.schedule(req)?;
                    }
                }
            }
        };

        let handle = std::thread::spawn(process);

        Self {
            sender,
            _handle: handle,
            remote: PhantomData,
        }
    }

    pub(super) fn send_request(
        &self,
        remote_path: PathBuf,
        local_path: PathBuf,
        local_options: OpenOptions,
        finish: oneshot::Sender<LocalState>,
    ) {
        let req = PrefillReq {
            remote_path,
            local_path,
            local_options,
            finish,
        };
        self.sender.send(req).expect("todo: handle this");
    }
}
