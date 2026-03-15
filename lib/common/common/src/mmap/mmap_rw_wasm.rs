use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::{fmt, io, mem};

use bitvec::slice::BitSlice;

use super::advice::{Advice, AdviceSetting};
use super::ops;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Mmap length must be {0} to match the size of type, but it is {1}")]
    SizeExact(usize, usize),
    #[error("Mmap length must be at least {0} to match the size of type, but it is {1}")]
    SizeLess(usize, usize),
    #[error("Mmap length must be multiple of {0} to match the size of type, but it is {1}")]
    SizeMultiple(usize, usize),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("File not found: {0}")]
    MissingFile(String),
}

pub type MmapFlusher = Box<dyn FnOnce() -> Result<(), Error> + Send>;

// --- MmapMutStub: WASM replacement for memmap2::MmapMut ---

unsafe extern "C" {
    fn wasi_mmap_register(fd: u32, ptr: *const u8, len: usize);
    fn wasi_mmap_flush(ptr: *const u8, len: usize);
}

#[derive(Debug)]
pub struct MmapMutStub {
    pub(crate) data: &'static mut [u8],
}

impl MmapMutStub {
    pub fn new() -> Self {
        Self {
            data: Box::leak(Box::new([])),
        }
    }

    /// Read a file's contents into a leaked heap allocation registered with the WASI host.
    unsafe fn read_file_to_leaked(file: &crate::fs::File) -> io::Result<&'static mut [u8]> {
        use std::io::{Read, Seek, SeekFrom};
        use std::os::wasi::io::AsRawFd;

        let std_file = file.file();
        let fd = std_file.as_raw_fd();
        let mut f: mem::ManuallyDrop<std::fs::File> =
            mem::ManuallyDrop::new(unsafe { std::os::wasi::io::FromRawFd::from_raw_fd(fd) });
        f.seek(SeekFrom::Start(0))?;
        let mut vec = Vec::new();
        f.read_to_end(&mut vec)?;

        let data = Box::leak(vec.into_boxed_slice());
        unsafe { wasi_mmap_register(fd as u32, data.as_ptr(), data.len()) };
        Ok(data)
    }

    pub unsafe fn map(file: &crate::fs::File) -> io::Result<Self> {
        let data = unsafe { Self::read_file_to_leaked(file)? };
        Ok(Self { data })
    }

    pub unsafe fn map_mut(file: &crate::fs::File) -> io::Result<Self> {
        unsafe { Self::map(file) }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn flush(&self) -> io::Result<()> {
        unsafe { wasi_mmap_flush(self.data.as_ptr(), self.data.len()) };
        Ok(())
    }

    pub fn make_read_only(self) -> io::Result<MmapMutStub> {
        Ok(self)
    }

    pub fn populate(&self) {}

    fn clone_stub(&self) -> Self {
        Self {
            data: unsafe {
                std::slice::from_raw_parts_mut(self.data.as_ptr() as *mut u8, self.data.len())
            },
        }
    }

    pub fn flush_range(&self, _offset: usize, _len: usize) -> io::Result<()> {
        self.flush()
    }
}

impl Drop for MmapMutStub {
    fn drop(&mut self) {
        // Data is Box::leak'd and intentionally not freed here.
        // Qdrant components expect mmap-like lifetime semantics.
    }
}

impl Deref for MmapMutStub {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl DerefMut for MmapMutStub {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
    }
}

impl crate::mmap::Madviseable for MmapMutStub {
    fn madvise(&self, _advice: crate::mmap::Advice) -> Result<(), io::Error> {
        Ok(())
    }

    fn populate(&self) {}
}

pub type Mmap = MmapMutStub;
pub type MmapMut = MmapMutStub;

// --- MmapOptions: WASM replacement for memmap2::MmapOptions ---

pub struct MmapOptions {
    offset: u64,
    len: Option<usize>,
}

impl MmapOptions {
    pub fn new() -> Self {
        Self {
            offset: 0,
            len: None,
        }
    }

    pub fn offset(&mut self, offset: u64) -> &mut Self {
        self.offset = offset;
        self
    }

    pub fn len(&mut self, len: usize) -> &mut Self {
        self.len = Some(len);
        self
    }

    pub unsafe fn map_mut(&self, file: &crate::fs::File) -> io::Result<MmapMutStub> {
        use std::io::{Read, Seek, SeekFrom};
        use std::os::wasi::io::AsRawFd;

        let std_file = file.file();
        let fd = std_file.as_raw_fd();
        let mut f: mem::ManuallyDrop<std::fs::File> =
            mem::ManuallyDrop::new(unsafe { std::os::wasi::io::FromRawFd::from_raw_fd(fd) });
        f.seek(SeekFrom::Start(self.offset))?;
        let vec = if let Some(len) = self.len {
            let mut v = vec![0u8; len];
            f.read_exact(&mut v)?;
            v
        } else {
            let mut v = Vec::new();
            f.read_to_end(&mut v)?;
            v
        };
        let data = Box::leak(vec.into_boxed_slice());
        unsafe { wasi_mmap_register(fd as u32, data.as_ptr(), data.len()) };
        Ok(MmapMutStub { data })
    }

    pub fn map_anon(&self) -> io::Result<MmapMutStub> {
        let len = self.len.unwrap_or(0);
        let vec = vec![0u8; len];
        let data = Box::leak(vec.into_boxed_slice());
        Ok(MmapMutStub { data })
    }
}

// --- MmapType<T>: typed view over an mmap ---

pub struct MmapType<T: ?Sized + 'static> {
    pub r#type: &'static mut T,
    pub mmap: MmapMutStub,
}

impl<T: ?Sized + 'static> fmt::Debug for MmapType<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MmapType").finish_non_exhaustive()
    }
}

impl<T: ?Sized + 'static> MmapType<T> {
    pub fn flusher(&self) -> MmapFlusher {
        let mmap_ptr = self.mmap.clone_stub();
        Box::new(move || mmap_ptr.flush().map_err(Error::Io))
    }

    pub fn populate(&self) -> io::Result<()> {
        Ok(())
    }
}

impl<T: 'static> MmapType<T>
where
    T: zerocopy::FromBytes + zerocopy::Immutable + zerocopy::KnownLayout,
{
    pub unsafe fn from(mmap: MmapMutStub) -> Self {
        unsafe { Self::try_from(mmap).unwrap() }
    }

    pub unsafe fn try_from(mmap: MmapMutStub) -> Result<Self, Error> {
        let size_t = mem::size_of::<T>();
        if mmap.len() < size_t {
            return Err(Error::SizeLess(size_t, mmap.len()));
        }
        let ptr = mmap.data.as_ptr() as *mut T;
        let r#type = unsafe { &mut *ptr };
        Ok(Self { r#type, mmap })
    }
}

impl<T: 'static> MmapType<[T]> {
    /// Transform a mmap into a typed slice mmap of type `&[T]`.
    pub unsafe fn try_slice_from(mmap: MmapMutStub) -> Result<Self, Error> {
        let size_t = mem::size_of::<T>();
        let len = if size_t == 0 {
            0
        } else {
            if !mmap.data.len().is_multiple_of(size_t) {
                return Err(Error::SizeMultiple(size_t, mmap.data.len()));
            }
            mmap.data.len() / size_t
        };
        let ptr = mmap.data.as_ptr() as *mut T;
        let r#type = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
        Ok(Self { r#type, mmap })
    }
}

impl<T: ?Sized + 'static> Deref for MmapType<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.r#type
    }
}

impl<T: ?Sized + 'static> DerefMut for MmapType<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.r#type
    }
}

// --- MmapSlice<T>: wraps MmapType<[T]>, matching native structure ---

pub struct MmapSlice<T: Sized + 'static> {
    mmap: MmapType<[T]>,
}

impl<T: Sized + 'static> fmt::Debug for MmapSlice<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MmapSlice")
            .field("mmap", &self.mmap)
            .finish_non_exhaustive()
    }
}

impl<T: Sized + 'static> MmapSlice<T> {
    pub unsafe fn try_from(mmap: MmapMutStub) -> Result<Self, Error> {
        unsafe { MmapType::try_slice_from(mmap) }.map(|mmap| Self { mmap })
    }

    pub unsafe fn from(mmap: MmapMutStub) -> Self {
        unsafe { Self::try_from(mmap).unwrap() }
    }

    pub fn flusher(&self) -> MmapFlusher {
        self.mmap.flusher()
    }

    pub fn populate(&self) -> io::Result<()> {
        Ok(())
    }

    pub fn create<I>(path: &Path, iter: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        let mut iter = iter.into_iter();
        let file_len = iter.len() * mem::size_of::<T>();

        let _file = ops::create_and_ensure_length(path, file_len)?;
        let mmap = ops::open_write_mmap(path, AdviceSetting::Advice(Advice::Normal), false)?;

        let mut mmap_slice = unsafe { Self::try_from(mmap)? };
        mmap_slice.fill_with(|| iter.next().expect("iterator size mismatch"));
        mmap_slice.flusher()()?;

        Ok(())
    }
}

impl<T: Sized + 'static> Deref for MmapSlice<T> {
    type Target = MmapType<[T]>;
    fn deref(&self) -> &Self::Target {
        &self.mmap
    }
}

impl<T: Sized + 'static> DerefMut for MmapSlice<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mmap
    }
}

// --- MmapBitSlice: wraps MmapType<BitSlice>, matching native structure ---

#[derive(Debug)]
pub struct MmapBitSlice {
    mmap: MmapType<BitSlice>,
}

impl MmapBitSlice {
    const MIN_FILE_SIZE: usize = mem::size_of::<usize>();

    pub fn try_from(mmap: MmapMutStub, header_size: usize) -> Result<Self, Error> {
        let data = &mut mmap.data[header_size..];
        let size_t = mem::size_of::<usize>();
        let len = data.len() / size_t;
        let ptr = data.as_ptr() as *mut usize;
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
        let bitslice = BitSlice::from_slice_mut(slice);

        Ok(Self {
            mmap: MmapType {
                r#type: unsafe { &mut *(bitslice as *mut BitSlice) },
                mmap,
            },
        })
    }

    pub fn from(mmap: MmapMutStub, header_size: usize) -> Self {
        Self::try_from(mmap, header_size).unwrap()
    }

    pub fn flusher(&self) -> MmapFlusher {
        self.mmap.flusher()
    }

    pub fn populate(&self) -> io::Result<()> {
        Ok(())
    }

    pub fn create(path: &Path, bitslice: &BitSlice) -> Result<(), Error> {
        let bits_count = bitslice.len();
        let bytes_count = bits_count
            .div_ceil(u8::BITS as usize)
            .next_multiple_of(Self::MIN_FILE_SIZE);

        let _file = ops::create_and_ensure_length(path, bytes_count)?;
        let mmap = ops::open_write_mmap(path, AdviceSetting::Advice(Advice::Normal), false)?;

        let mut mmap_bitslice = MmapBitSlice::try_from(mmap, 0)?;
        mmap_bitslice.fill_with(|idx| {
            bitslice
                .get(idx)
                .map(|bitref| bitref.as_ref().to_owned())
                .unwrap_or(false)
        });
        mmap_bitslice.flusher()()?;

        Ok(())
    }
}

impl Deref for MmapBitSlice {
    type Target = BitSlice;
    fn deref(&self) -> &BitSlice {
        &self.mmap
    }
}

impl DerefMut for MmapBitSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mmap
    }
}
