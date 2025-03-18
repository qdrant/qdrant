/// Contains all hardware metrics. Only serves as value holding structure without any semantics.
#[derive(Copy, Clone)]
pub struct HardwareData {
    pub cpu: RealCpuMeasurement,
    pub payload_io_read: usize,
    pub payload_io_write: usize,
    pub vector_io_read: RealVectorIoReadMeasurement,
    pub vector_io_write: usize,
    pub payload_index_io_read: usize,
}

/// Wrapper type to ensure we properly apply cpu-multiplier everywhere.
#[derive(Copy, Clone)]
pub struct RealCpuMeasurement(usize);

impl RealCpuMeasurement {
    pub fn new(cpu: usize, multiplier: usize) -> Self {
        Self(cpu * multiplier)
    }

    /// Returns the real CPU value.
    #[inline]
    pub fn get(self) -> usize {
        self.0
    }
}

#[derive(Copy, Clone)]
pub struct RealVectorIoReadMeasurement(usize);

impl RealVectorIoReadMeasurement {
    pub fn new(vector_io_read: usize, multiplier: usize) -> Self {
        Self(vector_io_read * multiplier)
    }

    /// Returns the real vector io read value.
    #[inline]
    pub fn get(self) -> usize {
        self.0
    }
}
