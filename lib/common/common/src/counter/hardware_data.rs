/// Contains all hardware metrics. Only serves as value holding structure without any semantics.
#[derive(Copy, Clone)]
pub struct HardwareData {
    pub cpu: RealCpuMeasurement,
    pub payload_io_read: usize,
    pub payload_io_write: usize,
    pub vector_io_read: usize,
    pub vector_io_write: usize,
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
