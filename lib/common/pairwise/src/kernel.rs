/// An implementation of [`top_k`](crate::top_k) for every element type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kernel {
    /// Scalar, one pair at a time: runs everywhere, and is the reference for the others.
    Fallback,
    /// Needs AVX2 and FMA.
    #[cfg(target_arch = "x86_64")]
    Avx2,
    /// AVX-512F for f32, the AVX2 kernels for the other element types. Needs AVX-512F, AVX2,
    /// and FMA.
    #[cfg(target_arch = "x86_64")]
    Avx512,
    /// AVX-512 VNNI for u8 and the code types, the [`Kernel::Avx512`] kernels for the other
    /// element types. Needs AVX-512F, AVX-512 VNNI, AVX2, and FMA.
    #[cfg(target_arch = "x86_64")]
    Avx512Vnni,
    /// Needs NEON. The u8 kernel also needs the dot product instructions (`dotprod`), and is
    /// the fallback on CPUs without them.
    #[cfg(target_arch = "aarch64")]
    Neon,
}

impl Kernel {
    /// The fastest kernel this CPU supports.
    pub fn best() -> Self {
        #[cfg(target_arch = "x86_64")]
        for kernel in [Kernel::Avx512Vnni, Kernel::Avx512, Kernel::Avx2] {
            if kernel.is_supported() {
                return kernel;
            }
        }
        #[cfg(target_arch = "aarch64")]
        if Kernel::Neon.is_supported() {
            return Kernel::Neon;
        }
        Kernel::Fallback
    }

    /// All kernels this CPU supports.
    pub fn available() -> Vec<Self> {
        let all = [
            Kernel::Fallback,
            #[cfg(target_arch = "x86_64")]
            Kernel::Avx2,
            #[cfg(target_arch = "x86_64")]
            Kernel::Avx512,
            #[cfg(target_arch = "x86_64")]
            Kernel::Avx512Vnni,
            #[cfg(target_arch = "aarch64")]
            Kernel::Neon,
        ];
        all.into_iter()
            .filter(|kernel| kernel.is_supported())
            .collect()
    }

    pub fn is_supported(self) -> bool {
        match self {
            Kernel::Fallback => true,
            #[cfg(target_arch = "x86_64")]
            Kernel::Avx2 => {
                std::arch::is_x86_feature_detected!("avx2")
                    && std::arch::is_x86_feature_detected!("fma")
            }
            #[cfg(target_arch = "x86_64")]
            Kernel::Avx512 => {
                std::arch::is_x86_feature_detected!("avx512f") && Kernel::Avx2.is_supported()
            }
            #[cfg(target_arch = "x86_64")]
            Kernel::Avx512Vnni => {
                std::arch::is_x86_feature_detected!("avx512vnni") && Kernel::Avx512.is_supported()
            }
            // Baseline on the aarch64 targets Qdrant builds for, so a compile-time `true` there
            #[cfg(target_arch = "aarch64")]
            Kernel::Neon => std::arch::is_aarch64_feature_detected!("neon"),
        }
    }
}
