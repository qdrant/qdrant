#include <stdlib.h>
#include <stdint.h>
#include <lsxintrin.h>
#include <lasxintrin.h>

#include "export_macro.h"

#ifdef __clang__
#define VREGS_PREFIX "$vr"
#define XREGS_PREFIX "$xr"
#else // GCC
#define VREGS_PREFIX "$f"
#define XREGS_PREFIX "$f"
#endif
#define __ALL_REGS "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31"

// Convert __m128i to __m256i
static inline __m256i lasx_cast128_256(__m128i in)
{
    __m256i out = __lasx_xvldi(0);
    __asm__ volatile (
        ".irp i," __ALL_REGS                "\n\t"
        " .ifc %[out], " XREGS_PREFIX"\\i    \n\t"
        "  .irp j," __ALL_REGS              "\n\t"
        "   .ifc %[in], " VREGS_PREFIX "\\j  \n\t"
        "    xvpermi.q $xr\\i, $xr\\j, 0x20  \n\t"
        "   .endif                           \n\t"
        "  .endr                             \n\t"
        " .endif                             \n\t"
        ".endr                               \n\t"
        : [out] "+f" (out) : [in] "f" (in)
    );
    return out;
}

// Convert __m256i low part to __m128i
static inline __m128i lasx_extract128_lo(__m256i in)
{
    __m128i out;
    __asm__ volatile (
        ".ifnc %[out], %[in]                 \n\t"
        ".irp i," __ALL_REGS                "\n\t"
        " .ifc %[out], " VREGS_PREFIX "\\i   \n\t"
        "  .irp j," __ALL_REGS              "\n\t"
        "   .ifc %[in], " XREGS_PREFIX "\\j  \n\t"
        "    vori.b $vr\\i, $vr\\j, 0        \n\t"
        "   .endif                           \n\t"
        "  .endr                             \n\t"
        " .endif                             \n\t"
        ".endr                               \n\t"
        ".endif                              \n\t"
        : [out] "=f" (out) : [in] "f" (in)
    );
    return out;
}

// Convert __m256i high part to __m128i
static inline __m128i lasx_extract128_hi(__m256i in)
{
    __m128i out;
    __asm__ volatile (
        ".irp i," __ALL_REGS                "\n\t"
        " .ifc %[out], " VREGS_PREFIX "\\i   \n\t"
        "  .irp j," __ALL_REGS              "\n\t"
        "   .ifc %[in], " XREGS_PREFIX "\\j  \n\t"
        "    xvpermi.q $xr\\i, $xr\\j, 0x11  \n\t"
        "   .endif                           \n\t"
        "  .endr                             \n\t"
        " .endif                             \n\t"
        ".endr                               \n\t"
        : [out] "=f" (out) : [in] "f" (in)
    );
    return out;
}

#define lasx_extracti128_lo(x) (__m128i)lasx_extract128_lo((__m256i)(x))
#define lasx_extracti128_hi(x) (__m128i)lasx_extract128_hi((__m256i)(x))

static inline __m256i lasx_ext_w_h(__m128i _a)
{
    return __lasx_vext2xv_w_h(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_hu_bu(__m128i _a)
{
    return __lasx_vext2xv_hu_bu(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_wu_hu(__m128i _a)
{
    return __lasx_vext2xv_wu_hu(lasx_cast128_256(_a));
}

static __m256i lasx_set_w(int e7, int e6, int e5, int e4, int e3, int e2, int e1, int e0)
{
    v8i32 __ret = {e0, e1, e2, e3, e4, e5, e6, e7};
    return (__m256i)__ret;
}

static float HSUM256_PS(__m256 X) {
    float R = 0.0f;
    __m128 x128 = __lsx_vfadd_s((__m128)lasx_extract128_hi((__m256i)X), (__m128)lasx_extract128_lo((__m256i)X));
    __m128 x64 = __lsx_vfadd_s(x128, (__m128)__lsx_vilvh_d((__m128i)x128, (__m128i)x128));
    __m128 x32 = (__m128)__lsx_vextrins_w((__m128i)x64, (__m128i)__lsx_vfadd_s(x64, (__m128)__lsx_vpermi_w((__m128i)x64, (__m128i)x64, 0x55)), 0);
    R = ((v4f32)x32)[0];
    return R;
}

static int HSUM256_EPI32(__m256i X) {
    __m128i x128 = __lsx_vadd_w(lasx_extract128_hi(X), lasx_extracti128_lo(X));
    __m128i x64 = __lsx_vadd_w(x128, __lsx_vbsrl_v(x128, 8));
    __m128i x32 = __lsx_vadd_w(x64, __lsx_vbsrl_v(x64, 4));
    return __lsx_vpickve2gr_w(x32, 0);
}

EXPORT float impl_score_dot_lasx(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t dim
) {
    const __m256i* v_ptr = (const __m256i*)vector_ptr;
    const __m256i* q_ptr = (const __m256i*)query_ptr;

    __m256i mul1 = __lasx_xvldi(0);
    __m256i mask_epu32 = __lasx_xvreplgr2vr_w(0xFFFF);
    for (uint32_t _i = 0; _i < dim / 32; _i++) {
        __m256i v = __lasx_xvld(v_ptr, 0);
        __m256i q = __lasx_xvld(q_ptr, 0);
        v_ptr++;
        q_ptr++;

        __m256i s = __lasx_xvsadd_h(__lasx_xvmulwev_h_bu_b(v, q), __lasx_xvmulwod_h_bu_b(v, q));
        __m256i s_low = lasx_ext_w_h(lasx_extracti128_lo(s));
        __m256i s_high = lasx_ext_w_h(lasx_extract128_hi(s));
        mul1 = __lasx_xvadd_w(mul1, s_low);
        mul1 = __lasx_xvadd_w(mul1, s_high);
    }

    // the vector sizes are assumed to be multiples of 16, check if one last 16-element part remaining
    if (dim % 32 != 0) {
        __m128i v_short = __lsx_vld((const __m128i*)v_ptr, 0);
        __m128i q_short = __lsx_vld((const __m128i*)q_ptr, 0);

        __m256i v1 = lasx_ext_hu_bu(v_short);
        __m256i q1 = lasx_ext_hu_bu(q_short);

        __m256i s = __lasx_xvmul_h(v1, q1);
        mul1 = __lasx_xvadd_w(mul1, __lasx_xvand_v(s, mask_epu32));
        mul1 = __lasx_xvadd_w(mul1, __lasx_xvsrli_w(s, 16));
    }
    __m256 mul_ps = __lasx_xvffint_s_w(mul1);
    return HSUM256_PS(mul_ps);
}

EXPORT float impl_score_l1_lasx(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t dim
) {
    const __m256i* v_ptr = (const __m256i*)vector_ptr;
    const __m256i* q_ptr = (const __m256i*)query_ptr;

    uint32_t m = dim - (dim % 32);
    __m256i sum256 = __lasx_xvldi(0);

    for (uint32_t i = 0; i < m; i += 32) {
        __m256i v = __lasx_xvld(v_ptr, 0);
        __m256i q = __lasx_xvld(q_ptr, 0);
        v_ptr++;
        q_ptr++;

        // Compute the difference in both directions and take the maximum for abs
        __m256i diff1 = __lasx_xvssub_bu(v, q);
        __m256i diff2 = __lasx_xvssub_bu(q, v);

        __m256i abs_diff = __lasx_xvmax_bu(diff1, diff2);

        __m256i abs_diff16_lo = __lasx_xvilvl_b(__lasx_xvldi(0), abs_diff);
        __m256i abs_diff16_hi = __lasx_xvilvh_b(__lasx_xvldi(0), abs_diff);

        sum256 = __lasx_xvadd_h(sum256, abs_diff16_lo);
        sum256 = __lasx_xvadd_h(sum256, abs_diff16_hi);
    }

    // the vector sizes are assumed to be multiples of 16, check if one last 16-element part remaining
    if (m < dim) {
        __m128i v_short = __lsx_vld((const __m128i * ) v_ptr, 0);
        __m128i q_short = __lsx_vld((const __m128i * ) q_ptr, 0);

        __m128i diff1 = __lsx_vssub_bu(v_short, q_short);
        __m128i diff2 = __lsx_vssub_bu(q_short, v_short);

        __m128i abs_diff = __lsx_vmax_bu(diff1, diff2);

        __m128i abs_diff16_lo_128 = __lsx_vilvl_b(__lsx_vldi(0), abs_diff);
        __m128i abs_diff16_hi_128 = __lsx_vilvh_b(__lsx_vldi(0), abs_diff);

        __m256i abs_diff16_lo = lasx_ext_wu_hu(abs_diff16_lo_128);
        __m256i abs_diff16_hi = lasx_ext_wu_hu(abs_diff16_hi_128);

        sum256 = __lasx_xvadd_h(sum256, abs_diff16_lo);
        sum256 = __lasx_xvadd_h(sum256, abs_diff16_hi);
    }

    __m256i sum_epi32 = __lasx_xvadd_w(__lasx_xvilvl_h(__lasx_xvldi(0), sum256), __lasx_xvilvh_h(__lasx_xvldi(0), sum256));

    return (float)HSUM256_EPI32(sum_epi32);

}

EXPORT uint32_t impl_xor_popcnt_scalar8_lasx_uint128(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    const uint64_t* v_ptr = (const uint64_t*)vector_ptr;
    const uint64_t* q_ptr = (const uint64_t*)query_ptr;

    __m256i sum1 = __lasx_xvreplgr2vr_w(0);
    __m256i sum2 = __lasx_xvreplgr2vr_w(0);
    for (uint32_t _i = 0; _i < count; _i++) {
        uint64_t v_1 = *v_ptr;
        uint64_t v_2 = *(v_ptr + 1);

        __m256i popcnt1 = lasx_set_w(__builtin_popcountll(v_1 ^ *(q_ptr + 0)), __builtin_popcountll(v_1 ^ *(q_ptr + 2)), __builtin_popcountll(v_1 ^ *(q_ptr + 4)), __builtin_popcountll(v_1 ^ *(q_ptr + 6)), __builtin_popcountll(v_2 ^ *(q_ptr + 1)), __builtin_popcountll(v_2 ^ *(q_ptr + 3)), __builtin_popcountll(v_2 ^ *(q_ptr + 5)), __builtin_popcountll(v_2 ^ *(q_ptr + 7)));
        sum1 = __lasx_xvadd_w(sum1, popcnt1);

        __m256i popcnt2 = lasx_set_w(__builtin_popcountll(v_1 ^ *(q_ptr + 8)), __builtin_popcountll(v_1 ^ *(q_ptr + 10)), __builtin_popcountll(v_1 ^ *(q_ptr + 12)), __builtin_popcountll(v_1 ^ *(q_ptr + 14)), __builtin_popcountll(v_2 ^ *(q_ptr + 9)), __builtin_popcountll(v_2 ^ *(q_ptr + 11)), __builtin_popcountll(v_2 ^ *(q_ptr + 13)), __builtin_popcountll(v_2 ^ *(q_ptr + 15)));
        sum2 = __lasx_xvadd_w(sum2, popcnt2);

        v_ptr += 2;
        q_ptr += 16;
    }
    __m256i factor1 = lasx_set_w(1, 2, 4, 8, 1, 2, 4, 8);
    __m256i factor2 = lasx_set_w(16, 32, 64, 128, 16, 32, 64, 128);
    __m256 result1_mm256 = __lasx_xvffint_s_w(__lasx_xvmul_w(sum1, factor1));
    __m256 result2_mm256 = __lasx_xvffint_s_w(__lasx_xvmul_w(sum2, factor2));
    return (uint32_t)HSUM256_PS(__lasx_xvfadd_s(result1_mm256, result2_mm256));
}

EXPORT uint32_t impl_xor_popcnt_scalar4_lasx_uint128(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    const uint64_t* v_ptr = (const uint64_t*)vector_ptr;
    const uint64_t* q_ptr = (const uint64_t*)query_ptr;

    __m256i sum = __lasx_xvreplgr2vr_w(0);
    for (uint32_t _i = 0; _i < count; _i++) {
        uint64_t v_1 = *v_ptr;
        uint64_t v_2 = *(v_ptr + 1);

        __m256i popcnt = lasx_set_w(__builtin_popcountll(v_1 ^ *(q_ptr + 0)), __builtin_popcountll(v_1 ^ *(q_ptr + 2)), __builtin_popcountll(v_1 ^ *(q_ptr + 4)), __builtin_popcountll(v_1 ^ *(q_ptr + 6)), __builtin_popcountll(v_2 ^ *(q_ptr + 1)), __builtin_popcountll(v_2 ^ *(q_ptr + 3)), __builtin_popcountll(v_2 ^ *(q_ptr + 5)), __builtin_popcountll(v_2 ^ *(q_ptr + 7)));
        sum = __lasx_xvadd_w(sum, popcnt);

        v_ptr += 2;
        q_ptr += 8;
    }
    __m256i factor = lasx_set_w(1, 2, 4, 8, 1, 2, 4, 8);
    __m256 result_mm256 = __lasx_xvffint_s_w(__lasx_xvmul_w(sum, factor));
    return (uint32_t)HSUM256_PS(result_mm256);
}

