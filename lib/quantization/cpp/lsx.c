#include <stdlib.h>
#include <stdint.h>
#include <lsxintrin.h>

#include "export_macro.h"

static float HSUM128_PS(__m128 X) {
    __m128 x64 = __lsx_vfadd_s(X, (__m128)__lsx_vilvh_d((__m128i)X, (__m128i)X));
    __m128 x32 = (__m128)__lsx_vextrins_w((__m128i)x64, (__m128i)__lsx_vfadd_s(x64, (__m128)__lsx_vpermi_w((__m128i)x64, (__m128i)x64, 0x55)), 0);
    return ((v4f32)x32)[0];
}

static int HSUM128_EPI16(__m128i X) {
    int r = 0;
    __m128i x64 = __lsx_vadd_h(X, __lsx_vbsrl_v(X, 8));
    __m128i x32 = __lsx_vadd_h(x64, __lsx_vbsrl_v(x64, 4));
    r = __lsx_vpickve2gr_h(x32, 0) + __lsx_vpickve2gr_h(x32, 1);
    return r;
}

static inline __m128i lsx_set_w(int32_t a, int32_t b, int32_t c, int32_t d)
{
    v4i32 __ret = {d, c, b, a};
    return (__m128i)__ret;
}

EXPORT float impl_score_point_lsx(
    const float* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t size,
    uint32_t offset
) {
    __m128 sum128 = (__m128)__lsx_vldi(0);
    for (uint32_t i = 0; i < size / 4; i++) {
        float buffer[4] = {
            *(query_ptr + *vector_ptr++),
            *(query_ptr + offset + *vector_ptr++),
            *(query_ptr + 2 * offset + *vector_ptr++),
            *(query_ptr + 3 * offset + *vector_ptr++)
	};

	__m128 c = (__m128)__lsx_vld(buffer, 0);
        sum128 = __lsx_vfadd_s(sum128, c);
        query_ptr += 4 * offset;
    }

    float sum = HSUM128_PS(sum128);

    for (uint32_t j = 0; j < size % 4; j++) {
        sum += *(query_ptr + *vector_ptr++);
        query_ptr += offset;
    }
    return sum;
}

EXPORT float impl_score_dot_lsx(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t dim
) {
    const __m128i* v_ptr = (const __m128i*)vector_ptr;
    const __m128i* q_ptr = (const __m128i*)query_ptr;

    __m128i mul = __lsx_vldi(0);
    for (uint32_t _i = 0; _i < dim / 16; _i++) {
        __m128i v = __lsx_vld(v_ptr, 0);
        __m128i q = __lsx_vld(q_ptr, 0);
        v_ptr++;
        q_ptr++;

        __m128i s = __lsx_vsadd_h(__lsx_vmulwev_h_bu_b(v, q), __lsx_vmulwod_h_bu_b(v, q));
        __m128i s_low = __lsx_vsllwil_w_h(s, 0);
        __m128i s_high = __lsx_vsllwil_w_h(__lsx_vbsrl_v(s, 8), 0);
        mul = __lsx_vadd_w(mul, s_low);
        mul = __lsx_vadd_w(mul, s_high);
    }
    __m128 mul_ps = __lsx_vffint_s_w(mul);
    return HSUM128_PS(mul_ps);
}

EXPORT uint32_t impl_xor_popcnt_lsx_uint128(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    int64_t result = 0;
    for (uint32_t _i = 0; _i < count; _i++) {
        const uint64_t* v_ptr_1 = (const uint64_t*)vector_ptr;
        const uint64_t* q_ptr_1 = (const uint64_t*)query_ptr;
        uint64_t x_1 = (*v_ptr_1) ^ (*q_ptr_1);
        result += __builtin_popcountll(x_1);

        const uint64_t* v_ptr_2 = v_ptr_1 + 1;
        const uint64_t* q_ptr_2 = q_ptr_1 + 1;
        uint64_t x_2 = (*v_ptr_2) ^ (*q_ptr_2);
        result += __builtin_popcountll(x_2);

        vector_ptr += 16;
        query_ptr += 16;
    }
    return (uint32_t)result;
}

EXPORT uint32_t impl_xor_popcnt_lsx_uint64(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    int64_t result = 0;
    for (uint32_t _i = 0; _i < count; _i++) {
        const uint64_t* v_ptr = (const uint64_t*)vector_ptr;
        const uint64_t* q_ptr = (const uint64_t*)query_ptr;
        uint64_t x = (*v_ptr) ^ (*q_ptr);
        result += __builtin_popcountll(x);

        vector_ptr += 8;
        query_ptr += 8;
    }
    return (uint32_t)result;
}

EXPORT uint32_t impl_xor_popcnt_lsx_uint32(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    int result = 0;
    for (uint32_t _i = 0; _i < count; _i++) {
        const uint32_t* v_ptr = (const uint32_t*)vector_ptr;
        const uint32_t* q_ptr = (const uint32_t*)query_ptr;
        uint32_t x = (*v_ptr) ^ (*q_ptr);
        result += __builtin_popcount(x);

        vector_ptr += 4;
        query_ptr += 4;
    }
    return (uint32_t)result;
}

EXPORT uint32_t impl_xor_popcnt_scalar8_lsx_uint128(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    const uint64_t* v_ptr = (const uint64_t*)vector_ptr;
    const uint64_t* q_ptr = (const uint64_t*)query_ptr;

    __m128i sum1 = __lsx_vreplgr2vr_w(0);
    __m128i sum2 = __lsx_vreplgr2vr_w(0);
    for (uint32_t _i = 0; _i < count; _i++) {
        uint64_t v_1 = *v_ptr;
        uint64_t v_2 = *(v_ptr + 1);

        __m128i popcnt1 = lsx_set_w(__builtin_popcountll(v_1 ^ *(q_ptr + 0)), __builtin_popcountll(v_1 ^ *(q_ptr + 2)), __builtin_popcountll(v_1 ^ *(q_ptr + 4)), __builtin_popcountll(v_1 ^ *(q_ptr + 6)));
        sum1 = __lsx_vadd_w(sum1, popcnt1);

        __m128i popcnt2 = lsx_set_w(__builtin_popcountll(v_1 ^ *(q_ptr + 8)), __builtin_popcountll(v_1 ^ *(q_ptr + 10)), __builtin_popcountll(v_1 ^ *(q_ptr + 12)), __builtin_popcountll(v_1 ^ *(q_ptr + 14)));
        sum2 = __lsx_vadd_w(sum2, popcnt2);

        __m128i popcnt3 = lsx_set_w(__builtin_popcountll(v_2 ^ *(q_ptr + 1)), __builtin_popcountll(v_2 ^ *(q_ptr + 3)), __builtin_popcountll(v_2 ^ *(q_ptr + 5)), __builtin_popcountll(v_2 ^ *(q_ptr + 7)));
        sum1 = __lsx_vadd_w(sum1, popcnt3);

        __m128i popcnt4 = lsx_set_w(__builtin_popcountll(v_2 ^ *(q_ptr + 9)), __builtin_popcountll(v_2 ^ *(q_ptr + 11)), __builtin_popcountll(v_2 ^ *(q_ptr + 13)), __builtin_popcountll(v_2 ^ *(q_ptr + 15)));
        sum2 = __lsx_vadd_w(sum2, popcnt4);

        v_ptr += 2;
        q_ptr += 16;
    }
    __m128i factor1 = lsx_set_w(1, 2, 4, 8);
    __m128i factor2 = lsx_set_w(16, 32, 64, 128);
    __m128 result1_mm128 = __lsx_vffint_s_w(__lsx_vmul_w(sum1, factor1));
    __m128 result2_mm128 = __lsx_vffint_s_w(__lsx_vmul_w(sum2, factor2));
    return (uint32_t)HSUM128_PS(__lsx_vfadd_s(result1_mm128, result2_mm128));
}

EXPORT uint32_t impl_xor_popcnt_scalar4_lsx_uint128(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    const uint64_t* v_ptr = (const uint64_t*)vector_ptr;
    const uint64_t* q_ptr = (const uint64_t*)query_ptr;

    __m128i sum = __lsx_vreplgr2vr_w(0);
    for (uint32_t _i = 0; _i < count; _i++) {
        uint64_t v_1 = *v_ptr;
        uint64_t v_2 = *(v_ptr + 1);

        __m128i popcnt1 = lsx_set_w(__builtin_popcountll(v_1 ^ *(q_ptr + 0)), __builtin_popcountll(v_1 ^ *(q_ptr + 2)), __builtin_popcountll(v_1 ^ *(q_ptr + 4)), __builtin_popcountll(v_1 ^ *(q_ptr + 6)));
        sum = __lsx_vadd_w(sum, popcnt1);

        __m128i popcnt2 = lsx_set_w(__builtin_popcountll(v_2 ^ *(q_ptr + 1)), __builtin_popcountll(v_2 ^ *(q_ptr + 3)), __builtin_popcountll(v_2 ^ *(q_ptr + 5)), __builtin_popcountll(v_2 ^ *(q_ptr + 7)));
        sum = __lsx_vadd_w(sum, popcnt2);

        v_ptr += 2;
        q_ptr += 8;
    }
    __m128i factor = lsx_set_w(1, 2, 4, 8);
    __m128 result_mm128 = __lsx_vffint_s_w(__lsx_vmul_w(sum, factor));
    return HSUM128_PS(result_mm128);
}

EXPORT uint32_t impl_xor_popcnt_scalar8_lsx_u8(
    const uint8_t* q_ptr,
    const uint8_t* v_ptr,
    uint32_t count
) {
    __m128i sum1 = __lsx_vreplgr2vr_w(0);
    __m128i sum2 = __lsx_vreplgr2vr_w(0);
    for (uint32_t _i = 0; _i < count / 8; _i++) {
        uint64_t v = *((const uint64_t*)v_ptr);

        uint8_t values1[8] = {
            *(q_ptr + 0),
            *(q_ptr + 8),
            *(q_ptr + 16),
            *(q_ptr + 24),
            *(q_ptr + 32),
            *(q_ptr + 40),
            *(q_ptr + 48),
            *(q_ptr + 56),
        };
        uint8_t values2[8] = {
            *(q_ptr + 1),
            *(q_ptr + 9),
            *(q_ptr + 17),
            *(q_ptr + 25),
            *(q_ptr + 33),
            *(q_ptr + 41),
            *(q_ptr + 49),
            *(q_ptr + 57),
        };
        uint8_t values3[8] = {
            *(q_ptr + 2),
            *(q_ptr + 10),
            *(q_ptr + 18),
            *(q_ptr + 26),
            *(q_ptr + 34),
            *(q_ptr + 42),
            *(q_ptr + 50),
            *(q_ptr + 58),
        };
        uint8_t values4[8] = {
            *(q_ptr + 3),
            *(q_ptr + 11),
            *(q_ptr + 19),
            *(q_ptr + 27),
            *(q_ptr + 35),
            *(q_ptr + 43),
            *(q_ptr + 51),
            *(q_ptr + 59),
        };
        uint8_t values5[8] = {
            *(q_ptr + 4),
            *(q_ptr + 12),
            *(q_ptr + 20),
            *(q_ptr + 28),
            *(q_ptr + 36),
            *(q_ptr + 44),
            *(q_ptr + 52),
            *(q_ptr + 60),
        };
        uint8_t values6[8] = {
            *(q_ptr + 5),
            *(q_ptr + 13),
            *(q_ptr + 21),
            *(q_ptr + 29),
            *(q_ptr + 37),
            *(q_ptr + 45),
            *(q_ptr + 53),
            *(q_ptr + 61),
        };
        uint8_t values7[8] = {
            *(q_ptr + 6),
            *(q_ptr + 14),
            *(q_ptr + 22),
            *(q_ptr + 30),
            *(q_ptr + 38),
            *(q_ptr + 46),
            *(q_ptr + 54),
            *(q_ptr + 62),
        };
        uint8_t values8[8] = {
            *(q_ptr + 7),
            *(q_ptr + 15),
            *(q_ptr + 23),
            *(q_ptr + 31),
            *(q_ptr + 39),
            *(q_ptr + 47),
            *(q_ptr + 55),
            *(q_ptr + 63),
        };

        uint64_t values1_u64 = *((const uint64_t*)values1);
        uint64_t values2_u64 = *((const uint64_t*)values2);
        uint64_t values3_u64 = *((const uint64_t*)values3);
        uint64_t values4_u64 = *((const uint64_t*)values4);
        uint64_t values5_u64 = *((const uint64_t*)values5);
        uint64_t values6_u64 = *((const uint64_t*)values6);
        uint64_t values7_u64 = *((const uint64_t*)values7);
        uint64_t values8_u64 = *((const uint64_t*)values8);

        __m128i popcnt1 = lsx_set_w(__builtin_popcountll(v ^ values1_u64), __builtin_popcountll(v ^ values2_u64), __builtin_popcountll(v ^ values3_u64), __builtin_popcountll(v ^ values4_u64));
        sum1 = __lsx_vadd_w(sum1, popcnt1);

        __m128i popcnt2 = lsx_set_w(__builtin_popcountll(v ^ values5_u64), __builtin_popcountll(v ^ values6_u64), __builtin_popcountll(v ^ values7_u64), __builtin_popcountll(v ^ values8_u64));
        sum2 = __lsx_vadd_w(sum2, popcnt2);

        v_ptr += 8;
        q_ptr += 64;
    }

    uint32_t dr1 = 0;
    uint32_t dr2 = 0;
    uint32_t dr3 = 0;
    uint32_t dr4 = 0;
    uint32_t dr5 = 0;
    uint32_t dr6 = 0;
    uint32_t dr7 = 0;
    uint32_t dr8 = 0;
    float r;
    for (uint32_t _i = count % 8; _i > 0; _i--) {
        uint8_t v = *(v_ptr++);
        uint8_t q1 = *(q_ptr++);
        uint8_t q2 = *(q_ptr++);
        uint8_t q3 = *(q_ptr++);
        uint8_t q4 = *(q_ptr++);
        uint8_t q5 = *(q_ptr++);
        uint8_t q6 = *(q_ptr++);
        uint8_t q7 = *(q_ptr++);
        uint8_t q8 = *(q_ptr++);
        
        uint8_t x1 = v ^ q1;
        uint8_t x2 = v ^ q2;
        uint8_t x3 = v ^ q3;
        uint8_t x4 = v ^ q4;
        uint8_t x5 = v ^ q5;
        uint8_t x6 = v ^ q6;
        uint8_t x7 = v ^ q7;
        uint8_t x8 = v ^ q8;

        dr1 += __builtin_popcount(x1);
        dr2 += __builtin_popcount(x2);
        dr3 += __builtin_popcount(x3);
        dr4 += __builtin_popcount(x4);
        dr5 += __builtin_popcount(x5);
        dr6 += __builtin_popcount(x6);
        dr7 += __builtin_popcount(x7);
        dr8 += __builtin_popcount(x8);
    }

    __m128i factor1 = lsx_set_w(1, 2, 4, 8);
    __m128i factor2 = lsx_set_w(16, 32, 64, 128);
    __m128 result_mm128 = __lsx_vffint_s_w(__lsx_vadd_w(__lsx_vmul_w(sum1, factor1), __lsx_vmul_w(sum2, factor2)));
    r = HSUM128_PS(result_mm128);
    return (uint32_t)r + dr1 + (dr2 << 1) + (dr3 << 2) + (dr4 << 3) + (dr5 << 4) + (dr6 << 5) + (dr7 << 6) + (dr8 << 7);
}

EXPORT uint32_t impl_xor_popcnt_scalar4_lsx_u8(
    const uint8_t* q_ptr,
    const uint8_t* v_ptr,
    uint32_t count
) {
    __m128i sum = __lsx_vreplgr2vr_w(0);
    for (uint32_t _i = 0; _i < count / 8; _i++) {
        uint64_t v = *((const uint64_t*)v_ptr);

        uint8_t values1[8] = {
            *(q_ptr + 0),
            *(q_ptr + 4),
            *(q_ptr + 8),
            *(q_ptr + 12),
            *(q_ptr + 16),
            *(q_ptr + 20),
            *(q_ptr + 24),
            *(q_ptr + 28),
        };
        uint8_t values2[8] = {
            *(q_ptr + 1),
            *(q_ptr + 5),
            *(q_ptr + 9),
            *(q_ptr + 13),
            *(q_ptr + 17),
            *(q_ptr + 21),
            *(q_ptr + 25),
            *(q_ptr + 29),
        };
        uint8_t values3[8] = {
            *(q_ptr + 2),
            *(q_ptr + 6),
            *(q_ptr + 10),
            *(q_ptr + 14),
            *(q_ptr + 18),
            *(q_ptr + 22),
            *(q_ptr + 26),
            *(q_ptr + 30),
        };
        uint8_t values4[8] = {
            *(q_ptr + 3),
            *(q_ptr + 7),
            *(q_ptr + 11),
            *(q_ptr + 15),
            *(q_ptr + 19),
            *(q_ptr + 23),
            *(q_ptr + 27),
            *(q_ptr + 31),
        };

        uint64_t values1_u64 = *((const uint64_t*)values1);
        uint64_t values2_u64 = *((const uint64_t*)values2);
        uint64_t values3_u64 = *((const uint64_t*)values3);
        uint64_t values4_u64 = *((const uint64_t*)values4);

        __m128i popcnt = lsx_set_w(__builtin_popcountll(v ^ values1_u64), __builtin_popcountll(v ^ values2_u64), __builtin_popcountll(v ^ values3_u64), __builtin_popcountll(v ^ values4_u64));
        sum = __lsx_vadd_w(sum, popcnt);

        v_ptr += 8;
        q_ptr += 32;
    }

    uint32_t dr1 = 0;
    uint32_t dr2 = 0;
    uint32_t dr3 = 0;
    uint32_t dr4 = 0;
    for (uint32_t _i = count % 8; _i > 0; _i--) {
        uint8_t v = *(v_ptr++);
        uint8_t q1 = *(q_ptr++);
        uint8_t q2 = *(q_ptr++);
        uint8_t q3 = *(q_ptr++);
        uint8_t q4 = *(q_ptr++);
        
        uint8_t x1 = v ^ q1;
        uint8_t x2 = v ^ q2;
        uint8_t x3 = v ^ q3;
        uint8_t x4 = v ^ q4;
        dr1 += __builtin_popcount(x1);
        dr2 += __builtin_popcount(x2);
        dr3 += __builtin_popcount(x3);
        dr4 += __builtin_popcount(x4);
    }

    __m128i factor = lsx_set_w(1, 2, 4, 8);
    __m128 result_mm128 = __lsx_vffint_s_w(__lsx_vmul_w(sum, factor));
    float r = HSUM128_PS(result_mm128);
    return (uint32_t)r + dr1 + (dr2 << 1) + (dr3 << 2) + (dr4 << 3);
}

EXPORT float impl_score_l1_lsx(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t dim
) {
    const __m128i* v_ptr = (const __m128i*)vector_ptr;
    const __m128i* q_ptr = (const __m128i*)query_ptr;

    uint32_t m = dim - (dim % 16);
    __m128i sum128 = __lsx_vldi(0);

    // the vector sizes are assumed to be multiples of 16, no remaining part here
    for (uint32_t i = 0; i < m; i += 16) {
        __m128i vec2 = __lsx_vld(v_ptr, 0);
        __m128i vec1 = __lsx_vld(q_ptr, 0);
        v_ptr++;
        q_ptr++;

        // Compute the difference in both directions
        __m128i diff1 = __lsx_vssub_bu(vec1, vec2);
        __m128i diff2 = __lsx_vssub_bu(vec2, vec1);

        // Take the maximum
        __m128i abs_diff = __lsx_vmax_bu(diff1, diff2);

        __m128i abs_diff16_low = __lsx_vilvl_b(__lsx_vldi(0), abs_diff);
        __m128i abs_diff16_high = __lsx_vilvh_b(__lsx_vldi(0), abs_diff);

        sum128 = __lsx_vadd_h(sum128, abs_diff16_low);
        sum128 = __lsx_vadd_h(sum128, abs_diff16_high);
    }

    // Convert 16-bit sums to 32-bit and sum them up
    __m128i sum_epi32 = __lsx_vadd_w(__lsx_vilvl_h(__lsx_vldi(0), sum128), __lsx_vilvh_h(__lsx_vldi(0), sum128));

    // Horizontal sum using the macro
    return (float)HSUM128_EPI16(sum_epi32);
}

