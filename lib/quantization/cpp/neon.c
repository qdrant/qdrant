#include <stdlib.h>
#include <arm_neon.h>

#include "export_macro.h"

EXPORT float impl_score_dot_neon(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t dim
) {
    uint32x4_t mul1 = vdupq_n_u32(0);
    uint32x4_t mul2 = vdupq_n_u32(0);
    for (uint32_t _i = 0; _i < dim / 16; _i++) {
        uint8x16_t q = vld1q_u8(query_ptr);
        uint8x16_t v = vld1q_u8(vector_ptr);
        query_ptr += 16;
        vector_ptr += 16;
        uint16x8_t mul_low = vmull_u8(vget_low_u8(q), vget_low_u8(v));
        uint16x8_t mul_high = vmull_u8(vget_high_u8(q), vget_high_u8(v));
        mul1 = vpadalq_u16(mul1, mul_low);
        mul2 = vpadalq_u16(mul2, mul_high);
    }
    return (float)vaddvq_u32(vaddq_u32(mul1, mul2));
}

EXPORT uint32_t impl_xor_popcnt_neon_uint128(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    uint32x4_t result = vdupq_n_u32(0);
    for (uint32_t _i = 0; _i < count; _i++) {
        uint8x16_t v = vld1q_u8(vector_ptr);
        uint8x16_t q = vld1q_u8(query_ptr);

        uint8x16_t x = veorq_u8(q, v);
        uint8x16_t popcnt = vcntq_u8(x);
        uint8x8_t popcnt_low = vget_low_u8(popcnt);
        uint8x8_t popcnt_high = vget_high_u8(popcnt);
        uint16x8_t sum = vaddl_u8(popcnt_low, popcnt_high);
        result = vpadalq_u16(result, sum);

        query_ptr += 16;
        vector_ptr += 16;
    }
    return (uint32_t)vaddvq_u32(result);
}

EXPORT uint32_t impl_xor_popcnt_neon_uint64(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    uint16x4_t result = vdup_n_u16(0);
    for (uint32_t _i = 0; _i < count; _i++) {
        uint8x8_t v = vld1_u8(vector_ptr);
        uint8x8_t q = vld1_u8(query_ptr);

        uint8x8_t x = veor_u8(q, v);
        uint8x8_t popcnt = vcnt_u8(x);
        result = vpadal_u8(result, popcnt);

        query_ptr += 8;
        vector_ptr += 8;
    }
    return (uint32_t)vaddv_u16(result);
}

EXPORT uint32_t impl_xor_popcnt_scalar8_neon_uint128(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    const uint8_t* v_ptr = vector_ptr;
    const uint8_t* q_ptr = query_ptr;

    uint16x8_t result1 = vdupq_n_u16(0);
    uint16x8_t result2 = vdupq_n_u16(0);
    uint16x8_t result3 = vdupq_n_u16(0);
    uint16x8_t result4 = vdupq_n_u16(0);
    uint16x8_t result5 = vdupq_n_u16(0);
    uint16x8_t result6 = vdupq_n_u16(0);
    uint16x8_t result7 = vdupq_n_u16(0);
    uint16x8_t result8 = vdupq_n_u16(0);
    for (uint32_t _i = 0; _i < count; _i++) {
        uint8x16_t v = vld1q_u8(v_ptr);
        uint8x16_t x1 = veorq_u8(vld1q_u8(q_ptr + 0), v);
        uint8x16_t x2 = veorq_u8(vld1q_u8(q_ptr + 16), v);
        uint8x16_t x3 = veorq_u8(vld1q_u8(q_ptr + 32), v);
        uint8x16_t x4 = veorq_u8(vld1q_u8(q_ptr + 48), v);
        uint8x16_t x5 = veorq_u8(vld1q_u8(q_ptr + 64), v);
        uint8x16_t x6 = veorq_u8(vld1q_u8(q_ptr + 80), v);
        uint8x16_t x7 = veorq_u8(vld1q_u8(q_ptr + 96), v);
        uint8x16_t x8 = veorq_u8(vld1q_u8(q_ptr + 112), v);

        result1 = vpadalq_u8(result1, vcntq_u8(x1));
        result2 = vpadalq_u8(result2, vcntq_u8(x2));
        result3 = vpadalq_u8(result3, vcntq_u8(x3));
        result4 = vpadalq_u8(result4, vcntq_u8(x4));
        result5 = vpadalq_u8(result5, vcntq_u8(x5));
        result6 = vpadalq_u8(result6, vcntq_u8(x6));
        result7 = vpadalq_u8(result7, vcntq_u8(x7));
        result8 = vpadalq_u8(result8, vcntq_u8(x8));

        v_ptr += 16;
        q_ptr += 128;
    }

    uint32_t r1 = vaddvq_u16(result1);
    uint32_t r2 = vaddvq_u16(result2);
    uint32_t r3 = vaddvq_u16(result3);
    uint32_t r4 = vaddvq_u16(result4);
    uint32_t r5 = vaddvq_u16(result5);
    uint32_t r6 = vaddvq_u16(result6);
    uint32_t r7 = vaddvq_u16(result7);
    uint32_t r8 = vaddvq_u16(result8);

    return r1 + (r2 << 1) + (r3 << 2) + (r4 << 3) + (r5 << 4) + (r6 << 5) + (r7 << 6) + (r8 << 7);
}

EXPORT uint32_t impl_xor_popcnt_scalar4_neon_uint128(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    const uint8_t* v_ptr = vector_ptr;
    const uint8_t* q_ptr = query_ptr;

    uint16x8_t result1 = vdupq_n_u16(0);
    uint16x8_t result2 = vdupq_n_u16(0);
    uint16x8_t result3 = vdupq_n_u16(0);
    uint16x8_t result4 = vdupq_n_u16(0);
    for (uint32_t _i = 0; _i < count; _i++) {
        uint8x16_t v = vld1q_u8(v_ptr);
        uint8x16_t x1 = veorq_u8(vld1q_u8(q_ptr + 0), v);
        uint8x16_t x2 = veorq_u8(vld1q_u8(q_ptr + 16), v);
        uint8x16_t x3 = veorq_u8(vld1q_u8(q_ptr + 32), v);
        uint8x16_t x4 = veorq_u8(vld1q_u8(q_ptr + 48), v);

        result1 = vpadalq_u8(result1, vcntq_u8(x1));
        result2 = vpadalq_u8(result2, vcntq_u8(x2));
        result3 = vpadalq_u8(result3, vcntq_u8(x3));
        result4 = vpadalq_u8(result4, vcntq_u8(x4));

        v_ptr += 16;
        q_ptr += 64;
    }

    uint32_t r1 = vaddvq_u16(result1);
    uint32_t r2 = vaddvq_u16(result2);
    uint32_t r3 = vaddvq_u16(result3);
    uint32_t r4 = vaddvq_u16(result4);
    return r1 + (r2 << 1) + (r3 << 2) + (r4 << 3);
}

EXPORT float impl_score_l1_neon(
   const uint8_t * query_ptr,
   const uint8_t * vector_ptr,
   uint32_t dim
) {
    const uint8_t* v_ptr = (const uint8_t*)vector_ptr;
    const uint8_t* q_ptr = (const uint8_t*)query_ptr;

    uint32_t m = dim - (dim % 16);
    uint16x8_t sum16_low = vdupq_n_u16(0);
    uint16x8_t sum16_high = vdupq_n_u16(0);

    // the vector sizes are assumed to be multiples of 16, no remaining part here
    for (uint32_t i = 0; i < m; i += 16) {
        uint8x16_t vec1 = vld1q_u8(v_ptr);
        uint8x16_t vec2 = vld1q_u8(q_ptr);

        uint8x16_t abs_diff = vabdq_u8(vec1, vec2);
        uint16x8_t abs_diff16_low = vmovl_u8(vget_low_u8(abs_diff));
        uint16x8_t abs_diff16_high = vmovl_u8(vget_high_u8(abs_diff));

        sum16_low = vaddq_u16(sum16_low, abs_diff16_low);
        sum16_high = vaddq_u16(sum16_high, abs_diff16_high);

        v_ptr += 16;
        q_ptr += 16;
    }

    // Horizontal sum of 16-bit integers
    uint32x4_t sum32_low = vpaddlq_u16(sum16_low);
    uint32x4_t sum32_high = vpaddlq_u16(sum16_high);
    uint32x4_t sum32 = vaddq_u32(sum32_low, sum32_high);

    uint32x2_t sum64_low = vadd_u32(vget_low_u32(sum32), vget_high_u32(sum32));
    uint32x2_t sum64_high = vpadd_u32(sum64_low, sum64_low);
    uint32_t sum = vget_lane_u32(sum64_high, 0);

    return (float) sum;
}
