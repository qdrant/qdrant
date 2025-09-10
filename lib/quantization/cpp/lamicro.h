#pragma once

#ifdef __loongarch_asx
#include <lasxintrin.h>
#endif
#ifdef __loongarch_sx
#include <lsxintrin.h>
#endif

#include <stdint.h>

#if defined(__loongarch_asx) && defined(__loongarch_sx)
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
// Convert two __m128i to __m256i
static inline __m256i lasx_set_q(__m128i inhi, __m128i inlo)
{
    __m256i out;
    __asm__ volatile (
        ".irp i," __ALL_REGS                "\n\t"
        " .ifc %[hi], " VREGS_PREFIX "\\i    \n\t"
        "  .irp j," __ALL_REGS              "\n\t"
        "   .ifc %[lo], " VREGS_PREFIX "\\j  \n\t"
        "    xvpermi.q $xr\\i, $xr\\j, 0x20  \n\t"
        "   .endif                           \n\t"
        "  .endr                             \n\t"
        " .endif                             \n\t"
        ".endr                               \n\t"
        ".ifnc %[out], %[hi]                 \n\t"
        ".irp i," __ALL_REGS                "\n\t"
        " .ifc %[out], " XREGS_PREFIX "\\i   \n\t"
        "  .irp j," __ALL_REGS              "\n\t"
        "   .ifc %[hi], " VREGS_PREFIX "\\j  \n\t"
        "    xvori.b $xr\\i, $xr\\j, 0       \n\t"
        "   .endif                           \n\t"
        "  .endr                             \n\t"
        " .endif                             \n\t"
        ".endr                               \n\t"
        ".endif                              \n\t"
        : [out] "=f" (out), [hi] "+f" (inhi)
        : [lo] "f" (inlo)
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
// Convert two __m128i to __m256i
static inline __m256i ____m256i_2(__m128i inhi, __m128i inlo)
{
    __m256i out;
    __asm__ volatile (
        ".irp i," __ALL_REGS                "\n\t"
        " .ifc %[hi], " VREGS_PREFIX "\\i    \n\t"
        "  .irp j," __ALL_REGS              "\n\t"
        "   .ifc %[lo], " VREGS_PREFIX "\\j  \n\t"
        "    xvpermi.q $xr\\i, $xr\\j, 0x20  \n\t"
        "   .endif                           \n\t"
        "  .endr                             \n\t"
        " .endif                             \n\t"
        ".endr                               \n\t"
        ".ifnc %[out], %[hi]                 \n\t"
        ".irp i," __ALL_REGS                "\n\t"
        " .ifc %[out], " XREGS_PREFIX "\\i   \n\t"
        "  .irp j," __ALL_REGS              "\n\t"
        "   .ifc %[hi], " VREGS_PREFIX "\\j  \n\t"
        "    xvori.b $xr\\i, $xr\\j, 0       \n\t"
        "   .endif                           \n\t"
        "  .endr                             \n\t"
        " .endif                             \n\t"
        ".endr                               \n\t"
        ".endif                              \n\t"
        : [out] "=f" (out), [hi] "+f" (inhi)
        : [lo] "f" (inlo)
    );
    return out;
}
static inline __m128i lasx_extract128( __m256i a, int pos)
{
    __m128i ret;
    if( pos == 0)
    {
       ret = lasx_extract128_lo(a);
    } else {
       ret = lasx_extract128_hi(a);
    }
    return ret;
}

//TODO check me
static inline __m256i lasx_insert128(__m256i a, __m128i b, int pos)
{
    __m256i b_256 = lasx_cast128_256(b);
    if (pos == 0)
        return __lasx_xvpermi_q(a, b_256, 0 | (3 << 4));
    else
        return __lasx_xvpermi_q(a, b_256, 2 | (0<<4));
}
#undef VREGS_PREFIX
#undef XREGS_PREFIX
#undef __ALL_REGS

static inline __m256i lasx_ext_h_b(__m128i _a)
{
    return __lasx_vext2xv_h_b(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_w_b(__m128i _a)
{
    return __lasx_vext2xv_w_b(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_d_b(__m128i _a)
{
    return __lasx_vext2xv_d_b(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_w_h(__m128i _a)
{
    return __lasx_vext2xv_w_h(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_d_h(__m128i _a)
{
    return __lasx_vext2xv_d_h(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_d_w(__m128i _a)
{
    return __lasx_vext2xv_d_w(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_hu_bu(__m128i _a)
{
    return __lasx_vext2xv_hu_bu(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_wu_bu(__m128i _a)
{
    return __lasx_vext2xv_wu_bu(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_du_bu(__m128i _a)
{
    return __lasx_vext2xv_du_bu(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_wu_hu(__m128i _a)
{
    return __lasx_vext2xv_wu_hu(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_du_hu(__m128i _a)
{
    return __lasx_vext2xv_du_hu(lasx_cast128_256(_a));
}

static inline __m256i lasx_ext_du_wu(__m128i _a)
{
    return __lasx_vext2xv_du_wu(lasx_cast128_256(_a));
}
#endif // __loongarch_asx && __loongarch_sx

#ifdef __loongarch_asx
static inline __m256i lasx_packs_w(__m256i a, __m256i b) {
    __m256i tmp, tmp1;
    tmp = __lasx_xvsat_w(a, 15);
    tmp1 = __lasx_xvsat_w(b, 15);
    return __lasx_xvpickev_h(tmp1, tmp);
}

static inline __m256i lasx_packs_h(__m256i a, __m256i b) {
    __m256i tmp, tmp1;
    tmp = __lasx_xvsat_h(a, 7);
    tmp1 = __lasx_xvsat_h(b, 7);
    return __lasx_xvpickev_b(tmp1, tmp);
}

static inline __m256i lasx_packus_h(__m256i a, __m256i b) {
    __m256i tmp, tmp1;
    tmp = __lasx_xvsat_hu(__lasx_xvmax_h(__lasx_xvldi(0), a), 7);
    tmp1 = __lasx_xvsat_hu(__lasx_xvmax_h(__lasx_xvldi(0), b), 7);
    return __lasx_xvpickev_b(tmp1, tmp);
}
static __m256i lasx_extu8_16(__m128i a)
{
        __m128i zero = __lsx_vldi(0);
        __m128i vlo = __lsx_vilvl_b(zero, a);
        __m128i vhi = __lsx_vilvh_b(zero, a);
        return ____m256i_2(vhi, vlo);
}

static __m256i lasx_exti8_16(__m128i a)  //TODO check exti8_16
{
     __m128i sign = __lsx_vslti_b(a, 0);
     __m128i vlo = __lsx_vilvl_b(sign, a);
     __m128i vhi = __lsx_vilvh_b(sign, a);
     return ____m256i_2(vhi, vlo);
}

static inline __m256 __lasx_xvreplfr2vr_s(float val)
{
    union { int i; float f; } fi_tmpval = {.f = val};
    return (__m256)__lasx_xvreplgr2vr_w(fi_tmpval.i);
}

static __m256i lasx_maddubs_h(__m256i a, __m256i b)
{
    __m256i tmp1, tmp2;
    tmp1 = __lasx_xvmulwev_h_b(a, b);
    tmp2 = __lasx_xvmulwod_h_b(a, b);
    return __lasx_xvsadd_h(tmp1, tmp2);
}

static __m256i lasx_madd_h(__m256i a, __m256i b)
{
    __m256i tmp1, tmp2;
    tmp1 = __lasx_xvmulwev_w_h(a, b);
    tmp2 = __lasx_xvmulwod_w_h(a, b);
    return __lasx_xvadd_w(tmp1, tmp2);
}

static __m256i lasx_hadd_w(__m256i a, __m256i b)
{
    __m256i tmp1 = __lasx_xvpickev_w(b, a);
    __m256i tmp2 = __lasx_xvpickod_w(b, a);
    return __lasx_xvadd_w(tmp1, tmp2);
}
static __m256i lasx_set_d(int64_t a, int64_t b, int64_t c, int64_t d)
{
    v4i64 __ret = {d, c, b, a};
    return (__m256i)__ret;
}

static __m256i lasx_set_w(int e7, int e6, int e5, int e4, int e3, int e2, int e1, int e0)
{
    v8i32 __ret = {e0, e1, e2, e3, e4, e5, e6, e7};
    return (__m256i)__ret;
}

static __m256 lasx_set_s(float e7, float e6, float e5, float e4, float e3, float e2, float e1, float e0)
{
    v8f32 __ret = {e0, e1, e2, e3, e4, e5, e6, e7};
    return (__m256)__ret;
}

static inline __m256i lasx_mulhrs_h(__m256i a, __m256i b)
{
    __m256i l = __lasx_xvmul_h(a, b);
    __m256i h = __lasx_xvmuh_h(a, b);
    l = __lasx_xvaddi_hu(__lasx_xvsrli_h(l, 14), 1);
    h = __lasx_xvslli_h(h, 1);
    return __lasx_xvadd_h(h, __lasx_xvsrli_h(l, 1));
}

static inline __m256i lasx_blendv_b(__m256i a, __m256i b, __m256i c)
{
    return __lasx_xvbitsel_v(a, b, __lasx_xvslti_b(c, 0));
}

static inline __m256i lasx_set1_b(uint8_t a)
{
    return __lasx_xvreplgr2vr_b(a);
}

static __m256i lasx_shuffle_b(__m256i a, __m256i b)
{
        __m256i mask_f, zero, tmp0, tmp2, mask;
        int f = 0x8f;
        mask_f = __lasx_xvreplgr2vr_b(f);
        zero = __lasx_xvldi(0);
        tmp0 = __lasx_xvand_v(b, mask_f); // get mask with low 4 bit and sign bits
        tmp0 = __lasx_xvori_b(tmp0, 0x10); // make each mask or  with 0x10 prepare for positive
        mask = __lasx_xvsle_b(zero, tmp0); // if mask >= 0, set mask
        tmp2 = __lasx_xvand_v(tmp0, mask); // maskout the in2 < ones
        return __lasx_xvshuf_b(a, zero, tmp2);
}
static inline __m256i lasx_replvr2xvr_q(__m128i d)
{
    return ____m256i_2(d, d);
}
static inline __m256i __lasx_ldrepl_q(void * mem)
{
    __m128i d = __lsx_vld(mem, 0);
    return ____m256i_2(d, d);
}
#define lasx_ldrepl_q(p) __lasx_ldrepl_q((void *)(p))
#endif // __loongarch_asx

#ifdef __loongarch_sx
static inline __m128i lsx_packs_w(__m128i a, __m128i b) {
    __m128i tmp, tmp1;
    tmp = __lsx_vsat_w(a, 15);
    tmp1 = __lsx_vsat_w(b, 15);
    return __lsx_vpickev_h(tmp1, tmp);
}

static inline __m128i lsx_packs_h(__m128i a, __m128i b) {
    __m128i tmp, tmp1;
    tmp = __lsx_vsat_h(a, 7);
    tmp1 = __lsx_vsat_h(b, 7);
    return __lsx_vpickev_b(tmp1, tmp);
}

static inline __m128i lsx_packus_h(__m128i a, __m128i b) {
    __m128i tmp, tmp1;
    tmp = __lsx_vsat_hu(__lsx_vmax_h(__lsx_vldi(0), a), 7);
    tmp1 = __lsx_vsat_hu(__lsx_vmax_h(__lsx_vldi(0), b), 7);
    return __lsx_vpickev_b(tmp1, tmp);
}

/* float type data load instructions */
static inline __m128 lsx_vreplfr2vr_s(float val)
{
    union { int i; float f; } fi_tmpval = {.f = val};
    return (__m128)__lsx_vreplgr2vr_w(fi_tmpval.i);
}
//_mm_load_ss
static inline __m128 lsx_fld1_s(void *p)
{

    union { int i; float f; } fi_tmpval = {.f = *(float *)p};
    return (__m128)__lsx_vinsgr2vr_w(__lsx_vldi(0), fi_tmpval.i, 0);
}
//_mm_load_sd
static inline __m128d lsx_fld1_d(void *p)
{

    union { long long i; double f; } fi_tmpval = {.f = *(double *)p};
    return (__m128d)__lsx_vinsgr2vr_d(__lsx_vldi(0), fi_tmpval.i, 0);
}

static __m128i lsx_hadd_h(__m128i a, __m128i b)
{
    __m128i tmp1 = __lsx_vpickev_h(b, a);
    __m128i tmp2 = __lsx_vpickod_h(b, a);
    return __lsx_vadd_h(tmp1, tmp2);
}

static __m128i lsx_hadd_w(__m128i a, __m128i b)
{
    __m128i tmp1 = __lsx_vpickev_w(b, a);
    __m128i tmp2 = __lsx_vpickod_w(b, a);
    return __lsx_vadd_w(tmp1, tmp2);
}

static __m128 lsx_hadd_s(__m128 a, __m128 b)
{
    __m128 tmp1 = (__m128)__lsx_vpickev_w((__m128i)b, (__m128i)a);
    __m128 tmp2 = (__m128)__lsx_vpickod_w((__m128i)b, (__m128i)a);

    return __lsx_vfadd_s(tmp1, tmp2);
}
static __m128i lsx_set_w(int32_t a, int32_t b, int32_t c, int32_t d)
{
    v4i32 __ret = {d, c, b, a};
    return (__m128i)__ret;
}
static __m128 lsx_set_s(float a, float b, float c, float d)
{
    v4f32 __ret = {d, c, b, a};
    return (__m128)__ret;
}

static inline __m128i lsx_mulhrs_h(__m128i a, __m128i b)
{
    __m128i l = __lsx_vmul_h(a, b);
    __m128i h = __lsx_vmuh_h(a, b);
    l = __lsx_vaddi_hu(__lsx_vsrli_h(l, 14), 1);
    h = __lsx_vslli_h(h, 1);
    return __lsx_vadd_h(h, __lsx_vsrli_h(l, 1));
}

static __m128i lsx_maddubs_h(__m128i a, __m128i b)
{
    __m128i tmp1, tmp2;
    tmp1 = __lsx_vmulwev_h_b(a, b);
    tmp2 = __lsx_vmulwod_h_b(a, b);
    return __lsx_vsadd_h(tmp1, tmp2);
}

static __m128i lsx_madd_h(__m128i a, __m128i b)
{
    __m128i tmp1, tmp2;
    tmp1 = __lsx_vmulwev_w_h(a, b);
    tmp2 = __lsx_vmulwod_w_h(a, b);
    return __lsx_vadd_w(tmp1, tmp2);
}

static inline __m128i lsx_blend_h(__m128i a, __m128i b, int imm8)
{
    __m128i c = __lsx_vldi(imm8 | 0xf900);
    return __lsx_vbitsel_v(a, b, __lsx_vilvl_b(c, c));
}

static inline __m128i lsx_blendv_epi8(__m128i a, __m128i b, __m128i c)
{
    return __lsx_vbitsel_v(a, b, __lsx_vslti_b(c, 0));
}

static __m128i lsx_shuffle_b(__m128i a, __m128i b)
{
        __m128i mask_f, zero, tmp0, tmp2, mask;
        int f = 0x8f;
        mask_f = __lsx_vreplgr2vr_b(f);
        zero = __lsx_vldi(0);
        tmp0 = __lsx_vand_v(b, mask_f); // get mask with low 4 bit and sign bits
        tmp0 = __lsx_vori_b(tmp0, 0x10); // make each mask or  with 0x10 prepare for positive
        mask = __lsx_vsle_b(zero, tmp0); // if mask >= 0, set mask
        tmp2 = __lsx_vand_v(tmp0, mask); // maskout the in2 < ones
        return __lsx_vshuf_b(a, zero, tmp2);
}
#endif // __loongarch_sx

#define _MM_SHUFFLE(z, y, x, w) (((z) << 6) | ((y) << 4) | ((x) << 2) | (w))

#define lasx_extracti128(x, y) lasx_extract128((__m256i)(x), y)
#define lasx_extractf128(x, y) ((__m128)lasx_extract128((__m256i)(x), y))

#define lasx_cast128_f256(x) (__m256)lasx_cast128_256((__m128i)(x))
#define lasx_cast128_i256(x) lasx_cast128_256((__m128i)(x))

#define lasx_inserti128(x, y, z) lasx_insert128(x, y, z)
#define lasx_insertf128(x, y, z) (__m256)lasx_insert128((__m256i)(x), (__m128i)(y), z)

#define lasx_extractf128_lo(x) (__m128)lasx_extract128_lo((__m256i)(x))
#define lasx_extracti128_lo(x) (__m128i)lasx_extract128_lo((__m256i)(x))

#define lasx_extractf128_hi(x) (__m128)lasx_extract128_hi((__m256i)(x))
#define lasx_extracti128_hi(x) (__m128i)lasx_extract128_hi((__m256i)(x))

#define lasx_xvxor_v(x, y) __lasx_xvxor_v((__m256i)(x),(__m256i)(y))
#define lasx_xvor_v(x, y) __lasx_xvor_v((__m256i)(x),(__m256i)(y))

#define lsx_vpermutevar_s(a, b) (__m128)__lsx_vshuf_w(__lsx_vand_v((__m128i)(b), __lsx_vreplgr2vr_w(3)), (__m128i)(a), (__m128i)(a))
#define lasx_xvpermutevar_s(a, b) (__m256)__lasx_xvshuf_w(__lasx_xvand_v((__m256i)(b), __lasx_xvreplgr2vr_w(3)), (__m256i)(a), (__m256i)(a))
#define lasx_permutevar8x32_w(a, msk) __lasx_xvshuf_w((__m256i)(msk), __lasx_xvpermi_q((a), (a), 0x11), __lasx_xvpermi_q((a), (a), 0x00))
#define lasx_permutevar4x64_d(a, msk) __lasx_xvshuf4i_d(__lasx_xvpermi_q((a), (a), 0x00), __lasx_xvpermi_q((a), (a), 0x11), (msk))
//_mm256_blendv_ps
#define lasx_xvbitsel_f(a, b, c) (__m256)__lasx_xvbitsel_v((__m256i)a, (__m256i)b, __lasx_xvslti_w((__m256i)c, 0))
#define lasx_blendv_s(a, b, c) (__m256)__lasx_xvbitsel_v((__m256i)(a), (__m256i)(b), __lasx_xvslti_w((__m256i)(c), 0))
//
#define lasx_maskload_s(a, b) (__m256)__lasx_xvand_v(__lasx_xvld((a), 0), __lasx_xvslti_w((__m256i)(b),0))
#define lasx_maskload_d(a, b) (__m256d)__lasx_xvand_v(__lasx_xvld((a), 0), __lasx_xvslti_d((__m256i)(b),0))
#define lasx_maskload_w(a, b) __lasx_xvand_v(__lasx_xvld(a, 0), __lasx_xvslti_w(b, 0))

#define lsx_cvtsi32_si128(a) __lsx_vinsgr2vr_w(__lsx_vldi(0), (a), 0)
#define lsx_cvtsi128_si64(a) __lsx_vpickve2gr_d((__m128i)(a), 0)
#define lsx_cvtb_h(a) __lsx_vsllwil_h_b(a, 0)
#define lsx_cvtbu_hu(a) __lsx_vsllwil_hu_bu(a, 0)
#define lsx_exti8_32(a) __lsx_vsllwil_w_h(__lsx_vsllwil_h_b(a, 0), 0)
#define lsx_extu8_32(a) __lsx_vsllwil_wu_hu(__lsx_vsllwil_hu_bu(a, 0), 0)
#define lsx_sra_w1(a, b)  __lsx_vsra_w((a), __lsx_vreplvei_w((b), 0))

#define lsx_blend_h(a, b, imm8) __lsx_blend_h(a, b, __lsx_vldi((imm8) | 0xf900))
#define lsx_loadl_d(a) __lsx_vinsgr2vr_d(__lsx_vldi(0), *(long*)(a), 0)
//#define lsx_shuffle_b(a, b) __lsx_vand_v(__lsx_vshuf_b(a, a, b), __lsx_vxori_b(__lsx_vslti_b(b, 0), 0xff))

#define LSX_TRANSPOSE4_S(row0, row1, row2, row3)       \
do {                                                   \
  __m128i __r0 = (__m128i)(row0);                      \
  __m128i __r1 = (__m128i)(row1);                      \
  __m128i __r2 = (__m128i)(row2);                      \
  __m128i __r3 = (__m128i)(row3);                      \
  __m128i __t0 = __lsx_vilvl_w(__r1, __r0);            \
  __m128i __t1 = __lsx_vilvh_w(__r1, __r0);            \
  __m128i __t2 = __lsx_vilvl_w(__r3, __r2);            \
  __m128i __t3 = __lsx_vilvh_w(__r3, __r2);            \
  (row0) = (__m128)__lsx_vilvl_d(__t2, __t0);          \
  (row1) = (__m128)__lsx_vilvh_d(__t2, __t0);          \
  (row2) = (__m128)__lsx_vilvl_d(__t3, __t1);          \
  (row3) = (__m128)__lsx_vilvh_d(__t3, __t1);          \
} while (0)
