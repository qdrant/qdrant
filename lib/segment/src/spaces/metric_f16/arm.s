	.section	__TEXT,__text,regular,pure_instructions
	.build_version macos, 14, 0	sdk_version 14, 2
	.globl	_dotProduct_half_4x4            ; -- Begin function dotProduct_half_4x4
	.p2align	2
_dotProduct_half_4x4:                   ; @dotProduct_half_4x4
	.cfi_startproc
; %armBB.0:
	stp	x28, x27, [sp, #-16]!           ; 16-byte Folded Spill
	.cfi_def_cfa_offset 16
	.cfi_offset w27, -8
	.cfi_offset w28, -16
	sub	sp, sp, #1232
	.cfi_def_cfa_offset 1248
	str	x0, [sp, #632]
	str	x1, [sp, #624]
	str	w2, [sp, #620]
	mov	w8, #0
	str	w8, [sp, #616]
	movi	d0, #0000000000000000
	str	h0, [sp, #574]
	add	x9, sp, #574
	ld1r.8h	{{ v1 }}, [x9]
	str	q1, [sp, #544]
	ldr	q1, [sp, #544]
	str	q1, [sp, #576]
	ldr	q1, [sp, #576]
	str	q1, [sp, #528]
	ldr	q1, [sp, #528]
	str	q1, [sp, #592]
	str	h0, [sp, #494]
	add	x9, sp, #494
	ld1r.8h	{{ v1 }}, [x9]
	str	q1, [sp, #464]
	ldr	q1, [sp, #464]
	str	q1, [sp, #496]
	ldr	q1, [sp, #496]
	str	q1, [sp, #448]
	ldr	q1, [sp, #448]
	str	q1, [sp, #512]
	str	h0, [sp, #414]
	add	x9, sp, #414
	ld1r.8h	{{ v1 }}, [x9]
	str	q1, [sp, #384]
	ldr	q1, [sp, #384]
	str	q1, [sp, #416]
	ldr	q1, [sp, #416]
	str	q1, [sp, #368]
	ldr	q1, [sp, #368]
	str	q1, [sp, #432]
	str	h0, [sp, #334]
	add	x9, sp, #334
	ld1r.8h	{{ v0 }}, [x9]
	str	q0, [sp, #304]
	ldr	q0, [sp, #304]
	str	q0, [sp, #336]
	ldr	q0, [sp, #336]
	str	q0, [sp, #288]
	ldr	q0, [sp, #288]
	str	q0, [sp, #352]
	str	w8, [sp, #284]
	str	w8, [sp, #284]
	b	LarmBB0_1
LarmBB0_1:                                 ; =>This Inner Loop Header: Depth=1
	ldr	w8, [sp, #284]
	ldr	w9, [sp, #620]
	and	w9, w9, #0xffffffe0
	subs	w8, w8, w9
	b.hs	LarmBB0_4
	b	LarmBB0_2
LarmBB0_2:                                 ;   in Loop: Header=armBB0_1 Depth=1
	ldr	q2, [sp, #592]
	ldr	x8, [sp, #632]
	ldr	q0, [x8]
	str	q0, [sp, #256]
	ldr	q0, [sp, #256]
	str	q0, [sp, #240]
	ldr	q1, [sp, #240]
	ldr	x8, [sp, #624]
	ldr	q0, [x8]
	str	q0, [sp, #224]
	ldr	q0, [sp, #224]
	str	q0, [sp, #208]
	ldr	q0, [sp, #208]
	str	q2, [sp, #880]
	str	q1, [sp, #864]
	str	q0, [sp, #848]
	ldr	q0, [sp, #880]
	ldr	q2, [sp, #864]
	ldr	q1, [sp, #848]
	fmla.8h	v0, v1, v2
	str	q0, [sp, #832]
	ldr	q0, [sp, #832]
	str	q0, [sp, #592]
	ldr	q2, [sp, #512]
	ldr	x8, [sp, #632]
	ldr	q0, [x8, #16]
	str	q0, [sp, #192]
	ldr	q0, [sp, #192]
	str	q0, [sp, #176]
	ldr	q1, [sp, #176]
	ldr	x8, [sp, #624]
	ldr	q0, [x8, #16]
	str	q0, [sp, #160]
	ldr	q0, [sp, #160]
	str	q0, [sp, #144]
	ldr	q0, [sp, #144]
	str	q2, [sp, #816]
	str	q1, [sp, #800]
	str	q0, [sp, #784]
	ldr	q0, [sp, #816]
	ldr	q2, [sp, #800]
	ldr	q1, [sp, #784]
	fmla.8h	v0, v1, v2
	str	q0, [sp, #768]
	ldr	q0, [sp, #768]
	str	q0, [sp, #512]
	ldr	q2, [sp, #432]
	ldr	x8, [sp, #632]
	ldr	q0, [x8, #32]
	str	q0, [sp, #128]
	ldr	q0, [sp, #128]
	str	q0, [sp, #112]
	ldr	q1, [sp, #112]
	ldr	x8, [sp, #624]
	ldr	q0, [x8, #32]
	str	q0, [sp, #96]
	ldr	q0, [sp, #96]
	str	q0, [sp, #80]
	ldr	q0, [sp, #80]
	str	q2, [sp, #752]
	str	q1, [sp, #736]
	str	q0, [sp, #720]
	ldr	q0, [sp, #752]
	ldr	q2, [sp, #736]
	ldr	q1, [sp, #720]
	fmla.8h	v0, v1, v2
	str	q0, [sp, #704]
	ldr	q0, [sp, #704]
	str	q0, [sp, #432]
	ldr	q2, [sp, #352]
	ldr	x8, [sp, #632]
	ldr	q0, [x8, #48]
	str	q0, [sp, #64]
	ldr	q0, [sp, #64]
	str	q0, [sp, #48]
	ldr	q1, [sp, #48]
	ldr	x8, [sp, #624]
	ldr	q0, [x8, #48]
	str	q0, [sp, #32]
	ldr	q0, [sp, #32]
	str	q0, [sp, #16]
	ldr	q0, [sp, #16]
	str	q2, [sp, #688]
	str	q1, [sp, #672]
	str	q0, [sp, #656]
	ldr	q0, [sp, #688]
	ldr	q2, [sp, #672]
	ldr	q1, [sp, #656]
	fmla.8h	v0, v1, v2
	str	q0, [sp, #640]
	ldr	q0, [sp, #640]
	str	q0, [sp, #352]
	ldr	x8, [sp, #632]
	add	x8, x8, #64
	str	x8, [sp, #632]
	ldr	x8, [sp, #624]
	add	x8, x8, #64
	str	x8, [sp, #624]
	b	LarmBB0_3
LarmBB0_3:                                 ;   in Loop: Header=armBB0_1 Depth=1
	ldr	w8, [sp, #284]
	add	w8, w8, #32
	str	w8, [sp, #284]
	b	LarmBB0_1
LarmBB0_4:
	ldr	q1, [sp, #592]
	ldr	q0, [sp, #512]
	str	q1, [sp, #1024]
	str	q0, [sp, #1008]
	ldr	q0, [sp, #1024]
	ldr	q1, [sp, #1008]
	fadd.8h	v0, v0, v1
	str	q0, [sp, #992]
	ldr	q0, [sp, #992]
	str	q0, [sp, #592]
	ldr	q1, [sp, #592]
	ldr	q0, [sp, #432]
	str	q1, [sp, #976]
	str	q0, [sp, #960]
	ldr	q0, [sp, #976]
	ldr	q1, [sp, #960]
	fadd.8h	v0, v0, v1
	str	q0, [sp, #944]
	ldr	q0, [sp, #944]
	str	q0, [sp, #592]
	ldr	q1, [sp, #592]
	ldr	q0, [sp, #352]
	str	q1, [sp, #928]
	str	q0, [sp, #912]
	ldr	q0, [sp, #928]
	ldr	q1, [sp, #912]
	fadd.8h	v0, v0, v1
	str	q0, [sp, #896]
	ldr	q0, [sp, #896]
	str	q0, [sp, #592]
	ldr	q0, [sp, #592]
	str	q0, [sp, #1104]
	ldr	q0, [sp, #1104]
	ext.16b	v0, v0, v0, #8
                                        ; kill: def $d0 killed $d0 killed $q0
	str	d0, [sp, #1096]
	ldr	d0, [sp, #1096]
	str	d0, [sp, #1088]
	ldr	d0, [sp, #1088]
	fcvtl	v0.4s, v0.4h
	str	q0, [sp, #1072]
	ldr	q0, [sp, #1072]
	str	q0, [sp]
	ldr	q1, [sp]
	ldr	q0, [sp, #592]
	str	q0, [sp, #1184]
	ldr	q0, [sp, #1184]
                                        ; kill: def $d0 killed $d0 killed $q0
	str	d0, [sp, #1176]
	ldr	d0, [sp, #1176]
	str	d0, [sp, #1064]
	ldr	d0, [sp, #1064]
	fcvtl	v0.4s, v0.4h
	str	q0, [sp, #1040]
	ldr	q0, [sp, #1040]
	str	q1, [sp, #1152]
	str	q0, [sp, #1136]
	ldr	q0, [sp, #1152]
	ldr	q1, [sp, #1136]
	fadd.4s	v0, v0, v1
	str	q0, [sp, #1120]
	ldr	q0, [sp, #1120]
	str	q0, [sp]
	ldr	q0, [sp]
	str	q0, [sp, #1216]
	ldr	q0, [sp, #1216]
	faddp.4s	v0, v0, v0
	fmov	x0, d0
	fmov	d0, x0
	faddp.2s	s0, v0
	str	s0, [sp, #1212]
	ldr	s0, [sp, #1212]
	str	s0, [sp, #616]
	mov	w8, #0
	str	w8, [sp, #284]
	b	LarmBB0_5
LarmBB0_5:                                 ; =>This Inner Loop Header: Depth=1
	ldr	w8, [sp, #284]
	ldr	w9, [sp, #620]
	and	w9, w9, #0x1f
	subs	w8, w8, w9
	b.hs	LarmBB0_8
	b	LarmBB0_6
LarmBB0_6:                                 ;   in Loop: Header=armBB0_5 Depth=1
	ldr	x8, [sp, #632]
	ldr	h0, [x8]
	fcvt	s0, h0
	ldr	x8, [sp, #624]
	ldr	h1, [x8]
	fcvt	s1, h1
	ldr	s2, [sp, #616]
	fmadd	s0, s0, s1, s2
	str	s0, [sp, #616]
	ldr	x8, [sp, #632]
	add	x8, x8, #2
	str	x8, [sp, #632]
	ldr	x8, [sp, #624]
	add	x8, x8, #2
	str	x8, [sp, #624]
	b	LarmBB0_7
LarmBB0_7:                                 ;   in Loop: Header=armBB0_5 Depth=1
	ldr	w8, [sp, #284]
	add	w8, w8, #1
	str	w8, [sp, #284]
	b	LarmBB0_5
LarmBB0_8:
	ldr	s0, [sp, #616]
	add	sp, sp, #1232
	ldp	x28, x27, [sp], #16             ; 16-byte Folded Reload
	ret
	.cfi_endproc
                                        ; -- End function
	.globl	_euclideanDist_half_4x4         ; -- Begin function euclideanDist_half_4x4
	.p2align	2
_euclideanDist_half_4x4:                ; @euclideanDist_half_4x4
	.cfi_startproc
; %armBB.0:
	stp	x28, x27, [sp, #-16]!           ; 16-byte Folded Spill
	.cfi_def_cfa_offset 16
	.cfi_offset w27, -8
	.cfi_offset w28, -16
	sub	sp, sp, #1760
	.cfi_def_cfa_offset 1776
	add	x9, sp, #144
	str	x9, [sp]                        ; 8-byte Folded Spill
	str	x0, [x9, #824]
	str	x1, [x9, #816]
	str	w2, [sp, #956]
	mov	w8, #0
	str	w8, [sp, #952]
	movi	d0, #0000000000000000
	str	h0, [sp, #10]                   ; 2-byte Folded Spill
	str	h0, [sp, #910]
	add	x10, sp, #910
	ld1r.8h	{{ v1 }}, [x10]
	str	q1, [x9, #736]
	ldr	q1, [x9, #736]
	str	q1, [x9, #768]
	ldr	q1, [x9, #768]
	str	q1, [x9, #720]
	ldr	q1, [x9, #720]
	str	q1, [x9, #784]
	str	h0, [sp, #830]
	add	x10, sp, #830
	ld1r.8h	{{ v1 }}, [x10]
	str	q1, [x9, #656]
	ldr	q1, [x9, #656]
	str	q1, [x9, #688]
	ldr	q1, [x9, #688]
	str	q1, [x9, #640]
	ldr	q1, [x9, #640]
	str	q1, [x9, #704]
	str	h0, [sp, #750]
	add	x10, sp, #750
	ld1r.8h	{{ v1 }}, [x10]
	str	q1, [x9, #576]
	ldr	q1, [x9, #576]
	str	q1, [x9, #608]
	ldr	q1, [x9, #608]
	str	q1, [x9, #560]
	ldr	q1, [x9, #560]
	str	q1, [x9, #624]
	str	h0, [sp, #670]
	add	x10, sp, #670
	ld1r.8h	{{ v1 }}, [x10]
	str	q1, [x9, #496]
	ldr	q1, [x9, #496]
	str	q1, [x9, #528]
	ldr	q1, [x9, #528]
	str	q1, [x9, #480]
	ldr	q1, [x9, #480]
	str	q1, [x9, #544]
	str	h0, [sp, #590]
	add	x10, sp, #590
	ld1r.8h	{{ v1 }}, [x10]
	str	q1, [x9, #416]
	ldr	q1, [x9, #416]
	str	q1, [x9, #448]
	ldr	q1, [x9, #448]
	str	q1, [x9, #400]
	ldr	q1, [x9, #400]
	str	q1, [x9, #464]
	str	h0, [sp, #510]
	add	x10, sp, #510
	ld1r.8h	{{ v1 }}, [x10]
	str	q1, [x9, #336]
	ldr	q1, [x9, #336]
	str	q1, [x9, #368]
	ldr	q1, [x9, #368]
	str	q1, [x9, #320]
	ldr	q1, [x9, #320]
	str	q1, [x9, #384]
	str	h0, [sp, #430]
	add	x10, sp, #430
	ld1r.8h	{{ v1 }}, [x10]
	str	q1, [x9, #256]
	ldr	q1, [x9, #256]
	str	q1, [x9, #288]
	ldr	q1, [x9, #288]
	str	q1, [x9, #240]
	ldr	q1, [x9, #240]
	str	q1, [x9, #304]
	str	h0, [sp, #350]
	add	x10, sp, #350
	ld1r.8h	{{ v0 }}, [x10]
	str	q0, [x9, #176]
	ldr	q0, [x9, #176]
	str	q0, [x9, #208]
	ldr	q0, [x9, #208]
	str	q0, [x9, #160]
	ldr	q0, [x9, #160]
	str	q0, [x9, #224]
	str	w8, [sp, #300]
	str	w8, [sp, #300]
	b	LarmBB1_1
LarmBB1_1:                                 ; =>This Inner Loop Header: Depth=1
	ldr	w8, [sp, #300]
	ldr	w9, [sp, #956]
	and	w9, w9, #0xffffffe0
	subs	w8, w8, w9
	b.hs	LarmBB1_4
	b	LarmBB1_2
LarmBB1_2:                                 ;   in Loop: Header=armBB1_1 Depth=1
	ldr	x9, [sp]                        ; 8-byte Folded Reload
	ldr	x8, [x9, #824]
	ldr	q0, [x8]
	str	q0, [x9, #128]
	ldr	q0, [x9, #128]
	str	q0, [x9, #112]
	ldr	q1, [x9, #112]
	ldr	x8, [x9, #816]
	ldr	q0, [x8]
	str	q0, [x9, #96]
	ldr	q0, [x9, #96]
	str	q0, [x9, #80]
	ldr	q0, [x9, #80]
	str	q1, [x9, #1600]
	str	q0, [x9, #1584]
	ldr	q0, [x9, #1600]
	ldr	q1, [x9, #1584]
	fsub.8h	v0, v0, v1
	str	q0, [x9, #1568]
	ldr	q0, [x9, #1568]
	str	q0, [x9, #704]
	ldr	q1, [x9, #784]
	ldr	q0, [x9, #704]
	str	q1, [x9, #1072]
	str	q0, [x9, #1056]
	str	q0, [x9, #1040]
	ldr	q0, [x9, #1072]
	ldr	q2, [x9, #1056]
	ldr	q1, [x9, #1040]
	fmla.8h	v0, v1, v2
	str	q0, [x9, #1024]
	ldr	q0, [x9, #1024]
	str	q0, [x9, #784]
	ldr	x8, [x9, #824]
	ldr	q0, [x8, #16]
	str	q0, [x9, #64]
	ldr	q0, [x9, #64]
	str	q0, [x9, #48]
	ldr	q1, [x9, #48]
	ldr	x8, [x9, #816]
	ldr	q0, [x8, #16]
	str	q0, [x9, #32]
	ldr	q0, [x9, #32]
	str	q0, [x9, #16]
	ldr	q0, [x9, #16]
	str	q1, [x9, #1552]
	str	q0, [x9, #1536]
	ldr	q0, [x9, #1552]
	ldr	q1, [x9, #1536]
	fsub.8h	v0, v0, v1
	str	q0, [x9, #1520]
	ldr	q0, [x9, #1520]
	str	q0, [x9, #544]
	ldr	q1, [x9, #624]
	ldr	q0, [x9, #544]
	str	q1, [x9, #1008]
	str	q0, [x9, #992]
	str	q0, [x9, #976]
	ldr	q0, [x9, #1008]
	ldr	q2, [x9, #992]
	ldr	q1, [x9, #976]
	fmla.8h	v0, v1, v2
	str	q0, [x9, #960]
	ldr	q0, [x9, #960]
	str	q0, [x9, #624]
	ldr	x8, [x9, #824]
	ldr	q0, [x8, #32]
	str	q0, [x9]
	ldr	q0, [x9]
	str	q0, [sp, #128]
	ldr	q1, [sp, #128]
	ldr	x8, [x9, #816]
	ldr	q0, [x8, #32]
	str	q0, [sp, #112]
	ldr	q0, [sp, #112]
	str	q0, [sp, #96]
	ldr	q0, [sp, #96]
	str	q1, [x9, #1504]
	str	q0, [x9, #1488]
	ldr	q0, [x9, #1504]
	ldr	q1, [x9, #1488]
	fsub.8h	v0, v0, v1
	str	q0, [x9, #1472]
	ldr	q0, [x9, #1472]
	str	q0, [x9, #384]
	ldr	q1, [x9, #464]
	ldr	q0, [x9, #384]
	str	q1, [x9, #944]
	str	q0, [x9, #928]
	str	q0, [x9, #912]
	ldr	q0, [x9, #944]
	ldr	q2, [x9, #928]
	ldr	q1, [x9, #912]
	fmla.8h	v0, v1, v2
	str	q0, [x9, #896]
	ldr	q0, [x9, #896]
	str	q0, [x9, #464]
	ldr	x8, [x9, #824]
	ldr	q0, [x8, #48]
	str	q0, [sp, #80]
	ldr	q0, [sp, #80]
	str	q0, [sp, #64]
	ldr	q1, [sp, #64]
	ldr	x8, [x9, #816]
	ldr	q0, [x8, #48]
	str	q0, [sp, #48]
	ldr	q0, [sp, #48]
	str	q0, [sp, #32]
	ldr	q0, [sp, #32]
	str	q1, [x9, #1456]
	str	q0, [x9, #1440]
	ldr	q0, [x9, #1456]
	ldr	q1, [x9, #1440]
	fsub.8h	v0, v0, v1
	str	q0, [x9, #1424]
	ldr	q0, [x9, #1424]
	str	q0, [x9, #224]
	ldr	q1, [x9, #304]
	ldr	q0, [x9, #224]
	str	q1, [x9, #880]
	str	q0, [x9, #864]
	str	q0, [x9, #848]
	ldr	q0, [x9, #880]
	ldr	q2, [x9, #864]
	ldr	q1, [x9, #848]
	fmla.8h	v0, v1, v2
	str	q0, [x9, #832]
	ldr	q0, [x9, #832]
	str	q0, [x9, #304]
	ldr	x8, [x9, #824]
	add	x8, x8, #64
	str	x8, [x9, #824]
	ldr	x8, [x9, #816]
	add	x8, x8, #64
	str	x8, [x9, #816]
	b	LarmBB1_3
LarmBB1_3:                                 ;   in Loop: Header=armBB1_1 Depth=1
	ldr	w8, [sp, #300]
	add	w8, w8, #32
	str	w8, [sp, #300]
	b	LarmBB1_1
LarmBB1_4:
	ldr	x8, [sp]                        ; 8-byte Folded Reload
	ldr	q1, [x8, #784]
	ldr	q0, [x8, #624]
	str	q1, [x8, #1216]
	str	q0, [x8, #1200]
	ldr	q0, [x8, #1216]
	ldr	q1, [x8, #1200]
	fadd.8h	v0, v0, v1
	str	q0, [x8, #1184]
	ldr	q0, [x8, #1184]
	str	q0, [x8, #784]
	ldr	q1, [x8, #784]
	ldr	q0, [x8, #464]
	str	q1, [x8, #1168]
	str	q0, [x8, #1152]
	ldr	q0, [x8, #1168]
	ldr	q1, [x8, #1152]
	fadd.8h	v0, v0, v1
	str	q0, [x8, #1136]
	ldr	q0, [x8, #1136]
	str	q0, [x8, #784]
	ldr	q1, [x8, #784]
	ldr	q0, [x8, #304]
	str	q1, [x8, #1120]
	str	q0, [x8, #1104]
	ldr	q0, [x8, #1120]
	ldr	q1, [x8, #1104]
	fadd.8h	v0, v0, v1
	str	q0, [x8, #1088]
	ldr	q0, [x8, #1088]
	str	q0, [x8, #784]
	ldr	q0, [x8, #784]
	str	q0, [x8, #1296]
	ldr	q0, [x8, #1296]
	ext.16b	v0, v0, v0, #8
                                        ; kill: def $d0 killed $d0 killed $q0
	str	d0, [x8, #1288]
	ldr	d0, [x8, #1288]
	str	d0, [x8, #1280]
	ldr	d0, [x8, #1280]
	fcvtl	v0.4s, v0.4h
	str	q0, [x8, #1264]
	ldr	q0, [x8, #1264]
	str	q0, [sp, #16]
	ldr	q1, [sp, #16]
	ldr	q0, [x8, #784]
	str	q0, [x8, #1376]
	ldr	q0, [x8, #1376]
                                        ; kill: def $d0 killed $d0 killed $q0
	str	d0, [x8, #1368]
	ldr	d0, [x8, #1368]
	str	d0, [x8, #1256]
	ldr	d0, [x8, #1256]
	fcvtl	v0.4s, v0.4h
	str	q0, [x8, #1232]
	ldr	q0, [x8, #1232]
	str	q1, [x8, #1344]
	str	q0, [x8, #1328]
	ldr	q0, [x8, #1344]
	ldr	q1, [x8, #1328]
	fadd.4s	v0, v0, v1
	str	q0, [x8, #1312]
	ldr	q0, [x8, #1312]
	str	q0, [sp, #16]
	ldr	q0, [sp, #16]
	str	q0, [x8, #1408]
	ldr	q0, [x8, #1408]
	faddp.4s	v0, v0, v0
	fmov	x0, d0
	fmov	d0, x0
	faddp.2s	s0, v0
	str	s0, [sp, #1548]
	ldr	s0, [sp, #1548]
	str	s0, [sp, #952]
	mov	w8, #0
	str	w8, [sp, #12]
	str	w8, [sp, #300]
	b	LarmBB1_5
LarmBB1_5:                                 ; =>This Inner Loop Header: Depth=1
	ldr	w8, [sp, #300]
	ldr	w9, [sp, #956]
	and	w9, w9, #0x1f
	subs	w8, w8, w9
	b.hs	LarmBB1_8
	b	LarmBB1_6
LarmBB1_6:                                 ;   in Loop: Header=armBB1_5 Depth=1
	ldr	x9, [sp]                        ; 8-byte Folded Reload
	ldr	x8, [x9, #824]
	ldr	h0, [x8]
	fcvt	s0, h0
	ldr	x8, [x9, #816]
	ldr	h1, [x8]
	fcvt	s1, h1
	fsub	s0, s0, s1
	str	s0, [sp, #12]
	ldr	s0, [sp, #12]
	ldr	s1, [sp, #952]
	fmadd	s0, s0, s0, s1
	str	s0, [sp, #952]
	ldr	x8, [x9, #824]
	add	x8, x8, #2
	str	x8, [x9, #824]
	ldr	x8, [x9, #816]
	add	x8, x8, #2
	str	x8, [x9, #816]
	b	LarmBB1_7
LarmBB1_7:                                 ;   in Loop: Header=armBB1_5 Depth=1
	ldr	w8, [sp, #300]
	add	w8, w8, #1
	str	w8, [sp, #300]
	b	LarmBB1_5
LarmBB1_8:
	ldr	s0, [sp, #952]
	add	sp, sp, #1760
	ldp	x28, x27, [sp], #16             ; 16-byte Folded Reload
	ret
	.cfi_endproc
                                        ; -- End function
	.globl	_manhattanDist_half_4x4         ; -- Begin function manhattanDist_half_4x4
	.p2align	2
_manhattanDist_half_4x4:                ; @manhattanDist_half_4x4
	.cfi_startproc
; %armBB.0:
	stp	x28, x27, [sp, #-16]!           ; 16-byte Folded Spill
	.cfi_def_cfa_offset 16
	.cfi_offset w27, -8
	.cfi_offset w28, -16
	sub	sp, sp, #1392
	.cfi_def_cfa_offset 1408
	add	x9, sp, #160
	str	x9, [sp, #16]                   ; 8-byte Folded Spill
	str	x0, [x9, #504]
	str	x1, [x9, #496]
	str	w2, [sp, #652]
	mov	w8, #0
	str	w8, [sp, #648]
	movi	d0, #0000000000000000
	str	h0, [sp, #606]
	add	x10, sp, #606
	ld1r.8h	{{ v1 }}, [x10]
	str	q1, [x9, #416]
	ldr	q1, [x9, #416]
	str	q1, [x9, #448]
	ldr	q1, [x9, #448]
	str	q1, [x9, #400]
	ldr	q1, [x9, #400]
	str	q1, [x9, #464]
	str	h0, [sp, #526]
	add	x10, sp, #526
	ld1r.8h	{{ v1 }}, [x10]
	str	q1, [x9, #336]
	ldr	q1, [x9, #336]
	str	q1, [x9, #368]
	ldr	q1, [x9, #368]
	str	q1, [x9, #320]
	ldr	q1, [x9, #320]
	str	q1, [x9, #384]
	str	h0, [sp, #446]
	add	x10, sp, #446
	ld1r.8h	{{ v1 }}, [x10]
	str	q1, [x9, #256]
	ldr	q1, [x9, #256]
	str	q1, [x9, #288]
	ldr	q1, [x9, #288]
	str	q1, [x9, #240]
	ldr	q1, [x9, #240]
	str	q1, [x9, #304]
	str	h0, [sp, #366]
	add	x10, sp, #366
	ld1r.8h	{{ v0 }}, [x10]
	str	q0, [x9, #176]
	ldr	q0, [x9, #176]
	str	q0, [x9, #208]
	ldr	q0, [x9, #208]
	str	q0, [x9, #160]
	ldr	q0, [x9, #160]
	str	q0, [x9, #224]
	str	w8, [sp, #316]
	str	w8, [sp, #316]
	b	LarmBB2_1
LarmBB2_1:                                 ; =>This Inner Loop Header: Depth=1
	ldr	w8, [sp, #316]
	ldr	w9, [sp, #652]
	and	w9, w9, #0xffffffe0
	subs	w8, w8, w9
	b.hs	LarmBB2_4
	b	LarmBB2_2
LarmBB2_2:                                 ;   in Loop: Header=armBB2_1 Depth=1
	ldr	x9, [sp, #16]                   ; 8-byte Folded Reload
	ldr	q1, [x9, #464]
	ldr	x8, [x9, #504]
	ldr	q0, [x8]
	str	q0, [x9, #128]
	ldr	q0, [x9, #128]
	str	q0, [x9, #112]
	ldr	q2, [x9, #112]
	ldr	x8, [x9, #496]
	ldr	q0, [x8]
	str	q0, [x9, #96]
	ldr	q0, [x9, #96]
	str	q0, [x9, #80]
	ldr	q0, [x9, #80]
	str	q2, [x9, #1216]
	str	q0, [x9, #1200]
	ldr	q0, [x9, #1216]
	ldr	q2, [x9, #1200]
	fabd.8h	v0, v0, v2
	str	q0, [x9, #1184]
	ldr	q0, [x9, #1184]
	str	q1, [x9, #832]
	str	q0, [x9, #816]
	ldr	q0, [x9, #832]
	ldr	q1, [x9, #816]
	fadd.8h	v0, v0, v1
	str	q0, [x9, #800]
	ldr	q0, [x9, #800]
	str	q0, [x9, #464]
	ldr	q1, [x9, #384]
	ldr	x8, [x9, #504]
	ldr	q0, [x8, #16]
	str	q0, [x9, #64]
	ldr	q0, [x9, #64]
	str	q0, [x9, #48]
	ldr	q2, [x9, #48]
	ldr	x8, [x9, #496]
	ldr	q0, [x8, #16]
	str	q0, [x9, #32]
	ldr	q0, [x9, #32]
	str	q0, [x9, #16]
	ldr	q0, [x9, #16]
	str	q2, [x9, #1168]
	str	q0, [x9, #1152]
	ldr	q0, [x9, #1168]
	ldr	q2, [x9, #1152]
	fabd.8h	v0, v0, v2
	str	q0, [x9, #1136]
	ldr	q0, [x9, #1136]
	str	q1, [x9, #784]
	str	q0, [x9, #768]
	ldr	q0, [x9, #784]
	ldr	q1, [x9, #768]
	fadd.8h	v0, v0, v1
	str	q0, [x9, #752]
	ldr	q0, [x9, #752]
	str	q0, [x9, #384]
	ldr	q1, [x9, #304]
	ldr	x8, [x9, #504]
	ldr	q0, [x8, #32]
	str	q0, [x9]
	ldr	q0, [x9]
	str	q0, [sp, #144]
	ldr	q2, [sp, #144]
	ldr	x8, [x9, #496]
	ldr	q0, [x8, #32]
	str	q0, [sp, #128]
	ldr	q0, [sp, #128]
	str	q0, [sp, #112]
	ldr	q0, [sp, #112]
	str	q2, [x9, #1120]
	str	q0, [x9, #1104]
	ldr	q0, [x9, #1120]
	ldr	q2, [x9, #1104]
	fabd.8h	v0, v0, v2
	str	q0, [x9, #1088]
	ldr	q0, [x9, #1088]
	str	q1, [x9, #736]
	str	q0, [x9, #720]
	ldr	q0, [x9, #736]
	ldr	q1, [x9, #720]
	fadd.8h	v0, v0, v1
	str	q0, [x9, #704]
	ldr	q0, [x9, #704]
	str	q0, [x9, #304]
	ldr	q1, [x9, #224]
	ldr	x8, [x9, #504]
	ldr	q0, [x8, #48]
	str	q0, [sp, #96]
	ldr	q0, [sp, #96]
	str	q0, [sp, #80]
	ldr	q2, [sp, #80]
	ldr	x8, [x9, #496]
	ldr	q0, [x8, #48]
	str	q0, [sp, #64]
	ldr	q0, [sp, #64]
	str	q0, [sp, #48]
	ldr	q0, [sp, #48]
	str	q2, [x9, #1072]
	str	q0, [x9, #1056]
	ldr	q0, [x9, #1072]
	ldr	q2, [x9, #1056]
	fabd.8h	v0, v0, v2
	str	q0, [x9, #1040]
	ldr	q0, [x9, #1040]
	str	q1, [x9, #688]
	str	q0, [x9, #672]
	ldr	q0, [x9, #688]
	ldr	q1, [x9, #672]
	fadd.8h	v0, v0, v1
	str	q0, [x9, #656]
	ldr	q0, [x9, #656]
	str	q0, [x9, #224]
	ldr	x8, [x9, #504]
	add	x8, x8, #64
	str	x8, [x9, #504]
	ldr	x8, [x9, #496]
	add	x8, x8, #64
	str	x8, [x9, #496]
	b	LarmBB2_3
LarmBB2_3:                                 ;   in Loop: Header=armBB2_1 Depth=1
	ldr	w8, [sp, #316]
	add	w8, w8, #32
	str	w8, [sp, #316]
	b	LarmBB2_1
LarmBB2_4:
	ldr	x8, [sp, #16]                   ; 8-byte Folded Reload
	ldr	q1, [x8, #464]
	ldr	q0, [x8, #384]
	str	q1, [x8, #640]
	str	q0, [x8, #624]
	ldr	q0, [x8, #640]
	ldr	q1, [x8, #624]
	fadd.8h	v0, v0, v1
	str	q0, [x8, #608]
	ldr	q0, [x8, #608]
	str	q0, [x8, #464]
	ldr	q1, [x8, #464]
	ldr	q0, [x8, #304]
	str	q1, [x8, #592]
	str	q0, [x8, #576]
	ldr	q0, [x8, #592]
	ldr	q1, [x8, #576]
	fadd.8h	v0, v0, v1
	str	q0, [x8, #560]
	ldr	q0, [x8, #560]
	str	q0, [x8, #464]
	ldr	q1, [x8, #464]
	ldr	q0, [x8, #224]
	str	q1, [x8, #544]
	str	q0, [x8, #528]
	ldr	q0, [x8, #544]
	ldr	q1, [x8, #528]
	fadd.8h	v0, v0, v1
	str	q0, [x8, #512]
	ldr	q0, [x8, #512]
	str	q0, [x8, #464]
	ldr	q0, [x8, #464]
	str	q0, [x8, #912]
	ldr	q0, [x8, #912]
	ext.16b	v0, v0, v0, #8
                                        ; kill: def $d0 killed $d0 killed $q0
	str	d0, [x8, #904]
	ldr	d0, [x8, #904]
	str	d0, [x8, #896]
	ldr	d0, [x8, #896]
	fcvtl	v0.4s, v0.4h
	str	q0, [x8, #880]
	ldr	q0, [x8, #880]
	str	q0, [sp, #32]
	ldr	q1, [sp, #32]
	ldr	q0, [x8, #464]
	str	q0, [x8, #992]
	ldr	q0, [x8, #992]
                                        ; kill: def $d0 killed $d0 killed $q0
	str	d0, [x8, #984]
	ldr	d0, [x8, #984]
	str	d0, [x8, #872]
	ldr	d0, [x8, #872]
	fcvtl	v0.4s, v0.4h
	str	q0, [x8, #848]
	ldr	q0, [x8, #848]
	str	q1, [x8, #960]
	str	q0, [x8, #944]
	ldr	q0, [x8, #960]
	ldr	q1, [x8, #944]
	fadd.4s	v0, v0, v1
	str	q0, [x8, #928]
	ldr	q0, [x8, #928]
	str	q0, [sp, #32]
	ldr	q0, [sp, #32]
	str	q0, [x8, #1024]
	ldr	q0, [x8, #1024]
	faddp.4s	v0, v0, v0
	fmov	x0, d0
	fmov	d0, x0
	faddp.2s	s0, v0
	str	s0, [sp, #1180]
	ldr	s0, [sp, #1180]
	str	s0, [sp, #648]
	mov	w8, #0
	str	w8, [sp, #28]
	str	w8, [sp, #316]
	b	LarmBB2_5
LarmBB2_5:                                 ; =>This Inner Loop Header: Depth=1
	ldr	w8, [sp, #316]
	ldr	w9, [sp, #652]
	and	w9, w9, #0x1f
	subs	w8, w8, w9
	b.hs	LarmBB2_11
	b	LarmBB2_6
LarmBB2_6:                                 ;   in Loop: Header=armBB2_5 Depth=1
	ldr	x8, [sp, #16]                   ; 8-byte Folded Reload
	ldr	x9, [x8, #504]
	ldr	h0, [x9]
	fcvt	s0, h0
	ldr	x8, [x8, #496]
	ldr	h1, [x8]
	fcvt	s1, h1
	fsub	s0, s0, s1
	str	s0, [sp, #28]
	ldr	s0, [sp, #28]
	fcmp	s0, #0.0
	b.le	LarmBB2_8
	b	LarmBB2_7
LarmBB2_7:                                 ;   in Loop: Header=armBB2_5 Depth=1
	ldr	s0, [sp, #28]
	str	s0, [sp, #12]                   ; 4-byte Folded Spill
	b	LarmBB2_9
LarmBB2_8:                                 ;   in Loop: Header=armBB2_5 Depth=1
	ldr	s0, [sp, #28]
	fneg	s0, s0
	str	s0, [sp, #12]                   ; 4-byte Folded Spill
	b	LarmBB2_9
LarmBB2_9:                                 ;   in Loop: Header=armBB2_5 Depth=1
	ldr	x9, [sp, #16]                   ; 8-byte Folded Reload
	ldr	s1, [sp, #12]                   ; 4-byte Folded Reload
	ldr	s0, [sp, #648]
	fadd	s0, s0, s1
	str	s0, [sp, #648]
	ldr	x8, [x9, #504]
	add	x8, x8, #2
	str	x8, [x9, #504]
	ldr	x8, [x9, #496]
	add	x8, x8, #2
	str	x8, [x9, #496]
	b	LarmBB2_10
LarmBB2_10:                                ;   in Loop: Header=armBB2_5 Depth=1
	ldr	w8, [sp, #316]
	add	w8, w8, #1
	str	w8, [sp, #316]
	b	LarmBB2_5
LarmBB2_11:
	ldr	s0, [sp, #648]
	add	sp, sp, #1392
	ldp	x28, x27, [sp], #16             ; 16-byte Folded Reload
	ret
	.cfi_endproc
                                        ; -- End function
.subsections_via_symbols
