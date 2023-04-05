;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;  Copyright(c) luzh@eng.ucsd.edu
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; uint32_t
; adler32_patch_avx2(uint32_t csum, void *data, uint64_t dlen, uint64_t off,
;                    void *range, uint64_t rlen)

; data and range should be 16-aligned

%define BUF_LIMIT 8000 ; see patch_buflen() in adler32.c
%define MOD_VALUE 0xFFF1 ; 65521, largest prime less than UINT16_MAX

%define PBYTES    8 ; processed bytes per iteration

%include "reg_sizes.asm"

default rel
[bits 64]

%ifidn __OUTPUT_FORMAT__, elf64
 ; arguments
 %define arg1   rdi
 %define arg2   rsi
 %define arg3   rdx
 %define arg4   rcx
 %define arg5   r8
 %define arg6   r9
 ; interpreted arguments
 %define csum   arg1 ; note the checksum only uses lower 32 bits
 %define data   arg2
 %define dlen   csum ; use the same register as csum (after it retires)
 %define off    arg4
 %define range  arg5
 %define rlen   arg6
 ; intermediate variables
 %define lo     r10
 %define hi     r11
 %define buflen r12
 %define endpr  r13
 %define retval eax  ; return value: new checksum

 %define func(x) x:
 %macro FUNC_SAVE 0
	push	r12
	push	r13
 %endmacro
 %macro FUNC_RESTORE 0
	pop	r13
	pop	r12
 %endmacro

 %macro MODULO 1
 ; compute %1 mod MOD_VALUE - clobbers eax, edx, ecx, and %1
 ; %1 is 64-bit unsigned and its lower 32-bit will store the remainder.
 ; The higher 32 bits of %1 will be zeroed.
	mov	eax, DWORD(%1)
	shr	%1, 32 ; logical right shift, also clearing higher 32 bits
	mov	edx, DWORD(%1)
	mov	ecx, MOD_VALUE
	div	ecx ; divide edx:eax by ecx, quot->eax, rem->edx
	mov	DWORD(%1), edx
 %endmacro

 %macro VPHADDQ 2
 ; compute horizontal qword adds of %2 and store the sum in %1 - clobbers rdx
	xor	%1, %1
	vpermq	%2, %2, 0xE4
	movq	rdx, %2x
	add	%1, rdx
	vpermq	%2, %2, 0xE5
	movq	rdx, %2x
	add	%1, rdx
	vpermq	%2, %2, 0xEA
	movq	rdx, %2x
	add	%1, rdx
	vpermq	%2, %2, 0xEF
	movq	rdx, %2x
	add	%1, rdx
 %endmacro
%endif

; sse registers
%define ydata0	ymm0
%define yrange0	ymm1
%define ydata1	ymm2
%define yrange1	ymm3
%define xdlen0	xmm4

%define ydlen0	ymm4
%define ydlen1	ymm5

%define xlo	xmm6
%define ylo	ymm6

%define xhi	xmm7
%define yhi	ymm7

%define yload0	ymm8
%define yload1	ymm8

global adler32_patch_avx2:function
func(adler32_patch_avx2)
	FUNC_SAVE

	xor	lo, lo
	mov	WORD(lo), WORD(csum)
	shr	csum, 16
	xor	hi, hi
	mov	WORD(hi), WORD(csum)

	test	rlen, rlen
	jz	.result

	add	data, off
	mov	dlen, arg3 ; take over csum's register
	sub	dlen, off ; off's register, rcx, is free from now

	MODULO	dlen
	mov	ecx, MOD_VALUE
	test	dlen, dlen
	cmovz	DWORD(dlen), ecx

	movq	xdlen0, dlen ; xmm is the lower half of ymm
	vpermq	ydlen0, ydlen0, 0
	vmovdqu	ydlen1, ydlen0

	vpsubq     ydlen0, [YDEC0]
	vpmaskmovq yload0, ydlen0, [YMOD]
	vpaddq     ydlen0, yload0

	vpsubq     ydlen1, [YDEC1]
	vpmaskmovq yload1, ydlen1, [YMOD]
	vpaddq     ydlen1, yload1

	xor	rcx, rcx
	xor	rdx, rdx
	mov	ecx, MOD_VALUE
	sub	dlen, PBYTES - 1
	cmovs	edx, ecx
	add	dlen, rdx

	vpxor	ylo, ylo
	vpxor	yhi, yhi

.avxwork:
	mov	buflen, BUF_LIMIT
	cmp	buflen, rlen
	cmova	buflen, rlen ; buflen = min(rlen, BUF_LIMIT)

	xor	rax, rax

	lea	endpr, [range + buflen - (PBYTES - 1)]
	cmp	range, endpr
	jae	.bytework

align 32
.avxloop:
	; add 2 packed quadwords
	vpmovzxbq  ydata0, [data]
	vpmovzxbq  ydata1, [data + PBYTES/2]
	vpmovzxbq  yrange0, [range]
	vpmovzxbq  yrange1, [range + PBYTES/2]

	vpaddq     ydata0, [YMOD]
	vpsubq     ydata0, yrange0
	vpaddq     ydata1, [YMOD]
	vpsubq     ydata1, yrange1

	vpaddq     ylo, ydata0
	vpaddq     ylo, ydata1

	vpmuludq   ydata0, ydlen0
	vpmuludq   ydata1, ydlen1
	vpaddq     yhi, ydata0
	vpaddq     yhi, ydata1

	vpsubq     ydlen0, [YDEC]
	vpmaskmovq yload0, ydlen0, [YMOD]
	vpaddq     ydlen0, yload0

	vpsubq     ydlen1, [YDEC]
	vpmaskmovq yload1, ydlen1, [YMOD]
	vpaddq     ydlen1, yload1

	add        rax, PBYTES ; for decrementing dlen
	add        data, PBYTES
	add        range, PBYTES
	cmp        range, endpr
	jb         .avxloop

;.reduce
	xor	rcx, rcx
	xor	rdx, rdx
	mov	ecx, MOD_VALUE
	sub	dlen, rax
	cmovs	edx, ecx
	add	dlen, rdx

	VPHADDQ rax, ylo
	add	lo, rax

	VPHADDQ rax, yhi
	add	hi, rax

	test	buflen, PBYTES - 1
	jnz	.bytework

	MODULO	lo
	MODULO	hi

	vpxor	ylo, ylo
	vpxor	yhi, yhi

	sub	rlen, buflen
	test	rlen, rlen
	jnz	.avxwork
	jmp	.result

.bytework:
	add	dlen, PBYTES - 1
	add	endpr, PBYTES - 1

.byteloop:
	movzx	rax, byte[data]
	movzx	rcx, byte[range]
	add	eax, MOD_VALUE ; the result is never greater than UINT32_MAX
	sub	rax, rcx ; diff in rax
	add	lo, rax
	imul	rax, dlen
	add	hi, rax
	inc	data
	mov	ecx, MOD_VALUE
	dec	dlen
	cmovz	DWORD(dlen), ecx
	inc	range
	; if the quotient of (edx:eax / ecx) is larger than eax capacity,
	; the div instruction raises a floating point exception.
	cmp	range, endpr
	jb	.byteloop

	MODULO	lo
	MODULO	hi

.result:
	shl	DWORD(hi), 16
	or	DWORD(hi), DWORD(lo)
	mov	retval, DWORD(hi)

.end:
	FUNC_RESTORE
	ret

endproc_frame

section .data
align 32
YDEC0:
	dq	0x00, 0x01, 0x02, 0x03
YDEC1:
	dq	0x04, 0x05, 0x06, 0x07
YDEC:
	dq	PBYTES, PBYTES, PBYTES, PBYTES
YMOD:
	dq	MOD_VALUE, MOD_VALUE, MOD_VALUE, MOD_VALUE
