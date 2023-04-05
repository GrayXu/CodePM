;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;  Copyright(c) luzh@eng.ucsd.edu
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; uint32_t
; adler32_patch_sse(uint32_t csum, void *data, uint64_t dlen, uint64_t off,
;                   void *range, uint64_t rlen)

; data and range should be 16-aligned

%define BUF_LIMIT 16320 ; see patch_buflen() in adler32.c
%define MOD_VALUE 0xFFF1 ; 65521, largest prime less than UINT16_MAX

%define PBYTES    4 ; processed bytes per iteration

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
%endif

; sse registers
%define xdata0	xmm0
%define xrange0	xmm1
%define xdata1	xmm2
%define xrange1	xmm3
%define xdlen0	xmm4
%define xdlen1	xmm5
%define xlo	xmm6
%define xhi	xmm7

%define xload0	xmm8
%define xload1	xmm9

global adler32_patch_sse:function
func(adler32_patch_sse)
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

	pinsrq	xdlen0, dlen, 0
	pinsrq	xdlen0, dlen, 1
	movdqu	xdlen1, xdlen0

	psubq	xdlen0, [XDEC0]
	vpmaskmovq xload0, xdlen0, [XMOD] ; vpmaskmovq requires avx2
	paddq   xdlen0, xload0

	psubq	xdlen1, [XDEC1]
	vpmaskmovq xload1, xdlen1, [XMOD] ; vpmaskmovq requires avx2
	paddq   xdlen1, xload1

	xor	rcx, rcx
	xor	rdx, rdx
	mov	ecx, MOD_VALUE
	sub	dlen, PBYTES - 1
	cmovs	edx, ecx
	add	dlen, rdx

	pxor	xlo, xlo
	pxor	xhi, xhi

.ssework:
	mov	buflen, BUF_LIMIT
	cmp	buflen, rlen
	cmova	buflen, rlen ; buflen = min(rlen, BUF_LIMIT)

	xor	rax, rax

	lea	endpr, [range + buflen - (PBYTES - 1)]
	cmp	range, endpr
	jae	.bytework

align 32
.sseloop:
	; add 2 packed quadwords
	pmovzxbq xdata0, [data]
	pmovzxbq xdata1, [data + PBYTES/2]
	pmovzxbq xrange0, [range]
	pmovzxbq xrange1, [range + PBYTES/2]
	paddq	xdata0, [XMOD]
	psubq	xdata0, xrange0
	paddq	xdata1, [XMOD]
	psubq	xdata1, xrange1
	paddq	xlo, xdata0
	paddq	xlo, xdata1
	pmuludq	xdata0, xdlen0
	pmuludq	xdata1, xdlen1
	paddq	xhi, xdata0
	paddq	xhi, xdata1

	psubq   xdlen0, [XDEC]
	vpmaskmovq xload0, xdlen0, [XMOD] ; vpmaskmovq requires avx2
	paddq   xdlen0, xload0

	psubq   xdlen1, [XDEC]
	vpmaskmovq xload1, xdlen1, [XMOD]; vpmaskmovq requires avx2
	paddq   xdlen1, xload1

	add	rax, PBYTES ; for decrementing dlen
	add	data, PBYTES
	add	range, PBYTES
	cmp	range, endpr
	jb	.sseloop

;.reduce
	xor	rcx, rcx
	xor	rdx, rdx
	mov	ecx, MOD_VALUE
	sub	dlen, rax
	cmovs	edx, ecx
	add	dlen, rdx

	pextrq	rax, xlo, 0
	add	lo, rax
	pextrq	rax, xlo, 1
	add	lo, rax

	pextrq	rax, xhi, 0
	add	hi, rax
	pextrq	rax, xhi, 1
	add	hi, rax

	test	buflen, (PBYTES - 1)
	jnz	.bytework

	MODULO	lo
	MODULO	hi

	pxor	xlo, xlo
	pxor	xhi, xhi

	sub	rlen, buflen
	test	rlen, rlen
	jnz	.ssework
	jmp	.result

.bytework:
	add	dlen, (PBYTES - 1)
	add	endpr, (PBYTES - 1)

align 32
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
XDEC0:
	dq	0x00, 0x01
XDEC1:
	dq	0x02, 0x03
XDEC:
	dq	PBYTES, PBYTES
XMOD:
	dq	MOD_VALUE, MOD_VALUE
