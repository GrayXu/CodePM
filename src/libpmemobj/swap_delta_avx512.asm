;; gen delta data, and write back new data in DRAM to PM (revised from `xor_gen_avx512`, and only for 8 B aligned)
;;; int swap_delta_avx512(int vects, int len, void **array)

;;; Optimized xor of N source vectors using AVX512
;;; Generates xor parity vector from N (vects-1) sources in array of pointers
;;; (**array).  Last pointer is the dest.
;;; Vectors must be aligned to 32 bytes.  Length can be any value.

%include "reg_sizes.asm"

;%ifdef HAVE_AS_KNOWS_AVX512

%ifidn __OUTPUT_FORMAT__, elf64
 %define arg0  rdi
 %define arg1  rsi
 %define arg2  rdx
 %define arg3  rcx
 %define arg4  r8
 %define arg5  r9
 %define tmp   r11
 %define tmp3  arg4
 %define func(x) x:
 %define return rax
 %define FUNC_SAVE
 %define FUNC_RESTORE

%endif	;output formats

%define vec arg0
%define len arg1
;;; array is arg2
%define ptr arg3
%define tmp2 rax
%define tmp2.b al
%define pos tmp3
%define PS 8

;;; %define NO_NT_LDST
;;; ;;; Use Non-temporal load/stor
;;; %ifdef NO_NT_LDST
;;;  %define XLDR vmovdqu8
;;;  %define XSTR vmovdqu8
;;; %else
;;;  %define XLDR vmovntdqa
;;;  %define XSTR vmovntdq
;;; %endif

%define XLDR vmovdqu8
%define XSTR vmovdqu8

default rel
[bits 64]

section .text

align 16
global swap_delta_avx512:ISAL_SYM_TYPE_FUNCTION
func(swap_delta_avx512)
	FUNC_SAVE
	sub	vec, 2			;Keep as offset to last source (vec>=3)
	jng	return_fail		;Must have at least 2 sources
	cmp	len, 0
	je	return_pass
	test	len, (64-1)		;Check alignment of length
	jnz	len_not_aligned

len_aligned_64bytes:
	sub	len, 64
	mov	pos, 0

loop64:
	mov	tmp, vec		;Back to last vector
	mov	tmp2, [arg2+vec*PS]	;Fetch last pointer in array
	sub	tmp, 1			;Next vect
	XLDR	zmm0, [tmp2+pos]	;Start with end of array in last vector (src)

;;;next_vect:
	mov 	ptr, [arg2+tmp*PS]
	sub	tmp, 1
	XLDR	zmm4, [ptr+pos]		;Get next vector (dst)
	vpxorq	zmm4, zmm0, zmm4	;Add to xor parity
	;;;jge	next_vect		;Loop for each source  (WON'T JUMP!)

	;;; write zmm0 to PM as new data
	;;;mov	tmp2, [arg2+vec*PS]
	vmovntdq	[ptr+pos], zmm0   ;nt-store
	;;;XSTR	[ptr+pos], zmm0
	;;;clwb	[ptr+pos]

	mov	ptr, [arg2+PS+vec*PS]	;Address of parity vector (the last vec, delta)
	XSTR	[ptr+pos], zmm4		;Write delta data to DRAM

	add	pos, 64
	cmp	pos, len
	jle	loop64

return_pass:
	FUNC_RESTORE
	mov	return, 0
	ret

	;; Unaligned length cases (assume that at least 8B aligned) 
len_not_aligned:
	mov	tmp3, len
	and	tmp3, (64-1)		;Do the unaligned bytes 8 at a time

	;; Run backwards 8 bytes at a time for (tmp3) bytes
loop8_bytes:
	mov	tmp, vec		;Back to last vector
	mov 	ptr, [arg2+vec*PS] 	;Fetch last pointer in array
	mov	tmp2, [ptr+len-PS]	;Get array n
	sub	tmp, 1
nextvect_8bytes:
	mov 	ptr, [arg2+tmp*PS] 	;Get pointer to next vector
	xor	tmp2, [ptr+len-PS]
	sub	tmp, 1
	jge	nextvect_8bytes	;Loop for each source

	mov	tmp, vec
	add	tmp, 1		  	;Add back to point to last vec
	mov	ptr, [arg2+tmp*PS]
	mov	[ptr+len-PS], tmp2	;Write parity
	sub	len, PS
	sub	tmp3, PS
	jg	loop8_bytes

	cmp	len, 64		;Now len is aligned to 64B
	jge	len_aligned_64bytes	;We can do the rest aligned

	cmp	len, 0
	je	return_pass

return_fail:
	FUNC_RESTORE
	mov	return, 1
	ret

endproc_frame

;%endif  ; ifdef HAVE_AS_KNOWS_AVX512