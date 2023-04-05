#
# Copyright 2018, University of California, San Diego
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in
#       the documentation and/or other materials provided with the
#       distribution.
#
#     * Neither the name of the copyright holder nor the names of its
#       contributors may be used to endorse or promote products derived
#       from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#
# examples/libpmemobj/simpleops/Makefile -- build the simpleops examples
#

PROGS = alloc_pobjs write_pobjs read_pobjs read_pobjs_inj

LIBS = -lpmemobj -lpmem -pthread

ifeq ($(DEBUG),)
# Use -O3 to have comparable results as src/benchmarks
EXTRA_CFLAGS += -O3 -fno-omit-frame-pointer
endif

include ../../Makefile.inc

alloc_pobjs: alloc_pobjs.o
write_pobjs: write_pobjs.o
read_pobjs: read_pobjs.o
read_pobjs_inj: read_pobjs_inj.o

# --- settings --- #

PMEMDEV = /dev/pmem0
PMEMDIR = /mnt/pmem
POOLSET = ./pobjpool.set
OBJPOOL = $(PMEMDIR)/testpool

# --- pool check --- #
# Change the following settings to check a pool created by another program
# CFLAGS += -DLAYOUT_NAME=\"pmemkv\"
# OBJPOOL = $(PMEMDIR)/pmemkv
# POOLSET = $(OBJPOOL)

# --- build --- #

# the same flags work for both GCC and Clang
asan:
	make EXTRA_CFLAGS="-fsanitize=address,undefined" \
	     EXTRA_LDFLAGS="-fsanitize=address,undefined" all

# --- run examples --- #

LOGDIR = $(PWD)/logs
SETENV = PMEM_IS_PMEM_FORCE=1

ifeq ($(LOG), 1)
SETENV += PMEM_LOG_LEVEL=99 PMEMOBJ_LOG_LEVEL=99
SETENV += PMEM_LOG_FILE=$(LOGDIR)/libpmem-$@.log
SETENV += PMEMOBJ_LOG_FILE=$(LOGDIR)/libpmemobj-$@.log
endif

# With perf.data, run the following command in FlameGraph path.
# perf script | ./stackcollapse-perf.pl | ./flamegraph.pl > perf.svg

# object size
D ?= 112
# write mode
M ?= 2
# number of objects
N ?= 20
# number of objects per operation
K ?= 1

ifeq ($(GDB), 1)
SETENV += gdb --args
endif

ifeq ($(PERF), 1)
SETENV += perf record -F 13999 -a -g --
endif

debug:
	@sed -i "/^LIBDIR/c\LIBDIR = \$$(TOP_SRC)/debug" ../../Makefile.inc
	@make clobber all

nondebug:
	@sed -i "/^LIBDIR/c\LIBDIR = \$$(TOP_SRC)/nondebug" ../../Makefile.inc
	@make clobber all

alloc:
	@mkdir -p $(LOGDIR)
	@rm -f $(OBJPOOL)
	@$(SETENV) ./alloc_pobjs $(POOLSET) $N $D $M

write:
ifeq ($(LIBDIR), ../../../debug)
	@echo ".--------------------------------------------------------."
	@echo "| NOTE: The program probably links to the debug library. |"
	@echo "'--------------------------------------------------------'"
endif
	@$(SETENV) ./write_pobjs $(POOLSET) $N $K $D $M

read:
	@$(SETENV) ./read_pobjs $(POOLSET) $D

read-inj:
	@$(SETENV) ./read_pobjs_inj $(POOLSET) $D

# --- inspection --- #

ls:
	@tree -L 5 --inodes $(PMEMDIR)

asm:
	@$(foreach prog, ${PROGS}, objdump -S -d $(prog) > $(prog).s;)

dump:
ifeq ($(X),)
	hexdump -C $(OBJPOOL) | less -R
else
	hexdump -C $(OBJPOOL) > $(LOGDIR)/dump.$(X)
endif
