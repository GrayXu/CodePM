#
# Copyright 2018-2019, University of California, San Diego
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

#
# examples/libpmemobj/record_store/Makefile
#

PROGS = writer reader

LIBS = -lpmem -lpmemobj

include ../../Makefile.inc

writer: writer.o
reader: reader.o

POOLFILE = /mnt/pmem/pmrecord
SETENV = PMEM_IS_PMEM_FORCE=1

ifeq ($(GDB), 1)
SETENV += gdb --args
endif

# Run 'make write-pangolin GDB=1' and use GDB to emulate a crash.
#
# One way to observe the effect of redo/undo logging is to check the content
# after a crashed write command, and then see how the read command fixes any
# partial updates. For example, using pmemobj mode:
#
# make create
# make write-pmemobj GDB=1
# > Quit GDB in the middle of execution to emulate a crash. For example, after
# > the name is updated but before the addr field is updated.
#
# hexdump -C /mnt/pmem/pmrecord
# > The record is partially updated, which is not a good state.
#
# make read
# > Libpmemobj should repair the record with undo logging.
#
# hexdump -C /mnt/pmem/pmrecord
# > The record should look good.

create:
	@rm -f $(POOLFILE)
	@$(SETENV) ./writer $(POOLFILE) create

write-pmemobj:
	@$(SETENV) ./writer $(POOLFILE) pmemobj

write-pangolin:
	@$(SETENV) ./writer $(POOLFILE) pangolin

read:
	@$(SETENV) ./reader $(POOLFILE)
