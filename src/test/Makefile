#
# Copyright 2014-2018, Intel Corporation
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
# src/test/Makefile -- build all unit tests
#
# Makefile -- build all unit tests
#

include ../common.inc

TEST_DEPS = \
	unittest\
	tools

BLK_TESTS = \
	blk_include\
	blk_nblock\
	blk_non_zero\
	blk_pool\
	blk_pool_lock\
	blk_recovery\
	blk_rw\
	blk_rw_mt

LOG_TESTS = \
	log_basic\
	log_include\
	log_pool\
	log_pool_lock\
	log_recovery\
	log_walker

OBJ_DEPS = \
	obj_list

# long tests first
OBJ_TESTS = \
	obj_basic_integration\
	obj_many_size_allocs\
	obj_realloc\
	obj_sync\
	\
	obj_action\
	obj_bucket\
	obj_check\
	obj_constructor\
	obj_ctl_alignment\
	obj_ctl_alloc_class\
	obj_ctl_alloc_class_config\
	obj_ctl_config\
	obj_ctl_debug\
	obj_ctl_heap_size\
	obj_ctl_stats\
	obj_cuckoo\
	obj_debug\
	obj_direct\
	obj_direct_volatile\
	obj_extend\
	obj_first_next\
	obj_fragmentation\
	obj_fragmentation2\
	obj_heap\
	obj_heap_interrupt\
	obj_heap_state\
	obj_include\
	obj_lane\
	obj_layout\
	obj_list_insert\
	obj_list_move\
	obj_list_recovery\
	obj_list_remove\
	obj_list_valgrind\
	obj_list_macro\
	obj_locks\
	obj_mem\
	obj_memblock\
	obj_memcheck\
	obj_memcheck_register\
	obj_memops\
	obj_oid_thread\
	obj_out_of_memory\
	obj_persist_count\
	obj_pmalloc_basic\
	obj_pmalloc_mt\
	obj_pmalloc_oom_mt\
	obj_pmalloc_rand_mt\
	obj_pmemcheck\
	obj_pool\
	obj_pool_lock\
	obj_pool_lookup\
	obj_ravl\
	obj_recovery\
	obj_recreate\
	obj_root\
	obj_reorder_basic\
	obj_strdup\
	obj_sds\
	obj_toid\
	obj_tx_alloc\
	obj_tx_add_range\
	obj_tx_add_range_direct\
	obj_tx_callbacks\
	obj_tx_flow\
	obj_tx_free\
	obj_tx_invalid\
	obj_tx_lock\
	obj_tx_locks\
	obj_tx_locks_abort\
	obj_tx_mt\
	obj_tx_realloc\
	obj_tx_strdup\
	obj_zones

OBJ_REMOTE_DEPS = \
	obj_basic_integration\
	obj_heap_interrupt\
	obj_heap_state

OBJ_REMOTE_TESTS = \
	obj_rpmem_basic_integration\
	obj_rpmem_heap_interrupt\
	obj_rpmem_heap_state

OTHER_TESTS = \
	arch_flags\
	bttdevice\
	checksum\
	compat_incompat_features\
	ctl_prefault\
	magic\
	out_err\
	out_err_mt\
	pmemobjcli\
	pmemspoil\
	scope\
	set_funcs\
	traces\
	traces_custom_function\
	traces_pmem\
	unicode_api\
	unicode_match_script\
	util_badblock\
	util_ctl\
	util_extent\
	util_file_create\
	util_file_open\
	util_is_absolute\
	util_is_poolset\
	util_is_zeroed\
	util_map_proc\
	util_parse_size\
	util_pool_hdr\
	util_poolset\
	util_poolset_foreach\
	util_poolset_parse\
	util_poolset_size\
	util_sds\
	util_uuid_generate\
	util_vec\
	util_vecq

ifeq ($(ARCH), x86_64)
OTHER_TESTS += \
	util_cpuid
endif

PMEM_TESTS = \
	pmem_include\
	pmem_is_pmem\
	pmem_is_pmem_posix\
	pmem_map_file\
	pmem_has_auto_flush\
	pmem_deep_persist\
	pmem_memcpy\
	pmem_memmove\
	pmem_memset\
	pmem_movnt\
	pmem_movnt_align\
	pmem_valgr_simple\
	pmem_unmap

PMEMPOOL_TESTS = \
	pmempool_check\
	pmempool_create\
	pmempool_dump\
	pmempool_feature\
	pmempool_help\
	pmempool_info\
	pmempool_rm\
	pmempool_sync\
	pmempool_transform

RPMEM_TESTS = \
	rpmem_addr\
	rpmem_addr_ext\
	rpmem_basic\
	rpmem_fip\
	rpmem_obc\
	rpmem_obc_int\
	rpmem_proto\
	rpmemd_config\
	rpmemd_db\
	rpmemd_dbg\
	rpmemd_log\
	rpmemd_obc\
	rpmemd_util

VMEM_TESTS = \
	vmem_aligned_alloc\
	vmem_calloc\
	vmem_check_allocations\
	vmem_check_version\
	vmem_check\
	vmem_create\
	vmem_create_error\
	vmem_create_in_region\
	vmem_custom_alloc\
	vmem_delete\
	vmem_malloc\
	vmem_malloc_usable_size\
	vmem_mix_allocations\
	vmem_multiple_pools\
	vmem_out_of_memory\
	vmem_pages_purging\
	vmem_realloc\
	vmem_realloc_inplace\
	vmem_stats\
	vmem_strdup\
	vmem_valgrind\
	vmem_valgrind_region

VMMALLOC_DUMMY_FUNCS_DEPS = \
	vmmalloc_dummy_funcs

VMMALLOC_DUMMY_FUNCS_TESTS = \
	vmmalloc_malloc_hooks\
	vmmalloc_memalign\
	vmmalloc_valloc

VMMALLOC_TESTS = \
	vmmalloc_calloc\
	vmmalloc_check_allocations\
	vmmalloc_fork\
	vmmalloc_init\
	vmmalloc_malloc\
	vmmalloc_malloc_usable_size\
	vmmalloc_out_of_memory\
	vmmalloc_realloc\
	vmmalloc_valgrind

EXAMPLES_TESTS = \
	ex_libpmem\
	ex_libpmemblk\
	ex_libpmemlog\
	ex_linkedlist

LIBPMEMPOOL_DEPS = \
	libpmempool_api

LIBPMEMPOOL_TESTS = \
	libpmempool_include\
	libpmempool_backup\
	libpmempool_bttdev\
	libpmempool_feature\
	libpmempool_map_flog\
	libpmempool_rm

LIBPMEMPOOL_MOD_DEPS = \
	libpmempool_sync

LIBPMEMPOOL_MOD_TESTS = \
	libpmempool_transform

DAXIO_TESTS = \
	daxio

PMREORDER_TESTS = \
	pmreorder_simple\
	pmreorder_stack

LOCAL_TESTS = \
	$(OBJ_TESTS)\
	$(BLK_TESTS)\
	$(LOG_TESTS)\
	$(OTHER_TESTS)\
	$(PMEM_TESTS)\
	$(PMEMPOOL_TESTS)\
	$(VMEM_TESTS)\
	$(VMMALLOC_DUMMY_FUNCS_TESTS)\
	$(VMMALLOC_TESTS)\
	$(EXAMPLES_TESTS)\
	$(LIBPMEMPOOL_TESTS)\
	$(LIBPMEMPOOL_MOD_TESTS)\
	$(DAXIO_TESTS)\
	$(PMREORDER_TESTS)

REMOTE_TESTS = \
	$(OBJ_REMOTE_TESTS)\
	$(RPMEM_TESTS)\
	libpmempool_rm_remote\
	remote_basic\
	remote_obj_basic\
	pmempool_info_remote\
	pmempool_feature_remote\
	pmempool_rm_remote\
	pmempool_sync_remote\
	pmempool_transform_remote

#
# ex_libpmemobj contains modified examples that only work with a pangolin build.
#
PANGOLIN_SKIPPED_TESTS = ex_libpmemobj

TESTS = \
	$(LOCAL_TESTS)\
	$(REMOTE_TESTS)

TESTS_BUILD = \
	$(TEST_DEPS)\
	$(OBJ_DEPS)\
	$(VMMALLOC_DUMMY_FUNCS_DEPS)\
	$(LIBPMEMPOOL_DEPS)\
	$(LIBPMEMPOOL_MOD_DEPS)\
	$(TESTS)

all     : TARGET = all
clean   : TARGET = clean
clobber : TARGET = clobber
test    : TARGET = test
cstyle  : TARGET = cstyle
format  : TARGET = format
check   : TARGET = check
pcheck  : TARGET = pcheck
sparse  : TARGET = sparse

DIR_SYNC=$(TOP)/src/test/.sync-dir
SYNC_EXT=synced

all test format sparse: $(TESTS_BUILD)

cstyle: $(TESTS_BUILD)
	$(CHECK_SHEBANG) $(foreach dir,$(TESTS_BUILD),$(dir)/TEST? $(dir)/TEST??)

clean clobber: $(TESTS_BUILD)
	$(RM) -r $(DIR_SYNC)
	$(RM) *.$(SYNC_EXT)

$(TESTS) $(OBJ_DEPS) $(LIBPMEMPOOL_DEPS) $(LIBPMEMPOOL_MOD_DEPS) $(VMMALLOC_DUMMY_FUNCS_DEPS): $(TEST_DEPS)

$(OBJ_TESTS): $(OBJ_DEPS)
$(OBJ_REMOTE_TESTS): $(OBJ_REMOTE_DEPS)
$(LIBPMEMPOOL_TESTS): $(LIBPMEMPOOL_DEPS)
$(LIBPMEMPOOL_MOD_TESTS): $(LIBPMEMPOOL_MOD_DEPS)
$(VMMALLOC_DUMMY_FUNCS_TESTS): $(VMMALLOC_DUMMY_FUNCS_DEPS)

$(TESTS_BUILD):
	$(MAKE) -C $@ $(TARGET)

memcheck-summary:
	grep ERROR */memcheck*.log

memcheck-summary-errors:
	grep ERROR */memcheck*.log | grep -v " 0 errors" || true

memcheck-summary-leaks:
	grep "in use at exit" */memcheck*.log | grep -v " 0 bytes in 0 blocks" || true

check: sync-remotes
	@./RUNTESTS $(RUNTEST_OPTIONS) $(TESTS)
	@echo "No failures."

check-remote-quiet: sync-remotes
	@MAKEFLAGS="$(MAKEFLAGS)" ./RUNTESTS $(RUNTEST_OPTIONS) $(REMOTE_TESTS)

sync-remotes: test

check-remote: check-remote-quiet
	@echo "No failures."

# XXX remote tests do not work in parallel mode
pcheck: pcheck-local-quiet check-remote-quiet
	@echo "No failures."

pcheck-blk: TARGET = pcheck
pcheck-blk: $(BLK_TESTS)
	@echo "No failures."

pcheck-log: TARGET = pcheck
pcheck-log: $(LOG_TESTS)
	@echo "No failures."

pcheck-obj: TARGET = pcheck
pcheck-obj: $(OBJ_TESTS)
	@echo "No failures."

pcheck-other: TARGET = pcheck
pcheck-other: $(OTHER_TESTS)
	@echo "No failures."

pcheck-pmem: TARGET = pcheck
pcheck-pmem: $(PMEM_TESTS)
	@echo "No failures."

pcheck-rpmem: TARGET = pcheck
pcheck-rpmem: $(RPMEM_TESTS)
	@echo "No failures."

pcheck-pmempool: TARGET = pcheck
pcheck-pmempool: $(PMEMPOOL_TESTS)
	@echo "No failures."

pcheck-libpmempool: TARGET = pcheck
pcheck-libpmempool: $(LIBPMEMPOOL_TESTS)
	@echo "No failures."

pcheck-vmem: TARGET = pcheck
pcheck-vmem: $(VMEM_TESTS)
	@echo "No failures."

pcheck-vmmalloc: TARGET = pcheck
pcheck-vmmalloc: $(VMMALLOC_TESTS)
	@echo "No failures."

pcheck-local-quiet: TARGET = pcheck
pcheck-local-quiet: $(LOCAL_TESTS)

pcheck-local: pcheck-local-quiet
	@echo "No failures."

pcheck-remote: TARGET = pcheck
pcheck-remote: $(REMOTE_TESTS)
	@echo "No failures."

TESTCONFIG=$(TOP)/src/test/testconfig.sh

$(TESTCONFIG):

SUPP_SYNC_FILES=$(shell echo *.supp | sed s/supp/$(SYNC_EXT)/g)

%.$(SYNC_EXT): %.supp $(TESTCONFIG)
	cp $(shell echo $^ | cut -d" " -f1) $(DIR_SYNC)
	@touch $@

# sync remote nodes
sync-remotes:
ifeq ($(FORCE_SYNC_REMOTES),y)
	@touch $(TESTCONFIG)
endif
ifneq ($(SKIP_SYNC_REMOTES),y)
	@rm -rf $(DIR_SYNC) && mkdir -p $(DIR_SYNC)
	$(MAKE) $(SUPP_SYNC_FILES)
	$(MAKE) -C tools $@
	$(MAKE) -C ../tools $@
	$(MAKE) -C sync-remotes $@
	@for test in $(REMOTE_TESTS); do \
		echo "$(MAKE) -C $$test sync-test" ; \
		$(MAKE) -C $$test sync-test || exit 1 ; \
	done
	@rm -rf $(DIR_SYNC)
endif

.PHONY: all check clean clobber cstyle pcheck pcheck-blk pcheck-log pcheck-obj\
	 pcheck-other pcheck-pmem pcheck-pmempool pcheck-vmem pcheck-vmmalloc\
	 test unittest tools check-remote format pcheck-libpmempool\
	 pcheck-rpmem pcheck-local pcheck-remote sync-remotes $(TESTS_BUILD)
