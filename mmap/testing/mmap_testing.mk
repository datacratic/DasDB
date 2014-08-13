LIBMMAP_TEST_SOURCES := mmap_test.cc merge_test.cc
LIBMMAP_TEST_LINK := arch utils pthread boost_thread mmap test_utils

$(eval $(call library,mmap_test,$(LIBMMAP_TEST_SOURCES),$(LIBMMAP_TEST_LINK)))


# $(eval $(call test,sigsegv_test,sigsegv boost_thread,boost manual))
# $(eval $(call test,snapshot_test,snapshot mmap_test boost_thread,boost))
# $(eval $(call test,simple_snapshot_test,snapshot mmap_test boost_thread,boost))
# $(eval $(call test,journal_test,snapshot mmap mmap_test,boost))
# $(eval $(call test,dirty_page_table_test,snapshot,boost))

# $(eval $(call test,memory_region_test,mmap mmap_test,boost))
# $(eval $(call test,page_allocator_test,mmap,boost noauto))
# $(eval $(call test,page_table_allocator_test,mmap,boost))
# $(eval $(call test,full_bitmap_test,mmap,boost))

# $(eval $(call test,mmap_trie_test,mmap mmap_test,boost))
# $(eval $(call test,mmap_trie_inplace_test,mmap mmap_test,boost))
# $(eval $(call test,mmap_trie_iterator_test,mmap,boost))
# $(eval $(call test,mmap_trie_node_test,mmap mmap_test,boost))
# $(eval $(call test,dense_branching_node_test,mmap mmap_test,boost))
# $(eval $(call test,mmap_trie_basics_test,mmap,boost))
# $(eval $(call test,mmap_trie_concurrency_test,mmap mmap_test,boost timed))
# $(eval $(call test,mmap_region_test,mmap mmap_test,boost manual))
# $(eval $(call test,mmap_typed_trie_test,mmap mmap_test,boost manual))

# $(eval $(call test,node_page_test,mmap test_utils,boost timed))
# $(eval $(call test,node_allocator_test,mmap types,boost))
# $(eval $(call test,string_allocator_test,mmap mmap_test,boost))
# $(eval $(call test,trie_allocator_test,mmap mmap_test,boost))

# $(eval $(call test,merge_utils_test,mmap mmap_test,boost))
# $(eval $(call test,merge_algo_test,mmap mmap_test,boost))
# $(eval $(call test,mmap_trie_merge_test,mmap mmap_test,boost manual))

# $(eval $(call test,sys_mmap_test,mmap mmap_test,boost))

# $(eval $(call test,mmap_compression_test,mmap mmap_test,boost manual))

# Disabled until mmap_map_js.cc can be fixed.
# $(eval $(call vowscoffee_test,mmap_js_test,mmap_js))

# Deprecated by tools/mmap_perf.cc
# $(eval $(call program,mmap_perf_test,mmap mmap_test boost_program_options))

