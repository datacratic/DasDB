# MMAP behaviour makefile
# Jeremy Barnes, 29 July 2010

#
# If true then dirty page tracking will be done entirely in userland as oposed
# to using hardware assisted kernel trickery.
#
ifeq ($(DASDB_SW_PAGE_TRACKING),1)

CXXFLAGS += -DDASDB_SW_PAGE_TRACKING
CFLAGS += -DDASDB_SW_PAGE_TRACKING

endif

#
# If true, NodeAllocator will allocate sentinels bytes before and after each
# nodes and the trie will check them regularly.
#
# WARNING: Breaks binary compatibility of the mmap file. Use for debugging only.
#
ifeq ($(DASDB_NODE_ALLOC_SENTINELS),1)

CXXFLAGS += -DDASDB_NODE_ALLOC_SENTINELS
CFLAGS += -DDASDB_NODE_ALLOC_SENTINELS

endif


$(eval $(call library,sigsegv,sigsegv.cc,arch))

LIBSNAPSHOT_SOURCES := \
	dirty_page_table.cc \
	snapshot.cc \
	simple_snapshot.cc \
	journal.cc

$(eval $(call library,snapshot,$(LIBSNAPSHOT_SOURCES),arch utils sigsegv))

LIBMMAP_SOURCES := \
	mmap_trie.cc \
	trie_key.cc \
	mmap_trie_node.cc \
	mmap_trie_merge.cc \
	key_fragment.cc \
	mmap_trie_ptr.cc \
	mmap_trie_path.cc \
	mmap_trie_stats.cc \
	memory_region.cc \
	page_allocator.cc \
	page_table_allocator.cc \
	memory_allocator.cc \
	node_allocator.cc \
	trie_allocator.cc \
	string_allocator.cc \
	page_table.cc \
	mmap_file.cc \
	memory_tracker.cc \
	profiler.cc \
	gc_list.cc \
	debug.cc \
	sync_stream.cc \
	mmap_const.cc

$(eval $(call library,mmap,$(LIBMMAP_SOURCES),arch utils gc snapshot))

$(eval $(call include_sub_make,mmap_js,js,mmap_js.mk))
$(eval $(call include_sub_make,mmap_testing,testing,mmap_testing.mk))
$(eval $(call include_sub_make,mmap_tools,tools,mmap_tools.mk))
