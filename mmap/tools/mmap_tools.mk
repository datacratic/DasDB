#------------------------------------------------------------------------------#
# mmap_tools.mk
# Rémi Attab, 29 Jan 2013
# Copyright (c) 2013 Datacratic.  All rights reserved.
#
# DasDB tools build targets
#------------------------------------------------------------------------------#

LIBMMAP_TOOLS_SOURCES := trie_check.cc
LIBMMAP_TOOLS_LINK := mmap

$(eval $(call library,mmap_tools,$(LIBMMAP_TOOLS_SOURCES), $(LIBMMAP_TOOLS_LINK)))

$(eval $(call program,mmap_perf,boost_program_options mmap test_utils))

$(eval $(call program,tls_perf,boost_program_options mmap test_utils))

$(eval $(call program,mmap_check,boost_program_options boost_filesystem boost_system mmap mmap_tools))
$(eval $(call include_sub_make,mmap_tools_testing,testing,mmap_tools_testing.mk))
