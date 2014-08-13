# mmap.mk
# Rémi Attab
# Copyright (c) 2012 Datacratic.  All rights reserved.
#
# MMap js library

LIBMMAP_JS_SOURCES := \
	mmap_js.cc \
	mmap_file_js.cc
# This is currently broken because of the c++ side refactoring. Needs a major overhaul.
#	mmap_map_js.cc

LIBMMAP_JS_LINK := \
	js sigslot mmap


$(eval $(call nodejs_addon,mmap_js,$(LIBMMAP_JS_SOURCES),$(LIBMMAP_JS_LINK)))
