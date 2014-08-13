/* sigsegv.h                                                       -*- C++ -*-
   Jeremy Barnes, 24 February 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   Handler for segfaults.
*/

#ifndef __mmap__sigsegv_h__
#define __mmap__sigsegv_h__

#include <vector>

namespace Datacratic {

int register_segv_region(const void * start, const void * end);
void unregister_segv_region(int region);
void install_segv_handler();
std::size_t get_num_segv_faults_handled();


} // namespace Datacratic

#endif /* __mmap__sigsegv_h__ */

   
