/* mmap_const.h                                                     -*- C++ -*-
   Rémi Attab, 28 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   File that contains a bunch of constants needed all over.
*/

#ifndef __mmap__mmap_const_h__
#define __mmap__mmap_const_h__

#include <cstdint>

#include "jml/arch/vm.h"

/** Determines whether we use the software page tracking (DirtyPageTable) with
    simple snapshoting or if we just use the complicated version that probably
    requires a kernel patch or two.

    This macro is only used within memory_region.h and memory_region.cc and can
    be set at compile time.
 */
#ifndef DASDB_SW_PAGE_TRACKING
#  define DASDB_SW_PAGE_TRACKING 0
#else
#  undef DASDB_SW_PAGE_TRACKING
#  define DASDB_SW_PAGE_TRACKING 1
#endif


namespace Datacratic {
namespace MMap {


/** The page size and cache line (in bytes) for our environment. */
enum {
    cache_shift = 6,
};

#define cache_line (1 << cache_shift)

using ML::page_size;
using ML::page_shift;

/** File permissions that can be ORed together. */
enum Permissions {
    PERM_READ = 0x1,
    PERM_WRITE = 0x2,

    PERM_READ_WRITE = PERM_READ | PERM_WRITE,
};

/** Constants that can be used to control how resources are opened.
    Note that these are structs so we can more easily overload constructors.
*/
extern struct ResCreate {} RES_CREATE; ///< Open and initialize a new resource.
extern struct ResOpen {} RES_OPEN;     ///< Open an existing resource.
extern struct ResCreateOpen {} RES_CREATE_OPEN; ///< Open or create a resource.



/** Returns the ceil of an unsigned integer division.
    \todo Shove this somewhere a little more useful
    \todo Make it work with signed ints as well for genericity.
    \todo Make this a constexpr function when gcc supports it.
*/
#define ceilDiv(n,d) ( (n) ? ((n)-1) / (d) +1 : 0 )


/** Determines the number words to allocate on the stack for key fragments
    before we move on to the heap.
 */
enum { KeyFragmentCompactSize = 4 };


/** Determines whether sentinel bytes are added to every node allocated by the
    node allocator. Note that this break binary compatibility of existing mmap
    files so it should only be used in a debugging environment.
 */
#ifndef DASDB_NODE_ALLOC_SENTINELS
#  define DASDB_NODE_ALLOC_SENTINELS false
#else
#  undef DASDB_NODE_ALLOC_SENTINELS
#  define DASDB_NODE_ALLOC_SENTINELS true
#endif

/** The code should only ever use this enum (keeps the code cleaner). This enum
    should only be set through the compile time constant because it breaks
    binary compatibility of the mmap file.

*/
enum { NodeAllocSentinels = DASDB_NODE_ALLOC_SENTINELS };

// Make sure we can't use it anymore.
#undef DASDB_NODE_ALLOC_SENTINELS

} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__mmap_const_h__ */
