/* page_table_allocator_itl.h                                      -*- C++ -*-
   Jeremy Barnes, 30 September 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

*/

#include "page_table.h"


namespace Datacratic {
namespace MMap {

// Lock that's safe to memory map because it can be preempted
struct SafeMmapLock {
    uint32_t type;        ///< Type of lock, for expansion
    uint32_t unused1[3];  ///< Pad it out to 16 bytes

    // These bits here are used for 
    uint32_t locked;      ///< Bit that gives the lock
    uint32_t locking_pid; ///< PID of locking process
    uint64_t timestamp;   ///< Timestamp at which locked
};



struct PageTableAllocator::Metadata : public PageTable {
    uint64_t magic;
    uint32_t allocatorType;
    uint32_t allocatorSize;
    uint32_t version;
    uint32_t unused1;

    bool valid() const
    {
        return magic == 0x9d49f027a0293fc7ULL;
    }

    void init()
    {
        memset(this, 0, page_size);
        magic = 0x9d49f027a0293fc7ULL;
    }

    /** Have we allocated the order 5 page yet? */
    uint32_t allocatedOrder5;

    // 1.  Page table group for L4 pages (1PB).  Presumably only one of
    //     these will actually exist.  As a result, since it always exist
    //     and it always contains the entire address space, we don't need
    //     to store anything.


    //SafeMmapLock lock;   ///< Used for things that need mutual exclusion

};

} // namespace MMap
} // namespace Datacratic

