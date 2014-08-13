/* memory_tracker.h                                                  -*- C++ -*-
   Rémi Attab, 8 Febuary 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Tracks allocations and deallocations in order to detect memory leaks
*/

#ifndef __mmap__memory_tracker_h__
#define __mmap__memory_tracker_h__

#include "mmap_trie_path.h"

#include <boost/noncopyable.hpp>

#include <string>
#include <map>
#include <mutex>
#include <thread>
#include <ostream>
#include <iostream>

namespace Datacratic {
namespace MMap {

/**
Used to check for memory leaks by tracking allocations and deallocations.

Note that the point of origin of the last allocation can be obtained by
setting backtrace to true during construction. This will slow things
down considerably.

Note that this class should only be used for debugging purposes. It is
completely unoptimized and in a scenario of heavy concurrency, it will spend a
ridiculous amount of time on system calls.
*/
struct MemoryTracker : public boost::noncopyable {

    // If backtrace is true then the last backtrace of all allocations
    // and deallocations will be kept. Useful to debug memory leaks.
    MemoryTracker(bool backtrace = false);

    // Keep track of an allocation done to a block of memory.
    // Throws if attempting to allocate an already allocated block.
    void trackAlloc(uint64_t offset, TriePtr node);
    void trackAlloc(uint64_t offset);

    // Keep track of a deallocation done to a block of memory.
    // Throws on double free.
    void trackDealloc(uint64_t offset);

    // Contains the information related to a memory block.
    struct Block {
        uint64_t offset;
        TriePtr node;
        uint32_t allocCounter;
        uint32_t deallocCounter;
        uint32_t readCounter;
        std::string lastBacktrace;

        void dump(std::ostream& stream = std::cerr) const;
    };

    // throws if the memory block isn't being tracked.
    Block getBlock(uint64_t offset) const;

    // throws if the block can't be read safely
    void checkRead(uint64_t offset, TriePtr node) const;
    void checkRead(uint64_t offset) const;

    // Defines a span of time where a block is being read.
    // throws if the block can't be read safely.
    void startRead(uint64_t offset);
    void endRead(uint64_t offset);

    // Checks that all the blocks being tracked have been properly deallocated.
    void checkForLeaks(std::function<void(const Block& block)> cb) const;
    void dumpLeaks(std::ostream& stream = std::cerr) const;

private:

    std::string getBacktrace() const;

    std::map<uint64_t, Block> blocks;
    mutable std::mutex blocksLock;
    
    bool backtrace;
    
};

}; // namespace MMap
}; // namespace Datacratic 


#endif //__mmap__memory_tracker_h__
