/** debug.h                                 -*- C++ -*-
    Rémi Attab, 06 Jun 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Consolidated debug flags and counters.
*/

#pragma once

#include <cstddef>
#include <cstdint>

namespace Datacratic {
namespace MMap {

struct MemoryTracker;

/** If true, RegionPtr will throw RegionResizeException in such a way as to
    exhaustively test all the possible scenarios in which this exception can be
    thrown.

    This will help flush out any related errors at the cost of ridiculously high
    performance overhead. It should therefor never be used outside of tests.
    It's also not thread-safe so it should not be enabled in a multi-threaded
    environment.

    \todo Turns out it's pretty hard to  make use of this in practice so for the
    moment it's being ignored by RegionPtr.
 */
extern bool regionExceptionTest;


// Enables debuging code (usually outputs).
extern bool trieDebug;

// Enables expensive checks on operand returns (usually trie paths).
extern bool trieValidate;

// Enables the the memory tracker for the trie.
extern bool trieMemoryCheck;
extern MemoryTracker trieMemoryTracker;

// Enables the memory tracker for key fragments.
extern bool kfMemoryCheck;
extern MemoryTracker kfMemoryTracker;

// These mesure the success rate of root writes (MutableTrieVersion)
extern size_t setRootSuccesses;
extern size_t setRootFailures;
extern size_t setRootFastRetries;
extern size_t setRootSlowRetries;

// Mesures the utilization of the merge itself (TransactionalTrieVersion)
extern int64_t mergeIdleTime;
extern int64_t mergeActiveTime;

// Mesures the overhead of the merge lock (TransactionalTrieVersion)
extern __thread double mergeAcquireTime;


} // namespace MMap
} // namespace Datacratic
