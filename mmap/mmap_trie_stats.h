/** mmap_trie_stats.h                                 -*- C++ -*-
    Rémi Attab, 20 Dec 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Stats gathering classes to mesure the performance of the trie.

*/

#ifndef __mmap__mmap_trie_stats_h__
#define __mmap__mmap_trie_stats_h__

#include "mmap_trie_ptr.h"

#include <unordered_map>
#include <iostream>
#include <cstdint>

namespace Datacratic {
namespace MMap {

/******************************************************************************/
/* NODE STATS                                                                 */
/******************************************************************************/

struct NodeStats
{
    uint64_t nodeCount;

    uint64_t totalBytes;
    uint64_t bookeepingBytes;
    uint64_t unusedBytes;
    uint64_t externalKeyBytes;

    uint64_t branches;
    uint64_t values;

    double avgBits;
    uint64_t maxBits;

    double avgBranchingBits;
    uint64_t maxBranchingBits;

    NodeStats(size_t count = 0);

    NodeStats& operator+= (const NodeStats& other);
    NodeStats toScale(double scale) const;
    void dump(
            uint64_t totalNodes, 
            uint64_t totalTrieBytes, 
            std::ostream& stream = std::cerr) const;
};


/******************************************************************************/
/* TRIE STATS                                                                 */
/******************************************************************************/

enum StatsSampling
{
    STATS_SAMPLING_RANDOM,       // Use a random sample
    STATS_SAMPLING_FULL,         // Use the entire population.
};

struct TrieStats
{
    TrieStats();

    StatsSampling sampling;
    double scale;
    uint64_t probedKeys;
    uint64_t totalKeys;

    uint64_t maxDepth;
    double avgDepth;

    uint64_t maxKeyLen;
    double avgKeyLen;

    std::unordered_map<int, NodeStats> nodeStats;

    NodeStats totalNodeStats() const;

    TrieStats toScale() const;

    void dump(std::ostream& stream = std::cerr) const;
};


} // namespace MMap
} // Datacratic

#endif // __mmap__mmap_trie_stats_h__
