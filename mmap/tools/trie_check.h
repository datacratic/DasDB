/** trie_check.h                                 -*- C++ -*-
    Mathieu Stefani, 16 May 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Trie checking structure
*/

#include "mmap/mmap_trie_path.h"
#include "mmap/mmap_trie.h"

namespace Datacratic {
namespace MMap {

struct CorruptionArea {
    TriePath leftValidPath;
    TriePath rightValidPath;

    TriePath leftInvalidPath;
    TriePath rightInvalidPath;
};


/**
 * Functor struct which implements the trie checking algorithm.
 * The algorithm runs through all the trie paths and
 * validates them. 
 * The algorithm keeps track of both valids and invalids path needed by the
 * repair algorithm.
 *
 * Returns true if a corruption has been detected, false otherwise.
 */
struct TrieChecker {

    TrieChecker(bool verbose); 

    bool operator()(MutableTrieVersion &trie); 

    CorruptionArea corruption() const {
        return mCorruption;
    }

private:
    CorruptionArea mCorruption;
    const bool verbose;

};

/** 
 * Functor struct which implements the "repair" algorithm.
 * When a corruption has been identified by the TrieChecker algorithm, the
 * TrieRepair struct can be used to attempt removing the corrupted nodes.
 *
 * Returns true if the corruption has been removed, false otherwise 
*/
struct TrieRepair {

    bool operator()(MutableTrieVersion &trie,
                    CorruptionArea &corruption); 

};

} // namespace MMap
} // namespace Datacratic
