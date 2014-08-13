/* merge_test.h                                               -*- C++ -*-
   Rémi Attab, 14 June 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Testing utilities for the merge tests.
*/


#ifndef __mmap__merge_test_h__
#define __mmap__merge_test_h__


#include "mmap/mmap_trie_merge.h"


namespace Datacratic {
namespace MMap {
namespace Merge {

// Quick and dirty way to create a key fragment for testing.
KeyFragment frag(uint64_t bits, int32_t n) {return KeyFragment(bits, n); }
KeyFragment key(const KeyFragment& k0, int branch, const KeyFragment& k1)
{
    return k0 + KeyFragment(branch, 1) + k1;
}

/** A light and more flexible interface to the NodeOps.

    The idea is that we can construct any trie we want using a composition of
    the branch() and term() functions. I heavily recomend pen and paper when
    reading a trie made out of these constructs.

*/
struct TrieBuilder
{
    // Construct a trie
    TriePtr term(KVList kvs);
    TriePtr branch(const KeyFragment& prefix, TriePtr b0, TriePtr b1);
    TriePtr branch(
            const KeyFragment& prefix, uint64_t value, TriePtr b0, TriePtr b1);
    TriePtr branch(
            unsigned branchBits,
            const KeyFragment& prefix,
            const KVList& branches);
    TriePtr branch(
            unsigned branchBits,
            const KeyFragment& prefix,
            uint64_t value,
            const KVList& branches);

    // Modify a trie
    TriePtr inplace(TriePtr node);
    TriePtr replaceBranch(TriePtr node, int branch, TriePtr newBranch);
    TriePtr replaceValue(TriePtr node, const KeyFragment& key, uint64_t value);
    TriePtr insert(TriePtr node, const KeyFragment& key, uint64_t value);
    TriePtr remove(TriePtr node, const KeyFragment& key);

    // Query a trie
    bool isTerm(TriePtr node);
    bool isBranch(TriePtr node);
    void dump(TriePtr root);

    KVList gather(TriePtr root);
    KVList gatherValues(TriePtr root);

    // Avoids verbose parameter repeating when manipulating the trie.
    TrieBuilder(MemoryAllocator& area, GCList& gc, TriePtr::State state) :
        area(area), gc(gc), state(state)
    {}

    MemoryAllocator& area;
    GCList& gc;
    TriePtr::State state;
};

} // namespace Merge
} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__merge_test_h__ */
