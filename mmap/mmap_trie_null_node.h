/* mmap_trie_null_node.h                                           -*- C++ -*-
   Jeremy Barnes, 16 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the null trie node.
*/

#ifndef __mmap__trie_null_node_h__
#define __mmap__trie_null_node_h__

#include "mmap_trie_node_impl.h"

namespace Datacratic {
namespace MMap {

/*****************************************************************************/
/* NULL NODE                                                                 */
/*****************************************************************************/

struct NullNode {
    GcLock::ThreadGcInfo * info;
};

struct NullNodeOps {

    typedef NullNode Node;

    static void deallocate(const Node & node, MemoryAllocator & area)
    {
    }

    static NullNode encode(const TriePtr & ptr, MemoryAllocator & area,
                           GcLock::ThreadGcInfo * info)
    {
        NullNode result;
        result.info = info;
        return result;
    }

    static size_t size(const NullNode & node, MemoryAllocator & area)
    {
        return 0;
    }

    static void
    forEachEntry(const NullNode & node, MemoryAllocator & area,
                 const NodeOps::OnEntryFn & fn, int what)
    {
        // Nothing to iterate over...
    }

    static TriePathEntry
    matchKey(const NullNode & node, MemoryAllocator & area, KeyFragment key)
    {
        // Never contains anything...
        return offTheEnd(node, area);
    }

    static TriePathEntry
    matchIndex(const NullNode & node, MemoryAllocator & area,
               size_t targetValueNumber)
    {
        // Never contains anything...
        return offTheEnd(node, area);
    }

    static KeyFragment
    extractKey(const Node & node, MemoryAllocator & area, size_t valueIndex)
    {
        throw ML::Exception("null node has no values and so no keys");
    }
               
    static TriePathEntry first(const Node & node, MemoryAllocator & area)
    {
        return offTheEnd(node, area);
    }

    static TriePathEntry offTheEnd(const Node & node, MemoryAllocator & area)
    {
        return TriePathEntry::offTheEnd(0);
    }

    static TriePathEntry 
    lowerBound(
            const Node& node, 
            MemoryAllocator& area, 
            const KeyFragment& key)
    {
        return offTheEnd(node, area);
    }

    static TriePathEntry 
    upperBound(
            const Node& node, 
            MemoryAllocator& area, 
            const KeyFragment& key)
    {
        return offTheEnd(node, area);
    }

    static KVList gatherKV(const Node & node, MemoryAllocator & area)
    {
        return {};
    }

    static bool isBranchingNode(const Node & node, MemoryAllocator & area)
    {
        return false;
    }

    static TriePath
    copyAndReplace(const NullNode & node, MemoryAllocator & area,
                   const TriePathEntry & path,
                   const TriePtr & replaceWith,
                   TriePtr::State newState,
                   GCList & gc)
    {
        throw ML::Exception("copyAndReplace on null node");
    }

    static TriePath
    inplaceReplace(const NullNode & node, MemoryAllocator & area,
                   const TriePathEntry & path,
                   const TriePtr & replaceWith,
                   GCList & gc)
    {
        throw ML::Exception("inplaceReplace on null node");
    }

    static TriePtr
    copyAndSetBranch(
            const NullNode & node, 
            MemoryAllocator & area,
            const KeyFragment& key,
            TriePtr newBranch,
            TriePtr::State newState,
            GCList & gc)
    {
        throw ML::Exception("copyAndSetBranch on null node");
    }

    static TriePtr
    inplaceSetBranch(
            const NullNode & node,
            MemoryAllocator & area,
            const KeyFragment& key,
            TriePtr newBranch,
            GCList & gc)
    {
        throw ML::Exception("inplaceSetBranch on null node");
    }


    /** Make a copy of the current node.  The extraValues parameter
        is a hint that the node will soon be expanded to include
        the given number of extra values in addition to the ones
        currently in the node.
    */
    static TriePath
    copyAndInsertLeaf(const NullNode & node,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      TriePtr::State newState,
                      GCList & gc)
    {
        return makeLeaf(area, node.info, key, value, newState, gc);
    }

    static TriePath
    inplaceInsertLeaf(const NullNode & node,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      GCList & gc)
    {
        return makeLeaf(area, node.info, key, value, TriePtr::IN_PLACE, gc);
    }

    static TriePtr 
    copyAndRemoveLeaf(
            const Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            TriePtr::State newState,
            GCList& gc)
    {
        throw ML::Exception("copyAndRemoveLeaf on null node");
    }

    static TriePtr 
    inplaceRemoveLeaf(
            const Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            GCList& gc)
    {
        throw ML::Exception("inplaceRemoveLeaf on null node");
    }

    static TriePtr
    copyAndPrefixKeys(
            const Node& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            TriePtr::State newState,
            GCList& gc)
    {
        throw ML::Exception("copyAndPrefixKeys on null node");
    }

    static TriePtr
    inplacePrefixKeys(
            const Node& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            GCList& gc)
    {
        throw ML::Exception("inplacePrefixKeys on null node");
    }

    static TriePath
    copyAndReplaceValue(
            Node& node,
            MemoryAllocator& area,
            const TriePathEntry& entry,
            uint64_t newValue,
            TriePtr::State newState,
            GCList& gc)
    {
        throw ML::Exception("copyAndReplaceValue on null node");
    }

    static TriePath
    inplaceReplaceValue(
            Node& node,
            MemoryAllocator& area,
            const TriePathEntry& entry,
            uint64_t newValue,
            GCList& gc)
    {
        throw ML::Exception("inplaceReplaceValue on null node");
    }


    static TriePtr
    changeState(
            Node & node,
            MemoryAllocator & area,
            TriePtr::State newState,
            GCList & gc)
    {
        throw ML::Exception("changeState on null node");
    }

    static NodeStats
    stats(const Node & node, MemoryAllocator & area)
    {
        return NodeStats(0);
    }

    static void dump(const NullNode & node,
                     MemoryAllocator & area,
                     int indent,
                     int maxDepth,
                     std::ostream & stream)
    {
        stream << "Null";
    }

    static std::string print(TriePtr ptr, const MemoryAllocator * area,
                             GcLock::ThreadGcInfo * info)
    {
        return "Null";
    }

    static size_t directMemUsage(const NullNode & node,
                                 MemoryAllocator & area)
    {
        return 0;
    }
};


} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__trie_null_node_h__ */

