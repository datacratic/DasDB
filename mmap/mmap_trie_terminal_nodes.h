/* mmap_trie_terminal_nodes.h                                      -*- C++ -*-
   Jeremy Barnes, 16 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the null trie node.
*/

#ifndef __mmap__trie_terminal_nodes_h__
#define __mmap__trie_terminal_nodes_h__

#include "mmap_trie_node_impl.h"
#include "jml/arch/format.h"

namespace Datacratic {
namespace MMap {

/*****************************************************************************/
/* BASIC TERMINAL                                                            */
/*****************************************************************************/

/** A node that has a 64 bit key to match and a 64 bit value. */

// Memory-mapped representation
struct BasicKeyedTerminalNodeRepr {
    uint64_t value;    ///< Value associated with that key

    KeyFragmentRepr key_;   ///< Key to match
    KeyFragment key(MemoryAllocator& area, GcLock::ThreadGcInfo* info) const 
    {
        return KeyFragment::loadRepr(key_, area, info); 
    }

    NodeStats stats() const
    {
        NodeStats stats;

        stats.nodeCount = 1; 
        stats.totalBytes = sizeof(BasicKeyedTerminalNodeRepr);
        stats.values = 1;
        stats.avgBits = stats.maxBits = key_.bits;
        stats += key_.nodeStats();

        return stats;
    }
};

static_assert(
        sizeof(BasicKeyedTerminalNodeRepr) <= cache_line,
        "Must fit into a cache line.");


/******************************************************************************/
/* BASIC KEYED TERMINAL OPS                                                   */
/******************************************************************************/

// Operations structure
struct BasicKeyedTerminalOps
    : public IndirectOpsBase<BasicKeyedTerminalNodeRepr, BasicKeyedTerm>
{
    
    static Node alloc(MemoryAllocator & area,
                      GcLock::ThreadGcInfo * info,
                      const KeyFragment & key,
                      const uint64_t & value,
                      TriePtr::State newState,
                      GCList& gc)
    {
        Node result = allocEmpty(area, info, newState);
        gc.addNewNode(result.offset, result);

        KeyFragmentRepr keyRepr = key.allocRepr(area, info);
        result->key_ = keyRepr;
        result->value = value;

        return result;
    }

    static void deallocate(const Node& node, MemoryAllocator& area)
    {
        KeyFragment::deallocRepr(node->key_, area, node.info);
        deallocNode(node, area);
    }

    static size_t directMemUsage(const Node & node, MemoryAllocator & area)
    {
        size_t size = sizeof(BasicKeyedTerminalNodeRepr);
        size += node->key_.directMemUsage();
        return size;
    }


    static size_t size(const Node & ptr, MemoryAllocator & area)
    {
        return 1;
    }

    static void
    forEachEntry(const Node & node, MemoryAllocator & area,
                 const NodeOps::OnEntryFn & fn, int what)
    {
        if (what & NodeOps::VALUE) {
            fn(node->key(area, node.info), TriePtr(), node->value);
        }
    }

    static TriePathEntry
    matchKey(const Node & node, MemoryAllocator & area, KeyFragment key)
    {
        KeyFragment nodeKey = node->key(area, node.info);

        if (key.consume(nodeKey) && key.empty())
            return TriePathEntry::terminal(nodeKey.bits, node->value, 0);

        return offTheEnd(node, area);
    }
    
    static TriePathEntry
    matchIndex(const Node & node, MemoryAllocator & area, size_t targetValueNumber)
    {
        if (targetValueNumber == 0)  // must be true
            return first(node, area);
        throw ML::Exception("matchIndex logic error");
        return offTheEnd(node, area);
    }

    static KeyFragment
    extractKey(const Node & node, MemoryAllocator & area, size_t valueIndex)
    {
        return node->key(area, node.info);
    }

    static TriePathEntry first(const Node & node, MemoryAllocator & area)
    {
        KeyFragment nodeKey = node->key(area, node.info);
        return TriePathEntry::terminal(nodeKey.bits, node->value, 0);
    }

    static TriePathEntry offTheEnd(const Node & node, MemoryAllocator & area)
    {
        return TriePathEntry::offTheEnd(1);
    }

    static TriePathEntry 
    lowerBound(
            const Node& node, 
            MemoryAllocator& area, 
            const KeyFragment& key)
    {
        KeyFragment nodeKey = node->key(area, node.info);
        int cpLen = key.commonPrefixLen(nodeKey);

        if (cpLen == key.bits)
            return first(node, area);

        if (cpLen == nodeKey.bits)
            return offTheEnd(node, area);

        if (key.getBits(1, cpLen) < nodeKey.getBits(1, cpLen))
            return first(node, area);

        return offTheEnd(node, area);
    }

    static TriePathEntry 
    upperBound(
            const Node& node, 
            MemoryAllocator& area, 
            const KeyFragment& key)
    {
        KeyFragment nodeKey = node->key(area, node.info);
        int cpLen = key.commonPrefixLen(nodeKey);

        if (cpLen == key.bits)
            return offTheEnd(node, area);

        if (cpLen == nodeKey.bits)
            return offTheEnd(node, area);

        if (key.getBits(1, cpLen) < nodeKey.getBits(1, cpLen))
            return TriePathEntry::terminal(nodeKey.bits, node->value, 0);

        return offTheEnd(node, area);
    }

    static KVList gatherKV(const Node & node, MemoryAllocator & area)
    {
        KVList kvs;
        kvs.push_back({ node->key(area, node.info), node->value });
        return kvs;
    }

    static bool isBranchingNode(const Node & node, MemoryAllocator & area)
    {
        return false;
    }

    static TriePath
    copyAndReplace(const Node & node,
                   MemoryAllocator & area,
                   const TriePathEntry & match,
                   const TriePtr & replaceWith,
                   TriePtr::State newState,
                   GCList & gc)
    {
        throw ML::Exception("copyAndReplace on terminal Node");
    }


    static TriePath
    inplaceReplace(const Node & node,
                   MemoryAllocator & area,
                   const TriePathEntry & match,
                   const TriePtr & replaceWith,
                   GCList & gc)
    {
        throw ML::Exception("inplaceReplace on terminal Node");
    }

    static TriePtr
    copyAndSetBranch(
            const Node & node, 
            MemoryAllocator & area,
            const KeyFragment& key,
            TriePtr newBranch,
            TriePtr::State newState,
            GCList & gc)
    {
        throw ML::Exception("copyAndSetBranch on terminal node");
    }

    static TriePtr
    inplaceSetBranch(
            const Node & node,
            MemoryAllocator & area,
            const KeyFragment& key,
            TriePtr newBranch,
            GCList & gc)
    {
        throw ML::Exception("inplaceSetBranch on terminal node");
    }

    /** Make a copy of the current node.  The extraValues parameter
        is a hint that the node will soon be expanded to include
        the given number of extra values in addition to the ones
        currently in the node.
    */
    static TriePath
    copyAndInsertLeaf(Node & node,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      TriePtr::State newState,
                      GCList & gc)
    {
        KeyFragment nkey = node->key(area, node.info);
        uint64_t nvalue = node->value;
        return makeDoubleLeafNode(
                area, node.info, key, value, nkey, nvalue, newState, gc);
    }

    static TriePath
    inplaceInsertLeaf(Node & node,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      GCList & gc)
    {
        TriePath path = copyAndInsertLeaf(
                node, area, key, value, TriePtr::IN_PLACE, gc);

        deallocNewNode(node, gc);
        return path;
    }

    static TriePtr
    copyAndRemoveLeaf(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            TriePtr::State newState,
            GCList& gc)
    {
        gc.addOldNode(node);

        ExcAssertEqual(key, node->key(area, node.info));

        return TriePtr();
    }

    static TriePtr
    inplaceRemoveLeaf(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            GCList& gc)
    {
        ExcAssertEqual(key, node->key(area, node.info));

        deallocNewNode(node, gc);

        return TriePtr();
    }

    static TriePtr
    copyAndPrefixKeys(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            TriePtr::State newState,
            GCList& gc)
    {
        gc.addOldNode(node);

        KeyFragment newKey = prefix + node->key(area, node.info);
        uint64_t newValue = node->value;
        
        return makeLeaf(area, node.info, newKey, newValue, newState, gc).root();
    }

    static TriePtr
    inplacePrefixKeys(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            GCList& gc)
    {
        KeyFragment newKey = prefix + node->key(area, node.info);
        KeyFragmentRepr newKeyRepr = newKey.allocRepr(area, node.info);

        KeyFragment::deallocRepr(node->key_, area, node.info);
        node->key_ = newKeyRepr;

        return node;
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
        KeyFragment key = node->key(area, node.info);

        ExcAssertEqual(entry.entryNum, 0);

        return makeLeaf(area, node.info, key, newValue, newState, gc);
    }

    static TriePath
    inplaceReplaceValue(
            Node& node,
            MemoryAllocator& area,
            const TriePathEntry& entry,
            uint64_t newValue,
            GCList& gc)
    {
        KeyFragment key = node->key(area, node.info);
        ExcAssertEqual(entry.entryNum, 0);

        node->value = newValue;

        return TriePath (node, TriePathEntry::terminal(
                        key.bits, newValue, 0, true /* exact */));
    }

    static TriePtr
    changeState(
            Node & node,
            MemoryAllocator & area,
            TriePtr::State newState,
            GCList & gc)
    {
        if (node.state == TriePtr::IN_PLACE) {
            node.state = newState;
            return node;
        }

        gc.addOldNode(node);
        KeyFragment key = node->key(area, node.info);
        return makeLeaf(
                area, node.info, key, node->value, newState, gc).root();
    }

    static NodeStats
    stats(const Node & node, MemoryAllocator & area)
    {
        return node->stats();
    }

    static void dump(const Node & node,
                     MemoryAllocator & area,
                     int indent,
                     int maxDepth,
                     std::ostream & stream)
    {
        stream << ((TriePtr)node) << " "
            << "key: " << node->key(area, node.info)
            << " value: " << node->value;
    }

    static std::string print(TriePtr ptr, MemoryAllocator * area,
                             GcLock::ThreadGcInfo * info)
    {
        std::string result = ML::format("Terminal: offset %lld",
                                        (long long)Node::decodeOffset(ptr.data));
        if (!area) return result;

        Node node = encode(ptr, *area, info);
        result += " key: " + node->key(*area, node.info).print();

        return result;
    }
};

} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__trie_terminal_nodes_h__ */

