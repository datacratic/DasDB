/* mmap_trie_inline_node.h                                         -*- C++ -*-
   Jeremy Barnes, 16 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the inline trie node.
*/

#ifndef __mmap__trie_inline_node_h__
#define __mmap__trie_inline_node_h__

#include "mmap_trie_node_impl.h"
#include "jml/utils/exc_assert.h"

namespace Datacratic {
namespace MMap {

/*****************************************************************************/
/* INLINE NODE                                                               */
/*****************************************************************************/

struct InlineNode {

    enum {
        UnusedBits = TriePtr::MetaBits,
        DataBits = TriePtr::DataBits - (7 + 6),
    };
    static_assert(DataBits + UnusedBits + 7 + 6 == 64, "Invalid bit count");

    union {
        uint64_t bits;
        struct {
            uint64_t keyLen:7;      //< How many bits does the key match?
            uint64_t valueBits:6;   //< Key fits in 47-n bits; value the rest
            uint64_t data:DataBits; //< Data: key(high) and value(low)
            uint64_t unused:UnusedBits; //< actually taken up by type
        };
    };

    GcLock::ThreadGcInfo * info;

    InlineNode()
        : bits(0), info(0)
    {
    }

    InlineNode(uint64_t data, GcLock::ThreadGcInfo * info)
        : bits(data), info(info)
    {
    }

    uint64_t value() const
    {
        return data & ((1ULL << valueBits) - 1);
    }

    KeyFragment key() const
    {
        return KeyFragment(data >> valueBits, keyLen);
    }

    TriePtr toPtr(TriePtr::State state = TriePtr::COPY_ON_WRITE) const
    {
        TriePtr result(InlineTerm, state, bits);
        ExcAssert(result.data == bits);
        return result;
    }


    // Encode it into a value.  The second argument is true if it can fit;
    // if it returns false the operation failed.
    static std::pair<InlineNode, bool>
    encode(KeyFragment key, uint64_t value, GcLock::ThreadGcInfo * info,
            TriePtr::State state, GCList& gc)
    {
        if (key.bits > 64)
            return std::make_pair(InlineNode(), false);

        int valueBitsNeeded = ML::highest_bit(value, -1) + 1;
        int keyBitsNeeded = ML::highest_bit(key.getKey(), -1) + 1;

        if (valueBitsNeeded + keyBitsNeeded > DataBits)
            return std::make_pair(InlineNode(), false);

        int valueBits = DataBits - keyBitsNeeded;

        InlineNode result;
        result.keyLen = key.bits;
        result.valueBits = valueBits;
        result.data = key.getKey() << valueBits | value;
        result.info = info;

        ExcAssert(result.key() == key);
        ExcAssert(result.value() == value);

        return std::make_pair(result, true);
    }

    NodeStats stats() const
    {
        NodeStats stats;
        stats.nodeCount = 1;
        stats.values = 1;
        stats.avgBits = stats.maxBits = keyLen;
        return stats;
    }
};


struct InlineNodeOps {
    
    typedef InlineNode Node;

    static InlineNode encode(const TriePtr & ptr, MemoryAllocator & area,
                             GcLock::ThreadGcInfo * info)
    {
        return InlineNode(ptr.data, info);
    }

    static void deallocate(const Node & node, MemoryAllocator & area)
    {
    }

    static size_t size(const InlineNode & node, MemoryAllocator & area)
    {
        return 1;
    }
    
    static void
    forEachEntry(const InlineNode & node, MemoryAllocator & area,
                 const NodeOps::OnEntryFn & fn, int what)
    {
        if (what & NodeOps::VALUE)
            fn(node.key(), TriePtr(), node.value());
    }

    static TriePathEntry
    matchKey(const InlineNode & node, MemoryAllocator & area, KeyFragment key)
    {
        KeyFragment keyToMatch = node.key();

        if (key.consume(keyToMatch) && key.empty()) {
            TriePathEntry result =
                TriePathEntry::terminal(keyToMatch.bits, node.value(), 0);
            return result;
        }
        return offTheEnd(node, area);
    }
        
    static TriePathEntry
    matchIndex(const Node & node, MemoryAllocator & area, size_t targetValueNumber)
    {
        if (targetValueNumber == 0)  // must be true
            return first(node, area);
        throw ML::Exception("matchIndex logic error");
    }

    static KeyFragment
    extractKey(const Node & node, MemoryAllocator & area, size_t valueIndex)
    {
        return node.key();
    }

    static TriePathEntry first(const Node & node, MemoryAllocator & area)
    {
        return TriePathEntry::terminal(node.key().bits, node.value(), 0);
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
        KeyFragment nodeKey = node.key();
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
        KeyFragment nodeKey = node.key();
        int cpLen = key.commonPrefixLen(nodeKey);

        if (cpLen == key.bits)
            return offTheEnd(node, area);

        if (cpLen == nodeKey.bits)
            return offTheEnd(node, area);

        if (key.getBits(1, cpLen) < nodeKey.getBits(1, cpLen))
            return first(node, area);

        return offTheEnd(node, area);
    }

    static KVList gatherKV(const Node & node, MemoryAllocator & area)
    {
        KVList kvs;
        kvs.push_back({ node.key(), node.value() });
        return kvs;
    }

    static bool isBranchingNode(const Node & node, MemoryAllocator & area)
    {
        return false;
    }

    static TriePath
    copyAndReplace(const InlineNode & node,
                   MemoryAllocator & area,
                   const TriePathEntry & match,
                   const TriePtr & replaceWith,
                   TriePtr::State newState,
                   GCList & gc)
    {
        throw ML::Exception("copyAndReplace on inline (terminal) node");
    }

    static TriePath
    inplaceReplace(const InlineNode & node,
                   MemoryAllocator & area,
                   const TriePathEntry & match,
                   const TriePtr & replaceWith,
                   GCList & gc)
    {
        throw ML::Exception("inplaceReplace on inline (terminal) node");
    }

    static TriePtr
    copyAndSetBranch(
            const InlineNode & node, 
            MemoryAllocator & area,
            const KeyFragment& key,
            TriePtr newBranch,
            TriePtr::State newState,
            GCList & gc)
    {
        throw ML::Exception("copyAndSetBranch on inline (terminal) node");
    }

    static TriePtr
    inplaceSetBranch(
            const InlineNode & node,
            MemoryAllocator & area,
            const KeyFragment& key,
            TriePtr newBranch,
            GCList & gc)
    {
        throw ML::Exception("inplaceSetBranch on inline (terminal) node");
    }

    /** Make a copy of the current node.  The extraValues parameter
        is a hint that the node will soon be expanded to include
        the given number of extra values in addition to the ones
        currently in the node.
    */
    static TriePath
    copyAndInsertLeaf(const InlineNode & node,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      TriePtr::State newState,
                      GCList & gc)
    {
        return makeDoubleLeafNode(
                area, node.info,
                key, value, node.key(), node.value(),
                newState, gc);
    }

    static TriePath
    inplaceInsertLeaf(const InlineNode & node,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      GCList & gc)
    {
        // Since there's no allocation we don't have to cleanup anything.
        return copyAndInsertLeaf(
                node, area, key, value, TriePtr::IN_PLACE, gc);
    }


    static TriePtr
    copyAndRemoveLeaf(
            const InlineNode& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            TriePtr::State newState,
            GCList& gc)
    {
        ExcAssertEqual(key, node.key());
        return TriePtr();
    }


    static TriePtr
    inplaceRemoveLeaf(
            const InlineNode& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            GCList& gc)
    {
        ExcAssertEqual(key, node.key());
        return TriePtr();
    }

    static TriePtr
    copyAndPrefixKeys(
            const InlineNode& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            TriePtr::State newState,
            GCList& gc)
    {
        KeyFragment newKey = prefix + node.key();
        
        // The node might no longer be inline-able after the prefixing.
        return makeLeaf(
                area, node.info, newKey, node.value(), newState, gc).root();
    }

    static TriePtr
    inplacePrefixKeys(
            const InlineNode& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            GCList& gc)
    {
        // There's nothing to cleanup so we have nothing to loose by punting the
        // work.
        return copyAndPrefixKeys(node, area, prefix, TriePtr::IN_PLACE, gc);
    }

    static TriePath
    copyAndReplaceValue(
            const InlineNode& node,
            MemoryAllocator& area,
            const TriePathEntry& entry,
            uint64_t newValue,
            TriePtr::State newState,
            GCList& gc)
    {
        KeyFragment key = node.key();

        ExcAssertEqual(entry.entryNum, 0);

        return makeLeaf(area, node.info, key, newValue, newState, gc);
    }

    static TriePath
    inplaceReplaceValue(
            const InlineNode& node,
            MemoryAllocator& area,
            const TriePathEntry& entry,
            uint64_t newValue,
            GCList& gc)
    {
        KeyFragment key = node.key();

        ExcAssertEqual(entry.entryNum, 0);

        // Since there's nothing to deallocate, calling this is fine.
        return makeLeaf(area, node.info, key, newValue, TriePtr::IN_PLACE, gc);
    }

    static TriePtr
    changeState(
            const InlineNode & node,
            MemoryAllocator & area,
            TriePtr::State newState,
            GCList & gc)
    {
        return node.toPtr(newState);
    }

    static NodeStats
    stats(const Node & node, MemoryAllocator & area)
    {
        return node.stats();
    }

    static void dump(const InlineNode & node,
                     MemoryAllocator & area,
                     int indent,
                     int maxDepth,
                     std::ostream & stream)
    {
        stream << "Inline: key: " << node.key()
               << " value: " << node.value();
    }

    static std::string print(TriePtr ptr, const MemoryAllocator * area,
                             GcLock::ThreadGcInfo * info)
    {
        InlineNode node(ptr.data, info);
        return ML::format("Inline: key: %lld:%d value: %lld",
                          (long long)node.key().bits, (int)node.keyLen,
                          (long long)node.value());
           
    }

    static size_t directMemUsage(const InlineNode & node,
                                 MemoryAllocator & area)
    {
        return 0;
    }
};


} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__trie_inline_node_h__ */

