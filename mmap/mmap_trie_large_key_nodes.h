/* mmap_trie_sparse_node.h                                      -*- C++ -*-
   Rémi Attab, 1 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   large key node header.
   Note that this node type will generally be used with large keys so we
   want to avoid making copies of the keys and comparing them too much.
*/

#ifndef __mmap__trie_keyed_nodes_h__
#define __mmap__trie_keyed_nodes_h__


#include "key_fragment.h"
#include "jml/compiler/compiler.h"
#include "jml/arch/format.h"

#include <boost/static_assert.hpp>
#include <algorithm>


namespace Datacratic {
namespace MMap {


/******************************************************************************/
/* LARGE KEY NODE REPR                                                        */
/******************************************************************************/

struct LargeKeyNodeRepr {
    struct Entry {

        uint64_t value;
        KeyFragmentRepr key_;

        KeyFragment key(MemoryAllocator& area, GcLock::ThreadGcInfo* info) const
        {
            return KeyFragment::loadRepr(key_, area, info);
        }

        uint32_t keyLen () const { return key_.bits; }

        /** std::swap won't work for attributes of this class because it's
            packed so use this instead.
        */
        JML_ALWAYS_INLINE void
        swap(KeyFragmentRepr& otherRepr, uint64_t& otherValue)
        {
            KeyFragmentRepr tmpRepr = key_;
            key_ = otherRepr;
            otherRepr = tmpRepr;

            uint64_t tmpValue = value;
            value = otherValue;
            otherValue = tmpValue;
        }

    } JML_PACKED;

    enum { MaxEntries = 3 };
    Entry entries[MaxEntries];
    uint32_t size;

    NodeStats stats() const
    {
        NodeStats stats;

        stats.nodeCount = 1;
        stats.values = size;
        stats.totalBytes = sizeof(LargeKeyNodeRepr);
        stats.bookeepingBytes += sizeof(size);
        stats.unusedBytes += sizeof(Entry) * (MaxEntries - size);

        for (size_t i = 0; i < size; ++i) {
            const KeyFragmentRepr& key = entries[i].key_;
            stats += key.nodeStats();
            stats.avgBits += key.bits;
            stats.maxBits = std::max<uint64_t>(stats.maxBits, key.bits);
        }

        stats.avgBits /= size;

        return stats;
    }

} JML_ALIGNED(cache_line);

static_assert(
        sizeof(LargeKeyNodeRepr) <= cache_line,
        "Must fit into a cache line.");


/******************************************************************************/
/* LARGE KEY NODE OPS                                                         */
/******************************************************************************/

struct LargeKeyNodeOps
    : public IndirectOpsBase<LargeKeyNodeRepr, LargeKeyTerm>
{
    static TriePtr
    allocMultiLeaf(
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KVList& kvs,
            TriePtr::State newState,
            GCList& gc)
    {
        if (kvs.size() > LargeKeyNodeRepr::MaxEntries)
            return TriePtr();

        // Setup the node
        Node node = allocEmpty(area, info, newState);
        gc.addNewNode(node.offset, node);

        node->size = kvs.size();

        // Finish building the kv array.
        for (int i = 0; i < kvs.size(); ++i) {
            node->entries[i].key_ = kvs[i].key.allocRepr(area, info);
            node->entries[i].value = kvs[i].value;
        }

        return node;
    }

    static void deallocate(const Node& node, MemoryAllocator& area)
    {
        for (int i = 0; i < node->size; ++i)
            KeyFragment::deallocRepr(node->entries[i].key_, area, node.info);

        deallocNode(node, area);
    }

    static size_t directMemUsage(const Node & node, MemoryAllocator & area)
    {
        size_t size = sizeof(LargeKeyNodeRepr);

        for (int i = 0; i < node->size; ++i)
            size += node->entries[i].key_.directMemUsage();

        return size;
    }

    static size_t size(const Node& node, MemoryAllocator& area)
    {
        return node->size;
    }

    static void
    forEachEntry(
            const Node& node,
            MemoryAllocator& area,
            const NodeOps::OnEntryFn& fn,
            int what)
    {
        if ((what& NodeOps::VALUE) == 0) return;

        for (int i = 0;  i < node->size;  ++i) {
            fn(
                    node->entries[i].key(area, node.info),
                    TriePtr(),
                    node->entries[i].value);
        }
    }

    static TriePathEntry
    matchKey(const Node& node, MemoryAllocator& area, KeyFragment key)
    {
        for (unsigned i = 0;  i < node->size;  ++i) {
            KeyFragment ithKey = node->entries[i].key(area, node.info);
            if (key == ithKey)
                return TriePathEntry::terminal(
                        ithKey.bits, node->entries[i].value, i);
        }

        return offTheEnd(node, area);
    }

    static TriePathEntry
    matchIndex(const Node& node, MemoryAllocator& area,
               size_t targetValueNumber)
    {
        ExcAssertLess(targetValueNumber, node->size);
        return TriePathEntry::terminal(
                node->entries[targetValueNumber].keyLen(),
                node->entries[targetValueNumber].value,
                targetValueNumber);
    }

    static KeyFragment
    extractKey(const Node& node, MemoryAllocator& area, size_t valueIndex)
    {
        ExcAssertLess(valueIndex, node->size);
        return node->entries[valueIndex].key(area, node.info);
    }

    static TriePathEntry first(const Node& node, MemoryAllocator& area)
    {
        if (node->size == 0) return offTheEnd(node, area);

        return TriePathEntry::terminal(
                node->entries[0].keyLen(), node->entries[0].value, 0);
    }

    static TriePathEntry offTheEnd(const Node& node, MemoryAllocator& area)
    {
        return TriePathEntry::offTheEnd(node->size);
    }

    static TriePathEntry 
    lowerBound(
            const Node& node, 
            MemoryAllocator& area, 
            const KeyFragment& key)
    {
        for (int i = 0; i < node->size; ++i) {
            KeyFragment entryKey = node->entries[i].key(area, node.info);
            int cpLen = key.commonPrefixLen(entryKey);

            if (cpLen == key.bits) {
                return TriePathEntry::terminal(
                        entryKey.bits, node->entries[i].value, i);
            }

            // If the element is greater then our key then we're done.
            if (cpLen != entryKey.bits && 
                key.getBits(1, cpLen) < entryKey.getBits(1, cpLen))
            {
                return TriePathEntry::terminal(
                        entryKey.bits, node->entries[i].value, i);
            }
        }

        return offTheEnd(node, area);
    }

    static TriePathEntry 
    upperBound(
            const Node& node, 
            MemoryAllocator& area, 
            const KeyFragment& key)
    {
        TriePathEntry lb = lowerBound(node, area, key);
        if (lb.isOffTheEnd())
            return offTheEnd(node, area);

        for (int i = lb.entryNum; i < node->size; ++i) {
            KeyFragment entryKey = node->entries[i].key(area, node.info);
            int cpLen = key.commonPrefixLen(entryKey);

            if (cpLen != key.bits) {
                return TriePathEntry::terminal(
                        entryKey.bits, node->entries[i].value, i);
            }
        }

        return offTheEnd(node, area);
    }


    static KVList gatherKV(const Node & node, MemoryAllocator & area)
    {
        KVList kvs;
        kvs.reserve(node->size);

        for (int i = 0; i < node->size; ++i) {
            auto& entry = node->entries[i];
            kvs.push_back({ entry.key(area, node.info), entry.value});
        }

        return kvs;
    }

    static bool isBranchingNode(const Node & node, MemoryAllocator & area)
    {
        return false;
    }

    static TriePath
    copyAndReplace(
            const Node& oldNode,
            MemoryAllocator& area,
            const TriePathEntry& match,
            const TriePtr& replaceWith,
            TriePtr::State newState,
            GCList& gc)
    {
        throw ML::Exception(
                "Large key node shouldn't have copyAndReplace");
    }

    static TriePath
    inplaceReplace(
            const Node& oldNode,
            MemoryAllocator& area,
            const TriePathEntry& match,
            const TriePtr& replaceWith,
            GCList& gc)
    {
        throw ML::Exception(
                "Large key node shouldn't have inplaceReplace");
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
        throw ML::Exception("copyAndSetBranch on large key node");
    }

    static TriePtr
    inplaceSetBranch(
            const Node & node,
            MemoryAllocator & area,
            const KeyFragment& key,
            TriePtr newBranch,
            GCList & gc)
    {
        throw ML::Exception("inplaceSetBranch on large key node");
    }

    static TriePath
    copyAndInsertLeaf(
            const Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            uint64_t value,
            TriePtr::State newState,
            GCList& gc)
    {
        KVList kvs = gatherKV(node, area);
        size_t entry = insertKv(kvs, { key, value });
        return makeMultiLeafNode(area, node.info, kvs, entry, newState, gc);
    }

    static TriePath
    inplaceInsertLeaf(
            const Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            uint64_t value,
            GCList& gc)
    {
        // Are we bursting?
        if (node->size >= LargeKeyNodeRepr::MaxEntries) {
            TriePath path = copyAndInsertLeaf(
                    node, area, key, value, TriePtr::IN_PLACE, gc);
            deallocNewNode(node, gc);
            return path;
        }

        int pos = -1;
        KV kv = { key, value };
        KeyFragmentRepr keyRepr = key.allocRepr(area, node.info);

        for (int i = 0; i < node->size; ++i) {
            KeyFragment entryKey = node->entries[i].key(area, node.info);

            if (kv.key < entryKey) {
                std::swap(kv.key, entryKey);
                node->entries[i].swap(keyRepr, kv.value);
                if (pos < 0) pos = i;
            }
        }

        node->entries[node->size].key_ = keyRepr;
        node->entries[node->size].value = kv.value;
        if (pos < 0) pos = node->size;

        node->size++;

        return TriePath (node, TriePathEntry::terminal(
                        key.bits, value, pos, true /* exact */));
    }

    static TriePtr
    copyAndRemoveLeaf(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            TriePtr::State newState,
            GCList& gc)
    {
        ExcAssertGreater(node->size, 0);

        gc.addOldNode(node);

        uint32_t newSize = node->size-1;
        KVList kvs(newSize);

        int j = 0;
        bool deletedNode = false;
        for (int i = 0; i < node->size; ++i) {
            KeyFragment entryKey = node->entries[i].key(area, node.info);

            if (entryKey == key) {
                deletedNode = true;
                continue;
            }

            if (j == newSize) break;

            kvs[j].key = entryKey;
            kvs[j].value = node->entries[i].value;
            j++;
        }
        ExcAssert(deletedNode);

        TriePath path = makeMultiLeafNode(
                area, node.info, kvs, newSize, newState, gc);
        return path.root();
    }


    static TriePtr
    inplaceRemoveLeaf(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            GCList& gc)
    {
        if (node->size <= 2) {
            TriePath path = copyAndRemoveLeaf(
                    node, area, key, TriePtr::IN_PLACE, gc);
            deallocNewNode(node, gc);
            return path.root();
        }

        bool doRemove = false;
        KeyFragmentRepr toDeleteRepr;

        for (int i = 0; i < node->size - 1; ++i) {
            if (!doRemove && key == node->entries[i].key(area, node.info)){
                doRemove = true;
                toDeleteRepr = node->entries[i].key_;
            }

            if (doRemove)
                node->entries[i] = node->entries[i+1];
        }

        if (!doRemove) {
            ExcAssertEqual(node->entries[node->size-1].key(area, node.info), key);
            std::swap(toDeleteRepr, node->entries[node->size-1].key_);
            node->entries[node->size-1].value = 0;
        }

        node->size--;
        KeyFragment::deallocRepr(toDeleteRepr, area, node.info);

        return node;
    }

    static TriePtr
    copyAndPrefixKeys(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            TriePtr::State newState,
            GCList& gc)
    {
        int size = node->size;
        KVList kvs(size);

        for (int i = 0; i < size; ++i) {
            kvs[i].key = prefix + node->entries[i].key(area, node.info);
            kvs[i].value = node->entries[i].value;
        }

        TriePtr result = allocMultiLeaf(area, node.info, kvs, newState, gc);
        ExcAssert(result);

        gc.addOldNode(node);
        return result;
    }

    static TriePtr
    inplacePrefixKeys(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            GCList& gc)
    {

        // Make all the allocations before we modify the node.
        // Necessay because the allocation may throw a resize exception.
        KeyFragmentRepr reprs[node->size];
        for (int i = 0; i < node->size; ++i) {
            KeyFragment newKey = prefix + node->entries[i].key(area, node.info);
            reprs[i] = newKey.allocRepr(area, node.info);
        }

        for (int i = 0; i < node->size; ++i) {
            KeyFragment::deallocRepr(node->entries[i].key_, area, node.info);
            node->entries[i].key_ = reprs[i];
        }

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
        ExcAssertLess(entry.entryNum, node->size);

        int size = node->size;
        KVList kvs(size);

        for (int i = 0; i < size; ++i) {
            kvs[i].key = node->entries[i].key(area, node.info);

            if (i == entry.entryNum)
                kvs[i].value = newValue;
            else
                kvs[i].value = node->entries[i].value;
        }

        TriePtr result = allocMultiLeaf(area, node.info, kvs, newState, gc);
        ExcAssert(result);

        return TriePath(result, TriePathEntry::terminal(
                        kvs[entry.entryNum].key.bits,
                        newValue,
                        entry.entryNum,
                        true /* exactMatch */));
    }

    static TriePath
    inplaceReplaceValue(
            Node& node,
            MemoryAllocator& area,
            const TriePathEntry& entry,
            uint64_t newValue,
            GCList& gc)
    {
        ExcAssertLess(entry.entryNum, node->size);

        node->entries[entry.entryNum].value = newValue;

        return TriePath(node, TriePathEntry::terminal(
                        node->entries[entry.entryNum].keyLen(),
                        newValue, entry.entryNum, true /* exact */));
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

        KVList kvs;
        kvs.reserve(node->size);

        for (int i = 0; i < node->size; ++i) {
            auto& entry = node->entries[i];
            kvs.push_back({ entry.key(area, node.info), entry.value });
        }

        return allocMultiLeaf(area, node.info, kvs, newState, gc);
    }

    static NodeStats
    stats(const Node & node, MemoryAllocator & area)
    {
        return node->stats();
    }

    static void
    dump(
            const Node& node,
            MemoryAllocator& area,
            int indent,
            int maxDepth,
            std::ostream& stream)
    {
        using namespace std;

        stream << ((TriePtr)node) << " "
                << node->size << " children";

        string id(indent, ' ');

        for (unsigned i = 0;  i < node->size;  ++i) {
            stream << endl << id << i << ": ";
            stream << node->entries[i].key(area, node.info)
                   << " -> " << node->entries[i].value;
        }
    }

    static std::string
    print(TriePtr ptr, MemoryAllocator* area, GcLock::ThreadGcInfo* info)
    {
        uint64_t offset = Node::decodeOffset(ptr.data);

        std::string result = ML::format(
                "Large Key: offset %lld", (long long) offset);

        if (!area) return result;

        Node node = encode(ptr, *area, info);
        result += ML::format(" size %d", (int)node->size);
        return result;
    }

};


} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__trie_keyed_nodes_h__*/

