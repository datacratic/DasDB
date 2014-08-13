/* mmap_trie_sparse_node.h                                      -*- C++ -*-
   Jeremy Barnes, 16 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Sparse node header.
*/

#ifndef __mmap__trie_sparse_node_h__
#define __mmap__trie_sparse_node_h__

#include <boost/static_assert.hpp>
#include "jml/utils/less.h"
#include "jml/compiler/compiler.h"

namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* SPARSE TERMINAL NODE                                                      */
/*****************************************************************************/

/** Node with four (key, value) pairs.  All keys are the same size, which
    is stored in metadata along with the number of entries.
*/

struct SparseNodeRepr {
    SparseNodeRepr()
    {
    }

    struct Entry {
        uint64_t key;
        uint64_t value;

        bool operator < (const Entry & other) const
        {
            return ML::less_all(key, other.key, value, other.value);
        }
    };

    Entry entries[4];

    void sort(int size)
    {
        std::sort(entries, entries + size);
    }

    NodeStats stats(int size, int keyLen) const
    {
        NodeStats stats;

        stats.nodeCount = 1;
        stats.values = size;
        stats.totalBytes = sizeof(SparseNodeRepr);
        stats.unusedBytes += sizeof(Entry) * (4 - size);
        stats.avgBits = stats.maxBits = keyLen;

        return stats;
    }

} JML_ALIGNED(cache_line);

static_assert(
        sizeof(SparseNodeRepr) <= cache_line,
        "Must fit into a cache line.");


/******************************************************************************/
/* SPARSE NODE METADATA                                                       */
/******************************************************************************/

struct SparseNodeMD {
    SparseNodeMD(uint32_t val = 0)
        : bits(val)
    {
    }

    operator uint32_t () const { return bits; }

    union {
        uint32_t bits;
        struct {
            uint32_t keyLen:7;
            uint32_t size:3;
            uint32_t unused:22;
        };
    };
    static_assert(
            42 + TriePtr::MetaBits + 7 + 3 <= 64,
            "Not enough bits for metadata");
};

inline std::ostream &
operator << (std::ostream & stream, const SparseNodeMD & md)
{
    return stream << "SparseNodeMD: keyLen " << md.keyLen << " size "
                  << md.size << " bits " << md.bits;
}


/******************************************************************************/
/* SPARSE NODE OPS                                                            */
/******************************************************************************/

struct SparseNodeOps
    : public IndirectOpsBase<SparseNodeRepr, SparseTerm, SparseNodeMD, 10>
{

    static void deallocate(const Node& node, MemoryAllocator& area)
    {
        deallocNode(node, area);
    }

    static size_t directMemUsage(const Node & node, MemoryAllocator & area)
    {
        return sizeof(SparseNodeRepr);
    }

    static TriePtr
    allocMultiLeaf(
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KVList& kvs,
            TriePtr::State newState,
            GCList& gc)
    {
        if (kvs.size() > 4) return TriePtr();

        Node node = allocEmpty(area, info, newState);
        node.metadata.size = kvs.size();
        node.metadata.keyLen = kvs[0].key.bits;
        gc.addNewNode(node.offset, node);

        for (unsigned i = 0; i < kvs.size(); ++i) {
            node->entries[i].key = kvs[i].key.getKey();
            node->entries[i].value = kvs[i].value;
        }

        return node;
    }

    static size_t size(const Node & node, MemoryAllocator & area)
    {
        return node.metadata.size;
    }

    static void
    forEachEntry(const Node & node, MemoryAllocator & area,
                 const NodeOps::OnEntryFn & fn, int what)
    {
        if ((what & NodeOps::VALUE) == 0) return;

        for (unsigned i = 0;  i < node.metadata.size;  ++i) {
            fn(KeyFragment(node->entries[i].key, node.metadata.keyLen),
               TriePtr(), node->entries[i].value);
        }
    }

    static TriePathEntry
    matchKey(const Node & node, MemoryAllocator & area, const KeyFragment & key)
    {
        if (key.bits != node.metadata.keyLen)
            return offTheEnd(node, area);

        uint64_t toMatch = key.getBits(node.metadata.keyLen);

        for (unsigned i = 0;  i < node.metadata.size;  ++i) {
            if (toMatch == node->entries[i].key)
                return TriePathEntry::terminal(node.metadata.keyLen,
                                             node->entries[i].value, i);
        }

        return offTheEnd(node, area);
    }
    
    static TriePathEntry
    matchIndex(const Node & node, MemoryAllocator & area,
               size_t targetValueNumber)
    {
        return TriePathEntry::terminal(node.metadata.keyLen,
                                     node->entries[targetValueNumber].value,
                                     targetValueNumber);
    }

    static KeyFragment
    extractKey(const Node & node, MemoryAllocator & area, size_t valueIndex)
    {
        return KeyFragment(node->entries[valueIndex].key,
                           node.metadata.keyLen);
    }
        
    static TriePathEntry first(const Node & node, MemoryAllocator & area)
    {
        if (node.metadata.size == 0) return offTheEnd(node, area);
        return TriePathEntry::terminal(node.metadata.keyLen,
                                     node->entries[0].value, 0);
    }

    static TriePathEntry offTheEnd(const Node & node, MemoryAllocator & area)
    {
        return TriePathEntry::offTheEnd(node.metadata.size);
    }

    static TriePathEntry 
    lowerBound(
            const Node& node, 
            MemoryAllocator& area, 
            const KeyFragment& key)
    {
        for (int i = 0; i < node.metadata.size; ++i) {
            KeyFragment entryKey(node->entries[i].key, node.metadata.keyLen);
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

        for (int i = lb.entryNum; i < node.metadata.size; ++i) {
            KeyFragment entryKey(node->entries[i].key, node.metadata.keyLen);
            int cpLen = key.commonPrefixLen(entryKey);

            if (cpLen == key.bits)
                continue;

            return TriePathEntry::terminal(
                    entryKey.bits, node->entries[i].value, i);
        }

        return offTheEnd(node, area);
    }

    static KVList gatherKV(const Node & node, MemoryAllocator & area)
    {
        KVList kvs;
        kvs.reserve(node.metadata.size);

        int32_t kb = node.metadata.keyLen;

        for (int i = 0; i < node.metadata.size; ++i) {
            auto& entry = node->entries[i];
            kvs.push_back({ KeyFragment(entry.key, kb), entry.value});
        }

        return kvs;
    }

    static bool isBranchingNode(const Node & node, MemoryAllocator & area)
    {
        return false;
    }

    static TriePath
    copyAndReplace(const Node & oldNode,
                   MemoryAllocator & area,
                   const TriePathEntry & match,
                   const TriePtr & replaceWith,
                   TriePtr::State newState,
                   GCList & gc)
    {
        throw ML::Exception("sparse node shouldn't have copyAndReplace");
    }

    static TriePath
    inplaceReplace(const Node & oldNode,
                   MemoryAllocator & area,
                   const TriePathEntry & match,
                   const TriePtr & replaceWith,
                   GCList & gc)
    {
        throw ML::Exception("sparse node shouldn't have inplaceReplace");
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
        throw ML::Exception("copyAndSetBranch on sparse node");
    }

    static TriePtr
    inplaceSetBranch(
            const Node & node,
            MemoryAllocator & area,
            const KeyFragment& key,
            TriePtr newBranch,
            GCList & gc)
    {
        throw ML::Exception("inplaceSetBranch on sparse node");
    }

    static TriePath
    copyAndInsertLeaf(Node & oldNode,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      TriePtr::State newState,
                      GCList & gc)
    {
        using namespace std;

        if (oldNode.metadata.size == 4
            || key.bits != oldNode.metadata.keyLen) {
            //cerr << "transform" << endl;
            // We need to transform into another node since we can't any
            // longer fit within this node

            KVList kvs = gatherKV(oldNode, area);
            size_t entry = insertKv(kvs, { key, value });

            return makeMultiLeafNode(
                    area, oldNode.info, kvs, entry, newState, gc);
        }
        else {
            //cerr << "no transform" << endl;
            Node n = allocCopy(area, oldNode.info, oldNode, newState);
            n.metadata.size += 1;
            gc.addNewNode(n.offset, n);

            int i = n.metadata.size - 1;
            
            n->entries[i].key = key.getKey();
            n->entries[i].value = value;
            
            n->sort(n.metadata.size);
            
            TriePath result(n, matchKey(n, area, key));
            ExcAssert(result.value() == value);
            return result;
        }
    }


    static TriePath
    inplaceInsertLeaf(Node & node,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      GCList & gc)
    {
        if (node.metadata.size >= 4 || key.bits != node.metadata.keyLen) {
            TriePath path = copyAndInsertLeaf(
                    node, area, key, value, TriePtr::IN_PLACE, gc);
            deallocNewNode(node, gc);
            return path;
        }

        int pos = -1;
        KV kv = { key, value };
        uint64_t keyRepr = key.getKey();

        size_t size = node.metadata.size;

        for (int i = 0; i < size; ++i) {
            KeyFragment entryKey(node->entries[i].key, node.metadata.keyLen);

            if (kv.key < entryKey) {
                std::swap(kv.key, entryKey);
                std::swap(kv.value, node->entries[i].value);
                std::swap(keyRepr, node->entries[i].key);
                if (pos < 0) pos = i;
            }
        }

        node->entries[size].key = keyRepr;
        node->entries[size].value = kv.value;
        if (pos < 0) pos = size;

        node.metadata.size++;

        gc.updateNewNode(node.offset, node);
        return TriePath(node, TriePathEntry::terminal(
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
        // std::cerr << "sprase.remove("
        //     << "node=" << ((TriePtr)node)
        //     << ")" << std::endl;

        gc.addOldNode(node);

        ExcAssertGreater(node.metadata.size, 1);
        int newSize = node.metadata.size -1;
        KVList kvs(newSize);

        bool deletedNode = false;
        for (int i = 0, j = 0; i < node.metadata.size; ++i) {
            KeyFragment entryKey (node->entries[i].key, node.metadata.keyLen);

            if (key == entryKey) {
                deletedNode = true;
                // std::cerr << "DEL::Spa - key=" << key 
                //     << ", entryKey=" << entryKey << std::endl;
                continue;
            }
            if (j == newSize) {
                break;
            }
            
            kvs[j].key = entryKey;
            kvs[j].value = node->entries[i].value;
            ++j;
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
        size_t sz = node.metadata.size;

        if (sz <= 2) {
            TriePath path = copyAndRemoveLeaf(
                    node, area, key, TriePtr::IN_PLACE, gc);
            deallocNewNode(node, gc);
            return path.root();
        }

        bool doRemove = false;

        for (int i = 0; i < sz - 1; ++i) {
            KeyFragment entryKey(node->entries[i].key, node.metadata.keyLen);

            if (!doRemove && entryKey == key)
                doRemove = true;

            if (doRemove)
                node->entries[i] = node->entries[i+1];
        }

        if (!doRemove) {
            KeyFragment entryKey(node->entries[sz-1].key, node.metadata.keyLen);
            ExcAssertEqual(entryKey, key);

            node->entries[sz-1].key = 0;
            node->entries[sz-1].value = 0;
        }

        node.metadata.size--;

        gc.updateNewNode(node.offset, node);
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
        // If keys will still fit then just make a copy and edit inplace
        if (prefix.bits + node.metadata.keyLen <= 64) {
            gc.addOldNode(node);

            Node nodeCopy = allocCopy(area, node.info, node, newState);
            gc.addNewNode(nodeCopy.offset, nodeCopy);
            TriePtr result = inplacePrefixKeys(nodeCopy, area, prefix, gc);

            // std::cerr << "sparse.prefix.inplace("
            //     << ((TriePtr)node) << " -> " << result << ")";

            return result;
        }

        // keys won't fit anymore, so gather the data and punt the work.
        uint64_t size = node.metadata.size;
        KVList kvs(size);

        for (int i = 0; i < size; ++i) {
            kvs[i].value = node->entries[i].value;

            KeyFragment oldKey(node->entries[i].key, node.metadata.keyLen);
            kvs[i].key = prefix + oldKey;
        }

        return makeMultiLeafNode(
                area, node.info, kvs, size, newState, gc).root();
    }

    static TriePtr
    inplacePrefixKeys(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            GCList& gc)
    {
        // If the keys don't fit anymore then burst.
        if (prefix.bits + node.metadata.keyLen > 64) {
            TriePtr nodeCopy = copyAndPrefixKeys(
                    node, area, prefix, TriePtr::IN_PLACE, gc);
            deallocNewNode(node, gc);
            return  nodeCopy;
        }

        for (int i = 0; i < node.metadata.size; ++i) {
            KeyFragment oldKey(node->entries[i].key, node.metadata.keyLen);
            node->entries[i].key = (prefix + oldKey).getKey();
        }

        node.metadata.keyLen += prefix.bits;
        gc.updateNewNode(node.offset, node);
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
        ExcAssertLess(entry.entryNum, node.metadata.size);

        Node nodeCopy = allocCopy(area, node.info, node, newState);
        gc.addNewNode(nodeCopy.offset, nodeCopy);

        auto& kv = nodeCopy->entries[entry.entryNum];
        kv.value = newValue;

        return TriePath(nodeCopy, TriePathEntry::terminal(
                        nodeCopy.metadata.keyLen,
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
        ExcAssertLess(entry.entryNum, node.metadata.size);

        node->entries[entry.entryNum].value = newValue;

        return TriePath(node, TriePathEntry::terminal(
                        node.metadata.keyLen,
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

        Node nodeCopy = allocCopy(area, node.info, node, newState);

        gc.addNewNode(nodeCopy.offset, nodeCopy);
        gc.addOldNode(node);

        return nodeCopy;
    }

    static NodeStats
    stats(const Node & node, MemoryAllocator & area)
    {
        return node->stats(node.metadata.size, node.metadata.keyLen);
    }

    static void dump(const Node & node,
                     MemoryAllocator & area,
                     int indent,
                     int maxDepth,
                     std::ostream & stream)
    {
        using namespace std;

        stream << ((TriePtr)node) << " " 
                << node.metadata.size << " children";
        
        string id(indent, ' ');

        for (unsigned i = 0;  i < node.metadata.size;  ++i) {
            stream << endl << id << i << ": ";
            stream << KeyFragment(node->entries[i].key,
                                  node.metadata.keyLen)
                   << " -> " << node->entries[i].value;
        }
    }

    static std::string print(TriePtr ptr, const MemoryAllocator * area,
                             GcLock::ThreadGcInfo * info)
    {
        SparseNodeMD metadata = Node::decodeMetadata(ptr.data);
        uint64_t offset = Node::decodeOffset(ptr.data);
        return ML::format("Sparse: size %d, keyLen %d, offset %lld",
                          (int)metadata.size, (int)metadata.keyLen,
                          (long long)offset);
    }
};



} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__trie_sparse_node_h__ */

