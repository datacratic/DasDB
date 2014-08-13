/* mmap_trie_compressed_node.h                                      -*- C++ -*-
   Jeremy Barnes, 16 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Compressed node header.
*/

#ifndef __mmap__trie_compressed_node_h__
#define __mmap__trie_compressed_node_h__

#include <boost/static_assert.hpp>
#include "jml/compiler/compiler.h"
#include "jml/arch/bit_range_ops.h"

namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* COMPRESSED TERMINAL NODE                                                  */
/*****************************************************************************/

/** Node with as many (key, value) pairs as can be stored bit-compressed.
    All keys are the same size.
*/

struct CompressedNodeRepr {
    CompressedNodeRepr()
        : keyLen(0), keyBits(0), valueBits(0), size(0), keyOffset(0)
    {
    }

    uint8_t keyLen;
    uint8_t keyBits;          ///< If zero then keys are implicit in place
    uint8_t valueBits;
    uint8_t size;
    uint32_t keyOffset;       ///< Offset added to read keys
    

    typedef uint64_t Data;

    Data data[7];  // bit-compressed storage for data
    // TODO: do this with 32 bits to recuperate the 32 bit hole

    enum { BITS_AVAIL = sizeof(data) * 8 };

    KeyFragment getKey(int i) const
    {
        return get(i).key;
    }

    uint64_t getValue(int i) const
    {
        return get(i).value;
    }

    KV get(int i) const
    {
        ML::Bit_Extractor<Data> buffer(data);
        buffer.advance(i * (keyBits + valueBits));
        uint64_t key, value;
        buffer.extract(key, keyBits, value, valueBits);

        if (keyBits == 0)
            key = i;
        key += keyOffset;

        //using namespace std;
        //cerr << "key = " << key << " value = " << value << endl;
        //cerr << "keyBits = " << (int)keyBits << endl;

        KV result = { KeyFragment(key, keyLen), value };
        return result;
    }

    int maxSize() const
    {
        if (keyBits + valueBits == 0)
            return (uint8_t)-1;
        return sizeof(data) * 8 / (keyBits + valueBits);
    }

    NodeStats stats() const
    {
        NodeStats stats;

        stats.nodeCount = 1;
        stats.values = size;
        stats.totalBytes = sizeof(CompressedNodeRepr);
        stats.bookeepingBytes = sizeof(uint64_t);
        stats.unusedBytes += stats.totalBytes - stats.bookeepingBytes -
            ceilDiv((keyBits + valueBits) * size, 8);

        stats.avgBits = stats.maxBits = keyLen;

        return stats;
    }

} JML_ALIGNED(cache_line);

static_assert(
        sizeof(CompressedNodeRepr) <= cache_line,
        "Must fit into a cache line.");


/******************************************************************************/
/* COMPRESSED NODE OPS                                                        */
/******************************************************************************/

// Operations structure
struct CompressedNodeOps
    : public IndirectOpsBase<CompressedNodeRepr, CompressedTerm>
{

    static TriePtr 
    allocMultiLeaf(
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KVList& kvs,
            TriePtr::State newState,
            GCList& gc)
    {
        // Safety check on the highest value that size can take.
        // And yes it's possible to achieve (seq keys + 0 values).
        if (kvs.size() > (uint8_t)-1) return TriePtr();

        uint64_t minKey = kvs[0].key.getKey(), maxKey = kvs[0].key.getKey();
        bool incrementing = true;

        // Look at the key space
        for (unsigned i = 0; i < kvs.size(); ++i) {
            minKey = std::min(minKey, kvs[i].key.getKey());
            maxKey = std::max(maxKey, kvs[i].key.getKey());

            if (incrementing
                && i > 0
                && kvs[i - 1].key.getKey() + 1 != kvs[i].key.getKey())
            {
                incrementing = false;
            }
        }

        // We only get 32 bits for this...
        uint32_t keyOffset = std::min<uint64_t>(minKey, (uint32_t)-1);

        // Look at how keys would be encoded
        int maxKeyBits = 0, maxValueBits = 0;
        for (unsigned i = 0; i < kvs.size(); ++i) {
            maxKeyBits = std::max(maxKeyBits,
                                  ML::highest_bit(kvs[i].key.getKey() 
                                          - keyOffset, -1)
                                  + 1);
            maxValueBits = std::max(maxValueBits,
                                    ML::highest_bit(kvs[i].value, -1) + 1);
        }
        
        if (incrementing && minKey == keyOffset)
            maxKeyBits = 0;

#if 0
        cerr << "BITS_AVAIL = " << CompressedNodeRepr::BITS_AVAIL
             << " size = " << kvs.size() << " maxKeyBits = "
             << maxKeyBits
             << " maxValueBits = " << maxValueBits
             << " bits needed = "
             << ((maxKeyBits + maxValueBits) * n) << endl;
#endif

        // If it doesn't fit, stop.
        if ((maxKeyBits + maxValueBits) * kvs.size() 
                > CompressedNodeRepr::BITS_AVAIL)
        {
            return TriePtr();
        }
            
        // This will fit in a compressed node... do it...
        Node node = allocEmpty(area, node.info, newState);
        gc.addNewNode(node.offset, node);

        node->size = kvs.size();
        node->keyBits = maxKeyBits;
        node->valueBits = maxValueBits;
        node->keyLen = kvs[0].key.bits;
        node->keyOffset = keyOffset;

        ML::Bit_Writer<uint64_t> writer(node->data);

        for (unsigned i = 0; i < kvs.size();  ++i) {
            if (maxKeyBits > 0)
                writer.write(kvs[i].key.getKey() - keyOffset, maxKeyBits);
            writer.write(kvs[i].value, maxValueBits);
        }

        return node;
    }


    static void deallocate(const Node & node, MemoryAllocator & area)
    {
        deallocNode(node, area);
    }

    static size_t directMemUsage(const Node & node, MemoryAllocator & area)
    {
        return sizeof(CompressedNodeRepr);
    }

    static size_t size(const Node & node, MemoryAllocator & area)
    {
        return node->size;
    }

    static void
    forEachEntry(const Node & node, MemoryAllocator & area,
                 const NodeOps::OnEntryFn & fn, int what)
    {
        if ((what & NodeOps::VALUE) == 0) return;

        for (unsigned i = 0;  i < node->size;  ++i) {
            KV kv = node->get(i);
            fn(kv.key, TriePtr(), kv.value);
        }
    }

    static TriePathEntry
    matchKey(const Node & node, MemoryAllocator & area, KeyFragment key)
    {
        if (key.bits != node->keyLen)
            return offTheEnd(node, area);

        uint64_t toMatch = key.getBits(node->keyLen);

        //using namespace std;
        //cerr << "toMatch = " << toMatch << endl;

        for (unsigned i = 0;  i < node->size;  ++i) {
            //cerr << "  entry " << i << ": key "
            //     << node->getKey(i) << " bits "
            //     << node->getKey(i).bits << endl;
            if (toMatch == node->getKey(i).getKey())
                return TriePathEntry::terminal(node->keyLen,
                                             node->getValue(i),
                                             i);
        }

        return offTheEnd(node, area);
    }

    static TriePathEntry
    matchIndex(const Node & node, MemoryAllocator & area,
               size_t targetValueNumber)
    {
        return TriePathEntry::terminal(node->keyLen,
                                       node->getValue(targetValueNumber),
                                       targetValueNumber);
    }

    static KeyFragment
    extractKey(const Node & node, MemoryAllocator & area, size_t valueIndex)
    {
        return node->getKey(valueIndex);
    }
            
    static TriePathEntry first(const Node & node, MemoryAllocator & area)
    {
        if (node->size == 0) return offTheEnd(node, area);
        return TriePathEntry::terminal(node->keyLen,
                                     node->getValue(0), 0);
    }

    static TriePathEntry offTheEnd(const Node & node, MemoryAllocator & area)
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
            KeyFragment entryKey = node->getKey(i);
            int cpLen = key.commonPrefixLen(entryKey);

            if (cpLen == key.bits) {
                return TriePathEntry::terminal(
                        entryKey.bits, node->getValue(i), i);
            }

            // If the element is greater then our key then we're done.
            if (cpLen != entryKey.bits && 
                key.getBits(1, cpLen) < entryKey.getBits(1, cpLen))
            {
                return TriePathEntry::terminal(
                        entryKey.bits, node->getValue(i), i);
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
            KeyFragment entryKey = node->getKey(i);
            int cpLen = key.commonPrefixLen(entryKey);

            if (cpLen == key.bits)
                continue;

            return TriePathEntry::terminal(
                    entryKey.bits, node->getValue(i), i);
        }

        return offTheEnd(node, area);
    }


    static KVList gatherKV(const Node & node, MemoryAllocator & area)
    {
        KVList kvs;
        kvs.reserve(node->size);

        for (int i = 0; i < node->size; ++i)
            kvs.push_back(node->get(i));

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
        throw ML::Exception("compressed node shouldn't have copyAndReplace");
    }

    static TriePath
    inplaceReplace(const Node & oldNode,
                   MemoryAllocator & area,
                   const TriePathEntry & match,
                   const TriePtr & replaceWith,
                   GCList & gc)
    {
        throw ML::Exception("compressed node shouldn't have inplaceReplace");
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
        throw ML::Exception("copyAndSetBranch on copressed node");
    }

    static TriePtr
    inplaceSetBranch(
            const Node & node,
            MemoryAllocator & area,
            const KeyFragment& key,
            TriePtr newBranch,
            GCList & gc)
    {
        throw ML::Exception("inplaceSetBranch on compressed node");
    }

    static TriePath
    copyAndInsertLeaf(Node & oldNode,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      TriePtr::State newState,
                      GCList & gc)
    {
        // TODO: lots of potential for optimization here for later...

        // Don't try to expand it; just create a new one
        KVList kvs = gatherKV(oldNode, area);
        size_t entry = insertKv(kvs, { key, value });

        // TODO: insert directly so we know the right leaf
        return makeMultiLeafNode(area, oldNode.info, kvs, entry, newState, gc);
    }

    static TriePath
    inplaceInsertLeaf(Node & node,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      GCList & gc)
    {
        // \todo Try to do it inplace.
        //       It's tricky to do without copying a ton of code.

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

        ExcAssertGreater(node->size, 1);

        int newSize = node->size - 1;
        KVList kvs(newSize);

        int i = 0;
        int j = 0;
        bool deletedNode = false;
        for (;  i < node->size;  ++i) {
            KV kv = node->get(i);

            if (key == kv.key) {
                // std::cerr << "DEL::Com - key=" << key 
                //     << ", entryKey=" << kv.key << std::endl;
                deletedNode = true;
                continue;
            }
            if (j == newSize) {
                break;
            }

            kvs[j] = kv;
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
        // \todo Try to do it inplace.
        //       It's tricky to do without copying a ton of code.

        TriePath path = copyAndRemoveLeaf(
                node, area, key, TriePtr::IN_PLACE, gc);

        deallocNewNode(node, gc);
        return path.root();
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

        uint64_t size = node->size;
        KVList kvs(size);

        for (int i = 0; i < size; ++i) {
            kvs[i].key = prefix + node->getKey(i);
            kvs[i].value = node->getValue(i);

            // std::cerr << "PRE::Com - prefix=" << prefix
            //     << ", old=" << node->getKey(i)
            //     << ", new=" << kvs[i].key << std::endl;
        }

        // The KV might no longer fit in a compressed node after we add the 
        // prefix. So makeMultiLeafNode might return an entire tree of stuff.
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
        // \todo Try to do it inplace.
        //       It's tricky to do without copying a ton of code.

        TriePath path = copyAndPrefixKeys(
                node, area, prefix, TriePtr::IN_PLACE, gc);

        deallocNewNode(node, gc);
        return path.root();
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

        uint64_t size = node->size;
        KVList kvs(size);

        // The KVs might no longer fit after we change the value so we gotta
        // do a full copy and reconstruction.
        for (int i = 0; i < size; ++i) {
            kvs[i].key = node->getKey(i);

            if (i == entry.entryNum)
                kvs[i].value = newValue;
            else
                kvs[i].value = node->getValue(i);
        }

        return makeMultiLeafNode(
                area, node.info, kvs, entry.entryNum, newState, gc);
    }

    static TriePath
    inplaceReplaceValue(
            Node& node,
            MemoryAllocator& area,
            const TriePathEntry& entry,
            uint64_t newValue,
            GCList& gc)
    {
        // \todo Try to do it inplace.
        //       It's tricky to do without copying a ton of code.

        TriePath path = copyAndReplaceValue(
                node, area, entry, newValue, TriePtr::IN_PLACE, gc);

        deallocNewNode(node, gc);
        return path;
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
        return node->stats();
    }

    static void dump(const Node & node,
                     MemoryAllocator & area,
                     int indent,
                     int maxDepth,
                     std::ostream & stream)
    {
        using namespace std;

        stream << ((TriePtr)node) << " "
            << (int)node->size << " children keyBits="
            << (int)node->keyBits << " valueBits="
            << (int)node->valueBits << " keyLen="
            << (int)node->keyLen;
        
        string id(indent, ' ');

        int max = std::min(node->size, uint8_t(8));

        for (unsigned i = 0; i < max; ++i) {
            stream << endl << id << i << ": ";
            KV kv = node->get(i);
            stream << kv.key << " -> " << kv.value;
        }

        // avoid spamming the screen.
        if (max != node->size) {
            stream << endl << id
                << max << "-" << uint16_t(node->size)
                << ": " << "...";
        }
    }

    static std::string print(TriePtr ptr, MemoryAllocator * area,
                             GcLock::ThreadGcInfo * info)
    {
        std::string result = ML::format("Compressed: offset %lld",
                                        (long long)Node::decodeOffset(ptr.data));
        if (!area) return result;
        Node node = encode(ptr, *area, info);
        result += ML::format(" size: %d keyLen: %d", node->size,
                             node->keyLen);
        return result;
    }
    
};


} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__trie_compressed_node_h__ */

