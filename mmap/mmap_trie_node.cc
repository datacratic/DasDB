/* mmap_trie_node.cc                                               -*- C++ -*-
   Jeremy Barnes, 11 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Implementation of nodes for mmap tries.
*/

#include "mmap_trie_node.h"
#include "jml/arch/format.h"
#include <boost/tuple/tuple.hpp>
#include "mmap_trie_node_impl.h"
#include "mmap_trie_null_node.h"
#include "mmap_trie_inline_nodes.h"
#include "mmap_trie_terminal_nodes.h"
#include "mmap_trie_sparse_nodes.h"
#include "mmap_trie_compressed_nodes.h"
#include "mmap_trie_large_key_nodes.h"
//#include "mmap_trie_dense_node.h"
#include "mmap_trie_binary_nodes.h"
#include "mmap_trie_dense_branching_node.h"
#include "soa/gc/gc_lock.h"


using namespace std;
using namespace ML;


namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* NODE OPS                                                                  */
/*****************************************************************************/

NodeOpsAdaptor<NullNodeOps> nullOps;
NodeOpsAdaptor<BinaryNodeOps> biOps;
NodeOpsAdaptor<InlineNodeOps> inlineOps;
NodeOpsAdaptor<BasicKeyedTerminalOps> bktOps;
NodeOpsAdaptor<SparseNodeOps> spOps;
NodeOpsAdaptor<CompressedNodeOps> cmpOps;
NodeOpsAdaptor<LargeKeyNodeOps> lKeyOps;
NodeOpsAdaptor<DenseBranchingNodeOps> denseBrOps;
//NodeOpsAdaptor<DenseInternalOps> diOps;

/** Must follow the same order as the one defined in the NodeType enum. */
const NodeOps * NodeOps::OPS[64] = {
    &nullOps,
    &biOps,
    &inlineOps,
    &bktOps,
    &spOps,
    &cmpOps,
    &lKeyOps,
    &denseBrOps
};


/*****************************************************************************/
/* NODE TRANSFORMATIONS                                                      */
/*****************************************************************************/

TriePath
addLeaf(MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        TriePtr ptr,
        const KeyFragment & key,
        uint64_t value,
        TriePtr::State newState, GCList & gc)
{

    return NodeOps::insertLeaf(ptr, area, info, key, value, newState, gc);
}

// Create the best possible leaf node to store the given key and value
TriePath
makeLeaf(
        MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        const KeyFragment & key, uint64_t value,
        TriePtr::State newState,
        GCList & gc)
{
    InlineNode inl;
    bool success;

    boost::tie(inl, success) = InlineNode::encode(
            key, value, info, newState, gc);

    TriePtr result;

    if (success)
        result = inl.toPtr(newState);
    else
        result = BasicKeyedTerminalOps::alloc(
                area, info, key, value, newState, gc);

    ExcAssert(result);

    return TriePath(result, TriePathEntry::terminal(
                    key.bits, value, 0, true /* exact */));
    //cerr << "path = ";
    //path.dump(cerr, &area);
    //cerr << endl;
}

// \deprecated use makeMultiLeafNode instead.
TriePath
makeDoubleLeafNode(
        MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        const KeyFragment & key1, uint64_t value1,
        const KeyFragment & key2, uint64_t value2,
        TriePtr::State newState,
        GCList & gc)
{
    KVList kvs;
    int entryToPointTo;

    // makeMultiLeafNode requires that the childs be sorted but we still need
    // the return path to point the {key1, value1}.
    if (key1 < key2) {
        kvs.push_back({ key1, value1 });
        kvs.push_back({ key2, value2 });
        entryToPointTo = 0;
    }
    else {
        kvs.push_back({ key2, value2 });
        kvs.push_back({ key1, value1 });
        entryToPointTo = 1;
    }

    return makeMultiLeafNode(area, info, kvs, entryToPointTo, newState, gc);
}

TriePath
makeMultiLeafNode(
        MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        const KVList& kvs,
        int entryToPointTo,
        TriePtr::State newState,
        GCList & gc)
{
    ExcCheckGreaterEqual(entryToPointTo, 0, "invalid entry pointed to");
    ExcCheckLessEqual(entryToPointTo, kvs.size(), "invalid entry pointed to");

    if (kvs.empty()) return TriePath(TriePtr(), TriePathEntry::offTheEnd(0));
    if (kvs.size() == 1)
        return makeLeaf(area, info, kvs[0].key, kvs[0].value, newState, gc);

    TriePtr result;
    TriePath path;

    KeyFragment toFind;
    uint64_t valueToFind;
    if (entryToPointTo < kvs.size()) {
        toFind = kvs[entryToPointTo].key;
        valueToFind = kvs[entryToPointTo].value;
    }
    else {
        valueToFind = 0;
    }

    int minLen, maxLen;
    if (!result) {
        ExcAssert(std::is_sorted(kvs.begin(), kvs.end()));

        minLen = maxLen = kvs[0].key.bits;

        for (unsigned i = 1; i < kvs.size(); ++i) {
            int l = kvs[i].key.bits;
            minLen = std::min(minLen, l);
            maxLen = std::max(maxLen, l);
        }
    }

    // Node type specialized for 64 bit keys.
    if (!result && maxLen <= 64 && minLen == maxLen)
        result = SparseNodeOps::allocMultiLeaf(area, info, kvs, newState, gc);

    // Try our fallback for large keys.
    if (!result)
        result = LargeKeyNodeOps::allocMultiLeaf(
                area, info, kvs, newState, gc);

    // If not, does it fit in a compressed sparse node?
    if (!result && maxLen <= 64 && minLen == maxLen)
        result = CompressedNodeOps::allocMultiLeaf(
                area, info, kvs, newState, gc);

    // If possible, build the path and skip some unecessay overhead.
    if (result && entryToPointTo < kvs.size())
        path = TriePath(result, NodeOps::matchKey(result, area, info, toFind));

    // Otherwise, create a branching node to split up the values.
    if (!result)
        result = makeBranchingNode(area, info, kvs, newState, gc);

    ExcAssert(result);
    ExcAssertEqual(result.state, newState);

    // Build the path the long way.
    if (entryToPointTo == kvs.size())
        path = TriePath(result, NodeOps::offTheEnd(result, area, info));
    else if (entryToPointTo < kvs.size() && path.empty())
        path = NodeOps::findKey(result, area, info, toFind);

    if (entryToPointTo != kvs.size()) {
        ExcAssert(path.valid());
        ExcAssertEqual(path.value(), valueToFind);
    }

    return path;
}

namespace {

/** Return <startBit, numBits> */
pair<int, int>
getArity(const KVList& kvs)
{
    enum { MaxBits = 4 };

    uint64_t cpLen = kvs.commonPrefix().bits;
    // return make_pair<int, int>(cpLen, 1);

    // cerr << endl;
    // cerr << "\tgetArity" << endl;
    // cerr << "\t\tcommonPrefix=" << cpLen << endl;

    bool valueOnCp = false;
    int minBits = std::numeric_limits<int>::max();
    for (auto it = kvs.begin(), end = kvs.end(); it != end; ++it) {
        // Ignore the KV if it's a value to be stored in the branching node.
        if (it->key.bits == cpLen && !it->isPtr) {
            valueOnCp = true;
            continue;
        }
        minBits = std::min(minBits, it->key.bits);
    }

    pair<int, int> result = { cpLen, std::min<int>(minBits - cpLen, MaxBits) };
    // cerr << "\t\tstartBit=" << result.first << ", numBits=" << result.second << endl;


    if (!valueOnCp) {

        // Try to move startBit to the left to maximize our bit usage.
        if (result.second < MaxBits) {
            // cerr << "\t\tminAdj" << endl;

            if (minBits >= MaxBits)
                result = { minBits - MaxBits, MaxBits };
            else result = { 0, minBits };
        }


#if 0 // \todo I'm not convinced this is a good idea.

        // aligning everything to 4 bits should increase the probability that we
        // have 4 or 8 bit holes in the keys which maximizes the usage of our
        // n-ary nodes.
        if ((result.first & ~0x3) != result.first) {
            cerr << "\t\talignAdj" << endl;
            int leftovers = result.first & 0x3;
            result.first -= leftovers;
            result.second = std::min<int>(result.second + leftovers, MaxBits);
        }
#endif

    }

    // cerr << "\t\tstartBit=" << result.first << ", numBits=" << result.second << endl;

    return result;
}


typedef compact_vector<KVList, 256> BucketList;

BucketList
splitKvs(int32_t startBit, int32_t numBits, const KVList& kvs)
{
    ExcAssertGreaterEqual(numBits, 1);
    ExcAssertLessEqual(numBits, 256);

    BucketList buckets;
    buckets.resize(1 << numBits);

    for (auto it = kvs.begin(), end = kvs.end(); it != end; ++it) {

        // Skip the value on the common prefix.
        if (it->key.bits == startBit) {
            ExcAssert(!it->isPtr);
            continue;
        }

        int bucket = it->key.getBits(numBits, startBit);
        buckets[bucket].push_back(*it);
    }

    // cerr << "input:" << endl;
    // for (size_t i = 0; i < kvs.size(); ++i)
    //     cerr << "  " << i << ": " << kvs[i] << endl;

    // cerr << "buckets:" << endl;
    // for (size_t i = 0; i < buckets.size(); ++i) {
    //     if (buckets[i].empty()) continue;
    //     cerr << "  " << i << ": " << buckets[i] << endl;
    // }
    // cerr << endl;

    return buckets;
}

} // namespace anonymous



/** Creates a branching node from the given list of kv.

    This function has a limitation where if a kv's key is a prefix of another
    kv's key then both the keys must be of different size and one of the kv must
    be a value.

    Handling these edge cases would only be useful in the 3-way trie merge and
    would slow down the regular operations on the trie.
 */
TriePtr
makeBranchingNode(
        MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        const KVList& kvs,
        TriePtr::State newState,
        GCList & gc)
{
    if (kvs.empty()) return TriePtr();

    if (kvs.size() == 1 && kvs.hasPtr()) {
        const KV& kv = kvs.front();

        /** Note that we don't want makeBranchingNode to modify the node because
            we may be within a recursive call to makeBranchingNode which could
            end up throwing an exception.
         */
        return NodeOps::copyAndPrefixKeys(
                kv.getPtr(), area, info, kv.key, newState, gc);
    }

    int32_t startBit, numBits;
    tie(startBit, numBits) = getArity(kvs);

    BucketList buckets = splitKvs(startBit, numBits, kvs);

    for (auto it = buckets.begin(), end = buckets.end(); it != end; ++it) {
        if (it->empty()) continue;

        const KV& front = it->front();

        // Move on if it's a perfect fit for our node.
        if (    it->size() == 1 &&
                front.isPtr &&
                front.key.bits == startBit + numBits)
        {
                continue;
        }

        KeyFragment key = front.key.prefix(startBit + numBits);
        KVList trimmed = it->trim(startBit + numBits);

        TriePtr ptr = makeNode(area, info, trimmed, newState, gc);

        it->clear();
        it->push_back({ key, ptr });
    }

    bool hasValue = false;
    uint64_t value;
    if (kvs.front().key.bits == startBit) {
        hasValue = true;
        value = kvs.front().getValue();
    }
    else {
        value = 0;
    }

    KVList children;
    for (auto it = buckets.begin(), end = buckets.end(); it != end; ++it) {
        if (it->empty()) continue;

        ExcAssertEqual(it->size(), 1);
        children.push_back(it->front());
    }

    return DenseBranchingNodeOps::allocBranchingNode(
            area, info,
            startBit, numBits,
            children, hasValue, value,
            newState, gc);
}

TriePtr makeNode(
        MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        const KVList& kvs,
        TriePtr::State newState,
        GCList & gc)
{
    if (kvs.hasPtr())
        return makeBranchingNode(area, info, kvs, newState, gc);

    return makeMultiLeafNode(area, info, kvs, kvs.size(), newState, gc).root();
}

TriePath
replaceSubtreeRecursive(MemoryAllocator & area,
                        GcLock::ThreadGcInfo * info,
                        const TriePath & path,
                        const TriePath & newSubtree,
                        GCList & gc,
                        const TriePtr & node,
                        int i)
{
    //cerr << "rotateRootRecursive: node " << node.print(area) << endl
    //     << "   i " << i << " path " << path << endl;

    if (i == path.size())
        return newSubtree;

    TriePathEntry entry = path.getRelative(i);

    if (!entry.isNonTerminal())
        throw ML::Exception("we shouldn't have a terminal entry here");

    TriePath childBit
        = replaceSubtreeRecursive(area, info, path, newSubtree,
                                  gc, entry.node(), i + 1);

    TriePath myBit = NodeOps::replace(
            node, area, info, entry, childBit.root(),
            TriePtr::COPY_ON_WRITE, gc);

    //cerr << i << ": childBit = " << endl;
    //childBit.dump(cerr, &area);
    //cerr << endl << "myBit = " << endl;
    //myBit.dump(cerr, &area);
    //cerr << endl;
    //cerr << "node = " << endl;
    //NodeOps::dump(node, area);
    //cerr << endl;

    myBit += childBit;

    return myBit;
}

} // namespace MMap
} // namespace Datacratic
