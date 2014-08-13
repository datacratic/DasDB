/** mmap_trie_dense_branching_node.h                                 -*- C++ -*-
    Rémi Attab, 15 Nov 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    N-ary branching node where each branch has an available space.

*/

#ifndef __mmap__trie_dense_branching_node_h__
#define __mmap__trie_dense_branching_node_h__

#include "mmap_const.h"
#include "key_fragment.h"
#include "kv.h"
#include "mmap_trie_node_impl.h"
#include "jml/arch/bitops.h"
#include "jml/compiler/compiler.h"

namespace Datacratic {
namespace MMap {


/******************************************************************************/
/* DENSE BRANCHING NODE BRANCH                                                */
/******************************************************************************/

struct DenseBranchingNodeBranch
{
    int64_t size;
    uint64_t child_;

    TriePtr child() const { return TriePtr::fromBits(child_); }
    void setChild(TriePtr c) { child_ = c.bits; }

};


/******************************************************************************/
/* DENSE BRANCHING NODE REPR                                                  */
/******************************************************************************/

/** The members of this struct have been laid out to minimize unaligned reads.
 */
struct DenseBranchingNodeRepr
{
    /** We have an maximum of 4 uint64_t to store our size index which means
        we're allowed only 4 cache line of external storage. We can stuff 4
        uncompressed branches per cacheline which gives us a branching factor of
        16 or 4 branching bits.
    */
    enum { MaxBits = 4 };

    DenseBranchingNodeRepr(uint8_t numBits = 0) :
        prefix_(),
        branchIndex(0),
        numBits(numBits),
        hasValue(false),
        storageOffset(0),
        sizeIndex{0,0,0,0}
    {}


    KeyFragmentRepr prefix_;

    /** Bitmap indicating which branches have an associated value */
    uint16_t branchIndex;

    /** Bits of the key consumed by the branches of this node: [2-4] */
    uint8_t numBits;

    bool hasValue;
    uint64_t value;

    /** Pointer to the actual branch data. */
    uint64_t storageOffset;

    /** Depending on how many branches we have we may be able to inline them
        directly into the node. Otherwise, we use the space to store a size
        index which is useless on binary nodes.
    */
    union {

        DenseBranchingNodeBranch inlineBranches[2];

        /** Holds the aggregate size of each cache line which contain 4 branches
            each. This ensures that we only have to do 2 cache hits (one on the
            node and one the fetch the branch data) in order to fetch a branch
            by its index.
        */
        uint64_t sizeIndex[4];
    };


    bool isInline() const { return numBits == 1; }


    int32_t prefixLen() const { return prefix_.bits; }

    KeyFragment prefix(MemoryAllocator& area) const
    {
        return KeyFragment::loadRepr(prefix_, area);
    }


    void markBranch(uint8_t branch)
    {
        branchIndex |= 1ULL << branch;
    }

    void clearBranch(uint8_t branch)
    {
        branchIndex &= ~(1ULL << branch);
    }

    bool isBranchMarked(uint8_t branch) const
    {
        return branchIndex & (1ULL << branch);
    }


    uint8_t numBranches() const { return 1 << numBits; }

    uint8_t countBranches() const
    {
        return ML::num_bits_set(branchIndex);
    }

    int8_t nextBranch(uint8_t start = 0, uint8_t end = -1) const
    {
        uint16_t mask = ~((1ULL << start) - 1);

        if (!(branchIndex & mask)) return -1;

        int8_t branch = ML::lowest_bit(branchIndex & mask);
        return branch < end ? branch : -1;
    }

    int8_t prevBranch(uint8_t start) const
    {
        uint16_t mask = (1ULL << (start + 1)) - 1;

        if (!(branchIndex & mask)) return -1;

        return ML::highest_bit(branchIndex & mask);
    }

    uint64_t storageSize() const
    {
        if (isInline()) return 0;
        return numBranches() * sizeof(DenseBranchingNodeBranch);
    }

    RegionPtr<DenseBranchingNodeBranch>
    branches(MemoryAllocator& area, uint64_t nodeOffset) const
    {
        uint64_t offset = storageOffset;
        if (isInline()) offset = nodeOffset + offsetof(DenseBranchingNodeRepr, 
                                                  inlineBranches);

        return RegionPtr<DenseBranchingNodeBranch>(
                area.region(), offset, numBranches());
    }

    void updateSize(uint8_t branch, ssize_t diff)
    {
        if (isInline()) return;
        sizeIndex[branch / 4] += diff;
    }

    NodeStats stats() const
    {
        NodeStats stats;

        stats.nodeCount = 1;
        stats.values = hasValue;
        stats.branches = countBranches();
        stats.totalBytes = sizeof(DenseBranchingNodeRepr) + storageSize();
        stats.bookeepingBytes = 12; // branchIndex hasValue numBits storageOffset
        if (!isInline()) stats.bookeepingBytes += sizeof(sizeIndex);

        stats.unusedBytes =
            sizeof(DenseBranchingNodeBranch) * (numBranches() - stats.branches);
        if (!hasValue) stats.unusedBytes += sizeof(value);

        stats.avgBranchingBits = stats.maxBranchingBits = numBits;
        stats.avgBits =
            (prefix_.bits * hasValue) +
            ((prefix_.bits + numBits) * stats.branches);
        stats.avgBits /= hasValue + stats.branches;
        stats.maxBits = prefix_.bits + numBits;

        stats += prefix_.nodeStats();

        return stats;
    }

} JML_PACKED JML_ALIGNED(cache_line);

static_assert(
        sizeof(DenseBranchingNodeRepr) <= cache_line,
        "Must fit into a cache line.");



/******************************************************************************/
/* DENSE BRANCHING NODE OPS                                                   */
/******************************************************************************/

struct DenseBranchingNodeOps :
        public IndirectOpsBase<DenseBranchingNodeRepr, DenseBranch>
{
    enum { MaxBits = DenseBranchingNodeRepr::MaxBits };


    static void allocStorage(Node& node, MemoryAllocator& area, bool init)
    {
        if (node->isInline()) return;

        node->storageOffset = area.nodeAlloc.allocate(node->storageSize());
        if (trieMemoryCheck)
            trieMemoryTracker.trackAlloc(node->storageOffset);

        if (init) {
            auto branches = node->branches(area, node.offset);
            for (size_t branch = 0; branch < node->numBranches(); ++branch) {
                branches[branch].size = 0;
                branches[branch].child_ = 0;
            }
        }
    }

    static void deallocStorage(const Node& node, MemoryAllocator& area)
    {
        if (node->isInline() || !node->storageOffset) return;

        if (trieMemoryCheck)
            trieMemoryTracker.trackDealloc(node->storageOffset);

        area.nodeAlloc.deallocate(node->storageOffset, node->storageSize());
    }



    static TriePtr
    allocBranchingNode(
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            int startBit, int numBits,
            const KVList& children,
            bool hasValue, uint64_t value,
            TriePtr::State newState,
            GCList& gc)
    {
        Node node = allocEmpty(area, info, newState);
        gc.addNewNode(node.offset, node);

        KeyFragment prefix = children.front().key.prefix(startBit);
        node->prefix_ = prefix.allocRepr(area, info);
        node->hasValue = hasValue;
        if (hasValue)
            node->value = value;

        ExcAssertGreaterEqual(numBits, 1);
        ExcAssertLessEqual(numBits, MaxBits);
        node->numBits = numBits;

        allocStorage(node, area, true);

        auto branches = node->branches(area, node.offset);

        for (auto it = children.begin(), end = children.end(); it != end; ++it){
            ExcAssert(it->isPtr);
            ExcAssertEqual(numBits + startBit, it->key.bits);

            uint8_t branch = it->key.getBits(numBits, startBit);
            ExcAssert(!node->isBranchMarked(branch));
            node->markBranch(branch);

            branches[branch].setChild(it->getPtr());
            branches[branch].size = NodeOps::size(it->getPtr(), area, info);
            node->updateSize(branch, branches[branch].size);
        }

        validate(node, area);
        return node;
    }


    static void deallocate(const Node& node, MemoryAllocator& area)
    {
        KeyFragment::deallocRepr(node->prefix_, area, node.info);
        deallocStorage(node, area);
        deallocNode(node, area);
    }


    static Node copyNode(
            const Node& oldNode,
            MemoryAllocator& area,
            TriePtr::State newState,
            GCList& gc,
            bool copyPrefix)
    {
        validate(oldNode, area);

        Node node = allocCopy(area, node.info, *oldNode, newState);
        node->prefix_ = KeyFragmentRepr();
        node->storageOffset = 0;
        gc.addNewNode(node.offset, node);

        if (!node->isInline()) {
            allocStorage(node, area, false);

            auto srcBranches = oldNode->branches(area, node.offset);
            auto destBranches = node->branches(area, node.offset);

            std::copy(
                    srcBranches.get(),
                    srcBranches.get() + node->numBranches(),
                    destBranches.get());
        }

        if (copyPrefix) {
            node->prefix_ = KeyFragment::copyRepr(
                    oldNode->prefix_, area, node.info);
        }

        validate(node, area, false);
        return node;
    }


    /** Only modifies the node once all exception throwing code is completed. */
    static void
    setNodePrefix(
            Node & node,
            MemoryAllocator& area,
            const KeyFragment& newPrefix)
    {
        KeyFragmentRepr oldRepr = node->prefix_;

        KeyFragmentRepr newRepr = newPrefix.allocRepr(area);

        if (oldRepr.isValid())
            KeyFragment::deallocRepr(oldRepr, area);

        node->prefix_ = newRepr;
    }


    /**************************************************************************/
    /* CONST OPS                                                              */
    /**************************************************************************/

    static size_t directMemUsage(const Node & node, MemoryAllocator & area)
    {
        size_t size = sizeof(DenseBranchingNodeRepr);
        size += node->prefix_.directMemUsage();
        size += node->storageSize();
        return size;
    }

    static size_t size(const Node & node, MemoryAllocator & area)
    {
        if (node->isInline()) {
            size_t size = node->hasValue;
            auto branches = node->branches(area, node.offset);

            for (uint8_t branch = 0; branch < 2; ++branch) {
                if (!node->isBranchMarked(branch)) continue;
                size += branches[branch].size;
            }
            return size;
        }

        size_t init = node->hasValue;
        return std::accumulate(node->sizeIndex, node->sizeIndex + 4, init);
    }

    static size_t sizeUpToBranch(
            const Node & node, MemoryAllocator& area, uint8_t endBranch)
    {
        size_t size = node->hasValue;
        auto branches = node->branches(area, node.offset);

        if (node->isInline()) {
            if (endBranch == 1 && node->isBranchMarked(0))
                size += branches[0].size;
            return size;
        }

        size += std::accumulate(
                node->sizeIndex, node->sizeIndex + (endBranch / 4), 0);

        uint8_t startBranch = endBranch & ~0x3;
        if (startBranch != endBranch) {
            for (uint8_t branch = startBranch; branch < endBranch; ++branch) {
                if (!node->isBranchMarked(branch)) continue;
                size += branches[branch].size;
            }
        }

        return size;
    }


    static int getIndex(
            const Node & node, MemoryAllocator & area, size_t valueNum)
    {
        if (node->hasValue && valueNum == 0)
            return -1;  // internal value
        valueNum -= node->hasValue;

        auto branches = node->branches(area, node.offset);

        if (node->isInline()) {
            for (uint8_t branch = 0; branch < 2; ++branch) {
                if (!node->isBranchMarked(branch)) continue;
                if (valueNum < branches[branch].size) return branch;
                valueNum -= branches[branch].size;
            }

            ExcCheck(false, "Index outside the node");
        }

        int bucket = 0;
        while(valueNum >= node->sizeIndex[bucket] && !node->sizeIndex[bucket]) {
            valueNum -= node->sizeIndex[bucket];
            bucket++;
            ExcAssertLess(bucket, 4);
        }

        int8_t branch = bucket * 4 - 1;
        while ((branch = node->nextBranch(branch + 1)) >= 0) {
            if (valueNum < branches[branch].size)
                return branch;
            valueNum -= branches[branch].size;
        }


        ExcCheck(false, "Index outside the node");
    }



    static void
    forEachEntry(const Node & node, MemoryAllocator & area,
                 const NodeOps::OnEntryFn & fn, int what)
    {
        KeyFragment nodePrefix = node->prefix(area);

        if ((what & NodeOps::VALUE) && node->hasValue) {
            fn(nodePrefix, {}, node->value);
        }

        if (what & NodeOps::CHILD) {
            auto branches = node->branches(area, node.offset);

            int8_t branch = -1;
            while ((branch = node->nextBranch(branch + 1)) >= 0) {
                KeyFragment childPrefix =
                    nodePrefix + KeyFragment(branch, node->numBits);
                fn(childPrefix, branches[branch].child(), 0);
            }
        }
    }


    static TriePathEntry
    matchKey(const Node & node, MemoryAllocator & area, KeyFragment key)
    {
        KeyFragment nodePrefix = node->prefix(area);

        if (!nodePrefix.empty()) {
            if (!key.consume(nodePrefix))
                return TriePathEntry::offTheEnd(size(node, area));
        }

        if (key.empty()) {
            if (node->hasValue)
                return TriePathEntry::terminal(
                        nodePrefix.bits, node->value, 0);
            else return TriePathEntry::offTheEnd(size(node, area));
        }

        if (key.bits < node->numBits)
            return TriePathEntry::offTheEnd(size(node, area));

        uint8_t branch = key.getBits(node->numBits);
        if (node->isBranchMarked(branch)) {
            auto branches = node->branches(area, node.offset);

            return TriePathEntry::nonTerminal(
                    nodePrefix.bits + node->numBits,
                    branches[branch].child(),
                    sizeUpToBranch(node, area, branch),
                    branches[branch].size);
        }

        return TriePathEntry::offTheEnd(size(node, area));
    }

    static TriePathEntry
    matchIndex(
            const Node & node, MemoryAllocator & area, size_t targetValueNumber)
    {
        int8_t branch = getIndex(node, area, targetValueNumber);

        int32_t prefixBits = node->prefixLen();

        if (branch == -1)
            return TriePathEntry::terminal(prefixBits, node->value, 0);

        auto branches = node->branches(area, node.offset);

        return TriePathEntry::nonTerminal(
                prefixBits + node->numBits,
                branches[branch].child(),
                sizeUpToBranch(node, area, branch),
                branches[branch].size);
    }


    static KeyFragment
    extractKey(const Node & node, MemoryAllocator & area, size_t valueIndex)
    {
        int8_t branch = getIndex(node, area, valueIndex);
        KeyFragment prefix = node->prefix(area);

        if (branch == -1) return prefix;

        prefix += KeyFragment(branch, node->numBits);
        return prefix;
    }


    static TriePathEntry first(const Node & node, MemoryAllocator & area)
    {
        if (node->hasValue)
            return TriePathEntry::terminal(node->prefixLen(), node->value, 0);

        int branch = node->nextBranch();
        auto branches = node->branches(area, node.offset);

        return TriePathEntry::nonTerminal(
                node->prefixLen() + node->numBits,
                branches[branch].child(),
                0,
                branches[branch].size);
    }

    static TriePathEntry offTheEnd(const Node & node, MemoryAllocator & area)
    {
        size_t sz = size(node, area);
        return TriePathEntry::offTheEnd(sz);
    }



    static TriePathEntry
    lowerBound(
            const Node& node,
            MemoryAllocator& area,
            const KeyFragment& key)
    {
        KeyFragment prefix = node->prefix(area);
        int cpLen = key.commonPrefixLen(prefix);

        if (cpLen == key.bits)
            return TriePathEntry::offTheEnd(0);

        if (cpLen == prefix.bits) {

            int8_t leftoverBits = key.bits - cpLen;
            if (leftoverBits > node->numBits) {
                leftoverBits = node->numBits;

                int8_t branch = key.getBits(node->numBits, cpLen);
                // If false, find() should have gone through this node.
                ExcAssert(!node->isBranchMarked(branch));
            }

            int8_t branch = key.getBits(leftoverBits, cpLen);

            // If we don't have enough bits to get through the node then append
            // enough zeros to make the left-most branch that we can get with
            // that prefix.
            branch <<= std::max<int8_t>(node->numBits - leftoverBits, 0);

            branch = node->nextBranch(branch);
            if (branch >= 0)
                return TriePathEntry::offTheEnd(
                        sizeUpToBranch(node, area, branch));

            return offTheEnd(node, area);
        }

        if (key.getBits(1, cpLen) < prefix.getBits(1, cpLen))
            return TriePathEntry::offTheEnd(0);

        return offTheEnd(node, area);
    }

    static TriePathEntry
    upperBound(
            const Node& node,
            MemoryAllocator& area,
            const KeyFragment& key)
    {
        KeyFragment prefix = node->prefix(area);
        int cpLen = key.commonPrefixLen(prefix);

        if (cpLen == prefix.bits) {

            int8_t leftoverBits = key.bits - cpLen;

            if (!leftoverBits) return offTheEnd(node, area);

            if (leftoverBits > node->numBits) {
                leftoverBits = node->numBits;

                int8_t branch = key.getBits(node->numBits, cpLen);
                // If false, find() should have gone through this node.
                ExcAssert(!node->isBranchMarked(branch));
            }

            int8_t branch = key.getBits(leftoverBits, cpLen);

            // If we don't have enough bits to get through the node then append
            // enough ones to make the right-most branch that we can get with
            // that prefix.
            while (leftoverBits < node->numBits) {
                branch = (branch << 1) | 1;
                leftoverBits++;
            }

            branch = node->nextBranch(branch + 1);
            if (branch >= 0)
                return TriePathEntry::offTheEnd(
                        sizeUpToBranch(node, area, branch));

            return offTheEnd(node, area);
        }

        if (cpLen == key.bits)
            return offTheEnd(node, area);

        if (key.getBits(1, cpLen) < prefix.getBits(1, cpLen))
            return TriePathEntry::offTheEnd(0);

        return offTheEnd(node, area);
    }


    static KVList gatherKV(const Node & node, MemoryAllocator & area)
    {
        KVList kvs;

        KeyFragment prefix = node->prefix(area);

        if (node->hasValue)
            kvs.push_back({ prefix, node->value});

        auto branches = node->branches(area, node.offset);

        int8_t branch = -1;
        while ((branch = node->nextBranch(branch + 1)) >= 0) {
            KeyFragment key = prefix + KeyFragment(branch, node->numBits);
            kvs.push_back({ key, branches[branch].child() });
        }

        return kvs;
    }


    static bool isBranchingNode(const Node & node, MemoryAllocator & area)
    {
        return true;
    }


    /**************************************************************************/
    /* MUTABLE OPS                                                            */
    /**************************************************************************/

    static TriePath
    copyAndReplace(
            Node & oldNode,
            MemoryAllocator & area,
            const TriePathEntry & oldMatch,
            const TriePtr & replaceWith,
            TriePtr::State newState,
            GCList & gc)
    {
        Node newNode = copyNode(oldNode, area, newState, gc, true);
        return inplaceReplace(newNode, area, oldMatch, replaceWith, gc);
    }

    static TriePath
    inplaceReplace(
            Node & node,
            MemoryAllocator & area,
            const TriePathEntry & oldMatch,
            const TriePtr & replaceWith,
            GCList & gc)
    {
        validate(node, area);

        ExcAssert(oldMatch.isRelative());

        int8_t branch = getIndex(node, area, oldMatch.entryNum);
        ExcAssertGreaterEqual(branch, 0);

        if (replaceWith) node->markBranch(branch);
        if (!replaceWith) node->clearBranch(branch);

        auto branches = node->branches(area, node.offset);

        ssize_t oldSize = branches[branch].size;
        branches[branch].size = NodeOps::size(replaceWith, area, node.info);
        branches[branch].setChild(replaceWith);
        node->updateSize(branch, branches[branch].size - oldSize);

        validate(node, area);

        return TriePath(node, TriePathEntry::nonTerminal(
                        oldMatch.bitNum,
                        replaceWith,
                        oldMatch.entryNum,
                        true /* exact match */));
    }



    static TriePtr
    copyAndSetBranch(
            const Node & oldNode,
            MemoryAllocator & area,
            const KeyFragment& key,
            TriePtr newBranch,
            TriePtr::State newState,
            GCList & gc)
    {

        Node node = copyNode(oldNode, area, newState, gc, true);
        return inplaceSetBranch(node, area, key, newBranch, gc);
    }

    static TriePtr
    inplaceSetBranch(
            const Node & node,
            MemoryAllocator & area,
            const KeyFragment& key,
            TriePtr newBranch,
            GCList & gc)
    {
        validate(node, area);

        ExcAssertEqual(key.bits - node->prefixLen(), uint64_t(node->numBits));
        ExcAssertEqual(
                key.commonPrefixLen(node->prefix(area)), node->prefixLen());

        auto branches = node->branches(area, node.offset);
        int8_t branch = key.getBits(node->numBits, node->prefixLen());

        if (newBranch) node->markBranch(branch);
        if (!newBranch) node->clearBranch(branch);

        ssize_t oldSize = branches[branch].size;
        branches[branch].size = NodeOps::size(newBranch, area, node.info);
        branches[branch].setChild(newBranch);
        node->updateSize(branch, branches[branch].size - oldSize);

        validate(node, area);

        return node;
    }



    static TriePtr
    copyAndPrefixKeys(
            const Node& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            TriePtr::State newState,
            GCList& gc)
    {
        validate(node, area);

        gc.addOldNode(node);
        Node nodeCopy = copyNode(node, area, newState, gc, false);

        KeyFragment nodePrefix = prefix + node->prefix(area);
        nodeCopy->prefix_ = nodePrefix.allocRepr(area, node.info);

        validate(nodeCopy, area);
        return nodeCopy;
    }

    static TriePtr
    inplacePrefixKeys(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            GCList& gc)
    {
        validate(node, area);

        KeyFragment newPrefix = prefix + node->prefix(area);

        KeyFragment::deallocRepr(node->prefix_, area, node.info);
        node->prefix_ = newPrefix.allocRepr(area, node.info);

        validate(node, area);
        return node;
    }



    static TriePath
    copyAndReplaceValue(
            Node& oldNode,
            MemoryAllocator& area,
            const TriePathEntry& entry,
            uint64_t newValue,
            TriePtr::State newState,
            GCList& gc)
    {
        Node node = copyNode(oldNode, area, newState, gc, true);
        return inplaceReplaceValue(node, area, entry, newValue, gc);
    }

    static TriePath
    inplaceReplaceValue(
            Node& node,
            MemoryAllocator& area,
            const TriePathEntry& entry,
            uint64_t newValue,
            GCList& gc)
    {
        validate(node, area);

        ExcAssertEqual(entry.entryNum, 0);
        ExcAssert(node->hasValue);

        node->value = newValue;

        validate(node, area);

        return TriePath(node, TriePathEntry::terminal(
                        node->prefixLen(), newValue, 0, true));
    }



    static TriePtr
    changeState(
            Node & node,
            MemoryAllocator & area,
            TriePtr::State newState,
            GCList & gc)
    {
        validate(node, area);

        if (node.state == TriePtr::COPY_ON_WRITE) {
            gc.addOldNode(node);
            return copyNode(node, area, newState, gc, true);
        }

        std::vector<TriePtr> children(node->numBranches());

        // To maintain the IN_PLACE invariant that all parents of an IN_PLACE
        // node are also IN_PLACE, we recurse to all the children and change
        // their states first.

        auto branches = node->branches(area, node.offset);

        int8_t branch = -1;
        while ((branch = node->nextBranch(branch + 1)) >= 0) {
            if (branches[branch].child().state == newState) continue;

            children[branch] = NodeOps::changeState(
                    branches[branch].child(), area, node.info, newState, gc);
        }

        // Now that all the exception throwing code is done we can safely modify
        // the node.

        branch = -1;
        while ((branch = node->nextBranch(branch + 1)) >= 0) {
            if (branches[branch].child().state == newState) continue;

            branches[branch].setChild(children[branch]);
        }

        node.state = newState;

        validate(node, area);
        return node;
    }



    /**************************************************************************/
    /* INSERT                                                                 */
    /**************************************************************************/
    /** Implementation note: The node should only be modified once all exception
        throwing code is complete.
     */


    /** Insert the value into the middle of a node's prefix so we need to break
        up the prefix by creating a new parent node.

        There's one basic problem we have here: makeBranchingNode can end up
        prefixing the node we're trying to create. Since we can't modify the
        node until we've called makeBranchingNode (could throw), we can't adjust
        the prefix to avoid unfortunate accidents. All that to say that we can't
        in-place the modification of the node. Poo.

        So what follows is the safe and extremely boring implementation.
     */
    static TriePath
    insertBreakPrefix(
            const Node & node,
            MemoryAllocator & area,
            const KeyFragment & key,
            uint64_t value,
            TriePtr::State newState,
            GCList & gc)
    {
        // std::cerr << "DenseBr.insertBreakPrefix" << std::endl;

        KVList kvs = gatherKV(node, area);
        insertKv(kvs, { key, value });

        TriePtr subtree = makeBranchingNode(area, node.info, kvs, newState, gc);

        // It's easier to look up the key then try and figure out where it
        // ended up. We could also make makeBranchingNode return a path.
        return NodeOps::findKey(subtree, area, node.info, key);
    }


    /** Easiest insert case where we just plop it into the node's value. */
    static TriePath
    insertIntoValue(
            Node & node,
            MemoryAllocator & area,
            const KeyFragment& nodePrefix,
            uint64_t value)
    {
        // std::cerr << "DenseBr.insertIntoValue" << std::endl;

        ExcAssert(!node->hasValue);

        node->hasValue = value;
        node->value = value;

        validate(node, area);

        return TriePath(node, TriePathEntry::terminal(
                        nodePrefix.bits, value, 0, true));
    }


    /** Here we need to insert the value into the middle of the branches which
        is somewhat awkward. Note that this function won't modify the current
        node because that would be too complicated.

        We'll break the node into one parent node and multiple child nodes
        where the number of child nodes and the arity of those nodes will depend
        on how many bits are on each side of the break.

        For example, if the current node has numBits == 4 and the break happens
        on bit 3 then we will end up with a parent that has up to 8 children who
        in turn have up to 2 children.

        This is where you'll need to put on your bit twiddling hat.
    */
    static TriePath
    insertBreakBranches(
            const Node & node,
            MemoryAllocator & area,
            const KeyFragment & key,
            uint64_t value,
            const KeyFragment & nodePrefix,
            TriePtr::State newState,
            GCList & gc)
    {
        // std::cerr << "DenseBr.insertBreakBranches" << std::endl;

        auto branches = node->branches(area, node.offset);

        int32_t parentBits = key.bits - nodePrefix.bits;
        int32_t childBits = node->numBits - parentBits;
        ExcAssertGreater(parentBits, 0);
        ExcAssertGreater(childBits, 0);

        int8_t valueBranch = key.getBits(parentBits, nodePrefix.bits);
        TriePtr valuePtr = {};

        KVList parentKvs;
        if (node->hasValue)
            parentKvs.push_back({ nodePrefix, node->value });

        for (size_t pBranch = 0; pBranch < (1 << parentBits); ++pBranch) {

            KVList childKvs;
            if (valueBranch == pBranch)
                childKvs.push_back({ KeyFragment(), value });

            // We want to iterate over all branches prefixed by the pBranch
            // bits. To get that range we zero out the child bits of that
            // parent branch and of the next parent branch.
            int8_t branch = (pBranch << childBits) - 1;
            int8_t end = (pBranch + 1) << childBits;

            while((branch = node->nextBranch(branch + 1, end)) >= 0) {
                uint64_t mask = (1ULL << childBits) -1;
                KeyFragment kf(branch & mask, childBits);

                childKvs.push_back({ kf, branches[branch].child() });
            }

            if (childKvs.empty()) continue;

            TriePtr childPtr = makeNode(area, node.info, childKvs, newState, gc);

            KeyFragment childKey =
                nodePrefix + KeyFragment(pBranch, parentBits);

            parentKvs.push_back({ childKey, childPtr });

            if (valueBranch == pBranch)
                valuePtr = parentKvs.back().getPtr();
        }

        ExcAssert(valuePtr);

        // We could overwrite the current node but that would be somewhat
        // complicated so, to save my sanity, we'll just use the usual
        // mechanism.
        TriePtr parent = makeBranchingNode(
                area, node.info, parentKvs, newState, gc);

        // It's easier to look up the key then try and figure out where it
        // ended up. We could also make makeBranchingNode return a path.
        return NodeOps::findKey(parent, area, node.info, key);
    }


    /** The value doesn't even go into our node so just pass the puck along to a
        children node.
     */
    static TriePath
    insertIntoChild(
            Node & node,
            MemoryAllocator & area,
            const KeyFragment & key,
            uint64_t value,
            const KeyFragment & nodePrefix,
            TriePtr::State newState,
            GCList & gc)
    {
        // std::cerr << "DenseBr.insertIntoChild" << std::endl;

        auto branches = node->branches(area, node.offset);

        int8_t branch = key.getBits(node->numBits, nodePrefix.bits);
        TriePtr child = branches[branch].child();

        KeyFragment suffix = key.suffix(nodePrefix.bits + node->numBits);
        TriePath inserted = addLeaf(
                area, node.info, child, suffix, value, newState, gc);

        node->markBranch(branch);
        branches[branch].setChild(inserted.root());
        branches[branch].size++;
        node->updateSize(branch, 1);

        TriePathEntry ours = TriePathEntry::nonTerminal(
                nodePrefix.bits + node->numBits,
                branches[branch].child(),
                sizeUpToBranch(node, area, branch),
                true);

        validate(node, area);

        return TriePath(node, ours, inserted);
    }



    static TriePath
    copyAndInsertLeaf(
            const Node & node,
            MemoryAllocator & area,
            const KeyFragment & key,
            uint64_t value,
            TriePtr::State newState,
            GCList & gc)
    {
        validate(node, area);

        KeyFragment nodePrefix = node->prefix(area);
        int32_t cpLen = nodePrefix.commonPrefixLen(key);

        if (cpLen < nodePrefix.bits) {
            // insertBreakPrefix doesn't touch the node.
            return insertBreakPrefix(node, area, key, value, newState, gc);
        }


        if (cpLen == key.bits) {
            Node nodeCopy = copyNode(node, area, newState, gc, true);
            return insertIntoValue(nodeCopy, area, nodePrefix, value);
        }


        int32_t leftoverBits = key.bits - nodePrefix.bits;
        ExcAssertGreater(leftoverBits, 0);

        if (leftoverBits < node->numBits) {
            // insertBreakBranches will not modify the existing node (too
            // complicated) so no need to make a copy.
            TriePath path = insertBreakBranches(
                    node, area, key, value, nodePrefix, newState, gc);

            return path;
        }

        Node nodeCopy = copyNode(node, area, newState, gc, true);
        return insertIntoChild(
                nodeCopy, area, key, value, nodePrefix, newState, gc);
    }

    static TriePath
    inplaceInsertLeaf(
            Node & node,
            MemoryAllocator & area,
            const KeyFragment & key,
            uint64_t value,
            GCList & gc)
    {
        validate(node, area);

        KeyFragment nodePrefix = node->prefix(area);
        int32_t cpLen = nodePrefix.commonPrefixLen(key);

        if (cpLen < nodePrefix.bits) {
            return insertBreakPrefix(
                    node, area, key, value, TriePtr::IN_PLACE, gc);
        }


        if (cpLen == key.bits)
            return insertIntoValue(node, area, nodePrefix, value);


        int32_t leftoverBits = key.bits - nodePrefix.bits;
        ExcAssertGreater(leftoverBits, 0);

        if (leftoverBits < node->numBits) {
            TriePath path = insertBreakBranches(
                    node, area, key, value, nodePrefix, TriePtr::IN_PLACE, gc);

            // insertBreakBranches will not modify the existing node (too
            // complicated) so we need to get rid of the current node.
            gc.deallocateNewNode(node);
            return path;
        }

        return insertIntoChild(
                node, area, key, value, nodePrefix, TriePtr::IN_PLACE, gc);
    }


    /**************************************************************************/
    /* REMOVE                                                                 */
    /**************************************************************************/


    /** If we're down to only a couple of values then we can simplify this whole
        subtree into a single terminal value.

        \todo Instead of simplifying at 3 remaining value we may want to raise
        the treshold as an automated defragmentation-like mechanism.
     */
    static TriePtr
    removeSimplifySubtree(
            const Node & node,
            MemoryAllocator & area,
            const KeyFragment & key,
            const KeyFragment & nodePrefix,
            TriePtr::State newState,
            GCList & gc)
    {
        // std::cerr << "NODE.remove-simplify-subtree" << std::endl;

        size_t totalSize = size(node, area);
        ExcAssertGreater(totalSize, 0);

        KVList kvs;

        // Gather all the values and create a node out of them.

        auto onValue = [&](const KeyFragment& kf, uint64_t value) {
            if (kf == key) return;
            kvs.push_back({ kf, value });
        };
        NodeOps::forEachValue(node, area, node.info, onValue);
        ExcAssertEqual(kvs.size(), totalSize - 1);

        TriePath path = makeMultiLeafNode(
                area, node.info, kvs, kvs.size(), newState, gc);

        // Cleanup the old nodes. Must be done only after all exception throwing
        // code is over with.

        std::vector<TriePtr> toDealloc;
        auto onNode = [&](const KeyFragment&, TriePtr child) {
            if (child.state == TriePtr::COPY_ON_WRITE)
                gc.addOldNode(child);
            else toDealloc.push_back(child);
        };
        NodeOps::forEachNode(node, area, node.info, onNode);

        for (TriePtr child : toDealloc) gc.deallocateNewNode(child);

        return path.root();
    }

    /** If we're down to one last branch then we can simplify the node by
        pushing the node' prefix down to the last child node.
     */
    static TriePtr
    removeSimplifyNode(
            const Node & node,
            MemoryAllocator & area,
            const KeyFragment & key,
            const KeyFragment & nodePrefix,
            TriePtr::State newState,
            GCList & gc)
    {
        // std::cerr << "NODE.remove-simplify-node" << std::endl;

        // First case: only the value left so make a terminal node.
        /* Should never happen because we should have hit simplifySubtree before
           we get here. While this is generally true, the merge the 3-way merge
           isn't always able to properly simplify nodes so it's better to have a
           safeguard.
        */
        if (key.bits != nodePrefix.bits && node->hasValue) {
            return makeLeaf(
                    area, node.info, nodePrefix, node->value, newState, gc)
                .root();
        }

        // Second case: push the prefix down.

        int8_t branch = -1;
        auto branches = node->branches(area, node.offset);

        // We're removing the value so find the remaining branch.
        if (key.bits == nodePrefix.bits) {
            ExcAssertEqual(node->countBranches(), 1);
            branch = node->nextBranch(branch + 1);
        }

        // We're removing the last branch so find the other branch.
        else {
            ExcAssertEqual(node->countBranches(), 2);

            int8_t branchToRemove = key.getBits(node->numBits, nodePrefix.bits);
            ExcAssert(node->isBranchMarked(branchToRemove));
            gc.addOldNode(branches[branchToRemove].child());

            while ((branch = node->nextBranch(branch + 1)) == branchToRemove);
        }

        ExcAssertGreaterEqual(branch, 0);

        KeyFragment prefix = nodePrefix + KeyFragment(branch, node->numBits);
        return NodeOps::prefixKeys(
                branches[branch].child(), area, node.info, prefix, newState, gc);

    }

    /** Checks whether we should apply the removeSimplifyNode operation. The
        idea is to count the number of entries in the node (value + number of
        branches) and check if the remove will bring to number of entries down
        to 1.
     */
    static bool isRemoveSimplifyNode(
            const Node& node, MemoryAllocator& area, const KeyFragment& key)
    {
        unsigned total = node->hasValue;
        total += node->countBranches();

        if (total > 2) return false;

        // If we remove the value then we'll be down to a single branch.
        if (key.bits == node->prefixLen()) {
            ExcAssert(node->hasValue);
            return true;
        }

        int8_t branch = key.getBits(node->numBits, node->prefixLen());
        ExcAssert(node->isBranchMarked(branch));

        auto branches = node->branches(area, node.offset);

        // If we remove this branch then we'll be down to a single value/branch.
        return branches[branch].size == 1;
    }


    /** Remove the node's value which is all kind of easy. */
    static TriePtr
    removeNodeValue(
            Node & node,
            MemoryAllocator & area,
            const KeyFragment & key,
            const KeyFragment & nodePrefix,
            GCList & gc)
    {
        // std::cerr << "NODE.remove-node-value" << std::endl;
        ExcAssert(node->hasValue);
        ExcAssertEqual(nodePrefix, key);
        node->hasValue = false;

        validate(node, area);

        return node;
    }

    /** The value to remove is somewhere beneath us so pass the puck down. */
    static TriePtr
    removeFromChild(
            Node & node,
            MemoryAllocator & area,
            const KeyFragment & key,
            const KeyFragment & nodePrefix,
            TriePtr::State newState,
            GCList & gc)
    {
        // std::cerr << "NODE.remove-from-child" << std::endl;

        ExcAssertGreaterEqual(key.bits, nodePrefix.bits + node->numBits);
        KeyFragment suffix = key.suffix(nodePrefix.bits + node->numBits);

        int8_t branch = key.getBits(node->numBits, nodePrefix.bits);
        ExcAssert(node->isBranchMarked(branch));

        auto branches = node->branches(area, node.offset);

        TriePtr newChild = NodeOps::removeLeaf(
                branches[branch].child(), area, node.info, suffix, newState, gc);

        branches[branch].setChild(newChild);
        branches[branch].size--;
        node->updateSize(branch, -1);

        if (!branches[branch].size) {
            node->clearBranch(branch);
            ExcAssertEqual(branches[branch].child(), TriePtr());
        }

        ExcAssertEqual(
                branches[branch].size,
                NodeOps::size(newChild, area, node.info));
        ExcAssertEqual(newChild == TriePtr(), branches[branch].size == 0);

        validate(node, area);

        return node;
    }


    /** Is set to the maximum number of values that our smallest terminal node
        type can hold. In other words, regardless of the situation, calling
        makeMultiLeafNode with this many values should return a terminal node.
     */
    enum { SIMPLIFICATION_THRESHOLD = 3 };


    static TriePtr
    copyAndRemoveLeaf(
            const Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            TriePtr::State newState,
            GCList& gc)
    {
        validate(node, area);

        gc.addOldNode(node);

        KeyFragment nodePrefix = node->prefix(area);
        ExcAssertEqual(nodePrefix.commonPrefixLen(key), nodePrefix.bits);

        if (size(node, area) - 1 <= SIMPLIFICATION_THRESHOLD) {
            // No need to copy the node because simplify creates an entirely new
            // node without touching the current one.
            return removeSimplifySubtree(
                    node, area, key, nodePrefix, newState, gc);
        }

        if (isRemoveSimplifyNode(node, area, key)) {
            // No need to copy the node because simplify creates an entirely new
            // node without touching the current one.
            return removeSimplifyNode(
                    node, area, key, nodePrefix, newState, gc);
        }

        if (nodePrefix.bits == key.bits) {
            Node nodeCopy = copyNode(node, area, newState, gc, true);
            return removeNodeValue(
                    nodeCopy, area, key, nodePrefix, gc);
        }

        Node nodeCopy = copyNode(node, area, newState, gc, true);
        return removeFromChild(nodeCopy, area, key, nodePrefix, newState, gc);
    }


    static TriePtr
    inplaceRemoveLeaf(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            GCList& gc)
    {
        validate(node, area);

        KeyFragment nodePrefix = node->prefix(area);
        ExcAssertEqual(nodePrefix.commonPrefixLen(key), nodePrefix.bits);

        if (size(node, area) - 1 <= SIMPLIFICATION_THRESHOLD) {
            TriePtr newNode = removeSimplifySubtree(
                    node, area, key, nodePrefix, TriePtr::IN_PLACE, gc);

            // when simplifying we create an entirely new node so we have the
            // deallocate the current one.
            gc.deallocateNewNode(node);

            return newNode;
        }

        if (isRemoveSimplifyNode(node, area, key)) {
            TriePtr newNode = removeSimplifyNode(
                    node, area, key, nodePrefix, TriePtr::IN_PLACE, gc);

            // when simplifying we create an entirely new node so we have the
            // deallocate the current one.
            gc.deallocateNewNode(node);

            return newNode;
        }

        if (nodePrefix.bits == key.bits)
            return removeNodeValue(node, area, key, nodePrefix, gc);

        return removeFromChild(
                node, area, key, nodePrefix, TriePtr::IN_PLACE, gc);
    }


    /**************************************************************************/
    /* DEBUGGING                                                              */
    /**************************************************************************/

    static NodeStats
    stats(const Node & node, MemoryAllocator & area)
    {
        return node->stats();
    }

    static void
    dump(   const Node & node,
            MemoryAllocator & area,
            int indent,
            int maxDepth,
            std::ostream & stream)
    {
        using namespace std;

        KeyFragment nodePrefix = node->prefix(area);

        stream << ((TriePtr)node);
        stream << " branches: " << static_cast<uint64_t>(node->numBranches());
        if (!nodePrefix.empty())
            stream << " prefix: " << nodePrefix;
        if (node->hasValue)
            stream << " value: " << node->value;

        string id(indent, ' ');

        auto branches = node->branches(area, node.offset);

        int8_t branch = -1;
        while ((branch = node->nextBranch(branch + 1)) >= 0) {
            stream << endl << id << static_cast<uint64_t>(branch)
                << ": size " << branches[branch].size << ": ";
            NodeOps::dump(
                    branches[branch].child(), area, node.info,
                    indent + 4, maxDepth, stream);
        }
    }

    static std::string
    print(TriePtr ptr, MemoryAllocator * area, GcLock::ThreadGcInfo * info)
    {
        std::stringstream ss;
        ss << "DenseBr: offset " << Node::decodeOffset(ptr.data);
        if (!area) return ss.str();

        Node node = encode(ptr, *area, info);

        KeyFragment nodePrefix = node->prefix(*area);

        ss << " branches: " << static_cast<uint64_t>(node->numBranches());
        if (!nodePrefix.empty())
            ss << " prefix: " << nodePrefix.print();
        if (node->hasValue)
            ss << " value" << node->value;

        auto branches = node->branches(*area, node.offset);

        int8_t branch = -1;
        ss << " sizes: [";
        while ((branch = node->nextBranch(branch + 1)) >= 0) {
            ss << std::to_string(static_cast<uint64_t>(branch)) << ":";
            ss << std::to_string(branches[branch].size) << ", ";
        }
        ss << "]";



        return ss.str();
    }

    static void validate(
            const Node& node, MemoryAllocator& area, bool checkPrefix = true)
    {
        if (!trieDebug) return;

        Profiler prof("dbn.validate");

        // Keep this disabled if we're working with the 3-way merge. The issue
        // is that the merge can't always perfectly restructure the tree.
        // ExcAssertGreater(node->hasValue + node->countBranches(), 1);

        if (checkPrefix) ExcAssert(node->prefix_.isValid());

        if (node->numBits == 1)
            ExcAssertEqual(node->storageOffset, 0);
        else ExcAssertGreater(node->storageOffset, 0);

        auto branches = node->branches(area, node.offset);

        for (uint8_t branch = 0; branch < node->numBranches(); ++branch) {
            if (node->isBranchMarked(branch)) {
                ExcAssertGreater(branches[branch].size, 0);
                ExcAssertGreater(branches[branch].child_, 0);
            }
            else {
                ExcAssertEqual(branches[branch].size, 0);
                ExcAssertEqual(branches[branch].child_, 0);
            }
        }

        if (!node->isInline()) {
            uint8_t buckets = std::max<uint8_t>(1, node->numBranches() / 4);

            for (uint8_t bucket = 0; bucket < buckets; ++bucket) {
                size_t sum = 0;

                uint8_t start = bucket * 4;
                uint8_t end = std::min<uint8_t>(
                        (bucket + 1) * 4, node->numBranches());

                for (uint8_t branch = start; branch < end; ++branch)
                    sum += branches[branch].size;

                ExcAssertEqual(node->sizeIndex[bucket], sum);
            }
        }
    }

};

} // namespace MMap
} // Datacratic

#endif // __mmap__trie_dense_branching_node_h__
