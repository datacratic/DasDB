/* mmap_trie_binary_node.h                                      -*- C++ -*-
   Jeremy Barnes, 16 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the null trie node.
*/

#ifndef __mmap__trie_binary_node_h__
#define __mmap__trie_binary_node_h__


#include <boost/static_assert.hpp>
#include <boost/tuple/tuple.hpp>
#include "jml/arch/format.h"
#include "sync_stream.h"

namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* BINARY INTERNAL NODE                                                      */
/*****************************************************************************/

/** A node that matches a prefix, then one bit.  Standard binary trie node.
*/

struct BinaryNodeRepr {
    BinaryNodeRepr()
        : hasValue(false), value(0)
    {
        children[0] = children[1] = TriePtr();
        childSizes[0] = childSizes[1] = 0;
    }

    KeyFragmentRepr prefix_;     ///< Node key prefix

    uint8_t padding_;

    bool hasValue;          ///< Does the node have a value?
    uint64_t value;         ///< Value at the node, if it has one

    TriePtr children[2];    ///< Child nodes; for each of the bits
    uint64_t childSizes[2]; ///< Number of values under each child

    KeyFragment prefix(MemoryAllocator& area, GcLock::ThreadGcInfo* info) const
    {
        return KeyFragment::loadRepr(prefix_, area, info);
    }

    int32_t prefixLen() const { return prefix_.bits; }


    NodeStats stats() const
    {
        NodeStats stats;

        stats.nodeCount = 1;
        stats.values = hasValue;
        stats.totalBytes = sizeof(BinaryNodeRepr);
        stats.bookeepingBytes = sizeof(children) + sizeof(hasValue);
        stats.unusedBytes = sizeof(padding_);
        stats.avgBranchingBits = stats.maxBranchingBits = 1;
        stats.maxBits = prefix_.bits + 1;
        stats.avgBits = prefix_.bits * hasValue;

        for (unsigned branch = 0; branch < 2; ++branch) {
            if(!childSizes) stats.unusedBytes += sizeof(uint64_t);
            else {
                stats.branches++;
                stats.avgBits += prefix_.bits + 1;
            }
        }

        stats.avgBits /= hasValue + stats.branches;

        stats += prefix_.nodeStats();

        return stats;
    }

} JML_ALIGNED(cache_line);

static_assert(
        sizeof(BinaryNodeRepr) <= cache_line,
        "Must fit into a cache line.");


/******************************************************************************/
/* BINARY NODE OPS                                                            */
/******************************************************************************/

// Operations structure
struct BinaryNodeOps :
        public IndirectOpsBase<BinaryNodeRepr, BinaryBranch>
{

    /** Safe version of allocCopy for a binary node that works around all the
        issues with copying a KeyFragment.

        Note that copyPrefix should passed as true if you don't plan on changing
        the prefix of the node. Otherwise the prefix will be invalid when this
        function returns.

        Note that we don't copy the prefix because it could either lead to the
        prefix of the original node to be deleted if a resize exception is
        thrown or an accidental leak if the copied prefix is deallocated before
        being changed.
    */
    static Node
    copyNode(
            const Node& oldNode,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            TriePtr::State newState,
            GCList& gc,
            bool copyPrefix = false)
    {
        Node node = allocCopy(area, info, *oldNode, newState);
        node->prefix_ = KeyFragmentRepr();
        gc.addNewNode(node.offset, node);

        if (copyPrefix)
            node->prefix_ = KeyFragment::copyRepr(oldNode->prefix_, area, info);

        return node;
    }

    static void deallocate(const Node& node, MemoryAllocator& area)
    {
        KeyFragment::deallocRepr(node->prefix_, area, node.info);
        deallocNode(node, area);
    }

    static size_t directMemUsage(const Node & node, MemoryAllocator & area)
    {
        size_t size = sizeof(BinaryNodeRepr);
        size += node->prefix_.directMemUsage();
        return size;
    }

    static TriePtr
    allocBranchingNode(
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& prefix,
            const TriePtr& branch0, const TriePtr& branch1,
            bool hasValue, uint64_t value,
            TriePtr::State newState,
            GCList& gc)
    {
        Node node = allocEmpty(area, info, newState);
        gc.addNewNode(node.offset, node);

        node->prefix_ = prefix.allocRepr(area, info);
        node->hasValue = hasValue;
        if (hasValue)
            node->value = value;

        node->children[0] = branch0;
        node->childSizes[0] = NodeOps::size(branch0, area, info);

        node->children[1] = branch1;
        node->childSizes[1] = NodeOps::size(branch1, area, info);

        return node;
    }

    static TriePtr 
    allocMultiLeaf(
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            KVList kvs,
            TriePtr::State newState,
            GCList& gc)
    {
        KeyFragment prefix = kvs.commonPrefix();

        // Remove the prefix from each of them
        for (auto it = kvs.begin(), end = kvs.end(); it != end; ++it)
            it->key = it->key.suffix(prefix.bits);

        // we now have:
        // 1.  possibly, a zero-length key with a value;
        // 2.  one or more keys starting with a zero bit;
        // 3.  one or more keys starting with a one bit
        // These all go into a binary node

        Node node = allocEmpty(area, info, newState);
        gc.addNewNode(node.offset, node);

        node->prefix_ = prefix.allocRepr(area, info);

        // Is there a value directly at the node?
        int startOfZeros = 0;
        if (kvs.front().key.empty()) {
            startOfZeros = 1;
            node->hasValue = true;
            node->value = kvs.front().value;
        }

        // Find where the first remaining bit changes from 0 to 1
        int startOfOnes = startOfZeros;
        for (; startOfOnes < kvs.size() && kvs[startOfOnes].key.getBits(1) == 0;
             ++startOfOnes) ;

        if (startOfZeros == 0) {
            ExcCheckNotEqual(startOfOnes, kvs.size(),
                    "Logic error: change bit mis-handled");
            ExcCheckNotEqual(startOfOnes, startOfZeros,
                    "Logic error: change bit mis-handled");
        }

        // Remove the first bit from each
        for (unsigned i = startOfZeros; i < kvs.size(); ++i)
            kvs[i].key.removeBits(1);

        uint64_t size1 = startOfOnes - startOfZeros;
        uint64_t size2 = kvs.size() - startOfOnes;

        KVList zerosKvs(kvs.begin() + startOfZeros, kvs.begin() + startOfOnes);
        node->childSizes[0] = size1;
        node->children[0] = makeMultiLeafNode(
                area, info, zerosKvs, size1, newState, gc).root();

        KVList onesKvs(kvs.begin() + startOfOnes, kvs.end());
        node->childSizes[1] = size2;
        node->children[1] = makeMultiLeafNode(
                area, info, onesKvs, size2, newState, gc).root();

        return node;
    }

    static size_t size(const Node & node, MemoryAllocator & area)
    {
        return node->hasValue + node->childSizes[0] + node->childSizes[1];
    }

    static void
    forEachEntry(const Node & node, MemoryAllocator & area,
                 const NodeOps::OnEntryFn & fn, int what)
    {
        KeyFragment nodePrefix = node->prefix(area, node.info);

        if ((what & NodeOps::VALUE) && node->hasValue) {
            fn(nodePrefix, {}, node->value);
        }

        if (what & NodeOps::CHILD) {
            for (unsigned i = 0;  i < 2;  ++i) {
                TriePtr child = node->children[i];
                if (!child) continue;

                fn(nodePrefix + KeyFragment(i, 1), child, 0);
            }
        }
    }

    static TriePathEntry
    matchKey(const Node & node, MemoryAllocator & area, KeyFragment key)
    {
        using namespace std;
        //cerr << "binary match: key " << key << endl;
        //cerr << "node->prefix = " << node->prefix << endl;
        size_t sz = size(node, area);

        // Consume the prefix, or fail if we can't
        KeyFragment nodePrefix = node->prefix(area, node.info);

        if (!nodePrefix.empty()) {
            if (!key.consume(nodePrefix))
                return TriePathEntry::offTheEnd(sz);
        }

        //cerr << "consumed key" << endl;

        if (key.empty()) {
            //cerr << "matched prefix " << node->prefix << endl;

            // Prefix matched but is now empty... we may have a value
            if (node->hasValue)
                return TriePathEntry::terminal(nodePrefix.bits,
                                             node->value,
                                             0);
            else return TriePathEntry::offTheEnd(sz);
        }
        
        // 3.  If not... consume the next bit to find which branch to
        //     take...
        int branch = key.getBits(1);

        //cerr << "branch " << branch << endl;

        if (node->children[branch])
            return TriePathEntry::nonTerminal(nodePrefix.bits + 1,
                                              node->children[branch],
                                              branch * node->childSizes[0]
                                                  + node->hasValue,
                                              node->childSizes[branch]);

        return TriePathEntry::offTheEnd(sz);
    }

    static int getIndex(const Node & node, size_t valueNum)
    {
        if (node->hasValue && valueNum == 0)
            return -1;  // internal value
        valueNum -= node->hasValue;
        int branch = valueNum >= node->childSizes[0];
        return branch;
    }

    static TriePathEntry
    matchIndex(const Node & node, MemoryAllocator & area,
               size_t targetValueNumber)
    {
        int branch = getIndex(node, targetValueNumber);

        int32_t prefixBits = node->prefixLen();

        if (branch == -1)
            return TriePathEntry::terminal(prefixBits, node->value, 0);

        return TriePathEntry::nonTerminal(
                prefixBits + 1,
                node->children[branch],
                branch * node->childSizes[0] + node->hasValue,
                node->childSizes[branch]);
    }

    static KeyFragment
    extractKey(const Node & node, MemoryAllocator & area, size_t valueIndex)
    {
        int branch = getIndex(node, valueIndex);
        if (branch == -1) return node->prefix(area, node.info);
        return node->prefix(area, node.info) + KeyFragment(branch, 1);
    }
        
    static TriePathEntry first(const Node & node, MemoryAllocator & area)
    {
        if (node->hasValue)
            return TriePathEntry::terminal(node->prefixLen(), node->value, 0);
        
        for (unsigned i = 0;  i < 2;  ++i)
            if (node->children[i])
                return TriePathEntry::nonTerminal(node->prefix_.bits + 1,
                                                  node->children[i],
                                                  0, node->childSizes[i]);
        
        size_t sz = size(node, area);
        return TriePathEntry::offTheEnd(sz);  // index is 2
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
        KeyFragment prefix = node->prefix(area, node.info);
        int cpLen = key.commonPrefixLen(prefix);

        if (cpLen == key.bits)
            return TriePathEntry::offTheEnd(0);

        if (cpLen == prefix.bits) {
            int branch = key.getBits(1, cpLen);

            // If true, find() should have gone through this node.
            ExcAssertEqual(node->childSizes[branch], 0);

            if (branch) return offTheEnd(node, area);
            return TriePathEntry::offTheEnd(0);
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
        KeyFragment prefix = node->prefix(area, node.info);
        int cpLen = key.commonPrefixLen(prefix);

        if (cpLen == key.bits)
            return offTheEnd(node, area);

        if (cpLen == prefix.bits) {
            int branch = key.getBits(1, cpLen);
            ExcAssertEqual(node->childSizes[branch], 0);

            if (branch) return offTheEnd(node, area);
            return TriePathEntry::offTheEnd(0);
        }

        if (key.getBits(1, cpLen) < prefix.getBits(1, cpLen))
            return TriePathEntry::offTheEnd(0);

        return offTheEnd(node, area);
    }
    
    static KVList gatherKV(const Node & node, MemoryAllocator & area)
    {
        KVList kvs;

        KeyFragment prefix = node->prefix(area, node.info);

        if (node->hasValue)
            kvs.push_back({ prefix, node->value});

        for (int i = 0; i < 2; ++i) {
            if (!node->childSizes[i]) continue;
            kvs.push_back({ prefix + KeyFragment(i, 1), node->children[i]});
        }

        return kvs;
    }

    static bool isBranchingNode(const Node & node, MemoryAllocator & area)
    {
        return true;
    }

    static TriePath
    copyAndReplace(Node & oldNode,
                   MemoryAllocator & area,
                   const TriePathEntry & oldMatch,
                   const TriePtr & replaceWith,
                   TriePtr::State newState,
                   GCList & gc)
    {
        Node newNode = copyNode(oldNode, area, oldNode.info, newState, gc, true);
        return inplaceReplace(newNode, area, oldMatch, replaceWith, gc);
    }

    static TriePath
    inplaceReplace(Node & node,
                   MemoryAllocator & area,
                   const TriePathEntry & oldMatch,
                   const TriePtr & replaceWith,
                   GCList & gc)
    {
        ExcAssert(oldMatch.isRelative());

        int i = getIndex(node, oldMatch.entryNum);
        ExcCheck(i == 0 || i == 1, "binary inplaceReplace: invalid index");

        node->children[i] = replaceWith;
        node->childSizes[i] = NodeOps::size(replaceWith, area, node.info);

        return TriePath(node, TriePathEntry::nonTerminal(
                        oldMatch.bitNum, replaceWith, oldMatch.entryNum,
                        true /* exact match */));
    }


    static TriePtr
    copyAndSetBranch(
            const Node & oldNode,
            MemoryAllocator & area,
            const KeyFragment& key, TriePtr newBranch,
            TriePtr::State newState,
            GCList & gc)
    {
        Node node = copyNode(oldNode, area, oldNode.info, newState, gc, true);
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
        ExcAssertEqual(key.bits - node->prefixLen(), 1);
        ExcAssertEqual(
                key.commonPrefixLen(node->prefix(area, node.info)),
                node->prefixLen());

        int branch = key.getBits(1, node->prefixLen());
        node->children[branch] = newBranch;
        node->childSizes[branch] = NodeOps::size(newBranch, area, node.info);
        return node;
    }


    /** \todo This function is a mess and should probably be rewritten.

        The problem is that we're trying to support both inplace and CoW
        modifications while having to be extremely careful about resize
        exceptions.

        Resize exceptions forces us to make sure we don't modify the given node
        before we've done all the code that could possibly trigger a resize. On
        the other hand, CoW nodes require we copy KFs when we're reusing a KF
        and in place nodes require that we deallocate old KFs when we're
        changing it's KF.

        To make this even more fun, the KF allocation code can actually trigger
        a resize as well. This is essentially a guaranteed receipie for leaks
        because our gc list doesn't support KF and the only way to make sure the
        KF gets deallocated on resize is to shove it in one of the nodes... The
        nodes we're not supposed to modify till we're done with the exception
        throwing code...

        While I think the current implementation manages to balance all this
        out, I still think this function needs to be reworked. It's just way too
        easy to break it by doing fairly benign modifications. These types of
        error usually show up as extra or missing bits in a subset of keys in
        the trie and this function isn't an obvious source for these problems...

        \todo See DenseBranchingNode for a much better pattern to write this
        function.
    */
    static TriePath
    insertLeaf(
            Node & node,
            KeyFragment nodePrefix,
            MemoryAllocator & area,
            const KeyFragment & key,
            uint64_t value,
            TriePtr::State oldState,
            TriePtr::State newState,
            GCList & gc)
    {
        // First, we look to see where we are with respect to prefixes.
        KeyFragment commonPrefix = nodePrefix.commonPrefix(key);

        if (commonPrefix == nodePrefix) {
            // Prefix hasn't and won't change

            KeyFragment suffix = key.suffix(commonPrefix.bits);

            // Add the value to our node.
            if (suffix.empty()) {
                ExcCheck(!node->hasValue,
                    "adding an extra value to something that already has one");

                if (oldState == TriePtr::COPY_ON_WRITE)
                    node->prefix_ = nodePrefix.allocRepr(area, node.info);

                node->value = value;
                node->hasValue = true;
                return TriePath(node, TriePathEntry::terminal(
                                nodePrefix.bits, value, 
                                0 /* skipped */, true /* exactMatch */));
            }

            // Add the value to one of the node's children.
            int branch = suffix.removeBits(1);
            TriePtr child = node->children[branch];

            TriePath inserted = addLeaf(
                    area, node.info, child, suffix, value, newState, gc);

            if (oldState == TriePtr::COPY_ON_WRITE)
                node->prefix_ = nodePrefix.allocRepr(area, node.info);

            node->children[branch] = inserted.root();
            node->childSizes[branch] += 1;

            ExcAssertEqual(node->childSizes[branch],
                    NodeOps::size(inserted.root(), area, node.info));

            size_t skipped = branch * node->childSizes[0] + node->hasValue;

            // Construct the path to this node
            TriePathEntry ours = TriePathEntry::nonTerminal(
                    nodePrefix.bits + 1, node->children[branch],
                    skipped, true /* exact match */);
            return TriePath(node, ours, inserted);
        }
        else {
            // The value goes between the node and its parent.  We needed to
            // reduce the prefix in length to get the new node means that we
            // need to allocate another new node

            nodePrefix = nodePrefix.suffix(commonPrefix.bits);

            Node node2 = allocEmpty(area, node.info, newState);
            gc.addNewNode(node2.offset, node2);
            KeyFragment node2Prefix = commonPrefix;

            int i1 = nodePrefix.removeBits(1);
            node2->children[i1] = node;
            node2->childSizes[i1] = size(node, area);

            KeyFragment suffix2 = key.suffix(commonPrefix.bits);

            // Save the keys before we return.
            node2->prefix_ = node2Prefix.allocRepr(area, node.info);

            if (suffix2.empty()) {
                if (oldState == TriePtr::IN_PLACE)
                    KeyFragment::deallocRepr(node->prefix_, area, node.info);
                node->prefix_ = nodePrefix.allocRepr(area, node.info);

                // Needs to go in the value
                node2->hasValue = true;
                node2->value = value;
                return TriePath(node2, TriePathEntry::terminal(
                                key.bits, value, 0, true /* exact match */));
            }

            int i2 = suffix2.removeBits(1);
            ExcCheckNotEqual(i1, i2, "Two children have the same suffix");
            
            TriePath inserted = makeLeaf(
                    area, node.info, suffix2, value, newState, gc);

            if (oldState == TriePtr::IN_PLACE)
                KeyFragment::deallocRepr(node->prefix_, area, node.info);
            node->prefix_ = nodePrefix.allocRepr(area, node.info);

            node2->children[i2] = inserted.root();
            node2->childSizes[i2] = 1;

            size_t skipped = i2 * node2->childSizes[0] + node2->hasValue;

            // Construct the path to this node
            TriePathEntry ours = TriePathEntry::nonTerminal(
                    node2Prefix.bits + 1, inserted.root(), skipped,
                    true /* exact match */);

            return TriePath(node2, ours, inserted);
        }
    }

    static TriePath
    copyAndInsertLeaf(Node & nodeToCopy,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      TriePtr::State newState,
                      GCList & gc)
    {
        KeyFragment prefix = nodeToCopy->prefix(area, nodeToCopy.info);

        // We delay the key copy till later because we may not have to copy it
        // and doing so prematurely could lead to leak in the case of
        // ResizeExceptions.
        Node node = copyNode(
                nodeToCopy, area, nodeToCopy.info, newState, gc, false);

        return insertLeaf(
                node, prefix, area, key, value,
                TriePtr::COPY_ON_WRITE, newState, gc);
    }

    static TriePath
    inplaceInsertLeaf(Node & node,
                      MemoryAllocator & area,
                      const KeyFragment & key,
                      uint64_t value,
                      GCList & gc)
    {
        KeyFragment prefix = node->prefix(area, node.info);
        return insertLeaf(
                node, prefix, area, key, value,
                TriePtr::IN_PLACE, TriePtr::IN_PLACE, gc);
    }

    // Indicates the maximum number of values that a leaf node can hold.
    // Currently this value is set at the size of a LargeKeyNode.
    // \todo Add some smarter logic to accomodate SparseNode (= 4).
    enum { MAX_LEAF_SIZE = 3 };

    /** \todo See DenseBranchingNode for a much better pattern to write this
        function.
     */
    static TriePtr
    removeLeaf(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            TriePtr::State newState,
            GCList& gc)
    {
        auto reclaimNode = [&] (const TriePtr& ptr) {
            if (ptr.state == TriePtr::COPY_ON_WRITE)
                gc.addOldNode(ptr);
            else gc.deallocateNewNode(ptr);
        };

        auto setupWrite = [&] () -> Node {
            if (node.state == TriePtr::COPY_ON_WRITE)
                return copyNode(node, area, node.info, newState, gc, true);
            else return node;
        };

        KeyFragment nodePrefix = node->prefix(area, node.info);
        ExcAssertEqual(nodePrefix.commonPrefixLen(key), nodePrefix.bits);

        int totalSize = size(node, area);
  
        uint64_t childSize0 = node->childSizes[0];
        uint64_t childSize1 = node->childSizes[1];
        int childCount = (childSize0 > 0) + (childSize1 > 0);
        ExcAssertGreater(childCount, 0);

        KeyFragment suffix = key.suffix(nodePrefix.bits);

        gc.addOldNode(node);

        // Are we deleting the value?
        if (suffix.empty()) {
            ExcAssert(node->hasValue);

            // if only one child, get rid of this node.
            if (childCount == 1) {
                int branch = childSize0 == 0 ? 1 : 0;
                TriePtr child = node->children[branch];
                KeyFragment prefix = nodePrefix + KeyFragment(branch, 1);

                TriePtr result =  NodeOps::prefixKeys(
                        child, area, node.info, prefix, newState, gc);

                if (node.state == TriePtr::IN_PLACE)
                    deallocNewNode(node, gc);

                return result;
            }

            // Deleting the value and can't reduce the leaf nodes.
            if (totalSize-1 > MAX_LEAF_SIZE) {
                Node writeNode = setupWrite();

                writeNode->hasValue = false;
                writeNode->value = 0;

                return writeNode;
            }

            // fall-through if we can simplify the node.
        }


        // Simplify this node and every child node to a single node.
        if (totalSize-1 <= MAX_LEAF_SIZE) {
            int newSize = totalSize-1;
            KVList kvs(newSize);

            int i = 0;
            bool deletedNode = false;

            // Copies the given key to the kvs array except for the key to 
            //   delete.
            auto copyKeysFn = [&](
                    const KeyFragment& entryKey, 
                    uint64_t entryValue) 
            {
                if (key == entryKey) {
                    deletedNode = true;
                    return;
                }
                if (i >= newSize) return;

                kvs[i].key = entryKey;
                kvs[i].value = entryValue;
                i++;
            };
            NodeOps::forEachValue(node, area, node.info, copyKeysFn);
            ExcAssert(deletedNode);

            // We need to build the node before we start reclaiming because if
            // this needs to resize then our tree is still intact.
            TriePath path = makeMultiLeafNode(
                    area, node.info, kvs, newSize, newState, gc);

            // Add every child node to the gc list.
            std::function<void (const KeyFragment&, TriePtr, uint64_t)> gcFn =
                [&](const KeyFragment&, TriePtr child, uint64_t) {

                    NodeOps::forEachEntry(
                            child, area, node.info, gcFn, NodeOps::CHILD);
                    reclaimNode(child);
                };
            NodeOps::forEachEntry(node, area, node.info, gcFn, NodeOps::CHILD);

            if (node.state == TriePtr::IN_PLACE)
                deallocNewNode(node, gc);

            return path.root();
        }


        ExcAssert(!suffix.empty());
        int branch = suffix.removeBits(1);

        // If we're about to delete one of the branches, get rid of the binary 
        //  node
        if (node->childSizes[branch] == 1 && !node->hasValue) {
            ExcAssertGreater(childCount, 1);

            TriePtr childToDelete = node->children[branch];
            TriePtr childToPrefix = node->children[1-branch];
            KeyFragment prefix = nodePrefix + KeyFragment(1-branch, 1);

            TriePtr result = NodeOps::prefixKeys(
                    childToPrefix, area, node.info, prefix, newState, gc);

            reclaimNode(childToDelete);
            if (node.state == TriePtr::IN_PLACE)
                deallocNewNode(node, gc);

            return result;
        }


        // Nothing we can do from this node, pass the puck to the correct child.

        TriePtr child = node->children[branch];
        TriePtr newChildRoot = NodeOps::removeLeaf(
                child, area, node.info, suffix, newState, gc);

        Node writeNode = setupWrite();
        writeNode->children[branch] = newChildRoot;
        writeNode->childSizes[branch] -= 1;

        ExcAssertEqual(
                writeNode->childSizes[branch],
                NodeOps::size(newChildRoot, area, node.info));
        ExcAssertEqual(
                newChildRoot.bits == 0,
                writeNode->childSizes[branch] == 0);

        return writeNode;
    }

    static TriePtr
    copyAndRemoveLeaf(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            TriePtr::State newState,
            GCList& gc)
    {
        return removeLeaf(node, area, key, newState, gc);
    }

    static TriePtr
    inplaceRemoveLeaf(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& key,
            GCList& gc)
    {
        return removeLeaf(node, area, key, TriePtr::IN_PLACE, gc);
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
        Node nodeCopy = copyNode(node, area, node.info, newState, gc);

        KeyFragment nodePrefix = prefix + node->prefix(area, node.info);
        nodeCopy->prefix_ = nodePrefix.allocRepr(area, node.info);

        return nodeCopy;
    }

    static TriePtr
    inplacePrefixKeys(
            Node& node,
            MemoryAllocator& area,
            const KeyFragment& prefix,
            GCList& gc)
    {
        KeyFragment newPrefix = prefix + node->prefix(area, node.info);

        KeyFragment::deallocRepr(node->prefix_, area, node.info);
        node->prefix_ = newPrefix.allocRepr(area, node.info);

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
        ExcAssertEqual(entry.entryNum, 0);
        ExcAssert(node->hasValue);

        Node nodeCopy = copyNode(node, area, node.info, newState, gc, true);
        nodeCopy->value = newValue;

        return TriePath(nodeCopy, TriePathEntry::terminal(
                        nodeCopy->prefix_.bits,
                        newValue,
                        0 /* skipped */,
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
        ExcAssertEqual(entry.entryNum, 0);
        ExcAssert(node->hasValue);

        node->value = newValue;

        return TriePath(node, TriePathEntry::terminal(
                        node->prefixLen(),
                        newValue,
                        0 /* skipped */,
                        true /* exactMatch */));
    }


    static TriePtr
    changeState(
            Node & node,
            MemoryAllocator & area,
            TriePtr::State newState,
            GCList & gc)
    {
        if (node.state == TriePtr::COPY_ON_WRITE) {
            gc.addOldNode(node);
            return copyNode(node, area, node.info, newState, gc, true);
        }

        TriePtr children[2];

        // To maintain the IN_PLACE invariant that all parents of an IN_PLACE
        // node are also IN_PLACE, we recurse to all the children and change
        // their states first.

        using namespace std;

        for (int branch = 0; branch < 2; ++branch) {
            if (node->children[branch].state == newState)
                continue;

            children[branch] = NodeOps::changeState(
                    node->children[branch], area, node.info, newState, gc);
        }

        // Now that all the exception throwing code is done we can safely modify
        // the node.
        for (int branch = 0; branch < 2; ++branch) {
            if (!children[branch]) continue;
            node->children[branch] = children[branch];
        }

        node.state = newState;
        return node;
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

        KeyFragment nodePrefix = node->prefix(area, node.info);

        stream << ((TriePtr)node);
        if (!nodePrefix.empty())
            stream << " prefix: " << nodePrefix;
        if (node->hasValue)
            stream << " value: " << node->value;
        
        string id(indent, ' ');

        for (unsigned i = 0;  i != 2;  ++i) {
            if (!node->children[i]) continue;
            stream << endl << id << i << ": size " << node->childSizes[i]
                   << ": ";
            NodeOps::dump(node->children[i], area, node.info,
                          indent + 4, maxDepth,
                          stream);
        }
    }

    static std::string print(TriePtr ptr, MemoryAllocator * area,
                             GcLock::ThreadGcInfo * info)
    {
        std::string result = ML::format("Binary: offset %lld",
                                        (long long)Node::decodeOffset(ptr.data));
        if (!area) return result;
        Node node = encode(ptr, *area, info);

        KeyFragment nodePrefix = node->prefix(*area, node.info);

        if (!nodePrefix.empty())
            result += " prefix: " + nodePrefix.print();
        if (node->hasValue)
            result += " value";
        result += ML::format(" sizes: [%lld, %lld]",
                             (long long)node->childSizes[0],
                             (long long)node->childSizes[1]);

        return result;
    }
};

} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__trie_binary_node_h__ */

