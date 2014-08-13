/* mmap_trie_node.h                                                -*- C++ -*-
   Jeremy Barnes, 11 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Trie node for mmap tries.
*/

#ifndef __mmap__trie_node_h__
#define __mmap__trie_node_h__

#include "gc_list.h"
#include "kv.h"
#include "debug.h"
#include "mmap_trie_stats.h"
#include "sync_stream.h"
#include "profiler.h"
#include "jml/arch/bitops.h"
#include "jml/arch/demangle.h"
#include "jml/utils/unnamed_bool.h"

#include <boost/function.hpp>
#include <boost/tuple/tuple.hpp>


namespace Datacratic {

struct GcLock;

namespace MMap {


/*****************************************************************************/
/* NODE OPS                                                                  */
/*****************************************************************************/

/** Abstract structure defining how to manipulate each kind of node. */

struct NodeOps {

    /** What things do we iterate over? */
    enum {
        CHILD = 1,
        VALUE = 2
    };

    virtual void deallocateImpl(const TriePtr & node, MemoryAllocator & area,
                                GcLock::ThreadGcInfo * info) const = 0;

    static void deallocate(const TriePtr & node, MemoryAllocator & area,
                           GcLock::ThreadGcInfo * info)
    {
        ExcAssert(area.region().isPinned());
        getOps(node).deallocateImpl(node, area, info);
    }

    /** Function that's called on each child or value in a node.
        The first argument is the key fragment under which it's found.
        If the entry is a child node, then the second argument will be
        non-null and point to the child node.  Otherwise it's null.
        It the entry is a value then the third argument will contain the
        value.  Otherwise it will be zero.
    */
    typedef boost::function<void (const KeyFragment &, TriePtr, uint64_t)>
        OnEntryFn;

    /** Do something for each entry (child or value) contained in this node. */
    virtual void forEachEntryImpl(const TriePtr & node,
                                  MemoryAllocator & area,
                                  GcLock::ThreadGcInfo * info,
                                  const OnEntryFn & fn,
                                  int what = CHILD | VALUE) const = 0;

    static void forEachEntry(const TriePtr & node,
                             MemoryAllocator & area,
                             GcLock::ThreadGcInfo * info,
                             const OnEntryFn & fn,
                             int what = CHILD | VALUE)
    {
        ExcAssert(area.region().isPinned());
        getOps(node).forEachEntryImpl(node, area, info, fn, what);
    }

    template<typename Fn>
    static void forEachValue(const TriePtr & node,
                             MemoryAllocator & area,
                             GcLock::ThreadGcInfo * info,
                             const Fn & onValue,
                             const KeyFragment & prefix = KeyFragment())
    {
        auto doEntry = [&] (const KeyFragment & entryPrefix,
                            const TriePtr & node,
                            uint64_t value)
            {
                if (!node)
                    onValue(prefix + entryPrefix, value);
                else {
                    forEachValue(node, area, info, onValue,
                                 prefix + entryPrefix);
                }
            };

        forEachEntry(node, area, info, doEntry);
    }

    /** Enumerates all the nodes that are descendants of a given node. */
    template<typename Fn>
    static void forEachNode(const TriePtr & node,
                             MemoryAllocator & area,
                             GcLock::ThreadGcInfo * info,
                             const Fn & onNode,
                             const KeyFragment & prefix = KeyFragment())
    {
        auto doEntry = [&] (const KeyFragment & entryPrefix,
                            const TriePtr & child,
                            uint64_t value)
            {
                onNode(prefix + entryPrefix, child);
                forEachNode(child, area, info, onNode, prefix + entryPrefix);
            };

        forEachEntry(node, area, info, doEntry, CHILD);
    }



    /** How many children does this node have? */
    virtual size_t sizeImpl(const TriePtr & node,
                            MemoryAllocator & area,
                            GcLock::ThreadGcInfo * info) const = 0;
    
    static size_t size(const TriePtr & node,
                       MemoryAllocator & area,
                       GcLock::ThreadGcInfo * info)
    {
        ExcAssert(area.region().isPinned());
        return getOps(node).sizeImpl(node, area, info);
    }

    /** Attempt to match (consume) part of the key.

        If the key wasn't found then an off-the-end result will be
        returned.
    */
    virtual TriePathEntry
    matchKeyImpl(const TriePtr & node,
                 MemoryAllocator & area,
                 GcLock::ThreadGcInfo * info,
                 const KeyFragment & key) const = 0;

    static TriePathEntry
    matchKey(const TriePtr & node,
             MemoryAllocator & area,
             GcLock::ThreadGcInfo * info,
             const KeyFragment & key)
    {
        ExcAssert(area.region().isPinned());
        return getOps(node).matchKeyImpl(node, area, info, key);
    }

    static TriePath
    findKey(TriePtr node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            KeyFragment key)
    {
        size_t fullLength = key.bits;

        TriePath result(node);
        
        for (;;) {
            ExcAssertEqual(result.totalBits() + key.bits, fullLength);

            TriePathEntry entry = matchKey(node, area, info, key);
            result += entry;

            if (entry.type_ != TriePathEntry::NONTERMINAL)
                return result;

            ExcAssertNotEqual(entry.bitNum, 0);
            key.removeBitVec(entry.bitNum);
            node = entry.node();
        }
    }

    /** Find either the entry or a child node that contains the value with
        the given count from the start of this node.  This should never
        fail to return a path unless the valueIndex is off the end (in
        which case an exception should be thrown).

        POST: result.valid() or exception thrown
    */
    virtual TriePathEntry
    matchIndexImpl(const TriePtr & node,
                   MemoryAllocator & area,
                   GcLock::ThreadGcInfo * info,
                   size_t valueIndex) const = 0;

    static TriePathEntry
    matchIndex(const TriePtr & node,
               MemoryAllocator & area,
               GcLock::ThreadGcInfo * info,
               size_t valueIndex)
    {
        ExcAssert(area.region().isPinned());
        return getOps(node).matchIndexImpl(node, area, info, valueIndex);
    }

    static TriePath
    findIndex(TriePtr node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            size_t valueIndex)
    {
        if (valueIndex >= size(node, area, info))
            return end(node, area, 0);

        TriePath result(node);
        int curIndex = valueIndex;
        TriePtr curNode = node;

        while(true) {
            TriePathEntry entry = NodeOps::matchIndex(
                    curNode, area, info, curIndex);
            result += entry;

            if (!entry.isNonTerminal()) break;

            curIndex -= entry.entryNum;
            curNode = entry.node();
        }

        return result;
    }

    /** Extract the matched portion of the key from a node for the given
        match.
    */
    virtual KeyFragment
    extractKeyImpl(const TriePtr & node,
                   MemoryAllocator & area,
                   GcLock::ThreadGcInfo * info,
                   size_t valueIndex) const = 0;

    static KeyFragment
    extractKey(const TriePtr & node,
               MemoryAllocator & area,
               GcLock::ThreadGcInfo * info,
               size_t valueIndex)
    {
        ExcAssert(area.region().isPinned());
        return getOps(node).extractKeyImpl(node, area, info, valueIndex);
    }

    /** Return the first entry for the node.  If there are absolutely no
        values in the node, then it should return the same as
        offTheEnd().
    */
    virtual TriePathEntry
    firstImpl(const TriePtr & node,
              MemoryAllocator & area,
              GcLock::ThreadGcInfo * info) const = 0;

    static TriePathEntry
    first(const TriePtr & node,
          MemoryAllocator & area,
          GcLock::ThreadGcInfo * info)
    {
        ExcAssert(area.region().isPinned());
        return getOps(node).firstImpl(node, area, info);
    }

    static TriePath
    begin(TriePtr node,
          MemoryAllocator & area,
          GcLock::ThreadGcInfo * info)
    {
        TriePath result(node);
        
        for (;;) {
            TriePathEntry entry = first(node, area, info);
            result += entry;
            
            if (entry.type_ != TriePathEntry::NONTERMINAL)
                return result;
            
            node = entry.node();
        }
    }

    /** Return a one-past-the-end entry for the node. */
    virtual TriePathEntry
    offTheEndImpl(const TriePtr & node,
                  MemoryAllocator & area,
                  GcLock::ThreadGcInfo * info)
        const = 0;

    static TriePathEntry
    offTheEnd(const TriePtr & node,
              MemoryAllocator & area,
              GcLock::ThreadGcInfo * info)
    {
        ExcAssert(area.region().isPinned());
        return getOps(node).offTheEndImpl(node, area, info);
    }

    static TriePath
    end(const TriePtr & node,
        MemoryAllocator & area,
        GcLock::ThreadGcInfo * info)
    {
        return TriePath(node, offTheEnd(node, area, info));
    }

    /**

    */
    virtual TriePathEntry
    lowerBoundImpl(
            const TriePtr& node, 
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info, 
            const KeyFragment& key) const = 0;

    static TriePathEntry
    lowerBound(
            const TriePtr& node, 
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info, 
            const KeyFragment& key)
    {
        ExcAssert(area.region().isPinned());
        return getOps(node).lowerBoundImpl(node, area, info, key);
    }

    /**

    */
    virtual TriePathEntry
    upperBoundImpl(
            const TriePtr& node, 
            MemoryAllocator& area,
            GcLock::ThreadGcInfo * info, 
            const KeyFragment& key) const = 0;

    static TriePathEntry
    upperBound(
            const TriePtr& node, 
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info, 
            const KeyFragment& key)
    {
        ExcAssert(area.region().isPinned());
        return getOps(node).upperBoundImpl(node, area, info, key);
    }

    /** Returns a sorted list of prefixes and values contained within the node.
        In the case of a branching node, the values will be TriePtrs to their
        children nodes.
    */
    virtual KVList
    gatherKVImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info) const = 0;

    static KVList
    gatherKV(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info)
    {
        ExcAssert(area.region().isPinned());
        KVList list = getOps(node).gatherKVImpl(node, area, info);

        // DEBUG: Sanity check that should be removed because it's expensive.
        ExcAssert(std::is_sorted(list.begin(), list.end()));

        return list;
    }

    /** Returns true if we're dealing with a branching node and false otherwise.
        This is mostly used internally during the merges to avoid calling
        gatherKV() needlessly.
    */
    virtual bool
    isBranchingNodeImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info) const = 0;

    static bool
    isBranchingNode(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info)
    {
        return getOps(node).isBranchingNodeImpl(node, area, info);
    }



    /** Make a copy of the current node, with the entry matching
        the key replaced with the given node.

        Returns the matchResult for the replaced leaf.

        All created nodes will be added to the GC list.
    */
    virtual TriePath
    copyAndReplaceImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const TriePathEntry & match,
            const TriePtr & replaceWith,
            TriePtr::State newState,
            GCList & gc) const = 0;

    virtual TriePath
    inplaceReplaceImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const TriePathEntry & match,
            const TriePtr & replaceWith,
            GCList & gc) const = 0;

    static TriePath
    replace(const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const TriePathEntry & match,
            const TriePtr & replaceWith,
            TriePtr::State newState,
            GCList & gc)
    {
        ExcAssert(area.region().isPinned());
        ExcAssert(match.isRelative());

        TriePath result;

        if (node.state == TriePtr::COPY_ON_WRITE) {
            result = getOps(node).copyAndReplaceImpl(
                    node, area, info, match, replaceWith, newState, gc);
        }
        else {
            result = getOps(node).inplaceReplaceImpl(
                    node, area, info, match, replaceWith, gc);
        }

        ExcAssert(result.back().isNonTerminal());
        ExcAssertEqual(result.lastNode(), replaceWith);
        if (trieValidate) result.validate(area, info, false);

        return result;
    }


    /** Replaces one of the branch in a branching node. Goes without saying that
        this only works on branching nodes.

        Used mostly by the merge algorithm to remove or insert entire subtrees.

    */
    virtual TriePtr
    copyAndSetBranchImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const KeyFragment& key,
            TriePtr newBranch,
            TriePtr::State newState,
            GCList & gc) const = 0;


    virtual TriePtr
    inplaceSetBranchImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const KeyFragment& key,
            TriePtr newBranch,
            GCList & gc) const = 0;

    static TriePtr
    setBranch(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const KeyFragment& key,
            TriePtr newBranch,
            TriePtr::State newState,
            GCList & gc)
    {
        ExcAssert(area.region().isPinned());

        TriePtr result;
        if (node.state == TriePtr::COPY_ON_WRITE) {
            result = getOps(node).copyAndSetBranchImpl(
                    node, area, info, key, newBranch, newState, gc);
        }
        else {
            result = getOps(node).inplaceSetBranchImpl(
                    node, area, info, key, newBranch, gc);
        }

        return result;
    }

    /** Make a copy of the current node.  The extraValues parameter
        is a hint that the node will soon be expanded to include
        the given number of extra values in addition to the ones
        currently in the node.

        Returns a path for the inserted leaf.

        All created nodes will be added to the GC list.
    */
    virtual TriePath
    copyAndInsertLeafImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const KeyFragment & key,
            uint64_t value,
            TriePtr::State newState,
            GCList & gc) const = 0;
    
    virtual TriePath
    inplaceInsertLeafImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const KeyFragment & key,
            uint64_t value,
            GCList & gc) const = 0;

    static TriePath
    insertLeaf(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const KeyFragment & key,
            uint64_t value,
            TriePtr::State newState,
            GCList & gc)
    {
        ExcAssert(area.region().isPinned());

        TriePath result;

        if (node.state == TriePtr::COPY_ON_WRITE) {
            result = getOps(node).copyAndInsertLeafImpl(
                    node, area, info, key, value, newState, gc);
        }
        else {
            result = getOps(node).inplaceInsertLeafImpl(
                    node, area, info, key, value, gc);
        }

        result.validate(area, info, true);

        ExcAssert(result.valid());
        ExcAssertEqual(result.value(), value);
        ExcAssertEqual(result.totalBits(), key.bits);

        return result;
    }

    /**
    Removes the given key from the subtree whoes root is the given node.
      The root of the subtree provided to this function should not be a terminal
      or leaf node. The reason is that we might need to simplify the subtree
      and to do so, we need the nonTerminal node that points to the terminal
      node that contains the key to delete.

    Returns the root of the newly created subtree.
    */
    virtual TriePtr
    copyAndRemoveLeafImpl(
            const TriePtr& node,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& key,
            TriePtr::State newState,
            GCList& gc) const = 0;

    virtual TriePtr
    inplaceRemoveLeafImpl(
            const TriePtr& node,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& key,
            GCList& gc) const = 0;

    static TriePtr 
    removeLeaf(
            const TriePtr& node,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& key,
            TriePtr::State newState,
            GCList& gc)
    {
        ExcAssert(area.region().isPinned());

        TriePtr result;
        if (node.state == TriePtr::COPY_ON_WRITE) {
            result = getOps(node).copyAndRemoveLeafImpl(
                    node, area, info, key, newState, gc);
        }
        else result = getOps(node).inplaceRemoveLeafImpl(
                node, area, info, key, gc);

        // \todo Should probably assert a thing or two.
        return result;
    }


    /** 
    Copies the given node and prepends the given prefix to every
      keys of the node.

    Returns the root of the modified subtree. This might be either
      the copy of the passed node or an entirely new subtree in case
      the old node could no longer fit in a single node.
    */
    virtual TriePtr
    copyAndPrefixKeysImpl(
            const TriePtr& node,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& prefix,
            TriePtr::State newState,
            GCList& gc) const = 0;

    virtual TriePtr
    inplacePrefixKeysImpl(
            const TriePtr& node,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& prefix,
            GCList& gc) const = 0;

    static TriePtr
    prefixKeys(
            const TriePtr& node,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& prefix,
            TriePtr::State newState,
            GCList& gc)
    {
        ExcAssert(area.region().isPinned());

        if (!prefix.bits) return node;

        TriePtr result;
        if (node.state == TriePtr::COPY_ON_WRITE) {
            result = getOps(node).copyAndPrefixKeysImpl(
                    node, area, info, prefix, newState, gc);
        }
        else {
            result = getOps(node).inplacePrefixKeysImpl(
                node, area, info, prefix, gc);
        }

        // \todo could use some assert (just to make me feel safe).
        return result;
    }

    /** Forces a node copy on regardless of the current state then prefixes all
        the keys of the node.

        This is a fairly special case of prefixKeys which is required in a very
        specific scenario of the 3-way merge during a call the makeBranchingNode

        A bit of a hack but it's a necessary evil.
     */
    static TriePtr
    copyAndPrefixKeys(
            const TriePtr& node,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& prefix,
            TriePtr::State newState,
            GCList& gc)
    {
        ExcAssert(area.region().isPinned());

        if (!prefix.bits) return node;

        return getOps(node).copyAndPrefixKeysImpl(
                    node, area, info, prefix, newState, gc);
    }


    /**
    Replaces the value at a given key.
    Note that this should be used carefully because there are no ABA checks.
    */
    virtual TriePath
    copyAndReplaceValueImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const TriePathEntry & entry,
            uint64_t value,
            TriePtr::State newState,
            GCList & gc) const = 0;

    virtual TriePath
    inplaceReplaceValueImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const TriePathEntry & entry,
            uint64_t value,
            GCList & gc) const = 0;

    static TriePath
    replaceValue(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const TriePathEntry & entry,
            uint64_t value,
            TriePtr::State newState,
            GCList & gc)
    {
        ExcAssert(area.region().isPinned());

        ExcAssert(entry.isTerminal());
        ExcAssert(entry.isRelative());

        TriePath result;
        if (node.state == TriePtr::COPY_ON_WRITE) {
            result = getOps(node).copyAndReplaceValueImpl(
                    node, area, info, entry, value, newState, gc);
        }
        else {
            result = getOps(node).inplaceReplaceValueImpl(
                    node, area, info, entry, value, gc);
        }

        result.validate(area, info, true);        
        ExcAssertEqual(result.value(), value);

        return result;
    }

    static TriePath
    replaceValue(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const KeyFragment & key,
            uint64_t value,
            TriePtr::State newState,
            GCList & gc)
    {
        ExcAssert(area.region().isPinned());

        ExcCheck(false, "Not implemented");

        TriePath path = findKey(node, area, 0, key);
        // Go to the bottom node and call replaceValue on it.
        // Then walk back up to the root of the path calling replace() on it.
        // Giant pain but oh well...
    }


    /** Changes the state of a node.

        For CoW nodes this means we need to copy the node and return a ptr with
        the new state on it.

        If we're transitioning from InPlace to CoW then we also change the state
        of every child of that node to an In Place node. We do this to maintain
        the invariant that every parent of an In Place node is also In Place.
    */
    virtual TriePtr
    changeStateImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            TriePtr::State newState,
            GCList & gc) const = 0;

    static TriePtr
    changeState(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            TriePtr::State newState,
            GCList & gc)
    {
        if (node.state == newState)
            return node;

        TriePtr result = getOps(node).changeStateImpl(
                node, area, info, newState, gc);

        ExcAssertEqual(result.state, newState);
        return result;
    }


    /** Gather stats about a given node. */
    virtual NodeStats statsImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info) const = 0;

    static NodeStats stats(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info)
    {
        ExcAssert(area.region().isPinned());
        return getOps(node).statsImpl(node, area, info);
    }


    /** Print out the current node. */
    virtual void dumpImpl(const TriePtr & node,
                          MemoryAllocator & area,
                          GcLock::ThreadGcInfo * info,
                          int indent = 0,
                          int maxDepth = -1,
                          std::ostream & stream = std::cerr) const = 0;

    static std::ostream & dump(const TriePtr & node,
                               MemoryAllocator & area,
                               GcLock::ThreadGcInfo * info,
                               int indent = 0,
                               int maxDepth = -1,
                               std::ostream & stream = std::cerr)
    {
        ExcAssert(area.region().isPinned());
        getOps(node).dumpImpl(node, area, info, indent, maxDepth, stream);
        return stream;
    }

    /** Print out information about a pointer.  Area may or may not be
        null.
    */
    virtual std::string printImpl(const TriePtr & ptr,
                                  MemoryAllocator * area,
                                  GcLock::ThreadGcInfo * info) const = 0;
    
    static std::string print(const TriePtr & ptr,
                             MemoryAllocator * area,
                             GcLock::ThreadGcInfo * info)
    {
        ExcAssert(!area || area->region().isPinned());
        return getOps(ptr).printImpl(ptr, area, info);
    }

    /** How much memory is directly used by this node? */
    virtual size_t
    directMemUsageImpl(const TriePtr & node,
                       MemoryAllocator & area,
                       GcLock::ThreadGcInfo * info) const = 0;

    static size_t
    directMemUsage(const TriePtr & node,
                   MemoryAllocator & area,
                   GcLock::ThreadGcInfo * info)
    {
        ExcAssert(area.region().isPinned());
        return getOps(node).directMemUsageImpl(node, area, info);
    }

    static size_t memUsage(const TriePtr & node,
                           MemoryAllocator & area,
                           GcLock::ThreadGcInfo * info)
    {
        size_t result = directMemUsage(node, area, info);

        auto doEntry = [&] (const KeyFragment & entryPrefix,
                            const TriePtr & child,
                            uint64_t value)
            {
                if (!child) return;
                result += NodeOps::memUsage(child, area, info);
            };
        
        forEachEntry(node, area, info, doEntry);

        return result;
    }
    

    static const NodeOps * OPS[64];

    static inline const NodeOps & getOps(const TriePtr & node)
    {
        const NodeOps * ops = OPS[node.type];
        if (!ops)
            throw ML::Exception("attempt to dereference invalid trie ptr");
        return *ops;
    }
};


} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__trie_node_h__ */
