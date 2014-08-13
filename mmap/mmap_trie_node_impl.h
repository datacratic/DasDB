/* mmap_trie_node_impl.h                                           -*- C++ -*-
   Jeremy Barnes, 16 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Methods to implement the trie nodes.
*/

#ifndef __mmap__trie_node_impl_h__
#define __mmap__trie_node_impl_h__

#include "debug.h"
#include "mmap_trie_node.h"
#include "jml/utils/less.h"
#include "jml/arch/atomic_ops.h"
#include "jml/compiler/compiler.h"
#include "memory_tracker.h"

namespace Datacratic {
namespace MMap {

/*****************************************************************************/
/* NODE ACCESSOR OPS ADAPTOR                                                 */
/*****************************************************************************/

template<typename Impl,
         typename Node = typename Impl::Node>
struct NodeOpsAdaptor : public NodeOps {

    static inline Node encode(const TriePtr & node,
                              MemoryAllocator & area,
                              GcLock::ThreadGcInfo * info)
    {
        return Impl::encode(node, area, info);
    }

    virtual void deallocateImpl(const TriePtr & node,
                                MemoryAllocator & area,
                                GcLock::ThreadGcInfo * info) const
    {
        return Impl::deallocate(encode(node, area, info), area);
    }

    virtual size_t sizeImpl(const TriePtr & node,
                            MemoryAllocator & area,
                            GcLock::ThreadGcInfo * info) const
    {
        return Impl::size(encode(node, area, info), area);
    }

    virtual void forEachEntryImpl(const TriePtr & node,
                                  MemoryAllocator & area,
                                  GcLock::ThreadGcInfo * info,
                                  const OnEntryFn & fn, int what) const
    {
        Impl::forEachEntry(Impl::encode(node, area, info), area, fn, what);
    }
    
    virtual TriePathEntry
    matchKeyImpl(const TriePtr & node,
                 MemoryAllocator & area,
                 GcLock::ThreadGcInfo * info,
                 const KeyFragment & key) const
    {
        return Impl::matchKey(encode(node, area, info), area, key);
    }

    virtual TriePathEntry
    matchIndexImpl(const TriePtr & node, MemoryAllocator & area,
                   GcLock::ThreadGcInfo * info,
                   size_t valueIndex) const
    {
        ssize_t t2 = valueIndex;
        if (t2 < 0 || t2 >= size(node, area, info)) {
            using namespace std;
            cerr << "MatchIndex out of range" << endl;
            cerr << "valueIndex = " << t2 << endl;
            cerr << "size: " << size(node, area, info) << endl;
            cerr << "node: " << endl;
            dump(node, area, info);
            cerr << endl;
            throw ML::Exception("matchIndex: out of range");
        }
        return Impl::matchIndex(encode(node, area, info), area,
                                valueIndex);
    }

    virtual KeyFragment
    extractKeyImpl(const TriePtr & node,
                   MemoryAllocator & area,
                   GcLock::ThreadGcInfo * info,
                   size_t valueIndex) const
    {
        ssize_t t2 = valueIndex;
        if (t2 < 0 || t2 >= size(node, area, info)) {
            using namespace std;
            cerr << "extractKey out of range" << endl;
            cerr << "valueIndex = " << t2 << endl;
            cerr << "size: " << size(node, area, info) << endl;
            cerr << "node: " << endl;
            dump(node, area, info);
            cerr << endl;
            throw ML::Exception("extractKey: out of range");
        }
        return Impl::extractKey(encode(node, area, info), area, valueIndex);
    }

    virtual TriePathEntry
    firstImpl(const TriePtr & node,
              MemoryAllocator & area,
              GcLock::ThreadGcInfo * info) const
    {
        return Impl::first(encode(node, area, info), area);
    }

    virtual TriePathEntry
    offTheEndImpl(const TriePtr & node,
                  MemoryAllocator & area,
                  GcLock::ThreadGcInfo * info) const
    {
        return Impl::offTheEnd(encode(node, area, info), area);
    }

    virtual TriePathEntry
    lowerBoundImpl(
            const TriePtr& node, 
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info, 
            const KeyFragment& key) const
    {
        return Impl::lowerBound(encode(node, area, info), area, key);
    }

    virtual TriePathEntry
    upperBoundImpl(
            const TriePtr& node, 
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info, 
            const KeyFragment& key) const
    {
        return Impl::upperBound(encode(node, area, info), area, key);
    }
    
    virtual KVList
    gatherKVImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info) const
    {
        return Impl::gatherKV(encode(node, area, info), area);
    }

    virtual bool
    isBranchingNodeImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info) const
    {
        return Impl::isBranchingNode(encode(node, area, info), area);
    }

    virtual TriePath
    copyAndReplaceImpl(const TriePtr & node_,
                       MemoryAllocator & area,
                       GcLock::ThreadGcInfo * info,
                       const TriePathEntry & match,
                       const TriePtr & replaceWith,
                       TriePtr::State newState,
                       GCList & gc) const
    {
        Node node = encode(node_, area, info);
        TriePath result
            = Impl::copyAndReplace(node, area, match, replaceWith, newState, gc);
#if 0
        ExcAssert(result.first);
        ExcAssert(result.second.nonTerminal());
        ExcAssert(result.second.next() == replaceWith);
#endif
        return result;
    }

    virtual TriePath
    inplaceReplaceImpl(const TriePtr & node_,
                       MemoryAllocator & area,
                       GcLock::ThreadGcInfo * info,
                       const TriePathEntry & match,
                       const TriePtr & replaceWith,
                       GCList & gc) const
    {
        Node node = encode(node_, area, info);
        TriePath result
            = Impl::inplaceReplace(node, area, match, replaceWith, gc);
        return result;
    }

    virtual TriePtr
    copyAndSetBranchImpl(
            const TriePtr & node_,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const KeyFragment& key,
            TriePtr newBranch,
            TriePtr::State newState,
            GCList & gc) const
    {
        Node node = encode(node_, area, info);
        return Impl::copyAndSetBranch(node, area, key, newBranch, newState, gc);
    }


    virtual TriePtr
    inplaceSetBranchImpl(
            const TriePtr & node_,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const KeyFragment& key,
            TriePtr newBranch,
            GCList & gc) const
    {
        Node node = encode(node_, area, info);
        return Impl::inplaceSetBranch(node, area, key, newBranch, gc);
    }

    virtual TriePath
    copyAndInsertLeafImpl(const TriePtr & node_,
                          MemoryAllocator & area,
                          GcLock::ThreadGcInfo * info,
                          const KeyFragment & key,
                          uint64_t value,
                          TriePtr::State newState,
                          GCList & gc) const
    {
        Node node = encode(node_, area, info);
        TriePath result
            = Impl::copyAndInsertLeaf(node, area, key, value, newState, gc);

        return result;
    }

    virtual TriePath
    inplaceInsertLeafImpl(const TriePtr & node_,
                          MemoryAllocator & area,
                          GcLock::ThreadGcInfo * info,
                          const KeyFragment & key,
                          uint64_t value,
                          GCList & gc) const
    {
        Node node = encode(node_, area, info);
        TriePath result
            = Impl::inplaceInsertLeaf(node, area, key, value, gc);

        return result;
    }

    virtual TriePtr
    copyAndRemoveLeafImpl(
            const TriePtr& node_,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& key,
            TriePtr::State newState,
            GCList& gc) const
    {
        Node node = encode(node_, area, info);
        return Impl::copyAndRemoveLeaf(node, area, key, newState, gc);
    }

    virtual TriePtr
    inplaceRemoveLeafImpl(
            const TriePtr& node_,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& key,
            GCList& gc) const
    {
        Node node = encode(node_, area, info);
        return Impl::inplaceRemoveLeaf(node, area, key, gc);
    }

    virtual TriePtr
    copyAndPrefixKeysImpl(
            const TriePtr& node_,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& prefix,
            TriePtr::State newState,
            GCList& gc) const
    {
        Node node = encode(node_, area, info);
        return Impl::copyAndPrefixKeys(node, area, prefix, newState, gc);
    }

    virtual TriePtr
    inplacePrefixKeysImpl(
            const TriePtr& node_,
            MemoryAllocator& area,
            GcLock::ThreadGcInfo* info,
            const KeyFragment& prefix,
            GCList& gc) const
    {
        Node node = encode(node_, area, info);
        return Impl::inplacePrefixKeys(node, area, prefix, gc);
    }

    virtual TriePath
    copyAndReplaceValueImpl(
            const TriePtr & node_,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const TriePathEntry & entry,
            uint64_t value,
            TriePtr::State newState,
            GCList & gc) const
    {
        Node node = encode(node_, area, info);
        return Impl::copyAndReplaceValue(node, area, entry, value, newState, gc);
    }

    virtual TriePath
    inplaceReplaceValueImpl(
            const TriePtr & node_,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            const TriePathEntry & entry,
            uint64_t value,
            GCList & gc) const
    {
        Node node = encode(node_, area, info);
        return Impl::inplaceReplaceValue(node, area, entry, value, gc);
    }

    virtual TriePtr
    changeStateImpl(
            const TriePtr & node_,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info,
            TriePtr::State newState,
            GCList & gc) const
    {
        Node node = encode(node_, area, info);
        return Impl::changeState(node, area, newState, gc);
    }

    virtual NodeStats
    statsImpl(
            const TriePtr & node,
            MemoryAllocator & area,
            GcLock::ThreadGcInfo * info) const
    {
        return Impl::stats(encode(node, area, info), area);
    }

    /** Print out the current node. */
    virtual void dumpImpl(const TriePtr & node,
                          MemoryAllocator & area,
                          GcLock::ThreadGcInfo * info,
                          int indent,
                          int maxDepth,
                          std::ostream & stream) const
    {
        Impl::dump(encode(node, area, info), area, indent, maxDepth, stream);
    }

    virtual std::string printImpl(const TriePtr & ptr,
                                  MemoryAllocator * area,
                                  GcLock::ThreadGcInfo * info) const
    {
        return Impl::print(ptr, area, info);
    }

    virtual size_t
    directMemUsageImpl(const TriePtr & node,
                       MemoryAllocator & area,
                       GcLock::ThreadGcInfo * info) const
    {
        return Impl::directMemUsage(encode(node, area, info), area);
    }

};

template<unsigned long long value>
struct Log2 {
};

template<> struct Log2<1> { enum { BITS = 0 }; };
template<> struct Log2<2> { enum { BITS = 1 }; };
template<> struct Log2<4> { enum { BITS = 2 }; };
template<> struct Log2<8> { enum { BITS = 3 }; };
template<> struct Log2<16> { enum { BITS = 4 }; };
template<> struct Log2<32> { enum { BITS = 5 }; };
template<> struct Log2<64> { enum { BITS = 6 }; };
template<> struct Log2<128> { enum { BITS = 7 }; };
template<> struct Log2<256> { enum { BITS = 8 }; };
template<> struct Log2<512> { enum { BITS = 9 }; };
template<> struct Log2<1024> { enum { BITS = 10 }; };
template<> struct Log2<2048> { enum { BITS = 11 }; };
template<> struct Log2<4096> { enum { BITS = 12 }; };

// For indirect nodes, we assume that we want 2^48 = 256TB addressable
// The pointers are stored assuming their alignment, and so can be shifted,
// eg if the alignment is 64 bytes (the usual case), then we can shift off
// 6 bits and thus we only need 42 bits for the offset.
// Thus, of the 57 bits we have for the offset, we have 57 - 48 + (alignment)
// bits available for metadata.
// In the case of 64 bit alignment, this makes 15 bits.

template<typename Repr, NodeType type, int metadataBits, int alignment,
         typename Metadata = uint32_t>
struct IndirectNode
{
    enum { alignmentBits = Log2<alignment>::BITS };

    IndirectNode(uint64_t offset, MemoryAllocator & area, Metadata metadata,
                 TriePtr::State state, GcLock::ThreadGcInfo * info)
        : offset(offset),
          state(state),
          area(&area),
          metadata(metadata),
          node(area.region(), offset),
          info(info),
          deallocated(false)
    {
        ExcCheck(offset, "attempt to dereference null indirect ptr");

        if (NodeAllocSentinels)
            area.nodeAlloc.checkSentinels(offset, sizeof(Repr));

    }

    IndirectNode(TriePtr ptr, MemoryAllocator & area,
                 GcLock::ThreadGcInfo * info)
        : offset(decodeOffset(ptr.data)),
          state(ptr.state),
          area(&area),
          metadata(decodeMetadata(ptr.data)),
          node(area.region(), offset),
          info(info),
          deallocated(false)
    {
        ExcCheck(offset, "attempt to dereference null indirect ptr");
        ExcCheckEqual(ptr.type, type, "attempt to deal with wrong typed ptr");
        ExcAssertEqual(this->state, ptr.state);

        if (trieMemoryCheck)
            trieMemoryTracker.checkRead(offset, ((TriePtr)*this));

        if (NodeAllocSentinels)
            area.nodeAlloc.checkSentinels(offset, sizeof(Repr));

#if 0
        TriePtr ptr2 = *this;
        if (ptr2 != ptr) {
            cerr << "type1 = " << ptr.type << endl;
            cerr << "type2 = " << ptr2.type << endl;
            cerr << "data1 = " << (uint64_t)ptr.data << endl;
            cerr << "data2 = " << (uint64_t)ptr2.data << endl;
            cerr << "state1 = " << ptr.state << endl;
            cerr << "state2 = " << ptr2.state << endl;
            throw ML::Exception("Bad ptr decode");
        }
#endif

    }

    ~IndirectNode()
    {
        if (NodeAllocSentinels && !deallocated)
            area->nodeAlloc.checkSentinels(offset, sizeof(Repr));
    }

    JML_ALWAYS_INLINE Repr * operator -> () const
    {
        return node;
    }
    
    Repr & operator * () const
    {
        return *operator -> ();
    }
    
    uint64_t offset;
    TriePtr::State state;
    MemoryAllocator * area;
    Metadata metadata;
    mutable RegionPtr<Repr> node;
    GcLock::ThreadGcInfo * info;
    mutable bool deallocated;

    static uint64_t
    encodePtr(uint64_t offset, MemoryAllocator & area, Metadata metadata)
    {
        if (decodeMetadata(metadata) != metadata) {
            using namespace std;
            cerr << "metadata = " << metadata << endl;
            cerr << "metadata int = " << (uint32_t)metadata << endl;
            cerr << "decodeMetadata(metadata) = " << decodeMetadata(metadata)
                 << endl;
            cerr << "as md = " << Metadata(decodeMetadata(metadata)) << endl;
            cerr << "metadataBits = " << metadataBits << endl;
            throw ML::Exception("metadata doesn't fit");
        }

        uint64_t result = offset;
        if (result & ((1 << alignmentBits) - 1))
            throw ML::Exception("encoding non-aligned offset");
        result >>= alignmentBits;
        result <<= metadataBits;
        result |= metadata;
        return result;
    }
    
    static uint32_t decodeMetadata(uint64_t data)
    {
        return data & ((1 << metadataBits) - 1);
    }

    static uint64_t decodeOffset(uint64_t data)
    {
        data >>= metadataBits;
        data <<= alignmentBits;
        return data;
    }

    operator TriePtr() const
    {
        return TriePtr(type, state, encodePtr(offset, *area, metadata));
    }
};

// Operations structure
template<typename Repr, NodeType type,
         typename Metadata = uint32_t,
         int metadataBits = 0,
         int alignment = alignof(Repr)>
struct IndirectOpsBase {

    typedef IndirectNode<Repr, type, metadataBits, alignment, Metadata> Node;

    static uint64_t allocated, deallocated;

    // Default allocate
    static Node allocEmpty(MemoryAllocator & area,
                           GcLock::ThreadGcInfo * info,
                           TriePtr::State state,
                           Metadata metadata = 0)
    {
        Node result(
                area.nodeAlloc.allocateT<Repr>(info),
                area, metadata, state, info);

        if (trieDebug)
            ML::atomic_add(allocated, 1);
        if (trieMemoryCheck)
            trieMemoryTracker.trackAlloc(result.offset, result);

        return result;
    }

    // Copy allocate
    static Node allocCopy(MemoryAllocator & area,
                          GcLock::ThreadGcInfo * info,
                          const Repr & other,
                          TriePtr::State state,
                          Metadata metadata = 0)
    {
        Node result(
                area.nodeAlloc.allocateT<Repr>(info, other),
                area, metadata, state, info);

        if (trieDebug)
            ML::atomic_add(allocated, 1);
        if (trieMemoryCheck)
            trieMemoryTracker.trackAlloc(result.offset, result);

        return result;
    }

    // Copy allocate
    static Node allocCopy(MemoryAllocator & area,
                          GcLock::ThreadGcInfo * info,
                          const Node & other,
                          TriePtr::State newState)
    {
        Node result(
                area.nodeAlloc.allocateT<Repr>(info, *other),
                area, other.metadata, newState, info);

        if (trieDebug)
            ML::atomic_add(allocated, 1);
        if (trieMemoryCheck)
            trieMemoryTracker.trackAlloc(result.offset, result);

        return result;
    }

    static void deallocNode(const Node & node, MemoryAllocator & area)
    {
        if (trieMemoryCheck)
            trieMemoryTracker.trackDealloc(node.offset);

        area.nodeAlloc.deallocate(node.offset, sizeof(Repr));
        node.deallocated = true;

        if (trieDebug)
            ML::atomic_add(deallocated, 1);
    }

    static void deallocNewNode(const Node & node, GCList & gc)
    {
        node.deallocated = true;
        gc.deallocateNewNode(node.offset, node.info);
    }

    static Node encode(const TriePtr & ptr, MemoryAllocator & area,
                       GcLock::ThreadGcInfo * info)
    {
        return Node(ptr, area, info);
    }
};

template<typename Repr, NodeType type,
         typename Metadata,
         int metadataBits,
         int alignment>
uint64_t
IndirectOpsBase<Repr, type, Metadata, metadataBits, alignment>::
allocated = 0;

template<typename Repr, NodeType type,
         typename Metadata,
         int metadataBits,
         int alignment>
uint64_t
IndirectOpsBase<Repr, type, Metadata, metadataBits, alignment>::
deallocated = 0;



/*****************************************************************************/
/* MAKE LEAF                                                                 */
/*****************************************************************************/

/** Returns the path to the leaf. */
TriePath
makeLeaf(
        MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        const KeyFragment & key, uint64_t value,
        TriePtr::State newState,
        GCList & gc);

/** Returns the path to the added leaf. */
TriePath
addLeaf(MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        TriePtr ptr,
        const KeyFragment & key, uint64_t value,
        TriePtr::State newState,
        GCList & gc);

/** Returns the path to the first key. */
TriePath
makeDoubleLeafNode(
        MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        const KeyFragment & key1, uint64_t value1,
        const KeyFragment & key2, uint64_t value2,
        TriePtr::State newState,
        GCList & gc);

/** Create a node with the given set of leaves.  Returns a TriePath
    to the given entry within the node.
*/
TriePath
makeMultiLeafNode(
        MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        const KVList& kvs,
        int iteratedEntry,
        TriePtr::State newState,
        GCList & gc);

/** Create a branching node using KVs containing TriePtrs.

    This function will make more sense when n-ary nodes are implemented but we
    still need it in the merging algo.
*/
TriePtr
makeBranchingNode(
        MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        const KVList& kvs,
        TriePtr::State newState,
        GCList & gc);

/** Dispatches to makeMultiLeafNode or makeBranchingNode dependending on the
    content of kvs. Useful when you have a random blob of KVs and you don't care
    what happens to it.
 */
TriePtr makeNode(
        MemoryAllocator & area,
        GcLock::ThreadGcInfo * info,
        const KVList& kvs,
        TriePtr::State newState,
        GCList & gc);

TriePath
replaceSubtreeRecursive(MemoryAllocator & area,
                        GcLock::ThreadGcInfo * info,
                        const TriePath & path,
                        const TriePath & newSubtree,
                        GCList & gc,
                        const TriePtr & node,
                        int i);

} // namespace MMap
} // namespace Datacratic



#endif /* __mmap__trie_node_impl_h__ */

