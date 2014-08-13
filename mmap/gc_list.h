/* gc_list.h                                                         -*- C++ -*-
   Rémi Attab, 13 Febuary 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   List of offsets in the mmap that needs to be garbage collected.
*/

#ifndef __mmap__gc_list_h__
#define __mmap__gc_list_h__

#include "mmap_trie_path.h"
#include "jml/utils/compact_vector.h"

#include <utility>
#include <unordered_map>

namespace Datacratic {

struct GcLock;

namespace MMap {

struct MemoryAllocator;

/*****************************************************************************/
/* GC LIST                                                                   */
/*****************************************************************************/

/** A list of objects to be garbage collected.
    Node that the object should be constructed within a pin area because it's
    destructor may need to deallocate some objects.
*/
struct GCList {
    GCList(MemoryAllocator & area)
        : area(area)
    {
    }

    ~GCList();

    static void doCleanup(void * data);

    /** Marks a node to be deleted when rolling back (destruction without
        commit)
    */
    void addNewNode(uint64_t offset, const TriePtr & node) {
        ExcAssert(newNodesOffset.find(node) == newNodesOffset.end());
        newNodesPtr[node] = offset;
        newNodesOffset[offset] = node;
    }

    /** Changes the TriePtr associated with an offset.

        Required to keep the stored pointer associated with an offset consistent
        after its metadata has been modified by an inplace operation.
    */
    void updateNewNode(uint64_t offset, const TriePtr & updatedNode) {
        TriePtr oldNode = newNodesOffset[offset];
        newNodesOffset[offset] = updatedNode;

        ExcAssert(oldNode);
        newNodesPtr.erase(oldNode);
        newNodesPtr[updatedNode] = offset;
    }

    /** Removes the node from the new nodes list and deallocates it. */
    void
    deallocateNewNode(const TriePtr & node, GcLockBase::ThreadGcInfo* info = 0);
    void deallocateNewNode(uint64_t offset, GcLockBase::ThreadGcInfo* info = 0);

    /** Marks a node to be deleted after comitting.
        Note that it should not be a node in the path provided to commit.
    */
    void addOldNode(const TriePtr & node) {
        if (node.state == TriePtr::COPY_ON_WRITE)
            oldNodes.push_back(node);

        // in-place old nodes are deallocated manually so we don't register
        // them for gc.
    }

    /**  Removes the node from the old node list and deallocates it. */
    void
    deallocateOldNode(const TriePtr & node, GcLockBase::ThreadGcInfo* info = 0);

    void commit(const TriePath & oldPath, GcLockBase & gc);

    /** Removes the path from the new node list and deallocates its nodes. */
    void deallocatePath(const TriePath & path, GcLockBase & gc);

    void dump(std::ostream& stream = std::cerr);

    MemoryAllocator & area;
    bool success;

    ML::compact_vector<TriePtr, 32> oldNodes;
    std::unordered_map<uint64_t, uint64_t> newNodesPtr; // TriePtr -> offset
    std::unordered_map<uint64_t, TriePtr> newNodesOffset; // offset -> TriePtr

    struct Cleanup {
        size_t numNodes;
        MemoryAllocator * area;
        TriePtr nodes[0];
    };
};


/*****************************************************************************/
/* GC Recursive                                                              */
/*****************************************************************************/

/** Recursively collect the entire tree at the given node once all
    grace periods have run out.
*/
void gcRecursive(TriePtr node, MemoryAllocator & area, GcLockBase & gc);

void gcNewNodeRecursive(TriePtr node, MemoryAllocator & area, GCList& gc);


} // namespace MMap
} // namespace Datacratic

#endif // __mmap__gc_list_h__
