/* gc_list.cc                                               -*- C++ -*-
   Rémi Attab, 13 Febuary 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Implementation of gc list stuff
*/

#include "gc_list.h"

#include "mmap_trie_node.h"
#include "mmap_trie_path.h"
#include "soa/gc/gc_lock.h"
#include "sync_stream.h"

#include <set>

using namespace std;
using namespace ML;


namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* GC LIST                                                                   */
/*****************************************************************************/


GCList::
~GCList()
{
    // Any nodes left here must have been from a failed operation...
    // if (!newNodes.empty())
    //    cerr << "cleaning up " << newNodes.size() << " failed nodes" << endl;

    if (newNodesOffset.empty()) return;

    GcLockBase::ThreadGcInfo * info = GcLockBase::GcInfo::getThisThread();

    // We don't want to attempt to deallocate the same node twice if deallocate
    // throws so we declare i outside.
    auto it = newNodesOffset.begin();
    auto end = newNodesOffset.end();

    MMAP_PIN_REGION(area.region())
    {
        for (; it != end; ++it) {
            if (!it->second) continue;
            NodeOps::deallocate(it->second, area, info);
        }
    }
    MMAP_UNPIN_REGION;
}

void
GCList::
doCleanup(void * data)
{
    Cleanup * cleanup = reinterpret_cast<Cleanup *>(data);

    ExcAssertGreater(cleanup->numNodes, 0);
    //cerr << "*** DOING CLEANUP OF " << cleanup->numNodes << " NODES ****"
    //         << endl;

    GcLockBase::ThreadGcInfo * info = GcLockBase::GcInfo::getThisThread();

    // We don't want to attempt to deallocate the same node twice if deallocate
    // throws so we declare i outside.
    unsigned i = 0;

    MMAP_PIN_REGION(cleanup->area->region())
    {
        for (; i < cleanup->numNodes; ++i)
            NodeOps::deallocate(cleanup->nodes[i], *cleanup->area, info);
    }
    MMAP_UNPIN_REGION;

    free(cleanup);
}

void
GCList::
commit(const TriePath & oldPath, GcLockBase & gc)
{
    newNodesPtr.clear();
    newNodesOffset.clear();


    // Filter out duplicates by first copying to a set.
    // \todo We really shouldn't have duplicate in the first place.
    //   Currently these comes from the remove ops and the setupRetry()
    //   function in MutableTrieVersion. Have to investigate further.

    set<TriePtr> toCleanup;
    for (unsigned i = 0;  i <= oldPath.size(); ++i)
        toCleanup.insert(oldPath.getNode(i));

    for (unsigned i = 0; i < oldNodes.size(); ++i)
        toCleanup.insert(oldNodes[i]);


    // Setup the struct we want to pass to defer.

    Cleanup * cleanup = reinterpret_cast<Cleanup *>
        (malloc(sizeof(Cleanup) + sizeof(TriePtr) * toCleanup.size()));
    
    cleanup->area = &area;
    cleanup->numNodes = toCleanup.size();

    unsigned i = 0;
    for(auto it = toCleanup.begin(), end = toCleanup.end(); it != end; ++it) {
        cleanup->nodes[i] = *it;
        i++;
    }

#if 0
    gc.visibleBarrier();
    doCleanup(cleanup);

#else
    gc.defer(doCleanup, cleanup);
#endif
}


void
GCList::
deallocateNewNode(const TriePtr & node, GcLockBase::ThreadGcInfo* info)
{
    uint64_t offset = newNodesPtr[node];
    newNodesPtr.erase(node);
    newNodesOffset.erase(offset);

    // It's safer to delete after we've cleaned up the newNodes list
    // because it's possible to find these leaks using graphs.
    NodeOps::deallocate(node, area, info);
}

void
GCList::
deallocateNewNode(uint64_t offset, GcLockBase::ThreadGcInfo* info)
{
    TriePtr node = newNodesOffset[offset];
    newNodesOffset.erase(offset);
    newNodesPtr.erase(node);

    // It's safer to delete after we've cleaned up the newNodes list
    // because it's possible to find these leaks using graphs.
    NodeOps::deallocate(node, area, info);
}


void
GCList::
deallocateOldNode(const TriePtr & node, GcLockBase::ThreadGcInfo* info)
{
    auto pos = find(oldNodes.begin(), oldNodes.end(), node);
    ExcAssert(pos != oldNodes.end());
    oldNodes.erase(pos);

    // It's safer to delete after we've cleaned up the newNodes list
    // because it's possible to find these leaks using graphs.
    NodeOps::deallocate(node, area, info);
}


void
GCList::
deallocatePath(const TriePath & path, GcLockBase & gc)
{
    GcLockBase::ThreadGcInfo * info = GcLockBase::GcInfo::getThisThread();

    for (auto it = path.begin(), end = path.end(); it != end; ++it) {
        ExcAssert(it->isNonTerminal());
        deallocateNewNode(it->node(), info);
    }
    deallocateNewNode(path.root(), info);
}


void
GCList::
dump(ostream& stream) {
    stream << "Old Gc Nodes: " << endl;
    for (auto it = oldNodes.begin(), end = oldNodes.end(); it != end; ++it) {
        stream << "  " << *it << endl;
    }

    stream << "New Gc Nodes: " << endl;
    for (auto it = newNodesPtr.begin(), end = newNodesPtr.end(); it != end; ++it) {
        stream << "  " << it->first << " -> " << it->second << endl;
    }
}


/*****************************************************************************/
/* GC Recursive                                                              */
/*****************************************************************************/

struct GcRecursiveData {
    TriePtr root;
    MemoryAllocator * area;
    GcLockBase * gc;
};

void doGcRecursive(TriePtr node, MemoryAllocator & area, GcLockBase & gc)
{
    auto onEntry = [&] (const KeyFragment &, TriePtr node, uint64_t)
        {
            doGcRecursive(node, area, gc);
        };

    GcLockBase::ThreadGcInfo * info = GcLockBase::GcInfo::getThisThread();
    
    NodeOps::forEachEntry(node, area, info, onEntry, NodeOps::CHILD);
    
    NodeOps::deallocate(node, area, info);
}

void doGcRecursive(void * data_)
{
    GcRecursiveData * data = reinterpret_cast<GcRecursiveData *>(data_);

    MMAP_PIN_REGION(data->area->region())
    {
        doGcRecursive(data->root, *data->area, *data->gc);
    }
    MMAP_UNPIN_REGION;
    
    free(data);
}

void gcRecursive(TriePtr node, MemoryAllocator & area, GcLockBase & gc)
{
    GcRecursiveData * data = reinterpret_cast<GcRecursiveData *>
        (malloc(sizeof(GcRecursiveData)));
    
    data->root = node;
    data->area = &area;
    data->gc = &gc;

    gc.defer(doGcRecursive, data);
}





} // namespace MMap
} // namespace Datacratic
