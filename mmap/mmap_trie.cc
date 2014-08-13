/* mmap_trie.cc
   Jeremy Barnes, 11 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

*/

#include "mmap_trie.h"
#include "mmap_trie_node.h"
#include "mmap_trie_node_impl.h"
#include "mmap_trie_merge.h"
#include "memory_region.h"
#include "debug.h"
#include "profiler.h"
#include "sync_stream.h"
#include "trie_key.h"
#include "soa/gc/gc_lock.h"
#include "jml/arch/cmp_xchg.h"
#include "jml/arch/format.h"
#include "jml/arch/atomic_ops.h"
#include "jml/arch/timers.h"

#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/tuple/tuple.hpp>
#include <unordered_set>
#include <chrono>

using namespace std;


namespace Datacratic {
namespace MMap {

/*****************************************************************************/
/* TRIE ITERATOR                                                             */
/*****************************************************************************/

TrieIterator::
TrieIterator(const TrieIterator& other, const ConstTrieVersion * owner)
    : path_(other.path_), owner_(owner)
{
    ExcCheckEqual(owner_->root, other.owner_->root,
            "Incompatible trie versions");
}

void
TrieIterator::
dump(std::ostream & stream) const
{
    stream << "TrieIterator: valid=" << valid() << " entry="
           << entryNum();
    if (valid())
        stream << " value=" << value();

    MMAP_PIN_REGION_READ(owner_->trie->area().region())
    {
        path_.dump(stream, owner_->trie->area(), owner_->info);
    }
    MMAP_UNPIN_REGION;
}

std::ostream &
operator << (std::ostream & stream, const TrieIterator & it)
{
    it.dump(stream);
    return stream;
}


/*****************************************************************************/
/* CONST TRIE VERSION                                                        */
/*****************************************************************************/

size_t
ConstTrieVersion::
size() const
{
    MMAP_PIN_REGION_READ(trie->area().region())
    {
        return NodeOps::size(root, trie->area(), info);
    }
    MMAP_UNPIN_REGION;
}

TrieIterator
ConstTrieVersion::
begin() const
{
    MMAP_PIN_REGION_READ(trie->area().region())
    {
        return TrieIterator(NodeOps::begin(root, trie->area(), info), this);
    }
    MMAP_UNPIN_REGION;
}

TrieIterator
ConstTrieVersion::
end() const
{
    MMAP_PIN_REGION_READ(trie->area().region())
    {
        return TrieIterator(NodeOps::end(root, trie->area(), info), this);
    }
    MMAP_UNPIN_REGION;
}

std::pair<TrieIterator, TrieIterator>
ConstTrieVersion::
beginEnd() const
{
    MMAP_PIN_REGION_READ(trie->area().region())
    {
        return std::make_pair(
                TrieIterator(NodeOps::begin(root, trie->area(), info), this),
                TrieIterator(NodeOps::end(root, trie->area(), info), this));
    }
    MMAP_UNPIN_REGION;
}
TrieIterator
ConstTrieVersion::
lowerBound(const TrieKey& key) const
{
    MMAP_PIN_REGION_READ(trie->area().region())
    {
        KeyFragment frag = key.getFragment();

        TriePath path = NodeOps::findKey(root, trie->area(), info, frag);
        path.pop_back(); // Remove the offthe end or terminal entry.

        frag.removeBitVec(path.totalBits());

        TriePathEntry entry = NodeOps::lowerBound(
                path.lastNode(), trie->area(), info, frag);

        path += entry;

        if (trieValidate) path.validate(trie->area(), info, true);

        return TrieIterator(path, this);
    }
    MMAP_UNPIN_REGION;
}

TrieIterator
ConstTrieVersion::
upperBound(const TrieKey& key) const
{
    MMAP_PIN_REGION_READ(trie->area().region())
    {
        KeyFragment frag = key.getFragment();
        TriePath path = NodeOps::findKey(root, trie->area(), info, frag);

        path.pop_back(); // Remove the offTheEnd or the terminal entry.
        frag.removeBitVec(path.totalBits());

        TriePathEntry entry = NodeOps::upperBound(
                path.lastNode(), trie->area(), info, frag);

        path += entry;

        // If the ub is the value after the last value in a node then we have to
        // move the iterator forward once to get to the next value.
        if (path.back().isOffTheEnd()) {
            path = NodeOps::findIndex(
                    root, trie->area(), info, path.back().entryNum);
        }

        if (trieValidate) path.validate(trie->area(), info, true);

        return TrieIterator(path, this);
    }
    MMAP_UNPIN_REGION;
}

std::pair<TrieIterator, TrieIterator>
ConstTrieVersion::
bounds(const TrieKey& key) const
{
    MMAP_PIN_REGION_READ(trie->area().region())
    {
        KeyFragment frag = key.getFragment();
        TriePath path = NodeOps::findKey(root, trie->area(), info, frag);

        path.pop_back(); // Remove the offTheEnd or the terminal entry.
        frag.removeBitVec(path.totalBits());

        TriePath lb = path;
        lb += NodeOps::lowerBound(path.lastNode(), trie->area(), info, frag);

        TriePath ub = path;
        ub += NodeOps::upperBound(path.lastNode(), trie->area(), info, frag);

        // If the path is the value after the last value in a node then we have
        // to move the iterator forward once to get to the next value.
        if (lb.back().isOffTheEnd())
            lb = NodeOps::findIndex(root, trie->area(), info, lb.back().entryNum);
        if (ub.back().isOffTheEnd())
            ub = NodeOps::findIndex(root, trie->area(), info, ub.back().entryNum);

        if (trieValidate) {
            lb.validate(trie->area(), info, true);
            ub.validate(trie->area(), info, true);
        }

        return make_pair(TrieIterator(lb, this), TrieIterator(ub, this));
    }
    MMAP_UNPIN_REGION;
}

TrieIterator
ConstTrieVersion::
find(const TrieKey& key) const
{
    MMAP_PIN_REGION_READ(trie->area().region())
    {
        KeyFragment frag = key.getFragment();
        TriePath path = NodeOps::findKey(root, trie->area(), info, frag);
        if (!path.valid())
            return end();
        return TrieIterator(path, this);
    }
    MMAP_UNPIN_REGION;
}

uint64_t
ConstTrieVersion::
operator [] (const TrieKey& key) const
{
    MMAP_PIN_REGION_READ(trie->area().region())
    {
        KeyFragment frag = key.getFragment();
        TriePath path = NodeOps::findKey(root, trie->area(), info, frag);
        if (!path.valid())
            return 0;
        return path.value();
    }
    MMAP_UNPIN_REGION;
}

TrieStats
ConstTrieVersion::
stats(StatsSampling method) const
{
    ExcAssert(
            method == STATS_SAMPLING_RANDOM ||
            method == STATS_SAMPLING_FULL);

    uint64_t size = this->size();

    uint64_t probes;
    if (method == STATS_SAMPLING_FULL)
        probes = size;
    else {
        // \todo That's probably not a statiscally sound number.
        probes = ML::highest_bit(size);
        probes = std::min(size, probes * probes);
    }

    std::unordered_set<uint64_t> visited;

    TrieStats stats;
    size_t i;

    MMAP_PIN_REGION_READ(trie->area().region())
    {
        for (i = 0; i < probes; ++i) {

            size_t index = method == STATS_SAMPLING_FULL ? i : random() % size;

            TriePath path = NodeOps::findIndex(root, trie->area(), info, index);

            stats.avgDepth += path.size();
            stats.maxDepth = std::max<uint64_t>(stats.maxDepth, path.size());

            stats.avgKeyLen += path.totalBits();
            stats.maxKeyLen = std::max<uint64_t>(stats.maxKeyLen, path.totalBits());

            for (size_t j = 0; j < path.size(); ++j) {
                TriePtr node = path.getNode(j);

                // You can have multiple InlineTerm that equal but are different
                // nodes. Since that would screw the pooch, skip them.
                if (node.type != InlineTerm) {
                    if (visited.count(node)) continue;
                    visited.insert(node);
                }

                stats.nodeStats[node.type] +=
                    NodeOps::stats(node, trie->area(), info);
            }
        }
    }
    MMAP_UNPIN_REGION;

    stats.sampling = method;
    stats.avgDepth /= probes;
    stats.avgKeyLen /= probes;
    stats.scale = static_cast<double>(size) / probes;
    stats.totalKeys = size;
    stats.probedKeys = probes;

    return stats;
}

void
ConstTrieVersion::
dump(int indent,
     int maxDepth,
     std::ostream & stream) const
{
    MMAP_PIN_REGION_READ(trie->area().region())
    {
        NodeOps::dump(root, trie->area(), info, indent, maxDepth, stream);
        stream << endl;
    }
    MMAP_UNPIN_REGION;
}

size_t
ConstTrieVersion::
memUsage() const
{
    MMAP_PIN_REGION_READ(trie->area().region())
    {
        return NodeOps::memUsage(root, trie->area(), info);
    }
    MMAP_UNPIN_REGION;
}

pair<TrieIterator, TrieIterator>
ConstTrieVersion::
check() const
{
    if (!root) return make_pair(end(), end());

    auto checkPath = [&] (
            const TriePath::iterator& first,
            const TriePath::iterator& last)
    {
        for (auto it = first; it != last; ++it) {
            if (!it->isNonTerminal()) return;
            NodeOps::gatherKV(it->node(), trie->area(), info);
        }
    };

    auto checkPathDelta = [&] (TriePath& oldPath, TriePath& newPath) {
        ExcAssertEqual(oldPath.key(trie->area(), info).bits % 8, 0);
        ExcAssertEqual(newPath.key(trie->area(), info).bits % 8, 0);

        auto it = newPath.commonSubPath(oldPath);
        checkPath(it, newPath.end());
    };

    auto checkPathSingle = [&] (TriePath& path) {
        ExcAssertEqual(path.key(trie->area(), info).bits % 8, 0);
        checkPath(path.begin(), path.end());
    };

    MMAP_PIN_REGION_READ(trie->area().region())
    {
        TrieIterator beginIt, endIt;

        // Begin validation
        try {
            beginIt = begin();
            checkPathSingle(beginIt.path_);
        }
        catch(const std::exception& ex) {
            cerr << "begin() is corrupted: " << ex.what() << endl;
        }

        // End validation
        try { endIt = end(); }
        catch(const std::exception& ex) {
            cerr << "end() is corrupted: " << ex.what() << endl;
        }

        pair<TrieIterator, TrieIterator> safeIt = { beginIt, endIt -1 };

        // Forward Validation
        if (beginIt.valid()) {
            try {
                for (TrieIterator it = beginIt; it.valid(); ++it) {
                    checkPathDelta(safeIt.first.path_, it.path_);
                    safeIt.first = it;
                }
            }
            catch (const std::exception& ex) {
                cerr << "forward corruption detected: " << ex.what() << endl;
            }
        }

        // Backwards validation
        if (safeIt.first != endIt) {
            try {
                for (TrieIterator it = endIt -1; it.valid(); --it) {
                    checkPathDelta(safeIt.second.path_, it.path_);
                    safeIt.second = it;

                    if (it.path_.entryNum() == 0) break;
                }
            }
            catch (const std::exception& ex) {
                cerr << "backward corruption detected: " << ex.what() << endl;
            }
        }

        ssize_t dist = safeIt.second - safeIt.first;

        // Does everything check out?
        if (safeIt.first.valid() && safeIt.second.valid() && dist <= 0)
            return make_pair(endIt, endIt);

        // Ruh-Roh!

        stringstream ss;
        ss << ">>> CORRUPTED KEYS DETECTED <<<" << endl << endl;
        ss << "first=" << safeIt.first << endl
            << "second=" << safeIt.second << endl
            << "size=" << size() << endl
            << "dist=" << dist << endl
            << endl;

        if (safeIt.first.valid()) {
            string key = safeIt.first.key().cast<std::string>();
            ss << "lower bound: " << key
                << " -> " << hex << safeIt.first.value() << dec << endl
                << "  path=" << safeIt.first.path_ << endl;
        }

        if (safeIt.second.valid()) {
            string key = safeIt.second.key().cast<std::string>();
            ss << "upper bound: " << key
                << " -> " << hex << safeIt.second.value() << dec << endl
                << "  path=: " << safeIt.second.path_ << endl;
        }

        if (safeIt.first.valid() && safeIt.second.valid()) {
            ss << "corrupted keys:"
                << distance(safeIt.first, safeIt.second) << endl;

            ss << "common nodes: " << endl;

            auto last = safeIt.first.path_.commonSubPath(safeIt.second.path_);

            int i = 0;
            for (auto it = safeIt.first.path_.begin(); it != last; ++it) {
                if (!it->isNonTerminal()) break;
                ss << "  " << i++ << ": ";

                KVList kvs = NodeOps::gatherKV(it->node(), trie->area(), info);
                for_each(kvs.begin(), kvs.end(), [&](const KV& kv) {
                            ss << kv << " ";
                        });

                ss << endl;
            }
            ss << endl;
        }

        cerr << ss.str();
        return safeIt;
    }
    MMAP_UNPIN_REGION;

}


/*****************************************************************************/
/* MUABLE TRIE VERSION                                                       */
/*****************************************************************************/

void
MutableTrieVersion::
clear()
{
    ExcAssert(trie);

    MMAP_PIN_REGION(trie->area().region())
    {
        while (!setRoot(TriePtr())) ;
        gcRecursive(root, trie->area(), trie->gc());
        root = TriePtr();
    }
    MMAP_UNPIN_REGION;
}


bool
MutableTrieVersion::
setupFastRetry(
        TriePath& pathToReplace,
        TriePath& subtreeToInsert,
        const TriePath& replaceAttempt,
        GCList & gc)
{
    TriePath current = NodeOps::findKey(
            root, trie->area(), info, replaceAttempt.key(trie->area(), info));

    auto currentCommonIt = current.commonSubPath(pathToReplace);
    if (currentCommonIt == current.end()) {

        if (trieDebug)
            ML::atomic_inc(setRootSlowRetries);

        return false;
    }

    auto replaceCommonIt = pathToReplace.commonSubPath(current);
    if (replaceCommonIt == pathToReplace.end())
        throw ML::Exception("Sanity check - null common subpath");

    // Since we're moving these nodes out of pathToReplace and into
    //   subtreeToInsert,they will no longer be part of the path sent to
    //   commit. We have to clean them up manually.
    for (auto it = replaceCommonIt+1, end = pathToReplace.end();
         it != end; ++it)
    {
        gc.addOldNode(it->node());
    }

    size_t numNodesToRemove = distance(pathToReplace.begin(), replaceCommonIt);
    ExcAssertLessEqual(numNodesToRemove, replaceAttempt.size());

    // Place in subtreeToInsert the nodes to reuse on our next attempt
    TriePath toCleanup;
    tie(toCleanup, subtreeToInsert) =
        replaceAttempt.splitAt(replaceAttempt.begin() + numNodesToRemove);

    // Cleanup attempt nodes that aren't going to be re-used.
    // This is safe because the nodes were visible only to the current
    //   thread.
    gc.deallocatePath(toCleanup, trie->gc());

    pathToReplace = TriePath (current.root());
    // We add 1 to commonIt so that we overlap with the subtreeToInsert tree.
    copy(current.begin(), currentCommonIt+1, back_inserter(pathToReplace));

    if (trieDebug)
        ML::atomic_inc(setRootFastRetries);

    return true;
}


TriePath
MutableTrieVersion::
replaceSubtree(const TriePath & pathToNode, const TriePath & newSubtree,
               GCList & gc)
{
    TriePath pathToReplace = pathToNode;
    TriePath subtreeToInsert = newSubtree;

    while (true) {
        TriePath newRoot = replaceSubtreeRecursive(
                trie->area(), info, pathToReplace, subtreeToInsert, gc, root, 0);

        if (trieValidate) newRoot.validate(trie->area(), info, true);

        if (setRoot(newRoot.root())) {
            ML::memory_barrier();

            gc.commit(pathToReplace, trie->gc());
            root = newRoot.root();

            return newRoot;
        }

        // Unable to change the root, try to redo as little as possible.
        if (!setupFastRetry(pathToReplace, subtreeToInsert, newRoot, gc)) {
            return TriePath();
        }
    }
}

bool
MutableTrieVersion::
replaceSubtree(const TriePath & pathToNode, const TriePtr & newSubtreeRoot,
               GCList & gc)
{
    TriePath pathToReplace = pathToNode;
    TriePath subtreeToInsert (newSubtreeRoot);

    while (true) {
        TriePath newRoot = replaceSubtreeRecursive(
                trie->area(), info, pathToReplace, subtreeToInsert, gc, root, 0);

        if (setRoot(newRoot.root())) {
            ML::memory_barrier();

            gc.commit(pathToReplace, trie->gc());
            root = newRoot.root();

            return true;
        }

        // Unable to change the root, try to redo as little as possible.
        if (!setupFastRetry(pathToReplace, subtreeToInsert, newRoot, gc)) {
            return false;
        }
    }
}


bool
MutableTrieVersion::
setRoot(TriePtr newRoot)
{

    ML::memory_barrier();

    bool result = ML::cmp_xchg(*trie->getRootPtr(), root.bits, newRoot.bits);

    if (trieDebug) {
        if (result) ML::atomic_add(setRootSuccesses, 1);
        else ML::atomic_add(setRootFailures, 1);
    }

    return result;
}

std::pair<TrieIterator, bool>
MutableTrieVersion::
insert(const TrieKey& key, uint64_t value)
{
    // Doing an insert involves the following steps:
    // 1.  We find as much of the key as we can, getting an iterator.
    //     This iterator traces our path up the old tree to the root.
    // 2.  The last node we got to is the node that needs to have the
    //     new value inserted into it, or if the leaf exists, the new
    //     value replaced.  Doing that gives us back the tail of a
    //     TrieIterator that shows the path through the new node.
    // 3.  Finally, we replace the path through the old iterator back
    //     to the root with the new nodes.
    // 4.  We do an atomic swap on the root.  If it succeeds, we can
    //     garbage collect the old iterator from the find.  If it fails,
    //     we garbage collect the new iterator and start again from 1.
    // 5.  We return the new iterator that points to our new node.

    MMAP_PIN_REGION(trie->area().region())
    {
        for (;;) {
            KeyFragment frag = key.getFragment();

            GCList gc(trie->area());

            // Find the old value
            TriePath path = NodeOps::findKey(root, trie->area(), info, frag);

            if (trieValidate) path.validate(trie->area(), info, false);

            // cerr << "inserting: path = " << path << endl;

            // Save the old value if it was found
            TriePath toNewLeaf;

            if (path.valid()) {
                // Already exists, so don't change anything.
                return make_pair(TrieIterator(path, this), false);
            }
            else {
                // Now for the insert.  We insert a leaf underneath the node
                // that we found.
                frag.removeBitVec(path.totalBits());

                // Path will have an OFFTHEEND entry... remove it
                path.pop_back();

               // cerr << "path.lastNode() = " << path.lastNode() << endl;

                toNewLeaf = NodeOps::insertLeaf(
                        path.lastNode(), trie->area(), info,
                        frag, value, TriePtr::COPY_ON_WRITE, gc);
            }

            if (trieValidate) toNewLeaf.validate(trie->area(), info, true);

            //cerr << "toNewLeaf = ";
            //toNewLeaf.dump(cerr, &area);
            //cerr << endl;
            ExcAssert(toNewLeaf.valid());

            // Modify the tree to put this new node in place
            TriePath result = replaceSubtree(path, toNewLeaf, gc);

            if (!result.empty())
                return make_pair(TrieIterator(result, this), true);

            // Something interfered with replacing the subtree... retry
        }
    }
    MMAP_UNPIN_REGION;
}

pair<bool, uint64_t>
MutableTrieVersion::
remove(const TrieKey& key)
{
    MMAP_PIN_REGION(trie->area().region())
    {
        for (;;) {
            KeyFragment frag = key.getFragment();

            GCList gc(trie->area());

            // Find the value to remove.
            TriePath oldPath = NodeOps::findKey(root, trie->area(), info, frag);

            if (!oldPath.back().isTerminal())
                return make_pair(false, -1);

            uint64_t value = oldPath.value();

            oldPath.pop_back(); // pop-off the terminal entry.

            // We backtrack once because we may need to simplify the parent of
            // the node.
            if (!oldPath.empty())
                oldPath.pop_back();

            frag.removeBitVec(oldPath.totalBits());

            TriePtr newSubtreeRoot = NodeOps::removeLeaf(
                    oldPath.lastNode(), trie->area(), info,
                    frag, TriePtr::COPY_ON_WRITE, gc);

            bool result = replaceSubtree(oldPath, newSubtreeRoot, gc);

            // Something interfered with replacing the subtree... retry
            if (!result) continue;

            return make_pair(true, value);
        }
    }
    MMAP_UNPIN_REGION;
}

pair<bool, uint64_t>
MutableTrieVersion::
compareAndRemove(const TrieKey& key, uint64_t oldValue)
{
    MMAP_PIN_REGION(trie->area().region())
    {
        for (;;) {
            KeyFragment frag = key.getFragment();

            GCList gc(trie->area());

            // Find the value to remove.
            TriePath oldPath = NodeOps::findKey(root, trie->area(), info, frag);

            if (!oldPath.back().isTerminal())
                return make_pair(false, -1);

            uint64_t value = oldPath.value();
            if (value != oldValue)
                return make_pair(true, value);

            oldPath.pop_back(); // pop-off the terminal entry.

            // We backtrack once because we may need to simplify the parent of the
            //   node.
            if (!oldPath.empty())
                oldPath.pop_back();

            frag.removeBitVec(oldPath.totalBits());

            TriePtr newSubtreeRoot = NodeOps::removeLeaf(
                    oldPath.lastNode(), trie->area(), info,
                    frag, TriePtr::COPY_ON_WRITE, gc);

            bool result = replaceSubtree(oldPath, newSubtreeRoot, gc);

            // Something interfered with replacing the subtree... retry
            if (!result) continue;

            return make_pair(true, value);
        }
    }
    MMAP_UNPIN_REGION;
}


pair<bool, uint64_t>
MutableTrieVersion::
compareAndSwap(const TrieKey& key, uint64_t oldValue, uint64_t newValue)
{
    MMAP_PIN_REGION(trie->area().region())
    {
        while(true) {

            KeyFragment frag = key.getFragment();

            GCList gc(trie->area());

            TriePath path = NodeOps::findKey(root, trie->area(), info, frag);

            if (!path.valid())
                return make_pair(false, -1);

            if (trieValidate) path.validate(trie->area(), info, true);

            uint64_t curValue = path.value();
            if (curValue != oldValue)
                return make_pair(true, curValue);

            TriePathEntry valueEntry = path.getRelative(path.size()-1);
            TriePath toNewLeaf = NodeOps::replaceValue(
                    path.lastNode(), trie->area(), info,
                    valueEntry, newValue, TriePtr::COPY_ON_WRITE, gc);

            // pop-off the terminal entry to be compatible with replaceSubtree
            path.pop_back();

            TriePath result = replaceSubtree(path, toNewLeaf, gc);

            if (!result.empty())
                return make_pair(true, oldValue);
        }
    }
    MMAP_UNPIN_REGION;
}


/*****************************************************************************/
/* TRANSACTIONAL TRIE VERSION                                                */
/*****************************************************************************/
/** We use more fine grained pinning for transactional tries then in the mutable
    trie because our NodeOps calls will most likely modify an inplace
    node. These modifications are permanent so if a later call
    (convertToInplacePath) throws a resize exception attempting to redo a
    modification could cause havok.

    So the idea is that once a modification is done, we can't go back. A
    modification itself can be restarted because all the inplace node ops have
    been built with the resize exception in mind.

    Commit is the exception here because it has its own internal GCList and it
    doesn't (or shouldn't) modify our trie. While it uses in place nodes,
    everything is cleaned up if a resize exception is thrown.

*/


TriePath
TransactionalTrieVersion::
convertToInplacePath(const TriePath& pathToReplace, TriePath childPath)
{
    // This should not reset if a resize exception is thrown otherwise we'd be
    // modifying nodes that are already modified because they're in place.
    int i = pathToReplace.size() -1;

    MMAP_PIN_REGION(trie->area().region())
    {
        // We always have to walk back to the root because the binary nodes need
        // to have their size values adjusted.
        for (; i >= 0; --i) {
            TriePtr binaryNode = pathToReplace.getNode(i);
            TriePathEntry entry = pathToReplace.getRelative(i);

            ExcAssert(!entry.isTerminal());
            size_t sz = NodeOps::size(childPath.root(), trie->area(), info);

            if (childPath.root() && sz > 0) {
                TriePath newPath = NodeOps::replace(
                        binaryNode, trie->area(), info, entry, childPath.root(),
                        TriePtr::IN_PLACE, *gc);

                childPath = newPath + childPath;
            }

            // Calling NodeOps::replace with a null or empty node will throw so
            // use NodeOps::setBranch instead.
            else {
                KeyFragment key = NodeOps::extractKey(
                        binaryNode, trie->area(), info, entry.entryNum);

                TriePtr newPtr = NodeOps::setBranch(
                        binaryNode, trie->area(), info, key, TriePtr(),
                        TriePtr::IN_PLACE, *gc);

                size_t skipped = NodeOps::size(newPtr, trie->area(), info);

                if (childPath.root() && sz == 0)
                    gc->deallocateNewNode(childPath.root(), info);

                childPath = TriePath(newPtr, TriePathEntry::offTheEnd(skipped));
            }
        }
    }
    MMAP_UNPIN_REGION;


    root = childPath.root();
    return childPath;
}

void
TransactionalTrieVersion::
clear()
{
    root = TriePtr();
    ExcAssert(false); // Reseting the trie ptr isn't enough. Need to do some
                      // cleanup too.
}

std::pair<TrieIterator, bool>
TransactionalTrieVersion::
insert(const TrieKey& key, uint64_t value)
{
    KeyFragment frag = key.getFragment();
    TriePath path;

    MMAP_PIN_REGION(trie->area().region())
    {
        path = NodeOps::findKey(root, trie->area(), info, frag);

        if (trieValidate) path.validate(trie->area(), info, false);
    }
    MMAP_UNPIN_REGION;


    // If it already exists, don't change anything.
    if (path.valid())
        return make_pair(TrieIterator(path, this), false);

    // Path will have an OFFTHEEND entry... remove it
    path.pop_back();
    frag.removeBitVec(path.totalBits());

    TriePath toNewLeaf;
    MMAP_PIN_REGION(trie->area().region())
    {
        toNewLeaf = NodeOps::insertLeaf(
                path.lastNode(), trie->area(), info,
                frag, value, TriePtr::IN_PLACE, *gc);

        ExcAssert(toNewLeaf.valid());
        if (trieValidate) toNewLeaf.validate(trie->area(), info, true);
    }
    MMAP_UNPIN_REGION;


    // Modify all the parent nodes by making them inplace.
    TriePath result = convertToInplacePath(path, toNewLeaf);
    return make_pair(TrieIterator(result, this), true);
}

std::pair<bool, uint64_t>
TransactionalTrieVersion::
replace(const TrieKey& key, uint64_t newValue)
{
    KeyFragment frag = key.getFragment();
    TriePath path;

    MMAP_PIN_REGION(trie->area().region())
    {
        path = NodeOps::findKey(root, trie->area(), info, frag);
    }
    MMAP_UNPIN_REGION;

    // If the path isn't there...
    if (!path.valid())
        return make_pair(false, -1);

    if (trieValidate) {
        MMAP_PIN_REGION_READ(trie->area().region())
        {
            path.validate(trie->area(), info, true);
        }
        MMAP_UNPIN_REGION;
    }

    uint64_t oldValue = path.value();
    TriePathEntry valueEntry = path.getRelative(path.size()-1);

    TriePath toNewLeaf;
    MMAP_PIN_REGION(trie->area().region())
    {
        toNewLeaf = NodeOps::replaceValue(
                path.lastNode(), trie->area(), info,
                valueEntry, newValue, TriePtr::IN_PLACE, *gc);
    }
    MMAP_UNPIN_REGION;

    path.pop_back();
    convertToInplacePath(path, toNewLeaf);
    return make_pair(true, oldValue);
}

std::pair<bool, uint64_t>
TransactionalTrieVersion::
remove(const TrieKey& key)
{
    KeyFragment frag = key.getFragment();
    TriePath oldPath;

    MMAP_PIN_REGION(trie->area().region())
    {
        // Find the value to remove.
        oldPath = NodeOps::findKey(root, trie->area(), info, frag);
    }
    MMAP_UNPIN_REGION;


    // If the key doesn't exist...
    if (!oldPath.back().isTerminal())
        return make_pair(false, -1);

    uint64_t value = oldPath.value();

    oldPath.pop_back(); // pop-off the terminal entry.

    // We backtrack once because we may need to simplify the parent of the node.
    if (!oldPath.empty())
        oldPath.pop_back();

    frag.removeBitVec(oldPath.totalBits());

    TriePtr newSubtreeRoot;

    MMAP_PIN_REGION(trie->area().region())
    {
        newSubtreeRoot = NodeOps::removeLeaf(
                oldPath.lastNode(), trie->area(), info,
                frag, TriePtr::IN_PLACE, *gc);
    }
    MMAP_UNPIN_REGION;

    convertToInplacePath(oldPath, newSubtreeRoot);
    return make_pair(true, value);
}

namespace {

/** Would be fun if I could avoid all these copies... */
ValueDeallocList
kvsToValues(const KVList& kvs)
{
    ValueDeallocList list;
    for (auto it = list.begin(), end = list.end(); it != end; ++it)
        list.push_back({ it->first, it->second });
    return list;
}

} // namespace anonymous


ValueDeallocList
TransactionalTrieVersion::
rollback(bool gatherInsertedList)
{
    ValueDeallocList insertedValues;
    if (gatherInsertedList) {

        KVList rawInsertedValues;
        MMAP_PIN_REGION(trie->area().region())
        {
            rawInsertedValues =
                Merge::MergeRollback::exec(originalRoot, root, trie->area());
        }
        MMAP_UNPIN_REGION;
        insertedValues = kvsToValues(rawInsertedValues);
    }

    gc.reset();
    reset();
    originalRoot = TriePtr();

    return insertedValues;
}


static uint64_t defaultInsertConflict(
        const TrieKey& key, uint64_t baseVal, uint64_t srcVal, uint64_t destVal)
{
    return srcVal;
}

static bool defaultRemoveConflict(
        const TrieKey& key, uint64_t baseVal, uint64_t destVal)
{
    return true;
}

ValueDeallocList
TransactionalTrieVersion::
commit()
{
    return commit(defaultInsertConflict, defaultRemoveConflict);
}

ValueDeallocList
TransactionalTrieVersion::
commit(MergeInsertConflict inConflict, MergeRemoveConflict rmConflict)
{
    namespace ipc = boost::interprocess;
    string name = "trie." + trie->area().region().name(trie->id_);
    ipc::named_mutex mutex(ipc::open_or_create, name.c_str());

    ML::Timer acquireTm;
    ipc::scoped_lock<ipc::named_mutex> lock(mutex);
    mergeAcquireTime += acquireTm.elapsed_wall();

    return commitImpl(inConflict, rmConflict);
}



std::pair<bool, ValueDeallocList>
TransactionalTrieVersion::
tryCommit(const std::function<void()>& preCommit)
{
    return tryCommit(defaultInsertConflict, defaultRemoveConflict, preCommit);
}

std::pair<bool, ValueDeallocList>
TransactionalTrieVersion::
tryCommit(
        MergeInsertConflict inConflict,
        MergeRemoveConflict rmConflict,
        const std::function<void()>& preCommit)
{
    namespace ipc = boost::interprocess;
    string name = "trie." + trie->area().region().name(trie->id_);
    ipc::named_mutex mutex(ipc::open_or_create, name.c_str());

    ML::Timer acquireTm;

    ipc::scoped_lock<ipc::named_mutex> lock(mutex, ipc::try_to_lock);
    mergeAcquireTime += acquireTm.elapsed_wall();

    if (!lock) return { false, ValueDeallocList() };

    if (preCommit) preCommit();

    return { true, commitImpl(inConflict, rmConflict) };
}


/** This is a fairly basic implementation.

    Eventually what we'd want is for the commits to be pushed to a queue and
    have a background do the merges. This way, user threads don't get blocked
    after a commit. The queue could be constructed from a trie so that it's both
    persisted and thread safe. Note that we'd also have to persist the gc list
    for the transactional trie in order to properly clean up after a crash.

    This is tricky to implement because we have to keep the RCU lock on the root
    until we're done merging. If we push the merge into another thread, we won't
    be able to unlock (GcLock doesn't support locking and unlocking from
    diferent threads).

*/
ValueDeallocList
TransactionalTrieVersion::
commitImpl(MergeInsertConflict inConflict, MergeRemoveConflict rmConflict)
{
    static bool initTm = false;
    static ML::Timer tm;

    auto sampleTime = [&]() -> int64_t {
        int64_t res = 0;

        if (initTm) res = tm.elapsed_wall() * 1000000000.0;
        else initTm = true;

        tm.restart();
        return res;
    };

    mergeIdleTime += sampleTime();

    KVList removedValues;

    MMAP_PIN_REGION(trie->area().region())
    {
        auto curRoot = trie->getRootPtr();
        TriePtr oldRoot = TriePtr::fromBits(*curRoot);

        TriePtr newRoot;
        tie(newRoot, removedValues) = threeWayMerge(
                originalRoot, root, oldRoot,
                inConflict, rmConflict,
                trie->area(), *gc);

        // Make sure everything is written.
        ML::memory_barrier();

        ExcAssertEqual(oldRoot, TriePtr::fromBits(*curRoot));
        *curRoot = newRoot.bits;

        gc->commit(TriePath(), trie->gc());
    }
    MMAP_UNPIN_REGION;

    // Save everything to disk.
    // \todo Too excessive and kills performances.
    // trie->area().region().snapshot();

    reset();
    originalRoot = TriePtr();

    mergeActiveTime += sampleTime();
    return kvsToValues(removedValues);
}


} // namespace MMap
} // namespace Datacratic
