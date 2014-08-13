/* mmap_trie_merge.cc                                               -*- C++ -*-
   Rémi Attab, 1 June 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Implementation details for the 3-way trie merge algorithm.

   \todo This file could use some splitting up.
*/

#include "mmap_trie_merge.h"
#include "sync_stream.h"
#include "profiler.h"

#include <algorithm>

using namespace std;
using namespace ML;

namespace Datacratic {
namespace MMap {
namespace Merge {


/*****************************************************************************/
/* MISC UTILS                                                                */
/*****************************************************************************/


void dbg_dump(const string& name, TriePtr node, MemoryAllocator& area)
{
    sync_cerr() << endl << "=== " << name << " ===" << endl;
    NodeOps::dump(node, area, 0, 4, 0, sync_cerr());
    sync_cerr() << endl << endl << sync_dump;
}

/** Useful for tracking down empty binary nodes. */
void dbg_check(
        const string& name, Cursor& dest,
        MemoryAllocator& area, bool die = true)
{
    auto check = [&](TriePtr node) {
        if (!node) return;

        size_t sz = NodeOps::size(node, area, 0);
        if (sz <= 0) {
            sync_cerr() << "\n\n" << name
                << " --------------------------------------------------" << endl
                << "node: " << node << endl
                << "sz: " << sz << endl
                << endl
                << "dest: " << dest.print() << endl
                << "dest.parent: " << dest.parent().print() << endl
                << endl;
            dbg_dump(name, dest.node(), area);
        }
        if (die) ExcAssertGreater(sz, 0);
    };

    check(dest.node());

    auto onNode = [&](const KeyFragment&, TriePtr node) {
        check(node);
    };
    NodeOps::forEachNode(dest.node(), area, 0, onNode);
}


int dbg_indent = 0;
struct DbgIndent
{
    DbgIndent() { dbg_indent += 4; }
    ~DbgIndent() { dbg_indent -= 4; }
};

string indent()
{
    return string(dbg_indent, ' ');
}

bool dbg_valExists(TriePtr node, MemoryAllocator& area)
{
    bool foundValue = false;
    auto onValue = [&](KeyFragment key, uint64_t value) {
        foundValue = foundValue || value == 18446744069481694119ULL;
    };

    NodeOps::forEachValue(node, area, 0, onValue);
    return foundValue;
}



/** Adjusts a src KV so that in can be used safely within dest.

    Essentially it prefixes the part of the src prefix that isn't present in the
    dest cursor.

*/
KV adjustKV(const Cursor& src, const KV& srcKV, const Cursor& dest)
{
    if (src.prefix().bits == dest.prefix().bits)
        return srcKV;

    if (src.prefix().bits > dest.prefix().bits) {
        return {src.prefix().suffix(dest.prefix().bits) + srcKV.key,
                srcKV.value, srcKV.isPtr};
    }

    KeyFragment suffix = dest.prefix().suffix(src.prefix().bits);
    ExcAssertEqual(src.kvsCP.commonPrefixLen(suffix), suffix.bits);

    return { srcKV.key.suffix(suffix.bits), srcKV.value, srcKV.isPtr };
}


/** Adjusts the relative KV so that it's prefix contains the full key since the
    begining of the trie.

    \todo only used in a single place. Should probably get rid of it.
*/
KV toAbsolute(const KV& absolute, const KV& relative)
{
    return { absolute.key + relative.key, relative.value, relative.isPtr };
}


/** Replaces the node at newBranch.key within node by newBranch.getPtr().

*/
void replaceBranch(
        TriePtr node, const KV& newBranch, MemoryAllocator& area, GCList& gc)
{
    TriePtr newNode = NodeOps::setBranch(
            node, area, 0, newBranch.key, newBranch.getPtr(),
            TriePtr::IN_PLACE, gc);

    // The node should have been inplaced before we called so it should be the
    // same.
    ExcAssertEqual(newNode, node);
}

/** If we're dealing with an empty node, then this function will get rid of it.

    \todo We could get fancier (eg. if branching && size == 1 then push
    prefix to child or make leaf.
*/
bool simplifyNode(Cursor& node, MemoryAllocator& area, GCList& gc)
{
    if (!node.node()) return false;
    if (NodeOps::size(node.node(), area, 0) > 0) return false;

    gc.addOldNode(node.node());
    node.setNode(TriePtr());

    if (!node.hasParent()) return true;
    replaceBranch(
            node.parent().node(), { node.relativePrefix(), node.node() },
            area, gc);

    return true;
}

/** When going back up the trie, we need to udpate the size information for each
    parent of modified node. This function updates the parent of a node with the
    correct size information.

*/
void updateDestNode(Cursor& node, MemoryAllocator& area, GCList& gc)
{
    // As we're backing out of the node, try to simplify it if possible. If
    // simplifyNode returns true, then our parent was already updated so move
    // on.
    if (simplifyNode(node, area, gc)) return;

    if (!node.hasParent()) return;

    // While not really designed for this, it works and it's lightweigth for
    // inplace nodes.
    replaceBranch(
            node.parent().node(), { node.relativePrefix(), node.node() },
            area, gc);
}

/** Returns all the inplace nodes in a src subtree.

    This is going to be used to find the subset of src nodes within the
    TransactionalTrie's gc list's newNodes that are no longer needed.

    noCowNodes is used to assert that there are no CoW nodes in the subtree
    we're looking at. It's a debugging tool used to validate the assumption that
    a subtree we're trying to insert into dest should only consist of in place
    nodes. If they're not then that means that we could be inserted a base node
    in dest after it was removed which would mean that we have a zombie
    infestion that can only be solved with a healthy dose of shotgun applied to
    the forhead of said zombies.
 */
void getSrcNodes(
        NodeList& list, TriePtr node, MemoryAllocator& area,
        bool noCowNodes = false)
{
    auto onEntry = [&] (const KeyFragment&, TriePtr child, uint64_t) {
        getSrcNodes(list, child, area);
    };

    if (node.state != TriePtr::IN_PLACE) {
        ExcAssert(!noCowNodes);
        return;
    }

    // cerr << "keepers: " << node << endl;

    list.push_back(node);
    NodeOps::forEachEntry(node, area, 0, onEntry, NodeOps::CHILD);
}


/** Adds all the dest node within a given subtree to the gc list's oldNode list.

    I don't think it's a good idea to replace this with gc_list.h's gcRecursive
    function because of the duplicate detection we have when we're processing
    the gc.

    Also if we're ever going to parallelize the merge, it's better to do the
    trie crawling during the merge so that it can be part of the parallized
    processing.

*/
void reclaimDestSubtree(
        const KeyFragment& prefix, TriePtr node,
        MemoryAllocator& area, GCList& gc,
        KVList& removedValues)
{
    auto onEntry = [&] (const KeyFragment& key, TriePtr child, uint64_t value) {
        if (child) {
            reclaimDestSubtree(
                prefix + key, child, area, gc, removedValues);
        }
        else removedValues.push_back({ prefix + key, value });
    };

    gc.addOldNode(node);
    NodeOps::forEachEntry(node, area, 0, onEntry);
}

/** When moving a src subtree into the dest trie and we're not in a 2-way merge
    scenario then we can assume that all the base nodes of that subtree were
    marked for GC by dest. This means that if we don't make a copy of those
    nodes we will lose them on the next GC pass.

    So look for any base nodes within a subtree (any nodes marked as CoW) and
    copy them over by making them in place. Also gather a list of all the src
    nodes (nodes marked as in place prior to the state change) within that
    subtree that we should keep after the merge.

    \deprecated I'm 90-ish% sure that we should never find a CoW node in a src
    subtree that we're trying to insert into dest. Otherwise that would mean
    zombies and zombies are bad. See getSrcNode's doc for more details.

 */
#if 0
void copySrcSubtree(
        TriePtr parent, const KV& node,
        NodeList& srcKeepers, MemoryAllocator& area, GCList& gc)
{
    auto onEntry = [&] (const KeyFragment& key, TriePtr child, uint64_t) {
        copySrcSubtree(node.getPtr(), {key, child}, srcKeepers, area, gc);
    };

    if (node.getPtr().state == TriePtr::IN_PLACE)
        srcKeepers.push_back(node.getPtr());

    else {
        GCList tempGc(area);

        TriePtr newNode = NodeOps::changeState(
                node.getPtr(), area, 0, TriePtr::IN_PLACE, tempGc);

        if (parent)
            replaceBranch(parent, {node.key, newNode}, area, tempGc);

        // The node is already marked for gc so we want to forget oldNodes.
        gc.newNodesOffset.insert(
                tempGc.newNodesOffset.begin(), tempGc.newNodesOffset.end());
        tempGc.newNodesOffset.clear();

        gc.newNodesPtr.insert(
                tempGc.newNodesPtr.begin(), tempGc.newNodesPtr.end());
        tempGc.newNodesPtr.clear();
    }

    NodeOps::forEachEntry(node.getPtr(), area, 0, onEntry, NodeOps::CHILD);
}
#endif

TriePtr prefixNode(
        TriePtr node, const KeyFragment& toPrefix,
        MemoryAllocator& area, GCList& gc);

/** We simply can't modify any nodes in src but we occasionally need to adjust a
    src prefix before using it in dest. This function works around this problem
    by creating a new node which is a copy of the src node but with the added
    prefix.

*/
TriePtr prefixNode(
        TriePtr node, const KVList& kvs_, const KeyFragment& toPrefix,
        MemoryAllocator& area, GCList& gc)
{
    // I predict tons of bugs as a result of this little tweak...
    if (kvs_.size() == 1) {
        const KV& child = kvs_.front();
        if (child.isPtr)
            return prefixNode(child.getPtr(), toPrefix + child.key, area, gc);
    }

    KVList kvs = kvs_;

    for (int i = 0; i < kvs.size(); ++i)
        kvs[i].key = toPrefix + kvs[i].key;

    gc.addOldNode(node);

    if (NodeOps::isBranchingNode(node, area, 0))
        return makeBranchingNode(area, 0, kvs, TriePtr::IN_PLACE, gc);

    TriePath res = makeMultiLeafNode(
            area, 0, kvs, kvs.size(), TriePtr::IN_PLACE, gc);
    return res.root();
}

TriePtr prefixNode(
        TriePtr node, const KeyFragment& toPrefix,
        MemoryAllocator& area, GCList& gc)
{
    return prefixNode(
            node, NodeOps::gatherKV(node, area, 0), toPrefix, area, gc);
}


/** If we're in a 2 way merge situation (base == dest) then all we need to do is
    replaces the dest subtree by the src subtree.

*/
void swapSubtrees(Cursor& src, Cursor& dest, MemoryAllocator& area, GCList& gc)
{
    TriePtr srcNode = src.node();

    if (src.prefix().bits > dest.prefix().bits) {
        KeyFragment suffix = src.prefix().suffix(dest.prefix().bits);
        srcNode = prefixNode(srcNode, src.kvs, suffix, area, gc);
    }

    // \todo this branch will leak src.node() because a call to this is always
    // preceded by a call to MergeGC which will assume we need the root of the
    // subtree that we're about to replace. Shouldn't be too hard to fix.
    else if (src.prefix().bits < dest.prefix().bits) {
        ExcAssertGreaterEqual(
                dest.prefix().commonPrefixLen(src.commonPrefix()),
                dest.prefix().bits);

        int32_t toRemove = dest.prefix().bits - src.prefix().bits;

        KVList kvs = src.kvs;
        for (int i = 0; i < kvs.size(); ++i)
            kvs[i].key.removeBitVec(toRemove);

        srcNode = makeBranchingNode(area, 0, kvs, TriePtr::IN_PLACE, gc);
    }

    dest.setNode(srcNode);

    if (!dest.hasParent()) return;
    replaceBranch(
            dest.parent().node(), { dest.relativePrefix(), srcNode }, area, gc);
}

/*****************************************************************************/
/* CURSOR                                                                    */
/*****************************************************************************/

KeyFragment
Cursor::
initKvsCommonPrefix()
{
    if (kvs.empty())
        return KeyFragment();

    if (kvs.size() > 1 || !kvs.front().isPtr)
        return kvs.commonPrefix();

    /** This little hack makes the merge algo more tolerant of irregular
        branching nodes.

        On their best days, branching node's KVList should ALWAYS have at least
        2 elements. Either 2 branches, a branch and a value or 2 branches and a
        value. In this scenario, the kvsCP will always become the prefix of the
        node while the key for the branches will have an extra bit to determine
        which branch they are.

        Because the algorithm operates on the kvsCP, it will ALWAYS find a
        BranchingPoint just after we've exhausted the entire prefix. This will
        allow us to then load the next set of KVs associated with whichever
        branch we want to dig into. If I remember correctly, there are other
        places that also depend on the idea that kvsCP is equal to the prefix of
        a branching node.

        Now every once in a blue moon, it's possible that we end up with a
        branching node's KVList that will have only 1 KV for one of it's
        branches. In this case the kvsCP will be equal to the key of
        branch. This will lead to all sorts of strange behavious in the algo
        because kvsCP no longer represents the prefix of a node.

        Note that for a narrowed n-ary node, it's perfectly acceptable that we
        have a lone ptr in the kvs. This makes the hack maginally more
        acceptable :)

    */
    const KV& kv = kvs.front();
    return kv.key.prefix(kv.key.bits - 1);
}

namespace {

/** Creates a chain of ephemeral cursors to link a cursor created by
    advanceToBranch to an existing cursor. This comes in handy if an n-ary node
    was broken multiple times by a series of insert ops and the current cursor
    isn't a direct parent of the new cursor. In this case we still need a valid
    parent for our new cursor so we construct a series of cursors to bridge the
    gap between our cursor and the new cursor.

    This op should be extremely rare so it doesn't matter if we take our time.
    This means that while we probably don't need to gather the KVs for the node,
    we still do it in case it's ever needed. Better safe then sorry when it
    doesn't impact performances.
 */
Cursor* makeCursorChain(
        MemoryAllocator& area,
        const TriePath& path,
        Cursor* root,
        KeyFragment branchKey,
        KeyFragment prefix)
{
    if (!path.back().isNonTerminal() && path.size() < 2)
        return root;

    Cursor* parent = root;
    for (size_t i = 1; path.getNode(i) != path.lastNode(); ++i) {
        int32_t splitBit = path.getRelative(i - 1).bitNum;
        prefix += KeyFragment(branchKey.removeBitVec(splitBit), splitBit);

        KVList kvs = NodeOps::gatherKV(path.getNode(i), area, 0);
        parent = new Cursor({ prefix, path.getNode(i) }, kvs, parent, parent != root);
    }

    return parent;
}

} // namespace anonymous

Cursor
Cursor::
advanceToBranch(const BranchingPoint& point, int branch, MemoryAllocator& area)
{
    /** Check to see if our cached kvs can still be reliably used to navigate
        the trie.

        When dealing with nary nodes, an insert may break up the branching
        portion of the node into multiple level of sub nodes. Unfortunately,
        when this happens we can't use our cached kvs to navigate the trie so
        we'll just have to use what's in the mmap directly.

        Note that this branch is generic enough that it can be used as the
        entire implementation of advanceToBranch. The downside is that
        invalidating our cache carries a heavy cost which we can avoid 99.9% of
        the time so it's better to just do a quick probe and avoid the cost when
        possible.

        Note that if the branching point is in our prefix then our kvs will not
        be checked in anyway so we can just skip ahead.
     */
    if (point.bitNum >= prefix().bits) {
        const KeyFragment branchPrefix = point.branchPrefix(branch);
        const TriePath path = NodeOps::findKey(node(), area, 0, branchPrefix);

        if (path.lastNode() != node()) {
            ExcAssert(!path.empty());

            KV kv = { prefix() + branchPrefix, path.lastNode() };
            KVList kvs = NodeOps::gatherKV(path.lastNode(), area, 0);

            if (!path.back().isNonTerminal()) {
                unsigned matchedBits = path.getAbsolute(path.size() - 2).bitNum;
                unsigned traillingBits = branchPrefix.bits - matchedBits;
                ExcAssertLessEqual(matchedBits, branchPrefix.bits);

                kv.key = kv.key.prefix(kv.key.bits - traillingBits);
                kvs = kvs.narrow(branchPrefix.suffix(matchedBits));
            }

            Cursor* parent = makeCursorChain(
                    area, path, this, branchPrefix, prefix());
            return Cursor(kv, kvs, parent, parent != this);
        }
    }

    /** Check if we're about to exhaust the current node. If that's the case
        then we need to load up the next node.
     */
    if (point.branchIsPtr(branch)) {

        const KV& kv = point.branch(branch);

        // We should always see a branching point on the next to last bit (hence
        // the - 1) which gives us a chance to load the child node. This
        // guarantee is provided by initKvsCommonPrefix().
        unsigned lastBit = prefix().bits + kv.key.bits - 1;

        // If we've exhausted every bit of the branch then load the child node.
        if (lastBit == point.bitNum) {
            ExcAssertEqual(lastBit, commonPrefixLen());

            KVList newKvs = NodeOps::gatherKV(kv.getPtr(), area, 0);
            return Cursor(toAbsolute(head->prefix, kv), newKvs, this);
        }
    }

    // Otherwise just reduce our currset set of KVs and return.
    return Cursor(*this, point.branchItPair(branch));
}

Cursor
Cursor::
advanceToValue(const BranchingPoint& point, MemoryAllocator& area) const
{
    ExcAssert(point.hasValue());
    return Cursor(*this, point.valueItPair());
}

pair<bool, Cursor>
Cursor::
advanceToCursor(const Cursor& c0, const Cursor& c1, MemoryAllocator& area)
{
    // Cursor is as far as we're willing to go (this function doesn't narrow).
    if (isTerminal())
        return make_pair(false, *this);

    // Go as far as we can without overtaking any of the 2 cursors. If we
    // overtake then the modifications may need to be applied to our parents
    // which is a big no-no.

    // Some quick pre-filtering to avoid the loop if possible.
    int32_t targetBit = std::min(c0.prefix().bits, c1.prefix().bits);
    if (targetBit < commonPrefixLen())
        return make_pair(false, *this);

    // ditto.
    KeyFragment toMatch = c0.prefix().suffix(prefix().bits);
    if (kvsCP.commonPrefixLen(toMatch) != kvsCP.bits)
        return make_pair(false, *this);

    // Alright, time to do it the hard way.
    for (auto it = kvs.begin(), end = kvs.end(); it != end; ++it) {
        if (!it->isPtr) continue;
        if (targetBit < it->key.bits + prefix().bits) continue;
        if (it->key.commonPrefixLen(toMatch) != it->key.bits) continue;

        KVList kvs = NodeOps::gatherKV(it->getPtr(), area, 0);

#if 0 // debug assertion.
        auto kv = toAbsolute(head->prefix, *it);
        ExcAssertEqual(kv.key.commonPrefixLen(c0.prefix()), kv.key.bits);
        ExcAssertEqual(kv.key.commonPrefixLen(c1.prefix()), kv.key.bits);
#endif

        return make_pair(true, Cursor(toAbsolute(head->prefix, *it), kvs, this));
    }

    return make_pair(false, *this);
}


void
Cursor::
reload(MemoryAllocator& area)
{
    ExcAssert(!isNarrowed());

    kvs = NodeOps::gatherKV(node(), area, 0);
    kvsCP = initKvsCommonPrefix();

    if (!hasParent()) return;

    Cursor& parent = this->parent();
    ExcAssert(!parent.isNarrowed());

    parent.kvs = NodeOps::gatherKV(parent.node(), area, 0);
    parent.kvsCP = parent.initKvsCommonPrefix();
}

void
Cursor::
toInPlace(MemoryAllocator& area, GCList& gc)
{
    TriePtr oldNode = node();
    if (!oldNode) return;

    TriePtr newNode = NodeOps::changeState(
            oldNode, area, 0, TriePtr::IN_PLACE, gc);

    if (newNode == oldNode) return;
    setNode(newNode);

    if (!hasParent()) return;
    replaceBranch(parent().node(), { relativePrefix(), newNode }, area, gc);

}

std::string
Cursor::
print() const
{
    stringstream ss;
    ss << "<C head=" << head.get();
    ss << ", prefix=" << prefix();
    ss << ", node={ " << node() << " }";
    if (hasParent()) ss << ", parent={ " << parent().node() << " }";
    if (head->ephemeralParent) ss << "-e";
    if (narrowed) ss << ", narrowed";
    ss << ", kvsCP=" << kvsCP;

    for (int i = 0; i < kvs.size(); ++i)
        ss << endl << "\t" << i << " -> " << kvs[i];
    ss << " >";
    return ss.str();
}


/*****************************************************************************/
/* BRANCHING POINT                                                           */
/*****************************************************************************/


BranchingPoint::
BranchingPoint(const Cursor& cursor, int32_t bitNum) :
    cursor(cursor), bitNum(bitNum)
{
    init();
}

void
BranchingPoint::
init()
{
    // Happens in weird degenerate cases.
    if (cursor.kvs.empty()) {
        branchIt[0] = make_pair(nullIt(), nullIt());
        branchIt[1] = make_pair(nullIt(), nullIt());
        valueIt = nullIt();
        return;
    }

    int32_t cpLen = cursor.kvsCP.bits;
    int32_t prefixLen = cursor.prefix().bits;

    // If we fall off within the common prefix then all the nodes go on one
    // side.
    if (bitNum < prefixLen || (bitNum - prefixLen) < cpLen) {
        int branch;
        if (bitNum < prefixLen)
            branch = cursor.prefix().getBits(1, bitNum);
        else
            branch = cursor.kvsCP.getBits(1, bitNum - prefixLen);

        branchIt[branch] = make_pair(cursor.kvs.begin(), cursor.kvs.end());
        branchIt[1 - branch] = make_pair(nullIt(), nullIt());
        valueIt = nullIt();

        return;
    }

    // This can't happen because otherwise we would have branched here and
    // reduced the cursor appropriately.
    ExcAssertEqual(bitNum - prefixLen, cursor.kvsCP.bits);

    // Otherwise we need to split up the kvs

    auto start = cursor.kvs.begin();
    auto end = cursor.kvs.end();
    valueIt = end;

    if (cursor.kvs.front().key.bits == cpLen) {
        valueIt = start;
        ExcAssert(!valueIt->isPtr);
        start++;
    }

    auto split = start;
    for (; split != end && split->key.getBits(1, cpLen) == 0; split++);

    branchIt[0] = make_pair(start, split);
    branchIt[1] = make_pair(split, end);
}


std::string
BranchingPoint::
print() const
{
    stringstream ss;
    ss << "<BP "
        << "bitNum=" << bitNum
        << ", cursor={ " << cursor.node() << " }";
    if (hasValue()) ss << ", value=" << *valueIt;

    for (int branch = 0; branch < 2; ++branch) {
        if (!hasBranch(branch)) continue;
        auto pair = branchItPair(branch);
        ss << endl << "\tb" << branch << "=" << KVList(pair.first, pair.second);
    }
    ss << " >";
    return ss.str();
}


/*****************************************************************************/
/* MERGE BASE                                                                */
/*****************************************************************************/

/** This recursion is bounded because at each step we take down the tree, we
    narrow either or both the c0 and c1 cursors. Meaning that if we're dealing
    with terminal nodes they will both eventually become empty and we won't
    recurse down. If we're dealing with branching nodes then they will
    eventually decay to terminal nodes.

*/
void
MergeBase::
mergeImpl(Cursor& c0, Cursor& c1, Cursor& extra)
{
    // Figure out where the next break happens.
    int32_t cpLen = c0.commonPrefix().commonPrefixLen(c1.commonPrefix());

    // The branching point can't be in both node's prefix. Technically it
    // shouldn't be in either but reloading a cursor can make this happen.
    ExcAssertGreaterEqual(cpLen, std::min(c0.prefix().bits, c1.prefix().bits));

    BranchingPoint p0(c0, cpLen);
    BranchingPoint p1(c1, cpLen);

    // cerr << "==============================================================="
    //     << endl
    //     << "c0: " << c0.print() << endl
    //     << "c1: " << c1.print() << endl
    //     << endl
    //     << "p0: " << p0.print() << endl
    //     << "p1: " << p1.print() << endl
    //     << endl;

    ExcAssertEqual(p0.bitNum, p1.bitNum);

    bool doMerge = false;

    for (int branch = 0; branch < 2; ++branch) {
        if (!p0.hasBranch(branch) && !p1.hasBranch(branch))
            continue;

        if (!p0.hasBranch(branch) || !p1.hasBranch(branch)) {
            doMerge = true;
            continue;
        }

        Cursor newC0 = c0.advanceToBranch(p0, branch, area);
        Cursor newC1 = c1.advanceToBranch(p1, branch, area);
        merge(newC0, newC1, extra);
    }

    if (p0.hasValue() || p1.hasValue())
        doMerge = true;

    if (doMerge)
        mergeBranchingPoints(c0, p0, c1, p1, extra);
}


/******************************************************************************/
/* MERGE ROLLBACK                                                             */
/******************************************************************************/

KVList
MergeRollback::
exec(TriePtr base, TriePtr src, MemoryAllocator& area)
{
    Profiler prof("merge.rollback");

    MergeRollback merge(area);

    Cursor baseCursor(base, NodeOps::gatherKV(base, area, 0));
    Cursor srcCursor (src,  NodeOps::gatherKV(src, area, 0));
    Cursor nil;
    merge.merge(baseCursor, srcCursor, nil);

    return merge.insertedValues;
}

void
MergeRollback::
merge(Cursor& base, Cursor& src, Cursor& nil)
{
    // If it belongs to base then stay away.
    if (base.node() == src.node())
        return;

    mergeImpl(base, src, nil);
}

void
MergeRollback::
mergeBranchingPoints(
        Cursor& base, const BranchingPoint& basePoint,
        Cursor& src, const BranchingPoint& srcPoint,
        Cursor& nil)
{
    for (int branch = 0; branch != 2; ++branch) {

        // We only want things that only exist in src (inserted values).
        if (basePoint.hasBranch(branch) || !srcPoint.hasBranch(branch))
            continue;

        BranchingPoint::iterator it, end;
        for (tie(it, end) = basePoint.branchItPair(branch); it != end; ++it) {

            // Gather all the values in the given subtree
            if (it->isPtr) {
                auto onValue = [&](const KeyFragment& kf, uint64_t value) {
                    insertedValues.push_back({ kf, value });
                };
                NodeOps::forEachValue(
                        it->getPtr(), area, 0, onValue, src.prefix());
            }

            // Add the single value.
            else {
                KV kv(src.prefix() + it->key, it->getValue());
                insertedValues.push_back(kv);
            }
        }
    }

    // Are we looking at a new value?
    if (srcPoint.hasValue()) {
        if (!basePoint.hasValue() || basePoint.value() != srcPoint.value()) {
            KV kv = srcPoint.valueKV();
            kv.key = src.prefix() + kv.key;
            insertedValues.push_back(kv);
        }
    }
}



/*****************************************************************************/
/* MERGE GC                                                                  */
/*****************************************************************************/

void
MergeGC::
exec(   Cursor& base, Cursor& src,
        MemoryAllocator& area, GCList& gc,
        NodeList& srcKeepers, KVList& removedValues)
{
    Profiler prof("merge.gc");

    Cursor nil;
    MergeGC(area, gc, removedValues).merge(base, src, nil);

    // While this could be merged into the main recursion loop, it's just far
    // less error prone and simpler to keep it here.
    getSrcNodes(srcKeepers, src.node(), area);
}

void
MergeGC::
merge(Cursor& base, Cursor& src, Cursor& nil)
{
    // Make sure we don't deallocate nodes in base that src points to.
    if (base.node() == src.node())
        return;

    // We're pointing to the same place in the trie but at 2 different nodes.
    if (base.prefix() == src.prefix()) {
        // The extra check avoids adding the same node twice when we're
        // repeatively recursing on terminal nodes to get the removedValues.
        if (gc.oldNodes.empty() || gc.oldNodes.back() != base.node())
            gc.addOldNode(base.node());
    }

    mergeImpl(base, src, nil);
}

void
MergeGC::
mergeBranchingPoints(
        Cursor& base, const BranchingPoint& basePoint,
        Cursor& src, const BranchingPoint& srcPoint,
        Cursor& nil)
{
    for (int branch = 0; branch != 2; ++branch) {

        // We only want everything that is in base and not in src
        if (!basePoint.hasBranch(branch) || srcPoint.hasBranch(branch))
            continue;

        BranchingPoint::iterator it, end;
        for (tie(it, end) = basePoint.branchItPair(branch); it != end; ++it) {

            if (!it->isPtr) {
                removedValues.push_back({
                            base.prefix() + it->key, it->getValue() });
                continue;
            }

            auto onEntry = [&](
                    const KeyFragment& key, TriePtr node, uint64_t value)
                {
                    if (node) gc.addOldNode(node);
                    else removedValues.push_back({base.prefix() + key, value});

                };

            gc.addOldNode(it->getPtr());
            NodeOps::forEachEntry(it->getPtr(), area, 0, onEntry);
        }
    }

    if (basePoint.hasValue()) {
        if (!srcPoint.hasValue() || basePoint.value() != srcPoint.value()) {
            KV kv = basePoint.valueKV();
            kv.key = base.prefix() + kv.key;
            removedValues.push_back(kv);
        }
    }
}


/** Commits All the various bits and pieces of gc'ed data.
    This is a bit hacky and could use a better solution like reference counting.

    When calling this function the various lists look like this:

    - mergeGc.newNodes: Nodes of the merge to be gc-ed in case of a resize.

    - mergeGc.oldNodes: Mix of base and dest nodes that were replaced during the
            merge. Note that dest is just a continuation of base so by cleaning
            up dest we're also cleaning up base which is done during regular
            trie manipulations. Unfortunately, the dest trie will also contain
            src subtrees which will not be cleaned-up by regular trie
            manipulations. The following blobs of text explain how we can
            recover this subset.

    - trieGc.newNodes: Nodes of the trie (src) to be gc-ed in case of a
            rollback. Note that only a subset of these nodes will make it to the
            merged trie. The rest should be deallocated. To figure out which
            subset to deallocate, we'll use srcKeepers

    - trieGc.oldNodes: Nodes of base that were replaced by src. There's a subset
            of these nodes we REALLY don't want to deallocate because they
            already are marked for a deallocation by a previous merge pass. The
            problem is it's hard to tell which is which so we use MergeGc to
            create a new safe set which is inserted directly into
            mergeGc.oldNodes. So all of that to say that we really don't care
            about these nodes anymore :)

    - srcKeepers: Subset of trieGc.newNodes that are present within the dest
            subtree. This is gathered by MergeGc.

    Note that we can't commit the nodes here because we haven't changed the root
    of the trie yet which would violate the whole RCU thing. Instead we just
    rebuild the trieGc with the right set of nodes and let the caller deal with
    it.

    \todo Change the name of this function since we don't really commit anything
    in this function anymore. Unfortunately I'm lacking imagination at this
    moment so I'll let future-me handle this one for me.

*/
void commitGc(GCList& mergeGc, GCList& trieGc, NodeList& srcKeepers)
{
    Profiler prof("merge.commit");

    auto toPtr = [] (const pair<uint64_t, TriePtr>& entry) -> TriePtr {
        return entry.second;
    };

    NodeList trieNewNodes;
    transform(
            trieGc.newNodesOffset.begin(), trieGc.newNodesOffset.end(),
            back_inserter(trieNewNodes), toPtr);

    // set_xxx functions require that the lists be sorted.
    sort(srcKeepers.begin(), srcKeepers.end());
    sort(trieNewNodes.begin(), trieNewNodes.end());

    // Debug sanity check.
    // All the src keeper nodes have to be in trieNewNodes.
    // \todo Shoving this in an assert like block would be nice.
    {
        NodeList intersect;
        set_intersection(
                srcKeepers.begin(), srcKeepers.end(),
                trieNewNodes.begin(), trieNewNodes.end(),
                back_inserter(intersect));

        if (intersect.size() != srcKeepers.size()) {
            NodeList diff;
            set_difference(
                    srcKeepers.begin(), srcKeepers.end(),
                    trieNewNodes.begin(), trieNewNodes.end(),
                    back_inserter(diff));

            // Turns out that inline nodes are never put into the newNodes list
            // during regular trie manipulations but our algorithm records
            // them. In any case, inline nodes are harmless so don't blow up if
            // they're the only things that don't match.
            ExcAssert(all_of(diff.begin(), diff.end(), [&] (TriePtr node) {
                                return node.type == 2;
                            }));
        }
    }

    trieGc.oldNodes.clear(); // Commiting these would lead to double-frees
    trieGc.oldNodes = move(mergeGc.oldNodes);
    mergeGc.oldNodes.clear();

    trieGc.newNodesOffset.clear();
    trieGc.newNodesOffset = move(mergeGc.newNodesOffset);
    mergeGc.newNodesOffset.clear();

    trieGc.newNodesPtr.clear();
    trieGc.newNodesPtr = move(mergeGc.newNodesPtr);
    mergeGc.newNodesPtr.clear();

    // The nodes in trieNewNodes that are not present in srcKeepers are
    // marked for deallocation.
    set_difference(
            trieNewNodes.begin(), trieNewNodes.end(),
            srcKeepers.begin(), srcKeepers.end(),
            back_inserter(trieGc.oldNodes));

    // somewhat pointless because newNodes will get cleared during commit()
    // trieGc.newNodes.insert(srcKeepers.begin(), srcKeepers.end());
}


/*****************************************************************************/
/* MERGE INSERT                                                              */
/*****************************************************************************/

void
MergeInsert::
exec(   Cursor& base, Cursor& src, Cursor& dest,
        MergeInsertConflict conflict,
        MemoryAllocator& area, GCList& gc,
        NodeList& srcKeepers, KVList& removedValues)
{
    Profiler prof("merge.insert");

    MergeInsert merger(conflict, area, gc, srcKeepers, removedValues);
    merger.merge(src, dest, base);
}


void
MergeInsert::
merge(Cursor& src, Cursor& dest, Cursor& base)
{
    Cursor newBase;
    bool advanced;
    tie(advanced, newBase) = base.advanceToCursor(src, dest, area);
    if (advanced) {
        merge(src, dest, newBase);
        return;
    }

    // 2-way merge, replace the dest subtree with the src subtree.
    if (base.node() == dest.node() && !src.isNarrowed() && !dest.isNarrowed()) {
        MergeGC::exec(base, src, area, gc, srcKeepers, removedValues);

        swapSubtrees(src, dest, area, gc);
        updateDestNode(dest, area, gc);
        return;
    }

    dest.toInPlace(area, gc);
    mergeImpl(src, dest, base);
    updateDestNode(dest, area, gc);
}

void
MergeInsert::
mergeBranchingPoints(
        Cursor& src, const BranchingPoint& srcPoint,
        Cursor& dest, const BranchingPoint& destPoint,
        Cursor& base)
{

    /** Calling simplifyNode() here is a bit of a hack.

        The problem is that if we get an empty binary node as the dest node and
        it has a prefix then inserting a value into its prefix will cause the
        prefix to be broken up. The empty binary node will therefor become a
        child of the resulting node which puts it just out of reach of the
        updateDestNode() function call. Annoying.

        By simplifying first, we avoid that problem and it shouldn't affect
        anything else since we're about to create a new node by inserting
        something in it anyways. Also its kvs is already empty.

        Note that this can happen multiple time and the empty binary node can be
        pushed down to an arbitrary depth. So we can't just fix this in
        updateDestNode() without scanning the entire subtree (BAD).
    */
    simplifyNode(dest, area, gc);


    // Handle the branches

    for (int branch = 0; branch < 2; ++branch) {

        if (!srcPoint.hasBranch(branch) || destPoint.hasBranch(branch))
            continue;

        insertBranch(src, srcPoint, dest, branch);
    }

    // Handle the value.

    if (srcPoint.hasValue()) {
        uint64_t toInsert = srcPoint.value();

        if (destPoint.hasValue() && destPoint.value() != toInsert) {
            uint64_t baseValue =
                getValue(base, adjustKV(src, srcPoint.valueKV(), base).key);

            toInsert = conflict(
                    TrieKey(dest.prefix() + destPoint.valueKV().key),
                    baseValue, toInsert, destPoint.value());
        }

        if (destPoint.hasValue() && destPoint.value() != toInsert) {
            KV kv = destPoint.valueKV();
            replaceValue(dest, { kv.key, toInsert });

            kv.key = dest.prefix() + kv.key;
            removedValues.push_back(kv);

        }

        else if (!destPoint.hasValue()) {
            const KeyFragment& key = srcPoint.valueKV().key;
            insertValue(dest, adjustKV(src, { key, toInsert }, dest));
        }
    }
}


void
MergeInsert::
insertBranch(
        Cursor& src, const BranchingPoint& srcPoint,
        Cursor& dest, unsigned branch)
{
    TriePtr newNode = dest.node();

    auto bounds = srcPoint.branchItPair(branch);
    for (auto it = bounds.first; it != bounds.second; ++it)
        newNode = insertKV(newNode, adjustKV(src, *it, dest));

    if (newNode == dest.node()) return;
    dest.setNode(newNode);

    // We burst the node so update the parent.
    if (!dest.hasParent()) return;
    replaceBranch(
            dest.parent().node(), { dest.relativePrefix(), newNode },
            area, gc);

}


/** This function exists to work around some of the limitations of
    makeBranchingNode while also sneaking in a few optimizations.

    The problem with makeBranchingNode is that if we have a 2 kvs where one is a
    strict prefix of the other and neither are values then we go kabloui. This
    only really happens in the merge where the dest node ended up being split up
    in the middle of its branching bits and the current branching point happens
    to be below that split. Note that we handle this through recursion but we
    know the bound on it is tight because the branching point says that there's
    a hole for our branch nearby.

    Note that a similar scenario also exists for the src branch to insert being
    shorter then the dest node. In this we can't insert dest in src so instead
    we make a copy of src's node and dump the work on makeBranchingNode.

 */
TriePtr
MergeInsert::
insertKV(TriePtr node, const KV& toInsert)
{

    // There's no sneaky way to handle values so just defer the work to
    // insertLeaf.
    if (!toInsert.isPtr) {
        return NodeOps::insertLeaf(
                node, area, 0, toInsert.key, toInsert.value,
                TriePtr::IN_PLACE, gc).root();
    }

    TriePathEntry entry = NodeOps::matchKey(node, area, 0, toInsert.key);

    // A prefix of toInsert's already exists in node. Recurse and retry.
    if (!entry.isOffTheEnd()) {
        ExcAssert(entry.isNonTerminal());

        const KeyFragment& key = toInsert.key;

        KV trimmedKv{ key.suffix(entry.bitNum), toInsert.value, toInsert.isPtr };
        TriePtr newNode = insertKV(entry.node(), trimmedKv);

        // We call this even if the newNode == entry.node() because we need to
        // update the size information.
        replaceBranch(node, { key.prefix(entry.bitNum), newNode }, area, gc);

        return node;
    }


    KVList destKvs = NodeOps::gatherKV(node, area, 0);
    KeyFragment destCP = destKvs.commonPrefix();

    // There's a nice hole in node for our branch. Shove it in.
    // \todo This won't work on nodes with branches of varying sizes.
    if (destKvs.hasPtr()
            && destKvs.back().key.bits == toInsert.key.bits
            && destCP.commonPrefixLen(toInsert.key) == destCP.bits)
    {
        getSrcNodes(srcKeepers, toInsert.getPtr(), area, true);
        replaceBranch(node, toInsert, area, gc);
        return node;
    }


    /** The only remaining possibilities is that either toInsert goes in the
        middle of node's prefix or toInsert's key stops in the middle of node's
        branching bits. In both case we want to call makeBranchingNode to
        re-structure the node.
     */
    KVList srcKvs =
        NodeOps::gatherKV(toInsert.getPtr(), area, 0).prefixKeys(toInsert.key);

    for (const KV& kv: srcKvs) {
        if (!kv.isPtr) continue;
        getSrcNodes(srcKeepers, kv.getPtr(), area, true);
    }

    KVList mergedKvs;
    mergedKvs.reserve(destKvs.size() + srcKvs.size());

    std::merge(
            destKvs.begin(), destKvs.end(),
            srcKvs.begin(), srcKvs.end(),
            std::back_inserter(mergedKvs));

    /** makeBranchingNode may occasionnaly call prefixKeys to optimize away
        nodes that contain only a single branch. We can't allow this to happen
        on a branch that belongs directly to src so we set the forceCopy flag
        which has the effect of turning any calls to prefixKeys into calls to
        copyAndPrefixKeys.
     */
    return makeBranchingNode(area, 0, mergedKvs, TriePtr::IN_PLACE, gc);
}


uint64_t
MergeInsert::
getValue(Cursor& base, const KeyFragment& key)
{
    TriePathEntry entry = NodeOps::matchKey(base.node(), area, 0, key);
    return entry.isTerminal() ? entry.value() : 0;
}

void
MergeInsert::
insertValue(Cursor& node, const KV& toInsert)
{
    TriePtr newNode = NodeOps::insertLeaf(
            node.node(), area, 0, toInsert.key, toInsert.value,
            TriePtr::IN_PLACE, gc).root();

    if (newNode == node.node()) return;
    node.setNode(newNode);

    // We burst the node so update the parent.

    if (!node.hasParent()) return;
    replaceBranch(
            node.parent().node(), { node.relativePrefix(), newNode }, area, gc);
}

void
MergeInsert::
replaceValue(Cursor& nodeCur, const KV& toReplace)
{
    TriePtr parent = nodeCur.hasParent() ? nodeCur.parent().node() : TriePtr();
    TriePtr node = nodeCur.node();
    KeyFragment key = toReplace.key;
    KeyFragment relativePrefix = nodeCur.relativePrefix();

    TriePathEntry entry = NodeOps::matchKey(node, area, 0, toReplace.key);

    // It's possible that the node was expanded into a subtree while we were not
    // looking so in that case we have to dig down to find the node we need to
    // change. Note that we need to inplace the nodes as we're digging so even
    // if NodeOps provided a similar functionality, we wouldn't be able to use
    // it.
    while (entry.isNonTerminal()) {
        relativePrefix = key.prefix(entry.bitNum);

        TriePtr newNode = NodeOps::changeState(
                entry.node(), area, 0, TriePtr::IN_PLACE, gc);

        if (newNode != entry.node())
            replaceBranch(parent, { relativePrefix, newNode }, area, gc);

        parent = node;
        node = newNode;

        key.removeBitVec(entry.bitNum);
        entry = NodeOps::matchKey(node, area, 0, key);
    }

    TriePtr newNode = NodeOps::replaceValue(
            node, area, 0, entry, toReplace.value, TriePtr::IN_PLACE, gc).root();

    if (newNode == node) return;

    if (node == nodeCur.node())
        nodeCur.setNode(newNode);

    // We burst the node so update the parent.

    if (!parent) return;
    replaceBranch(parent, { relativePrefix, newNode }, area, gc);
}


/*****************************************************************************/
/* MERGE REMOVE                                                              */
/*****************************************************************************/

void
MergeRemove::
exec(   Cursor& base, Cursor& dest,
        MergeRemoveConflict conflict,
        MemoryAllocator& area, GCList& gc,
        KVList& removedValues)
{
    Profiler prof("merge.remove");

    MergeRemove merger(conflict, area, gc, removedValues);
    Cursor nil;

    merger.merge(base, dest, nil);
}

void
MergeRemove::
merge(Cursor& base, Cursor& dest, Cursor& nil)
{
    // 2-way merge, remove the whole branch.
    if (base.node() == dest.node() && !base.isNarrowed()) {

        if (dest.hasParent())
            removeBranch(dest.parent(), { dest.relativePrefix(), dest.node() });

        dest.setNode(TriePtr());
        updateDestNode(dest, area, gc);
        return;
    }

    dest.toInPlace(area, gc);
    mergeImpl(base, dest, nil);
    updateDestNode(dest, area, gc);
}

void
MergeRemove::
mergeBranchingPoints(
        Cursor& base, const BranchingPoint& basePoint,
        Cursor& dest, const BranchingPoint& destPoint,
        Cursor& nil)
{

    // Branches are handled implicitly by doing nothing.
    if (!basePoint.hasValue() || !destPoint.hasValue())
        return;

    bool doRemove = true;
    if (basePoint.value() != destPoint.value()) {
        doRemove = conflict(
                TrieKey(base.prefix() + basePoint.valueKV().key),
                basePoint.value(), destPoint.value());

        // We don't add the base value to removedValues because if it was
        // replaced then it was previously marked for deallocation in the
        // operation that did the replacement.
    }

    if (doRemove) {
        removeValue(dest, destPoint.valueKV());
    }
}

void
MergeRemove::
removeBranch(Cursor& node, const KV& branch)
{
    ExcAssertEqual(node.node().state, TriePtr::IN_PLACE);

    reclaimDestSubtree(
            node.prefix() + branch.key, branch.getPtr(),
            area, gc, removedValues);

    replaceBranch(node.node(), { branch.key, TriePtr() }, area, gc);
}


void
MergeRemove::
removeValue(Cursor& node, const KV& toRemove)
{
    ExcAssert(!toRemove.isPtr);
    removedValues.push_back({
                node.prefix() + toRemove.key, toRemove.getValue() });

    TriePtr newNode = NodeOps::removeLeaf(
            node.node(), area, 0, toRemove.key, TriePtr::IN_PLACE, gc);

    if (newNode == node.node()) return;
    node.setNode(newNode);

    if (!node.hasParent()) return;
    replaceBranch(
            node.parent().node(), { node.relativePrefix(), newNode }, area, gc);
}



/*****************************************************************************/
/* MERGE DIFF                                                                */
/*****************************************************************************/

TriePtr
MergeDiff::
exec(   TriePtr base,
        TriePtr src,
        TriePtr dest,
        MergeInsertConflict insertConflict,
        MergeRemoveConflict removeConflict,
        MemoryAllocator& area,
        GCList& gc,
        NodeList& srcKeepers,
        KVList& removedValues)
{
    Profiler prof("merge.diff");

    MergeDiff merger(
            insertConflict, removeConflict,
            area, gc,
            srcKeepers, removedValues);

    Cursor baseCursor(base, NodeOps::gatherKV(base, area, 0));
    Cursor srcCursor (src,  NodeOps::gatherKV(src,  area, 0));
    Cursor destCursor(dest, NodeOps::gatherKV(dest, area, 0));

    merger.merge(baseCursor, srcCursor, destCursor);
    return destCursor.node();
}


/** Keep the dest cursor up to date while also in placing them. The inplacing is
    required to attach the merged subtrees back together into the final trie. By
    pre-emtively implacing, we can avoid having to return trie ptrs during the
    recursion which makes things easier.
*/
void
MergeDiff::
merge(Cursor& base, Cursor& src, Cursor& dest)
{
    // No modifications down this branch, so nothing to diff.
    if (src.node() == base.node())
        return;

    // catch up dest to base and src
    {
        Cursor newDest;
        bool advanced;
        tie(advanced, newDest) = dest.advanceToCursor(src, base, area);
        if (advanced) {
            dest.toInPlace(area, gc);

            /** This call is to make sure that the inplacing gets visibility in
                our parent cursor. Why is this important? Well I'm not sure. But
                some weird edge case seems to require it.

                Note that I think that this is not required in MergeRemove and
                RemoveInsert because the abstract algorithm guarantees that we
                never drill into the same dest branch twice.
            */
            dest.reload(area);

            merge(base, src, newDest);

            updateDestNode(dest, area, gc);
            return;
        }
    }

    // If we're dealing with a 2 way merge don't bother going any deeper.
    // Switch the branches and keep on trucking.
    if (dest.node() == base.node() && !src.isNarrowed() && !base.isNarrowed()) {

        // Figure out which nodes in base and src which will need to be cleaned
        // up and which will stay after the merge.
        MergeGC::exec(base, src, area, gc, srcKeepers, removedValues);

        swapSubtrees(src, dest, area, gc);

        updateDestNode(dest, area, gc);
        dest.reload(area);
        return;
    }

    mergeImpl(base, src, dest);
    updateDestNode(dest, area, gc);
}


/** The dest.reload(...) calls are there to ensure that if we ever drill twice
    into the same dest branch, our cursor's view will be consistant with what's
    in the mmap (you get weird bugs otherwise). Note that this is only required
    here because the abstract algorithm guarantees that we never drill into the
    same branch twice but in MergeDiff the dest cursor is handled manually.

*/
void
MergeDiff::
mergeBranchingPoints(
        Cursor& base, const BranchingPoint& basePoint,
        Cursor& src,  const BranchingPoint& srcPoint,
        Cursor& dest)
{
    // Diff the branches.

    for (int branch = 0; branch < 2; ++branch) {
        if (basePoint.hasBranch(branch) == srcPoint.hasBranch(branch))
            continue;

        if (basePoint.hasBranch(branch)) {
            Cursor newBase = base.advanceToBranch(basePoint, branch, area);
            MergeRemove::exec(
                    newBase, dest, removeConflict, area, gc, removedValues);
            dest.reload(area);
        }

        else if (srcPoint.hasBranch(branch)) {
            Cursor newSrc = src.advanceToBranch(srcPoint, branch, area);
            MergeInsert::exec(
                    base, newSrc, dest,
                    insertConflict, area, gc, srcKeepers, removedValues);
            dest.reload(area);
        }
    }

    // Diff the value

    if (basePoint.hasValue() || srcPoint.hasValue()) {

        if (basePoint.hasValue() && !srcPoint.hasValue()) {
            Cursor newBase = base.advanceToValue(basePoint, area);
            MergeRemove::exec(newBase, dest,
                    removeConflict, area, gc, removedValues);
            dest.reload(area);
        }

        else if (srcPoint.hasValue()) {

            // we treat replaces as inserts so if the values aren't equal we
            // do a mergeInsert.
            if(!basePoint.hasValue() ||
                    srcPoint.value() != basePoint.value())
            {
                Cursor newSrc = src.advanceToValue(srcPoint, area);
                MergeInsert::exec(
                        base, newSrc, dest,
                        insertConflict, area, gc, srcKeepers, removedValues);
                dest.reload(area);
            }
        }
    }
}


} // namespace Merge


/*****************************************************************************/
/* INTERFACE                                                                 */
/*****************************************************************************/

pair<TriePtr, KVList>
twoWayMerge(
        TriePtr baseRoot, TriePtr srcRoot,
        MemoryAllocator& area, GCList& trieGc)
{
    auto nilIns = [] (const TrieKey&, uint64_t, uint64_t, uint64_t) -> uint64_t
    {
        ExcAssert(false);
        return 0;
    };
    auto nilRm = [] (const TrieKey&, uint64_t, uint64_t) -> bool {
        ExcAssert(false);
        return false;
    };

    return threeWayMerge(
            baseRoot, srcRoot, baseRoot, nilIns, nilRm, area, trieGc);
}

pair<TriePtr, KVList>
threeWayMerge(
        TriePtr baseRoot,
        TriePtr srcRoot,
        TriePtr destRoot,
        MergeInsertConflict insertConflict,
        MergeRemoveConflict removeConflict,
        MemoryAllocator& area,
        GCList& trieGc)
{
    Profiler profAll("merge");

    MMAP_PIN_REGION(area.region())
    {
        GCList mergeGc(area);
        Merge::NodeList trieKeepers;

        KVList rmvVals;
        TriePtr newDest = Merge::MergeDiff::exec(
                baseRoot, srcRoot, destRoot,
                insertConflict, removeConflict,
                area, mergeGc,
                trieKeepers, rmvVals);

        // Switch all the nodes back to CoW
        // Probably wrong but mergeGc shouldn't change during this operation.
        {
            Profiler profCommit("merge.changeState");
            newDest = NodeOps::changeState(
                    newDest, area, 0, TriePtr::COPY_ON_WRITE, mergeGc);
        }

        // Commit the gc lists. This has to be called after all resize exception
        // throwing code is done with.
        {
            Profiler profCommit("merge.commit");
            Merge::commitGc(mergeGc, trieGc, trieKeepers);
        }

        return make_pair(newDest, rmvVals);
    }
    MMAP_UNPIN_REGION;
}

} // namespace MMap
} // namespace Datacratic
