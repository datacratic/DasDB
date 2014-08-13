/* mmap_trie_merge.h                                               -*- C++ -*-
   Rémi Attab, 14 June 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Header for the 3 way merge algo.

   The header exists mostly just exists for testing purposes. Anything declared
   within the Merge namespace is for internal use only.
*/


#ifndef __mmap__trie_merge_h__
#define __mmap__trie_merge_h__


#include "mmap_trie.h"
#include "mmap_trie_node.h"
#include "mmap_trie_node_impl.h"

#include <memory>
#include <iterator>


namespace Datacratic {
namespace MMap {

/*****************************************************************************/
/* INTERFACE                                                                 */
/*****************************************************************************/

/** 2-way merge of tries.

    Returns the new root and the list of values and associated keys that were
    removed.

    Never tested it but it should work just fine. Although the only thing this
    will actually do is swap the 2 tries and do a of gc. More detailed doc will
    be forthcoming.

*/
std::pair<TriePtr, KVList>
twoWayMerge(
        TriePtr baseRoot, TriePtr srcRoot,
        MemoryAllocator& area, GCList& trieGc);

/** 3-way merge of tries.

    Returns the new root and the list of values and associated keys that were
    removed.

    More detailed doc will be forthcoming.
*/
std::pair<TriePtr, KVList>
threeWayMerge(
        TriePtr baseRoot,
        TriePtr srcRoot,
        TriePtr destRoot,
        MergeInsertConflict insertConflict,
        MergeRemoveConflict removeConflict,
        MemoryAllocator& area,
        GCList& trieGc);


namespace Merge {

struct BranchingPoint;
typedef ML::compact_vector<TriePtr, 64> NodeList;


/*****************************************************************************/
/* CURSOR                                                                    */
/*****************************************************************************/

/** Indicates a position within the trie along with a set of KVs associated with
    that position.

    Note that the only mutable function or attribute in this class is setNode()
    (all the other non-const functions endds up calling setNode()). So it's safe
    to pass it around without adding a const modifier to it.

    Note that the position is defined by a common prefix. This means that the
    set of KVs in the cursor all share the same common prefix and so we're not
    limitted to only pointing to a single node. If we're within a terminal node,
    then the cursor can be moved forward to a set of KVs that all share a common
    prefix.

    The advantage of this is that we can keep moving down the branches of one
    trie while keeping track of the values of a terminal node that are
    associated with that branch.

 */
class Cursor
{

    /** This data needs to be shared between multiple cursors.

        The problem is that when we add and remove leafs during the recursion,
        it may burst the node which will deallocate the old node and create a or
        many new nodes. If the we need to do multiple inserts and removes across
        multiple stages of the recursion on the same node, then the new node
        will have to be made visible to all those stages.

        Trying to propagate this information through parameters and return
        values would be pretty damn ugly and would force many copies of the
        cursor which we want to avoid.

        So instead, all the stages of a recursion that operate on a given node
        will share the same head information and so we can easily propagate new
        node changes with the setNode() function.

    */
    struct SharedHeader
    {

        SharedHeader(
                Cursor* parent,
                const KV& prefix,
                bool ephemeralParent = false) :
            parent(parent),
            ephemeralParent(ephemeralParent),
            prefix(prefix)
        {
            ExcAssert(!ephemeralParent || parent);
        }

        ~SharedHeader()
        {
            if (ephemeralParent) {
                std::cerr << "deleting: " << parent->print() << std::endl;
                delete parent;
            }
        }

        Cursor* parent;
        bool ephemeralParent;

        KV prefix;
    };

    std::shared_ptr<SharedHeader> head;

    KeyFragment initKvsCommonPrefix();

public:

    /** Set of KVs associated with this cursor.

        Should really be const but wouldn't be able to move or copy properly and
        encapsulating would just be a pain. So I'll just trust myself to not do
        something stupid (bad idea, I know).
    */
    KVList kvs;

    /** Precalculated commonPrefix of kvs. */
    KeyFragment kvsCP;

    bool narrowed;


    /** Boring plumbing */

    Cursor() : head(new SharedHeader(NULL, {KeyFragment(), TriePtr()})) {}

    Cursor(const Cursor& other) :
        head(other.head),
        kvs(other.kvs),
        kvsCP(other.kvsCP),
        narrowed(other.narrowed)
    {}

    Cursor& operator=(const Cursor& other)
    {
        if (&other == this) return *this;
        head = other.head;
        kvs = other.kvs;
        kvsCP = other.kvsCP;
        narrowed = other.narrowed;
        return *this;
    }

    Cursor(Cursor&& other) :
        head(std::move(other.head)),
        kvs(std::move(other.kvs)),
        kvsCP(std::move(other.kvsCP)),
        narrowed(std::move(other.narrowed))
    {}

    Cursor& operator=(Cursor&& other)
    {
        if (&other == this) return *this;
        head = std::move(other.head);
        kvs = std::move(other.kvs);
        kvsCP = std::move(other.kvsCP);
        narrowed = std::move(other.narrowed);
        return *this;
    }


    /** New cursor constructor */

    Cursor( const KV& prefix,
            const KVList& kvs,
            Cursor* parent = nullptr,
            bool ephemeralParent = false) :
        head(new SharedHeader(parent, prefix, ephemeralParent)),
        kvs(kvs),
        kvsCP(initKvsCommonPrefix()),
        narrowed(false)
    {}

    Cursor( const TriePtr& node,
            const KVList& kvs,
            Cursor* parent = nullptr,
            bool ephemeralParent = false) :
        head(new SharedHeader(parent, { KeyFragment(), node }, ephemeralParent)),
        kvs(kvs),
        kvsCP(initKvsCommonPrefix()),
        narrowed(false)
    {}

    /** Narrowing constructor.

        When we want to move down a terminal node and only keep a subset of
        the KVs that share a common prefix.
    */
    template<typename It>
    Cursor(const Cursor& base, const std::pair<It, It>& itPair) :
        head(base.head),
        kvs(itPair.first, itPair.second),
        kvsCP(initKvsCommonPrefix()),
        narrowed(true)
    {}


    bool hasParent() const { return head->parent; }
    Cursor& parent() {
        ExcAssert(head->parent);
        return *(head->parent);
    }
    const Cursor& parent() const {
        ExcAssert(head->parent);
        return *(head->parent);
    }

    const KeyFragment& prefix() const { return head->prefix.key; }

    TriePtr node() const { return head->prefix.getPtr(); }
    void setNode(TriePtr newNode) { head->prefix.value = newNode; }

    /** Prefix of the node starting after the parent node. */
    KeyFragment relativePrefix() const
    {
        if (hasParent())
            return prefix().suffix(parent().prefix().bits);
        return prefix();
    }

    /** Absolute common prefix from the root of the trie. */
    KeyFragment commonPrefix() const { return prefix() + kvsCP; }
    int32_t commonPrefixLen() const { return prefix().bits + kvsCP.bits; }

    bool isBranching() const { return kvs.hasPtr(); }
    bool isTerminal() const { return !kvs.hasPtr(); }

    bool isNarrowed() const { return narrowed; }

    Cursor advanceToBranch(
            const BranchingPoint& point, int branch, MemoryAllocator& area);

    Cursor advanceToValue(
            const BranchingPoint& point, MemoryAllocator& area) const;

    /** Moves the cursor up to the smallest of the two given cursor's prefix. */
    std::pair<bool, Cursor> advanceToCursor(
            const Cursor& c0, const Cursor& c1, MemoryAllocator& area);

    /** Change the state of the node to in place and updates the cursor. */
    void toInPlace(MemoryAllocator& area, GCList& gc);

    /** Updates the cursor with the content of the node in the region. */
    void reload(MemoryAllocator& area);

    std::string print() const;
};


/*****************************************************************************/
/* BRANCHING POINT                                                           */
/*****************************************************************************/

/** Parses a cursor at a given bit prefix to determine the distribution of the
    KV set at that point. Usually used to compare where two cursors differ and
    inform the merging algorithm.

    Another way to see it, is that it simplifies the merging problem to a simple
    mathematical operation. Given 2 branching point (composed of a value and 2
    branches), it's quite easy to determine what needs to happen at any given
    stage of the merge.

*/
struct BranchingPoint
{
    typedef KVList::const_iterator iterator;

    const Cursor& cursor;
    const int32_t bitNum;

private:

    iterator nullIt() const { return cursor.kvs.end(); }

public:

    BranchingPoint(const Cursor& cursor, const int32_t bitNum);

    BranchingPoint(const BranchingPoint&) = delete;
    BranchingPoint(const BranchingPoint&&) = delete;
    BranchingPoint& operator= (const BranchingPoint&) = delete;
    BranchingPoint& operator= (const BranchingPoint&&) = delete;

    bool hasValue() const { return valueIt != cursor.kvs.end(); }

    uint64_t value() const
    {
        ExcAssert(hasValue());
        return valueIt->value;
    }

    const KV& valueKV() const
    {
        ExcAssert(hasValue());
        return *valueIt;
    }

    std::pair<iterator, iterator> valueItPair() const
    {
        ExcAssert(hasValue());
        return std::make_pair(valueIt, valueIt + 1);
    }

    size_t branchSize(int b) const
    {
        return std::distance(branchIt[b].first, branchIt[b].second);
    }

    bool hasBranch(int b) const
    {
        return branchIt[b].first != branchIt[b].second;
    }

    std::pair<iterator, iterator> branchItPair(int b) const
    {
        ExcAssert(hasBranch(b));
        return branchIt[b];
    }

    const KV& branch(int b) const
    {
        ExcAssert(hasBranch(b));
        ExcAssertEqual(branchSize(b), 1);
        return *(branchIt[b].first);
    }

    bool branchIsPtr(int b) const
    {
        return branchSize(b) == 1 && branch(b).isPtr;
    }

    KeyFragment branchPrefix(int b) const
    {
        ExcAssert(hasBranch(b));
        const KeyFragment& key = branchIt[b].first->key;

        if (bitNum < cursor.prefix().bits)
            return cursor.prefix().suffix(cursor.prefix().bits - bitNum) + key;
        return key.prefix(bitNum - cursor.prefix().bits);
    }

    std::string print() const;

private:

    void init();

    iterator valueIt;
    std::pair<iterator, iterator> branchIt[2];

    // Exists for the reload function which may call init().
    friend class Cursor;
};



/*****************************************************************************/
/* MERGE BASE                                                                */
/*****************************************************************************/

/** Abstract merging algorithm which is in charge of moving down the trie and
    looking for branching points.

    While the BranchingPoint concept along with the Cursors greatly simplifies
    the merging algorithm, it's still pretty difficult to handle all 3 tries at
    once. So the idea is that we seperate the algorithm in 3 stages that only
    require that we look at 2 tries at any given time. These 3 stages are
    represented by MergeDiff, MergeInsert, MergeRemove.

    This class provides the core mechanism to explore the trie and reduce the
    merging problem to a pair of branching point. Those branching points are
    then given over to the subclasses to be handled however appropriate by the
    given stage of the algorithm.

    Also note that this is a depth first recursion. Since we often need to make
    multiple modifications to a tree, we can be sure any modifications required
    to one of the subtrees will be completed before we modifiy the root of the
    tree. The cursors also have a mechanism (setNode()) to share any node
    changes back up the recursion tree.

 */
struct MergeBase
{
    MergeBase(MemoryAllocator& area, GCList& gc) :
        area(area), gc(gc)
    {}

protected:

    /** Merge two branching point together.

        Branching points are guaranteed to have some kind of problem in them
        which will take one of these forms:

        - branch present in b0 not present in b1 or vice versa.
        - value present in either b0 or b1.
    */
    virtual void mergeBranchingPoints(
            Cursor& c0, const BranchingPoint& b0,
            Cursor& c1, const BranchingPoint& b1,
            Cursor& extra) = 0;


    /** Main entry point to the algorithm.

        Note that while we are only managing 2 tries at once, we often need to
        keep a cursor to the other trie up to date with the other 2 cursors. To
        make this happen, the cursors can be updated by overiding this function.

        Overiding this function can also be used as a way to stop the recursion
        of the algorithm for various cutoff points. This can be accomplished by
        simply returning without calling mergeImpl.

        Any overides of this function should call mergeImpl() unless it wants to
        stop the recursion.
    */
    virtual void merge(Cursor& c0, Cursor& c1, Cursor& extra)
    {
        return mergeImpl(c0, c1, extra);
    }

    /** Looks for the next branching point and recurses down a branch if it's
        present in both points. A mergeBranchingPoints call is emitted if one of
        the required conditions are detected.
    */
    void mergeImpl(Cursor& c0, Cursor& c1, Cursor& extra);

    MemoryAllocator& area;
    GCList& gc;
};


/*****************************************************************************/
/* MERGE DIFF                                                                */
/*****************************************************************************/

/** First stage of the merging algorithm that looks for a difference between the
    src and base tries. The extra cursor in this case is used to keep the dest
    cursor up to date.

    This stage emits calls to the MergeInsert stage when a branch is detected in
    src that is missing in base and to the MergeRemove stage when a branch is
    detected in base but missing in src.

    The MergeInsert and MergeRemove stages are responsible for modifying the
    parent node of their passed cursor which is guaranteed to have been in
    placed by this stage.

    Note that in the case of a 2 way merge (a base node is equal to a dest
    node), this algorithm will never emit calls to the other 2 stages and will
    handle the merges using some shortcuts. This applies to any subtrees of the
    given cursors.

*/
struct MergeDiff : public MergeBase
{

    /** Entry point. */
    static TriePtr exec(
            TriePtr base, TriePtr src, TriePtr dest,
            MergeInsertConflict insertConflict,
            MergeRemoveConflict removeConflict,
            MemoryAllocator& area, GCList& gc,
            NodeList& srcKeepers, KVList& removedValues);

protected:

    MergeDiff(
            MergeInsertConflict insConflict,
            MergeRemoveConflict rmConflict,
            MemoryAllocator& area, GCList& gc,
            NodeList& srcKeepers, KVList& removedValues) :
        MergeBase(area, gc),
        insertConflict(insConflict),
        removeConflict(rmConflict),
        srcKeepers(srcKeepers),
        removedValues(removedValues)
    {}

    virtual void merge(Cursor& base, Cursor& src, Cursor& dest);
    virtual void mergeBranchingPoints(
            Cursor& base, const BranchingPoint& basePoint,
            Cursor& src,  const BranchingPoint& srcPoint,
            Cursor& dest);


private:

    MergeInsertConflict insertConflict;
    MergeRemoveConflict removeConflict;
    NodeList& srcKeepers;
    KVList& removedValues;
};

/*****************************************************************************/
/* MERGE REMOVE                                                              */
/*****************************************************************************/


/** Remove stage responsible for removing base branches from a dest trie.

    Branches only need to be removed if they are present in both the dest trie
    and the src trie. If it's only present in the base trie then it's already
    gone. If it's only present in the dest trie then it's not marked for
    removal.

    Conflicts are raised when a value is present in both the base and dest but
    aren't equal to each other. This means that we're trying to remove a value
    that was modified after we did the src fork. In this case the
    MergeRemoveConflict function is called to resolve the conflict.

    This stage does it's best to simplify branching nodes where possible but it
    may not always succeed. This means that the returned trie may not be
    perfect. Needs more testing to be sure.

    Note that this algorithm also in places in dest node that it skips over so
    that any modifications can easily be linked back to the final trie (a simple
    replacement in the parent node will do).

*/
struct MergeRemove : public MergeBase
{
    /** Entry point. */
    static void exec(
            Cursor& base, Cursor& dest,
            MergeRemoveConflict conflict, MemoryAllocator& area,
            GCList& gc, KVList& removedValues);

protected:

    MergeRemove(
            MergeRemoveConflict conflict, MemoryAllocator& area,
            GCList& gc, KVList& removedValues) :
        MergeBase(area, gc),
        conflict(conflict),
        removedValues(removedValues)
    {}

    virtual void merge(Cursor& base, Cursor& dest, Cursor& nil);
    virtual void mergeBranchingPoints(
            Cursor& base, const BranchingPoint& basePoint,
            Cursor& dest, const BranchingPoint& destPoint,
            Cursor& nil);

private:

    // void simplifyNode(Cursor& node, const KV& toRemove);
    void removeBranch(Cursor& node, const KV& branch);
    void removeValue(Cursor& node, const KV& toRemove);

    MergeRemoveConflict conflict;
    KVList& removedValues;
};


/*****************************************************************************/
/* MERGE INSERT                                                              */
/*****************************************************************************/


/** Insert stage responsible for inserting branches from a src trie into
    dest. Note that we also represent Replace operations by an insert operation.

    The extra cursor is used to keep track of the base trie in order to detect
    2-way merge situations.

    Branches only need to be inserted if it's present in src but not in dest.
    Values need to be inserted if it's present in src.

    Conflicts are raised when a value is present in both src and dest but are
    not equal. This means that we're trying to replace a value whose value has
    changed since we forked src .In this case the MergeInsertConflict function
    is called to resolve the conflict.

    I think this stage is guaranteed to return a well formed trie but let's just
    assume that it might not and save everyone some headaches.

    Note that this algorithm also in places in dest node that it skips over so
    that any modifications can easily be linked back to the final trie (a simple
    replacement in the parent node will do).

*/
struct MergeInsert : public MergeBase
{

    /** Entry point. */
    static void exec(
            Cursor& base, Cursor& src, Cursor& dest,
            MergeInsertConflict conflict,
            MemoryAllocator& area, GCList& gc,
            NodeList& srcKeepers, KVList& removedValues);

protected:

    MergeInsert(
            MergeInsertConflict conflict,
            MemoryAllocator& area, GCList& gc,
            NodeList& srcKeepers, KVList& removedValues) :
        MergeBase(area, gc),
        conflict(conflict),
        srcKeepers(srcKeepers),
        removedValues(removedValues)
    {}

    virtual void merge(Cursor& src, Cursor& dest, Cursor& base);
    virtual void mergeBranchingPoints(
            Cursor& src, const BranchingPoint& srcPoint,
            Cursor& dest, const BranchingPoint& destPoint,
            Cursor& base);

private:

    void insertBranch(
        Cursor& src, const BranchingPoint& srcPoint,
        Cursor& dest, unsigned branch);
    TriePtr insertKV(TriePtr node, const KV& toInsert);

    void replaceValue(Cursor& node, const KV& toReplace);
    void insertValue(Cursor& node, const KV& toInsert);

    uint64_t getValue(Cursor& base, const KeyFragment& key);

    MergeInsertConflict conflict;
    NodeList& srcKeepers;

    // Can grow when replacing values.
    KVList& removedValues;
};


/*****************************************************************************/
/* MERGE GC                                                                  */
/*****************************************************************************/

/** We're reusing our recursion mechanism (MergeBase) to find the nodes in base
    that were replaced in src. These nodes will be added to the gc's old nodes
    list.

    MegeGC should only be used on src subtrees that we wish to insert into dest
    because it's only safe to do that operation when we're in a 2-way merge
    (base subtree is equal to the dest subtree). In this case we don't have to
    clean up anything within dest.

    MergeGC also gathers all the src nodes in srcKeepers that should be kept
    after we commit the trie. This can then be used to figure out which src node
    need to be deallocated.

*/
struct MergeGC : public MergeBase
{
    /** Entry Point. */
    static void exec(
            Cursor& base, Cursor& src,
            MemoryAllocator& area, GCList& gc,
            NodeList& srcKeepers, KVList& removedValues);

protected:

    MergeGC(MemoryAllocator& area, GCList& gc, KVList& removedValues) :
        MergeBase(area, gc), removedValues(removedValues)
    {}

    virtual void merge(Cursor& base, Cursor& src, Cursor& nil);
    virtual void mergeBranchingPoints(
            Cursor& base, const BranchingPoint& basePoint,
            Cursor& src, const BranchingPoint& srcPoint,
            Cursor& nil);

    KVList& removedValues;
};



/******************************************************************************/
/* MERGE ROLLBACK                                                             */
/******************************************************************************/

/** Gathers all the values inserted in the current trie version.

    This is usefull when we're rolling-back a set of changes in a version and we
    our values are pointers to manually allocated structures. In this case,
    any values that we created in the current version needs to be deallocated.

    This is actually quite similar to the MergeGc algo but it differs in the
    type of values it gathers. MergeGc is interested in nodes and we're
    interested in values.

    \todo MergeBase never uses its GcList and it only requires it as a
    convenience thing for all the other algorithm. This algo doesn't need it
    so...
 */
struct MergeRollback : public MergeBase
{
    static KVList exec(TriePtr base, TriePtr src, MemoryAllocator& area);

protected:

    MergeRollback(MemoryAllocator& area) :
        MergeBase(area, nilGc),
        nilGc(area)
    {}

    virtual void merge(Cursor& base, Cursor& src, Cursor& nil);
    virtual void mergeBranchingPoints(
            Cursor& base, const BranchingPoint& basePoint,
            Cursor& src,  const BranchingPoint& srcPoint,
            Cursor& nil);


    KVList insertedValues;
    GCList nilGc; // Never used but required by MergeBase
};


} // namespace Merge
} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__trie_merge_h__ */
