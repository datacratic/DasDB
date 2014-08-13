/* mmap_trie.h                                                     -*- C++ -*-
   Jeremy Barnes, 9 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Trie structure that can be memory mapped.
*/

#ifndef __behaviour__mmap_trie_h__
#define __behaviour__mmap_trie_h__

#include "soa/gc/gc_lock.h"
#include "gc_list.h"
#include "mmap_trie_path.h"
#include "memory_allocator.h"
#include "bin_string.h"
#include "trie_key.h"
#include "sync_stream.h"
#include "mmap_trie_stats.h"
#include "jml/utils/compact_vector.h"
#include "jml/utils/unnamed_bool.h"

#include <boost/iterator/iterator_facade.hpp>
#include <iostream>
#include <unordered_map>


namespace Datacratic {
namespace MMap {

struct GCList;
struct Trie;
struct ConstTrieVersion;
struct KeyFragment;
struct TrieKey;


/*****************************************************************************/
/* MERGE CONFLICTS                                                           */
/*****************************************************************************/

typedef
    std::function< uint64_t (const TrieKey&, uint64_t, uint64_t, uint64_t) >
    MergeInsertConflict;

typedef
    std::function< bool (const TrieKey&, uint64_t, uint64_t) >
    MergeRemoveConflict;

/** List of values that need to be deallocated after a commit or rollback */
typedef std::vector< std::pair<TrieKey, uint64_t> > ValueDeallocList;


/*****************************************************************************/
/* TRIE ITERATOR                                                             */
/*****************************************************************************/

/** Wraps the TriePath class into something that looks like an iterator that
    we know and love.
*/

struct TrieIterator
    : public boost::iterator_facade
         <TrieIterator,
          std::pair<const KeyFragment, const uint64_t>,
          std::random_access_iterator_tag,
          std::pair<const KeyFragment, const uint64_t> > {

    TrieIterator()
        : owner_(0)
    {
    }

    /** Copy constructor but with a change of owner.
        Both the old and new owner should still point to the same trie version.
    */
    TrieIterator(const TrieIterator& other, const ConstTrieVersion * owner);

    inline ~TrieIterator();

    inline TrieKey key() const;

    uint64_t value() const
    {
        return path_.value();
    }

    size_t entryNum() const { return path_.entryNum(); }

    inline bool valid() const;

    JML_IMPLEMENT_OPERATOR_BOOL(valid());

    void dump(std::ostream & stream) const;

private:
    TrieIterator(const TriePath & path, const ConstTrieVersion * owner)
        : path_(path), owner_(owner)
    {
    }

    ssize_t distance_to(const TrieIterator & other) const
    {
        return other.entryNum() - entryNum();
    }

    bool equal(const TrieIterator & other) const
    {
        if (path_.root() != other.path_.root())
            throw ML::Exception("comparison of Trie iterators for different "
                                "epochs or incompatible sizes");
        return path_ == other.path_;
    }

    std::pair<const uint64_t, const uint64_t> dereference() const;

    void increment()
    {
        advance(1);
    }

    void decrement()
    {
        advance(-1);
    }

    inline void advance(ssize_t n);

    friend class boost::iterator_core_access;
    friend class MatchResult;
    friend class Trie;
    friend class ConstTrieVersion;
    friend class MutableTrieVersion;
    friend class TransactionalTrieVersion;
    friend class AccessorKey;

    TriePath path_;      ///< Actual path
    const ConstTrieVersion * owner_;   ///< Version of the trie we're referring to
};

std::ostream &
operator << (std::ostream & stream, const TrieIterator & it);


/*****************************************************************************/
/* CONST TRIE VERSION                                                        */
/*****************************************************************************/

struct ConstTrieVersion {

    inline GcLockBase &  gc();

    inline void lock()
    {
        if (trie) gc().lockShared(info);
    }

    inline void unlock()
    {
        if (trie) gc().unlockShared(info);
        //trie = 0;
    }

    inline void reset()
    {
        unlock();
        trie = 0;
    }

    ConstTrieVersion()
        : trie(0)
    {
    }

    // Must be called with gc locked to avoid race condition
    ConstTrieVersion(const Trie * trie, TriePtr root)
        : trie(trie), root(root), info(GcLock::GcInfo::getThisThread())
    {
        ExcAssert(gc().isLockedShared(info));
    }

    ConstTrieVersion(const ConstTrieVersion & other)
        : trie(other.trie), root(other.root), info(other.info)
    {
        lock();
    }

    ConstTrieVersion(ConstTrieVersion && other)
        : trie(other.trie), root(other.root), info(other.info)
    {
        // Take over other's version and look
        other.trie = 0;
    }

    virtual ~ConstTrieVersion()
    {
        unlock();
    }

    ConstTrieVersion & operator = (const ConstTrieVersion & other)
    {
        root = other.root;
        if (trie != other.trie) {
            unlock();
            trie = other.trie;
            info = other.info;
            lock();
        }
        else info = other.info;
        // else it's already locked

        return *this;
    }

    ConstTrieVersion & operator = (ConstTrieVersion && other)
    {
        root = other.root;

        if (trie != other.trie) {
            reset();
            // Take over other's version and lock.
            std::swap(trie, other.trie);
            info = other.info;
        }
        // We already have our own lock so unlock other.
        else other.reset();

        return *this;
    }

    size_t size() const;

    bool count(const TrieKey& key) const
    {
        return find(key).valid();
    }

    TrieIterator begin() const;
    TrieIterator end() const;
    std::pair<TrieIterator, TrieIterator> beginEnd() const;

    TrieIterator lowerBound(const TrieKey& key) const;
    TrieIterator upperBound(const TrieKey& key) const;
    std::pair<TrieIterator, TrieIterator> bounds(const TrieKey& key) const;

    /** Return an iterator to the given value. */
    TrieIterator find(const TrieKey& key) const;

    /* returns 0 if the key doesn't exist. */
    uint64_t operator [] (const TrieKey& key) const;

    /** Looks at the entire trie and gathers stats about every node. */
    TrieStats stats(StatsSampling method) const;

    /** Returns a pair of keys that delimit a corrupted region of the trie. The
        iterator themselves are safe to access. If the iterators are equal to
        each other then no corruption exists within the trie.

     */
    std::pair<TrieIterator, TrieIterator> check() const;

    void dump(int indent = 0,
              int maxDepth = 0,
              std::ostream & stream = std::cerr) const;

    /** How many bytes are used? */
    size_t memUsage() const;

    //protected:
    const Trie * trie;
    TriePtr root;
    GcLock::ThreadGcInfo * info;
};


/*****************************************************************************/
/* MUTABLE TRIE VERSION                                                      */
/*****************************************************************************/

struct MutableTrieVersion : public ConstTrieVersion {

    MutableTrieVersion()
    {
    }

    MutableTrieVersion(Trie * trie, TriePtr root)
        : ConstTrieVersion(trie, root)
    {
    }

    virtual ~MutableTrieVersion() {}

    /** Remove all contents of the trie. */
    void clear();

    /** Insert the given (key, bool) pair.  Return value is (iterator, true) is
        the insert succeeded (nothing was already there), or (iterator, false)
        if the insert failed (because there was already a value there).
    */
    std::pair<TrieIterator, bool>
    insert(const TrieKey& key, uint64_t value);

    /** Removes the value associated with the key.

        Returns (false, -1) If the key wasn't in the trie.
        Returns (true, oldValue) If the key was in the trie. oldValue is the
          value that was removed from the trie.
    */
    std::pair<bool, uint64_t> remove(const TrieKey& key);

    /** Removes the value associated with the key only if it matches oldValue.

        Returns (false, -1) If the key wasn't in the trie.
        Returns (true, curValue) If the key was in the trie. curValue is the
          value that was in the trie.
    */
    std::pair<bool, uint64_t> compareAndRemove(
            const TrieKey& key, uint64_t oldValue);


    /** Looks for the given key and replaces its value only if the current value
        matches the given oldValue.

       Returns (false, -1) if the key isn't in the trie.
       Returns (true, value) If the key was found. If value == oldValue then the
       value of the key was modified. Otherwise value is the current value in
       the trie.
    */
    std::pair<bool, uint64_t> compareAndSwap(
            const TrieKey& key, uint64_t oldValue, uint64_t newValue);

private:
    bool setRoot(TriePtr newRoot);

    /** Modify the trie so that node identified by pathToNode is replaced
        by newSubtree.  Returns the path of the node pointed to by newSubtree
        from the root.

        If the rotation fails (due to someone else having modified the
        root) it will return an empty path.
    */
    TriePath replaceSubtree(const TriePath & pathToNode,
                            const TriePath & newSubtree,
                            GCList & gc);

    /** Modify the trie so that node identified by pathToNode is replaced
        by newSubtreeRoot. This version of replaceSubtree should be used
        in instances where we can't represent the new subtree by a path.

        If the rotation fails (due to someone else having modified the
        root) it will return false.
    */
    bool replaceSubtree(
            const TriePath & pathToNode,
            const TriePtr & newSubtreeRoot,
            GCList & gc);

    /** Is used by replace subtree as a fast retry when setRoot fails.
        The basic idea is to look for a common subpath between pathToReplace
        and the current root. We then modify pathToReplace and subtreeToInsert
        so that we only have to retry the nodes that have changed.

        Return true if it's possible to do a fast retry.
        Return false if we have to redo the operation from scratch.
    */
    bool setupFastRetry(
            TriePath& pathToReplace,
            TriePath& subtreeToInsert,
            const TriePath& replaceAttempt,
            GCList & gc);

};


/*****************************************************************************/
/* TRANSACTIONAL TRIE VERSION                                                */
/*****************************************************************************/

/** Version of the trie optimized for speed that uses CoW to make all
    modifications inplace. We wrap it in transactional semantic for convenience.

    This version is built for single threaded use with the exception of the
    merge function which can be called concurrently from multiple instances of
    TransactionalTrieVersion that originate from the same trie.

    Essentially, we start by copying the original root of the trie (effectively
    making a copy of it). Modifying a node from the original trie will CoW it
    and it's chain of parents into the new trie. Subsequent writes will happen
    in place. We can then reintegrate our changes back into the trie by calling
    the merge() function.

    Note we can't support the full range of operations supported by
    MutableTrieVersion. The compareAndSwap and compareAndRemove operations
    simply don't make sense in a single-threaded environment and are therefore
    not supported. The replace operation was added to fill in the gap.

    Note that gc will also be disabled in the original while an unmerge inplace
    version exists. This means that while it's valid to operate on the original
    trie, these operations will not be garbage collected until the inplace
    version is merged.
*/
struct TransactionalTrieVersion : public ConstTrieVersion
{
    TransactionalTrieVersion() {}

    inline TransactionalTrieVersion(const Trie * trie, TriePtr root);

    virtual ~TransactionalTrieVersion()
    {
        rollback();
    }

    /** Remove all contents of the trie. */
    void clear();

    /** Insert the given (key, bool) pair.  Return value is (iterator, true) if
        the insert succeeded (nothing was already there), or (iterator, false)
        if the insert failed (because there was already a value there).
    */
    std::pair<TrieIterator, bool>
    insert(const TrieKey& key, uint64_t value);

    /** Replaces the value associated with the given key by the given value.
        Returns (true, old value) if the key was found and (false, unspecified)
        if the key doesn't exist.
    */
    std::pair<bool, uint64_t>
    replace(const TrieKey& key, uint64_t newValue);

    /** Removes the value associated with the key.

        Returns (false, -1) If the key wasn't in the trie.
        Returns (true, oldValue) If the key was in the trie. oldValue is the
          value that was removed from the trie.
    */
    std::pair<bool, uint64_t>
    remove(const TrieKey& key);

    /** Merges the changes made on this trie back into the original trie and
        releases the gc lock on the original trie.

        The function returns a list of keys and their associated values that was
        deallocated during the lifetime of the transaction. If these values
        represent allocated resources then they should be deallocated using the
        RCU defer mechanism.
    */
    ValueDeallocList commit();


    /** Merges the changes made on this trie back into the original trie and
        releases the gc lock on the original trie.

        Returns a list of keys and their associated values that was deallocated
        during the lifetime of the transaction. If these values represent
        allocated resources then they should be deallocated using the RCU defer
        mechanism.
    */
    ValueDeallocList commit(MergeInsertConflict, MergeRemoveConflict);


    /** TODO */
    std::pair<bool, ValueDeallocList>
    tryCommit(const std::function<void()>& preCommit = std::function<void()>());


    /** TODO */
    std::pair<bool, ValueDeallocList>
    tryCommit(
            MergeInsertConflict insertConflict,
            MergeRemoveConflict removeConflict,
            const std::function<void()>& preCommit = std::function<void()>());


    /** Discards any changes made by on this version and recopies the current
        root of the original trie.

        Returns a list of keys and their associated values that were inserted
        during the lifetime of the transaction. If these values represent
        allocated resources then they should be deallocated using the RCU defer
        mechanism.

        Note that gathering the values that need to deallocated is somewhat
        expensive. So if no deallocation is required then the gatherInsertedList
        param can be set to false to avoid doing this operation.
    */
    ValueDeallocList rollback(bool gatherInsertedList = false);

private:

    /** Converts a path that is partially or fully CoW into a path that is
        entirely in place.
    */
    TriePath
    convertToInplacePath(const TriePath& pathToReplace, TriePath childPath);

    /** Does the actual commit once the lock has been acquired. */
    ValueDeallocList commitImpl(MergeInsertConflict, MergeRemoveConflict);

    TriePtr originalRoot;
    std::shared_ptr<GCList> gc;

};


/*****************************************************************************/
/* TRIE                                                                      */
/*****************************************************************************/

struct Trie {

    Trie(MemoryAllocator * area, int id) :
        id_(id),
        area_(area),
        rootOffset_(area_->trieAlloc.trieOffset(id)),
        gc_(area_->region().gcLock(id))
    {}

    GcLockBase & gc() const { return *gc_; }
    MemoryAllocator & area() const { return *area_; }

    MutableTrieVersion current()
    {
        // MUST do this before we read root to make sure it isn't removed
        // before time
        gc_->lockShared();

        TriePtr rootCopy;
        MMAP_PIN_REGION_READ(area_->region())
        {
            rootCopy = TriePtr::fromBits(*getRootPtr());
        }
        MMAP_UNPIN_REGION;

        return MutableTrieVersion(this, rootCopy);
    }

    MutableTrieVersion operator * () { return current(); }


    ConstTrieVersion current() const { return constCurrent(); }
    ConstTrieVersion constCurrent() const
    {
        gc_->lockShared();

        TriePtr rootCopy;
        MMAP_PIN_REGION_READ(area_->region())
        {
            rootCopy = TriePtr::fromBits(*getRootPtr());
        }
        MMAP_UNPIN_REGION;

        return ConstTrieVersion(this, rootCopy);
    }

    ConstTrieVersion operator * () const { return current(); }

    TransactionalTrieVersion transaction() const
    {
        gc_->lockShared();

        TriePtr rootCopy;
        MMAP_PIN_REGION_READ(area_->region())
        {
            rootCopy = TriePtr::fromBits(*getRootPtr());
        }
        MMAP_UNPIN_REGION;

        return TransactionalTrieVersion(this, rootCopy);
    }

    RegionPtr<uint64_t> getRootPtr() const
    {
        // We have to create a new RegionPtr everytime because an instance
        //   can't be visible to multiple threads at once.
        return RegionPtr<uint64_t>(area_->region(), rootOffset_);
    }

private:

    // Trie id
    int id_;

    // Allocator for the trie
    MemoryAllocator * area_;

    // location of the root we're currently using.
    const uint64_t rootOffset_;

    // gc lock for the trie.
    mutable std::shared_ptr<GcLockBase> gc_;

    friend class TrieIterator;
    friend class ConstTrieVersion;
    friend class MutableTrieVersion;
    friend class TransactionalTrieVersion;
};


/*****************************************************************************/
/* METHODS                                                                   */
/*****************************************************************************/

TrieIterator::
~TrieIterator()
{
}

void
TrieIterator::
advance(ssize_t n)
{
    MMAP_PIN_REGION_READ(owner_->trie->area().region())
    {
        path_.advance(n, owner_->trie->area(), owner_->info);
    }
    MMAP_UNPIN_REGION;
}

bool
TrieIterator::
valid() const
{
    MMAP_PIN_REGION_READ(owner_->trie->area().region())
    {
        return path_.valid();
    }
    MMAP_UNPIN_REGION;
}

TrieKey
TrieIterator::
key() const
{
    MMAP_PIN_REGION_READ(owner_->trie->area().region())
    {
        return TrieKey(path_.key(owner_->trie->area(), owner_->info));
    }
    MMAP_UNPIN_REGION;

    /* this code is never executed but is put there to help gcc with its
       "uninitialized value" warnings */
    return TrieKey();
}

GcLockBase &
ConstTrieVersion::
gc()
{
    return trie->gc();
}

TransactionalTrieVersion::
TransactionalTrieVersion(const Trie * trie, TriePtr root)
    : ConstTrieVersion(trie, root),
      originalRoot(root),
      gc(new GCList(trie->area()))
{}

} // namespace MMap
} // namespace Datacratic

#endif /* __behaviour_mmap_trie_h__ */
