/* mmap_map.h                                                 -*- C++ -*-
   Rémi Attab, 17 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   A typed associative container based on the trie.
*/


#ifndef __mmap__mmap_map_h__
#define __mmap__mmap_map_h__


#include "mmap_trie.h"
#include "mmap_typed_utils.h"
#include "mmap_trie_stats.h"
#include "jml/utils/exc_check.h"
#include "jml/compiler/compiler.h"

#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <utility>
#include <unordered_set>


namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* FORWARD DECLARATION                                                       */
/*****************************************************************************/

template<typename Key, typename Value> struct ConstMapBase;
template<typename Key, typename Value> struct MapVersion;
template<
    typename Key, typename Value, typename Dealloc = ValueDeallocator<Value> >
    struct MapTransaction;
template<
    typename Key, typename Value, typename Dealloc = ValueDeallocator<Value> >
    struct Map;


/*****************************************************************************/
/* MAP ITERATOR                                                              */
/*****************************************************************************/

/** Wrapper for the TrieIterator that does type conversions of keys
    and values.

    Note that in order to compare 2 iterator, they must both originate from the
    same trie version.

    Also note that while this iterator mostly conforms to the
    random_access_iterator concept, it doesn't properly support the
    writable_iterator concept. This means that trying to write to the iterator
    will modify a copy of the value and not the value within the trie.

    This iterator is unlikely to ever support the writable_iterator concept.
    The reason is that we can't directly change the value into the trie without
    mutating the trie. Mutating the trie would change it's version which would
    make the iterator incompatible with itself. If we updated the version
    associated with this iterator, it would break very common iterator idioms
    (eg. iterating from begin() to end()).

    What all of this means is that any stl algorithm that mutate the trie can
    only be used through the insert_iterator utility.
*/
template<typename Key, typename Value>
struct MapIterator : public boost::iterator_facade<
    MapIterator<Key, Value>,
    std::pair<Key, Value>,
    std::random_access_iterator_tag,
    std::pair<Key, Value> >
{

    /** Internal Constructor */

    MapIterator(
            MemoryAllocator* area,
            const TrieIterator& it,
            ConstTrieVersion version) :
        area_(area),
        version_(std::make_shared<ConstTrieVersion>(version)),
        it_(it, version_.get())
    {}


    /** User constructors */

    MapIterator() {}

    MapIterator(const MapIterator& other) :
        area_(other.area_),
        version_(other.version_),
        it_(other.it_)
    {}

    MapIterator(const MapIterator&& other) :
        area_(std::move(other.area_)),
        version_(std::move(other.version_)),
        it_(std::move(other.it_))
    {}


    MapIterator& operator=(const MapIterator& other)
    {
        if (this == &other) return *this;

        /** This is a safety check that prevents some fairly obscure bug with
            the gc locks.

            Changing the iterator's version should cause us to drop the previous
            gc lock and grab a new one which would cycle the gc lock epoch. What
            really happends is that we grab a new gc lock and drop the old one
            which will not cycle to epoch.

            In a nutshell, no gc passes will be triggered when overwriting an
            iterator's version. This is bad because on long running loop this
            can cause huge memory build up. So we guard against this by only
            allowing iterators associated witht he same version to interact with
            each other.
         */
        if (version_ && other.version_) {
            ExcCheckEqual(version_->root, other.version_->root,
                    "Attempting to change the iterator's version.");
        }

        area_    = other.area_;
        version_ = other.version_;
        it_      = other.it_;

        return *this;
    }


    MapIterator& operator=(const MapIterator&& other)
    {
        if (this == &other) return *this;

        /** see copy operator= for more details. */
        if (version_ && other.version_) {
            ExcCheckEqual(version_->root, other.version_->root,
                    "Attempting to change the iterator's version.");
        }

        area_    = std::move(other.area_);
        version_ = std::move(other.version_);
        it_      = std::move(other.it_);

        return *this;
    }



    /** Shorthand to access the key pointed to by the iterator. */
    Key key() const
    {
        Key key = it_.key().cast<Key>();
        return key;
    }

    /** Shorthand to access the value pointed to by the iterator. */
    Value value() const
    {
        return details::TrieUtils<Key, Value>::load(*area_, it_.value());
    }

    /** Returns whether the iterator is off the end of the map. This can be used
        to avoid grabbing a map version when we only want to check against
        end().

    */
    bool isOffTheEnd() const
    {
        return !it_.valid();
    }

    /** Returns the iterator to its default constructed state. This allows the
        iterator to be safely reused with a new version of the map.
     */
    void reset()
    {
        area_ = nullptr;
        it_ = TrieIterator();
        version_.reset();
    }

private:

    /** Because we're not returning a reference, we don't support the
        writable_iterator concept
    */
    std::pair<Key, Value> dereference() const
    {
        return std::make_pair(key(), value());
    }

    bool equal(const MapIterator<Key,Value>& rhs) const
    {
        ExcCheckEqual(version_->root, rhs.version_->root,
                "Comparing iterators from different trie versions");

        return it_ == rhs.it_;
    }

    ssize_t distance_to(const MapIterator<Key,Value>& rhs) const
    {
        ExcCheckEqual(version_->root, rhs.version_->root,
                "Comparing iterators from different trie versions");

        return std::distance(it_, rhs.it_);
    }

    void increment() { it_++; }
    void decrement() { it_--; }
    void advance(ssize_t n) { std::advance(it_, n); }

    friend class boost::iterator_core_access;

    // Allocators needed to load values out of the region.
    MemoryAllocator* area_;

    // Keeps the iterator valid and usable after it is returned to the user.
    std::shared_ptr<ConstTrieVersion> version_;

    // Our wrapped iterator.
    TrieIterator it_;
};


/******************************************************************************/
/* CONST MAP BASE                                                             */
/******************************************************************************/

/** Base class for all read operations that can take place on the mmap. It
    models the read operations of an associative container from the STLs but
    with an important difference in how iterators are handled. See the
    MapIterator class for more details.

    All map types should derive from this class which makes it possible to pass
    around any of the map types as a reference to this class. This is the
    desired behaviour because reads are treated the same way on all map types.

    Iterators constructed by this class are aware of the version that they were
    constructed from. This means that they can be used safely and consistently
    after the function returns.
*/
template<typename Key, typename Value>
struct ConstMapBase
{
    // stl compatible typedefs

    typedef std::pair<Key, Value> value_type;

    typedef MapIterator<Key, Value> iterator;
    // Our iterators are inherently const because they're not writable.
    typedef iterator const_iterator;

    typedef value_type& reference;
    typedef const value_type& const_reference;
    typedef value_type* pointer;

    typedef typename iterator::difference_type difference_type;
    typedef size_t size_type;


    size_t size() const
    {
        return get().size();
    }

    bool empty() const
    {
        return size() == 0;
    }

    /** Returns true if the key exists in the map. */
    bool count(const Key& key) const
    {
        return get().count(key);
    }

    iterator begin() const
    {
        auto version = get();
        return iterator(area, version.begin(), version);
    }

    iterator end() const
    {
        auto version = get();
        return iterator(area, version.end(), version);
    }

    /** Ensures that the two iterators returned come from the same version
        of the trie.
    */
    std::pair<iterator, iterator> beginEnd() const
    {
        auto version = get();
        return std::make_pair(
                iterator(area, version.begin(), version),
                iterator(area, version.end(), version));
    }

    /** Returns an iterator to the first element in the trie whoes key contains
        starts by the given prefix. If no such element exists then and iterator
        to the first element whose key is greater then the given prefix is
        returned.

        Note that the semantics of this operation is different then the std
        containers.
    */
    iterator lowerBound(const Key& prefix) const
    {
        auto version = get();
        return iterator(area, version.lowerBound(prefix), version);
    }

    /** Returns an iterator to the last element in the trie whoes key contains
        starts by the given prefix. If no such element exists then and iterator
        to the first element whose key is greater then the given prefix is
        returned.

        Note that the semantics of this operation is different then the std
        containers.
    */
    iterator upperBound(const Key& prefix) const
    {
        auto version = get();
        return iterator(area, version.upperBound(prefix), version);
    }

    /** Returns a pair of iterator that contains all the element whose keys
        starts by the given prefix. If no such elements exists then an empty
        range located at the first element whose key is greater then the given
        prefix is returned.

        This function is prefered over calling lowerBound() and uppderBound()
        seperately because it ensures that both iterators will belong to the
        same version of the trie.
    */
    std::pair<iterator, iterator> bounds(const Key& prefix) const
    {
        auto version = get();
        auto bounds = version.bounds(prefix);

        return std::make_pair(
                iterator(area, bounds.first, version),
                iterator(area, bounds.second, version));
    }

    /** Returns an iterator to the given key if it exists within the
        map. Returns end() otherwise.
     */
    iterator find(const Key& key) const
    {
        auto version = get();
        return iterator(area, version.find(key), version);
    }

    /** Tweaked find function for use in the JS interface. */
    std::pair<bool, Value> get(const Key& key) const
    {
        auto it = get().find(key);

        if (!it.valid())
            return make_pair(false, Value());

        return make_pair(
                true, details::TrieUtils<Key, Value>::load(*area, it.value()));
    }

    TrieStats stats(StatsSampling method = STATS_SAMPLING_RANDOM) const
    {
        return get().stats(method);
    }

    /** Debugging helper */
    void dump(unsigned indent = 0, std::ostream& stream = std::cerr) const
    {
        std::string id(indent, ' ');
        std::string id2(indent+4, ' ');

        stream << id <<  "MAP:" << std::endl;

        iterator it, end;
        for (tie(it, end) = beginEnd(); it != end; ++it)
            stream << id2 << it->first << " -> " << it->second << std::endl;

        stream << std::endl << id << "TRIE: ";
        get().dump(indent+4, 0, stream);
        stream << std::endl;
    }

    /** Debugging helper. */
    Trie& raw()
    {
        return *trie;
    }

    void forceUnlock()
    {
        area->region().gc->forceUnlock();
    }


protected:

    /** Creates a trie interface associated with the given id. */
    ConstMapBase(MemoryAllocator* area, unsigned id) :
        area(area), trie(new Trie(area->trie(id)))
    {}

    /** Constructor used to change from one map type to another */
    ConstMapBase(MemoryAllocator* area, const std::shared_ptr<Trie>& trie) :
        area(area), trie(trie)
    {}

    virtual ~ConstMapBase() {}

    /** Returns the trie version that will be used by the read operations.

        While I would love to return a reference or a pointer and avoid the
        extra copies and their associated gc locks and unlocks, Map<> makes that
        difficult. I *THINK* I could return a shared_ptr though.

        On the bright side, these are small objects and gc locks operations will
        hit the thread local for MapVersion and MapTransaction so the cost
        should be minimal.
     */
    virtual ConstTrieVersion get() const = 0;

    MemoryAllocator* area;
    std::shared_ptr<Trie> trie;
};


/******************************************************************************/
/* MAP VERSION                                                                */
/******************************************************************************/

/** A const version of a map which can be used to do consistent reads.

    Note that holding on a version of the map essentially disables GC for all
    versions of the map. For long running processes this can lead to excessive
    memory build ups if a version is kept for too long. To work around this
    problem you can either periodically fetch a new version of the map or use a
    MapTransaction to do inserts because it's more memory efficient.

*/
template<typename Key, typename Value>
struct MapVersion : public ConstMapBase<Key, Value>
{
    typedef ConstMapBase<Key, Value> MapBaseT;
    using MapBaseT::trie;

    /** Fetch the current version of a given map id in a DasDB file. The
        underlying trie is created if the id wasn't already registered.
     */
    MapVersion(MemoryAllocator* area, unsigned id) :
        MapBaseT(area, id), version(trie.constCurrent())
    {
        ExcAssertEqual(trie.get(), version.trie);
    }

    /** Fetch the current version from an existing map. */
    template<typename Dealloc>
    MapVersion(const Map<Key, Value, Dealloc>& other) :
        MapBaseT(other.area, other.trie),
        version(trie->constCurrent())
    {
        ExcAssertEqual(trie.get(), version.trie);
    }

private:

    /** Always return the same one version which guarantees consistency across
        all reads.
    */
    virtual ConstTrieVersion get() const
    {
        ExcAssertEqual(trie.get(), version.trie);
        return version;
    }
    ConstTrieVersion version;

};


/******************************************************************************/
/* MAP TRANSACTION                                                            */
/******************************************************************************/

/** Map transaction that can either be atomically comitted or rolled-back.

    WARNING: Mutating a MapTransaction object is NOT thread safe!
    * 2 threads mutating the same transaction            => corrupted writes
    * 2 threads reading and writing the same transaction => corrupted reads
    * 2 threads mutating 2 transaction                   => safe
    * 2 threads mutating a transaction and a regular map => safe.

    Note that any uncomitted modifications will be rolled-back when the object
    is destructed. Also note that a comitted object will no longer be usable and
    a new object should be created to continue mutating the trie.

    \todo Should we make comitted objects begin a new transaction? If not, make
    sure we throw after a commit.

    A transaction object will hold onto the version from which it originates
    which has an impact on the gc passes. Fortunately, the underlying trie for
    transactions is significantly more space efficient so long as the writes are
    restricted to the same key-space (maximum node reuse which reduces the
    number of inital CoW).

    Note that since this object is single threaded, reads can be considered
    consistent and on the same version as long as no writes take place. A
    transaction object should only be created and manipulated by a single
    thread (The trie's GcLock is not yet transferable).

    A transaction is essentially an in-place (as opposed to copy-on-write) fork
    of a given trie. This means that the in-place version is significantly
    faster and more memory efficient at the cost of long merge time during
    commits. Merge time can be improved by ensuring that the key space touched
    by multiple transactions don't overlap. For example, thread one has all keys
    starting with A-M while thread be has all keys starting by N-Z.

    The template parameter Dealloc can be used to provide a custom deallocator
    for values. This is useful when manually managing the allocation of values
    in the trie which is only a good idea if you need multiple types of values
    in the trie that can be differentiated by their keys.

    The deallocator will not be called until it can safely deallocate
    values. That is to say that it is defered through the RCU mechanism. The
    deallocator must be a function with the following signature:

                void (MemoryAllocator&, const Key&, const Value&)

    Note this class will take ownership of any values given to it even if the
    call fails. As an example, if we're inserting a key that already exists then
    the deallocator will still be called on the value even if it never made it
    to the trie.

*/
template<typename Key, typename Value, typename Dealloc>
struct MapTransaction : public ConstMapBase<Key, Value>
{
    typedef ConstMapBase<Key, Value> MapBaseT;
    using MapBaseT::get;
    using MapBaseT::area;
    using MapBaseT::trie;
    typedef typename MapBaseT::iterator iterator;


    /** Create a transaction at the current version of given map id. If the
        underlying trie doesn't exist, then it is created.
     */
    MapTransaction(
            MemoryAllocator* area,
            unsigned id,
            const Dealloc& deallocator = Dealloc()) :
        MapBaseT(area, id),
        tx(trie.transaction()),
        deallocator(deallocator),
        committed(false)
    {
        ExcAssertEqual(trie.get(), tx.trie);
    }


    /** Create a transaction at the current version of the given map. */
    MapTransaction(
            const Map<Key, Value, Dealloc>& other,
            const Dealloc& deallocator = Dealloc()) :
        MapBaseT(other.area, other.trie),
        tx(trie->transaction()),
        deallocator(deallocator),
        committed(false)
    {
        ExcAssertEqual(trie.get(), tx.trie);
    }


    virtual ~MapTransaction()
    {
        if (!committed) {
            // We have to do this here to properly deallocate values returned by
            // TransactionalTrie's rollbak.
            rollback();
        }
    }


    /** Doesn't make sense to copy a transaction. It would also screw up the
        internal state of the object.
     */
    MapTransaction(const MapTransaction& other) = delete;
    MapTransaction& operator=(const MapTransaction& other) = delete;

    MapTransaction(MapTransaction&& other) :
        MapBaseT    (std::move(other)),
        tx          (std::move(other.tx)),
        deallocator (std::move(other.deallocator)),
        committed   (std::move(other.committed))
    {
        // Prevents rollback when destroying the moved object.
        other.committed = true;
    }


    MapTransaction& operator=(MapTransaction&& other)
    {
        if (this == &other) return *this;

        MapBaseT::operator=(std::move(other));
        tx          = std::move(other.tx);
        deallocator = std::move(other.deallocator);
        committed   = std::move(other.committed);

        // Prevents rollback when destroying the moved object.
        other.committed = true;

        return *this;
    }


    /** Associates a value to a key in the map if it doesn't exist.

        The returned pair contains an iterator to the key along with a bool
        indicating whether the key was value or not.
     */
    std::pair<iterator, bool>
    insert(const Key& key, const Value& value)
    {
        auto& version = get();
        using namespace details;

        uint64_t offset = TrieUtils<Key, Value>::store(*area, value);

        auto ret = version.insert(key, offset);

        if (!ret.second)
            TrieUtils<Key, Value>::deallocNow(*area, key, offset, deallocator);
        else insertedKeys.insert(key);

        return std::make_pair(iterator(area, ret.first, version), ret.second);
    }


    /** Replaces the value of the given key by the given value.

        Returns:
        - (it, oldValue) if the key exists and it's value was replaced.
        - (end(), Value()) if the key didn't exist.
    */
    std::pair<iterator, Value>
    replace(const Key& key, const Value& value)
    {
        auto& version = get();
        using namespace details;

        uint64_t newOffset = TrieUtils<Key, Value>::store(*area, value);
        auto ret = version.replace(key, newOffset);

        if (!ret.first) {
            TrieUtils<Key, Value>::deallocNow(*area, key, newOffset, deallocator);
            return make_pair(iterator(area, version.end(), version), Value());
        }

        if (insertedKeys.count(key)) {
            // If the value was added in the same transaction then this is the
            // last chance we have to dealloc it.
            TrieUtils<Key, Value>::deallocNow(*area, key, ret.second, deallocator);
        }
        else insertedKeys.insert(key);

        auto it = version.find(key);
        Value oldValue = TrieUtils<Key, Value>::load(*area, ret.second);

        return make_pair(iterator(area, it, version), oldValue);
    }


    /** Removes all entries from the map. */
    void clear()
    {
        auto& version = get();
        using namespace details;

        for (const Key& key : insertedKeys) {
            auto it = version.find(key);
            if (it != version.end()) continue;

            TrieUtils<Key, Value>::deallocNow(area, it.key(), it.value(), deallocator);
        }
        insertedKeys.clear();

        // Value deallocation happens during commit.
        version.clear();
    }


    /** Removes the key from the map if it exists. */
    bool remove(const Key& key)
    {
        auto& version = get();
        using namespace details;

        auto ret = version.remove(key);

        if (ret.first && insertedKeys.count(key)) {
            TrieUtils<Key, Value>::deallocNow(*area, key, ret.second, deallocator);
            insertedKeys.erase(key);
        }

        // Rest of the values are deallocated during commit.
        return ret.first;
    }


    /** Callback used for resolving insertion or replacement conflicts in a
        commit.

        This can happen if the key was inserted/replaced in both the transaction
        and the main version of the datastore. Note that this callback won't be
        called if a value was removed in dest but replaced in src. In the final
        trie, the src value will prevail.

        The given values refer to the following versions:
        - base: The value of the key when we first forked our trie.
        - src : The value of the key in our transaction.
        - dest: The value of the key in the current version of teh trie.

        Note if the key doesn't have an associated value in base then the
        passed parameter will be default constructed.

        The return value will replace the existing dest value in the final trie.

        Usage examples for this callback:
        - Values that represent accumulators can be merged like so:
                           return dest + (src - base)

        - Values that represent min or max elements can be merged like so:
                             return min(src, dest)

     */
    typedef std::function<
        Value
        (const Key&, const Value& base, const Value& src, const Value& dest)>
    InsertConflict;


    /** Callback used for resolving remove conflicts in a commit.

        This can happen if the value associated to a key to be removed, doesn't
        match the value in the current version of the trie. In other words, the
        key to be removed was modified and this callback can prevent the
        accidental removal of new content.

        The given values refer to the following versions:
        - base: The value of the key when we first forked our trie.
        - dest: The value of the key in the current version of teh trie.

        The return value indicates whether the dest value should be deleted or
        not.

        \todo We really should return a pair where true indicates that the value
        should be deleted and false indicates that it should be replaced by a
        new value.

     */
    typedef std::function<
        bool (const Key&, const Value& base, const Value& dest)>
    RemoveConflict;


    /** Commits the transaction with a default conflict resolution mechanism
        which is unspecified.

        Note that the object is no longer usable once this function is called.
     */
    void commit()
    {
        auto removedValues = get().commit();
        committed = true;

        details::TrieUtils<Key, Value>::deallocDefer(
                *area, removedValues, trie->gc(), deallocator);
    }

    /** Merges the changes of the transaction into the mainline version.

        Since the underlying operation is a 3-way trie merge, conflicts can
        occur during the merge. These have to be resolved manually through the
        insert conflict callback and the remove conflict callbacks.

        Note that the object is no longer usable once this function is called.
     */
    void commit(InsertConflict insConflict, RemoveConflict rmvConflict)
    {

        auto removedValues = get().commit(
                getInsertConflictFn(insConflict),
                getRemoveConflictFn(rmvConflict));

        committed = true;

        details::TrieUtils<Key, Value>::deallocDefer(
                *area, removedValues, trie->gc(), deallocator);
    }

    bool tryCommit(
            const std::function<void()>& preCommitFn = std::function<void()>())
    {
        ValueDeallocList removedValues;

        std::tie(committed, removedValues) = get().tryCommit(preCommitFn);
        if (!committed) return false;

        details::TrieUtils<Key, Value>::deallocDefer(
                *area, removedValues, trie->gc(), deallocator);

        return true;
    }

    bool tryCommit(
            InsertConflict insConflict,
            RemoveConflict rmvConflict,
            const std::function<void()>& preCommitFn = std::function<void()>())
    {
        ValueDeallocList removedValues;

        std::tie(committed, removedValues) = get().tryCommit(
                getInsertConflictFn(insConflict),
                getRemoveConflictFn(rmvConflict),
                preCommitFn);

        if (!committed) return false;

        details::TrieUtils<Key, Value>::deallocDefer(
                *area, removedValues, trie->gc(), deallocator);

        return true;
    }



    /** Discards all changes made in this transaction.

        Note that the object is no longer usable once this function is called.
     */
    void rollback()
    {
        auto insertedValues = get().rollback();
        committed = true;

        details::TrieUtils<Key, Value>::deallocDefer(
                *area, insertedValues, trie->gc(), deallocator);
    }


protected:

    MergeInsertConflict getInsertConflictFn(InsertConflict insConflict)
    {
        return [&, insConflict](
                const TrieKey& key, uint64_t base, uint64_t src, uint64_t dest)
            -> uint64_t
        {
            using namespace details;

            // There might not be a base value so keep default constructed.
            Value baseValue;
            if (ValueTraits<Value>::isInline || base)
                baseValue = TrieUtils<Key, Value>::load(*this->area, base);

            Value destValue = TrieUtils<Key, Value>::load(*this->area, dest);
            Value srcValue = TrieUtils<Key, Value>::load(*this->area, src);

            Value toInsert = insConflict(key.cast<Key>(), baseValue, srcValue, destValue);

            return TrieUtils<Key, Value>::store(*this->area, toInsert);

            // Dealloc of dest's value if different will happen on commit.
        };
    }

    MergeRemoveConflict getRemoveConflictFn(RemoveConflict rmvConflict)
    {
        return [&, rmvConflict](
                const TrieKey& key, uint64_t base, uint64_t dest) -> uint64_t
        {
            using namespace details;
            Value baseValue = TrieUtils<Key, Value>::load(*this->area, base);
            Value destValue = TrieUtils<Key, Value>::load(*this->area, dest);

            return rmvConflict(key.cast<Key>(), baseValue, destValue);

            // Dealloc of dest's value if returns true will happen on commit.
        };
    }


    /** Return the same one transaction everytime which ensures that we always
        operate on a consistent version.
     */
    virtual ConstTrieVersion get() const
    {
        ExcCheck(!committed, "Can't manipulate a committed object.");
        ExcAssertEqual(trie.get(), tx.trie);
        return tx;
    }

    TransactionalTrieVersion& get()
    {
        ExcCheck(!committed, "Can't manipulate a committed object.");
        ExcAssertEqual(trie.get(), tx.trie);
        return tx;
    }

    TransactionalTrieVersion tx;
    Dealloc deallocator;
    bool committed;

    /** Best explained by example:

            auto tx = map.transaction();

            tx.insert(X, Y);
            tx.remove(X); // Y should be dealocated but...

            tx.rollback(); // Will not deallocate Y because it's not in base
            tx.commit(); // Will not deallocate Y because it's not in src.

        So if we rely exclusively on the lists returned from commit or rollback
        then we would not detect values that are inserted then removed or
        replaced within the same transaction.

        To fix this, we keep track of all inserted keys by hash. This is where
        things get interesting: ML::Lightweight_Hash_Set doesn't support remove
        but it doesn't matter! The reason is that the trie won't allow a value
        to be removed if it was already removed so as long as only read this set
        after an op succeeded then we're sure that the key still exists.

     */
    std::unordered_set<Key> insertedKeys;
};


/******************************************************************************/
/* MAP                                                                        */
/******************************************************************************/

/** Thread-safe mutable associative container.

    Reads are only guaranteed to be consistent across a single operation. To
    have consistency across multiple operation, create a MapVersion object
    through the current() function.

    Each writes will generate a new version of the map. To group multiple writes
    into a single version, create a MapTransaction object through the
    transaction() function.

    Note that currently the Key types that can be used in the template parameter
    is restricted to uint64_t, std::string and MMap::BinString. Any value type
    can be used as long as it is supported by one of the specialization of the
    MMap::Type::Value template. Out of the box this includes, uint64_t and
    std::string.

    The template parameter Dealloc can be used to provide a custom deallocator
    for values. This is useful when manually managing the allocation of values
    in the trie which is only a good idea if you need multiple types of values
    in the trie that can be differentiated by their keys.

    The deallocator will not be called until it can safely deallocate
    values. That is to say that it is defered through the RCU mechanism. The
    deallocator must be a function with the following signature:

               void (MemoryAllocator&, const Key&, const Value&);

    Note this class will take ownership of any values given to it even if the
    call fails. As an example, if we're inserting a key that already exists then
    the deallocator will still be called on the value even if it never made it
    to the trie.

*/
template<typename Key, typename Value, typename Dealloc>
struct Map : public ConstMapBase<Key, Value>
{
    typedef ConstMapBase<Key, Value> MapBaseT;
    using MapBaseT::get;
    using MapBaseT::area;
    using MapBaseT::trie;
    typedef typename MapBaseT::iterator iterator;


    /** Creates a trie interface associated with the given id. */
    Map(MemoryAllocator* area, unsigned id, const Dealloc& dealloc= Dealloc()) :
        MapBaseT(area, id), deallocator(dealloc)
    {}

    /** Returns a snapshot of the map that is guaranteed to not change. See
        MapVersion for more details.
    */
    MapVersion<Key, Value> current() const
    {
        return MapVersion<Key, Value>(*this);
    }

    /** Starts a new isolated transaction which is encapsulated by the returned
        object. See MapTransaction for more details.
    */
    MapTransaction<Key, Value, Dealloc> transaction() const
    {
        return MapTransaction<Key, Value, Dealloc>(*this, deallocator);
    }


    /** Associates a value to a key in the map if it doesn't exist.

        The returned pair contains an iterator to the key along with a bool
        indicating whether the key was value or not.
     */
    std::pair<iterator, bool>
    insert(const Key& key, const Value& value)
    {
        auto version = get();
        using namespace details;

        uint64_t offset = details::TrieUtils<Key, Value>::store(*area, value);

        auto ret = version.insert(key, offset);

        if (!ret.second)
            TrieUtils<Key, Value>::deallocNow(*area, key, offset, deallocator);

        return std::make_pair(iterator(area, ret.first, version), ret.second);
    }


    /** Removes all entry from the map. */
    void clear()
    {
        auto version = get();
        using namespace details;

        ValueDeallocList valueList;
        for (auto it = version.begin(), end = version.end(); it != end; ++it)
            valueList.push_back(std::make_pair(it.key(), it.value()));

        TrieUtils<Key, Value>::deallocDefer(
                *area, valueList, trie->gc(), deallocator);

        version.clear();
    }

    /** Removes the key from the map if it exists. */
    bool remove(const Key& key)
    {
        auto version = get();
        using namespace details;

        auto ret = version.remove(key);

        if (ret.first) {
            ValueDeallocList valueList = {{ key, ret.second }};
            TrieUtils<Key, Value>::deallocDefer(
                    *area, valueList, trie->gc(), deallocator);
        }

        return ret.first;
    }

    /** Atomic compare and swap operand which is meant to be used as a
        thread-safe replace function.

        Returns:
        - (it, curValue) if curValue == oldValue
        - (it, curValue) if curValue != oldValue
        - (end, Value()) if the key doesn't exist

        \todo Could use a less round-about path for inline values which would
              save a find() call.
    */
    std::pair<iterator, Value>
    compareAndSwap(
            const Key& key, const Value& oldValue, const Value& newValue)
    {
        auto version = get();
        using namespace details;

        Value curValue;
        uint64_t curOffset;

        bool newOffsetInit = false;
        uint64_t newOffset;

        do {
            TrieIterator it = version.find(key);
            if (it == version.end()) {
                if (newOffsetInit)
                    TrieUtils<Key, Value>::deallocNow(*area, key, newOffset, deallocator);
                return std::make_pair(
                    iterator(area, version.end(), version), Value());
            }

            curOffset = it.value();
            curValue = TrieUtils<Key, Value>::load(*area, curOffset);

            // operator== is more likely to be defined then operator!=
            if (!(curValue == oldValue)) {
                if (newOffsetInit)
                    TrieUtils<Key, Value>::deallocNow(*area, key, newOffset, deallocator);
                return std::make_pair(iterator(area, it, version), curValue);
            }

            // If Value is inlinable then 0 is a valid value but store() becomes
            // a no-op so it doesn't matter.
            if (!newOffsetInit) {
                newOffset = TrieUtils<Key, Value>::store(*area, newValue);
                newOffsetInit = true;
            }
            else {
                newOffset = 0;
            }

        } while(!version.compareAndSwap(key, curOffset, newOffset).first);

        ValueDeallocList valueList = {{ key, curValue }};
        TrieUtils<Key, Value>::deallocDefer(*area, valueList, trie->gc(), deallocator);

        // Update the iterator to the current version.
        // \todo the trie compare and swap should probably return an iterator
        // so we can avoid this.
        TrieIterator it = version.find(key);

        return std::make_pair(iterator(area, it, version), curValue);
    }

protected:

    /** Generate a new version for each operation.

        Note that we don't want to cache a local MutableTrieVersion because if
        we have 2 Map object that refer to the same trie then any writes we make
        on Map A will not be readable on Map B until we make a write on Map
        B. This would be all kinds of confusing so read the root everything we
        do a read.

        Note that the above could be solved by reseting the root each time get()
        is called but then that would mean we're not thread-safe which is a
        requirement of this subclass.
     */
    virtual ConstTrieVersion get() const { return trie->constCurrent(); }
    MutableTrieVersion get() { return trie->current(); }

    Dealloc deallocator;
};


/******************************************************************************/
/* UTILITIES                                                                  */
/******************************************************************************/

/** Atomic get and increment ops for an MMap::Map's key. If the key isn't
    present then count will become the value for the key. Otherwise the oldValue
    will be incremented by count using the + operator.

    The previous value of the key will be returned or a default constructed
    Value will be returned if the key is not present.

    Requirements: Value must be default constructable and must support the +
    operator.
 */
template<typename Key, typename Value, typename Dealloc>
Value fetchAndAdd(
        Map<Key, Value, Dealloc>& map,
        const Key& key, const Value& count)
{
    typename Map<Key, Value, Dealloc>::iterator it;
    bool inserted;

    do {
        tie(it, inserted) = map.insert(key, count);
        if (inserted) return Value();

        Value oldValue, curValue, newValue;
        do {
            oldValue = it.value();
            newValue = oldValue + count;

            it.reset();
            tie(it, curValue) = map.compareAndSwap(key, oldValue, newValue);
        } while(!it.isOffTheEnd() && curValue != oldValue);

        if (!it.isOffTheEnd()) return oldValue;

    } while(it.isOffTheEnd()); // Start from scratch if the value was removed.

    ExcAssert(false);
}

/** Get and increment ops for an MMap::MapTransaction's key. If the key isn't
    present then count will become the value for the key. Otherwise the oldValue
    will be incremented by count using the + operator.

    The previous value of the key will be returned or a default constructed
    Value will be returned if the key is not present.

    Requirements: Value must be default constructable and must support the +
    operator.
 */
template<typename Key, typename Value, typename Dealloc>
Value fetchAndAdd(
        MapTransaction<Key, Value, Dealloc>& tx,
        const Key& key, const Value& amount)
{
    typename Map<Key, Value, Dealloc>::iterator it;
    bool inserted;

    tie(it, inserted) = tx.insert(key, amount);
    if (inserted) return Value();

    Value newValue = it.value() + amount;
    it.reset();

    Value oldValue;
    tie(it, oldValue) = tx.replace(key, newValue);

    return oldValue;
}


} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__mmap_map_h__ */
