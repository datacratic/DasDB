/* mmap_typed_trie.h                                                 -*- C++ -*-
   Rémi Attab, 17 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Utilities for typed tries.

   \todo This file needs a better name...
*/

#ifndef __mmap__mmap_typed_trie_h__
#define __mmap__mmap_typed_trie_h__


#include "memory_allocator.h"
#include "soa/gc/gc_lock.h"
#include "mmap_types.h"

#include <vector>
#include <string>
#include <algorithm>


namespace Datacratic {
namespace MMap {

/******************************************************************************/
/* VALUE TRAITS                                                               */
/******************************************************************************/

/** Traits for trie values which is used by the template magic to determine how
    the value should be saved.

    \todo Move this to the Types namespace so that it can be specialized by a
    user. This is required to avoid accidently recognizing an non-inlineable
    struct as inlineable (eg. struct with only a pointer in it).

*/
template<typename T>
struct ValueTraits
{
    enum { isInline = sizeof(T) <= 8 };
};


/** A reference counted string could be implemented with only a pointer so
    just to be sure, specialize the traits class.

*/
template<>
struct ValueTraits<std::string>
{
    enum { isInline = false };
};


/******************************************************************************/
/* VALUE DEALLOCATOR                                                          */
/******************************************************************************/

/** Default deallocator functor which just forwards the deallocation to
    Types::Value<Value>.dealloc().
*/
template<typename Value>
struct ValueDeallocator
{
    void operator() (MemoryAllocator& area, const TrieKey&, const Value& bits)
    {}
};


/******************************************************************************/
/* TRIE UTILS                                                                 */
/******************************************************************************/

namespace details {


/** See the specialization below for more details. */
template<typename Key, typename Value, bool Inline = ValueTraits<Value>::isInline>
struct TrieUtils
{};


/** Manages values that can fit in a 64 bit word and can therefor be saved
    directly into the mmap.

    \todo Won't really work with a struct so we probably need some kind of
    reinterpret cast to make it work. Note sure what's the safest way to do it.

*/
template<typename Key, typename Value>
struct TrieUtils<Key, Value, true>
{

    /** Load the value from the memory region. */
    static Value load(MemoryAllocator&, uint64_t value) { return value; }

    /** Save the value to the memory region. */
    static uint64_t store(MemoryAllocator&, const Value& value) { return value; }


    /** Deallocates the value now without deffering. */
    template<typename Dealloc>
    static void deallocNow(
            MemoryAllocator& area,
            const TrieKey& key,
            uint64_t offset,
            Dealloc& dtor)
    {
        dtor(area, key.cast<Key>(), load(area, offset));
    }


    /** Defers the deallocation of multiple values. Deallocation goes through
        the given Deallocator functor.

        \todo If we're using the default deallocator then this becomes an overly
        elaborate noop. Would be nice if we could skip the deffer in these
        cases.
    */
    template<typename Dealloc>
    static void deallocDefer(
            MemoryAllocator& area,
            const ValueDeallocList& list,
            GcLockBase& gc,
            Dealloc& dtor)
    {
        if (list.empty()) return;

        auto doDeallocate = [&, list] () {
            uint64_t i; // If we need to resize, don't redeallocate blocks.

            MMAP_PIN_REGION(area.region())
            {
                for (i = 0; i < list.size(); ++i) {
                    // No idea why I have to go through TrieUtils to get to
                    // deallocNow.
                    TrieUtils<Key, Value>::deallocNow(
                            area, list[i].first, list[i].second, dtor);
                }
            }
            MMAP_UNPIN_REGION;
        };
        gc.defer(doDeallocate);
    }

};


/** Manages values that can't fit directly into the trie and need some kind of
    external allocation in order to work.

    This relies on Types::Value<T> to do its work so any types supported by one
    of its specialization will work just fine.

*/
template<typename Key, typename Value>
struct TrieUtils<Key, Value, false>
{

    static std::string load(MemoryAllocator& area, uint64_t offset)
    {
        return Types::load<Value>(area, offset);
    }

    static uint64_t store(MemoryAllocator& area, const Value& value)
    {
        return Types::alloc(area, value).offset();
    }

    /** Deallocates the value now without deffering. */
    template <typename Dealloc>
    static void deallocNow(
            MemoryAllocator& area,
            const TrieKey& key,
            uint64_t offset,
            Dealloc& dtor)
    {
        dtor(area, key.cast<Key>(), Types::load<Value>(area, offset));
        Types::dealloc<Value>(area, offset);
    }


    /** Defers the deallocation of multiple values. Deallocation goes through
        the given Deallocator functor.
    */
    template<typename Dealloc>
    static void deallocDefer(
            MemoryAllocator& area,
            const ValueDeallocList& list,
            GcLockBase& gc,
            Dealloc& dtor)
    {
        if (list.empty()) return;

        auto doDeallocate = [&, list] () {
            uint64_t i; // If we need to resize, don't redeallocate blocks.

            MMAP_PIN_REGION(area.region())
            {
                for (i = 0; i < list.size(); ++i) {
                    TrieUtils<Key, Value>::deallocNow(
                            area, list[i].first, list[i].second, dtor);
                }
            }
            MMAP_UNPIN_REGION;
        };
        gc.defer(doDeallocate);
    }

};


}; // namespace details


} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__mmap_typed_trie_h__ */
