/** kv.h                                 -*- C++ -*-
    Rémi Attab, 15 Nov 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Key-value class and utilties.
*/

#ifndef __mmap__kv_h__
#define __mmap__kv_h__

#include "mmap_trie_ptr.h"
#include "key_fragment.h"
#include "jml/utils/less.h"
#include "jml/utils/compact_vector.h"
#include "jml/utils/vector_utils.h"

#include <iostream>

namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* KV                                                                        */
/*****************************************************************************/

/** Key Value pair used to pass around stuff. */
struct KV
{
    KeyFragment key;
    uint64_t value; // Prefer getValue() and getPtr() if possible.
    bool isPtr;

    KV() : isPtr(false) {}

    KV(const KeyFragment& key, uint64_t value) :
        key(key), value(value), isPtr(false)
    {}

    KV(const KeyFragment& key, const TriePtr& ptr) :
        key(key), value(ptr.bits), isPtr(true)
    {}

    KV(const KeyFragment& key, uint64_t value, bool isPtr) :
        key(key), value(value), isPtr(isPtr)
    {}

    uint64_t getValue() const
    {
        ExcAssert(!isPtr);
        return value;
    }

    TriePtr getPtr() const
    {
        ExcAssert(isPtr);
        return TriePtr::fromBits(value);
    }

    bool operator < (const KV & other) const
    {
        return ML::less_all(key, other.key, value, other.value);
    }

    bool operator == (const KV & other) const
    {
        // \todo: Might want to have a different version for when isPtr is set.
        return key == other.key && value == other.value && isPtr == other.isPtr;
    }
};

inline std::ostream& operator<< (std::ostream & stream, const KV & kvs)
{
    stream << "{ " << kvs.key << " - ";
    if (kvs.isPtr)
        stream << kvs.getPtr();
    else stream << kvs.value;
    stream << " }";
    return stream;
}


/******************************************************************************/
/* KV LIST                                                                    */
/******************************************************************************/

/** Reserver 16 spot by default because of our N-ary node which can contain up
    to 16 branches. Technically it should be 17 because of the value but I doubt
    most n-ary nodes will be completely saturated.
 */
struct KVList : public ML::compact_vector<KV, 16>
{
    // Really could use this but not yet supported...
    // using compact_vector::compact_vector;

    // Remove all these damned constructors when that's supported.

    KVList() {}


    template<typename It>
    KVList(It first, It last) : compact_vector(first, last) {}

    // These don't really exist in compact_vector, could be added.
    KVList(std::initializer_list<KV> l) : compact_vector(l.begin(), l.end()) {}
    KVList(size_t n, KV kv = KV())
    {
        resize(n, kv);
    }

    KVList(const KVList& other) : compact_vector(other) {}
    KVList& operator=(const KVList& other) {
        compact_vector::operator=(other);
        return *this;
    }

    KVList(KVList&& other) : compact_vector(std::move(other)) {}
    KVList& operator=(KVList&& other) {
        compact_vector::operator=(std::move(other));
        return *this;
    }

    KV* get() { return &operator[](0); }

    bool hasPtr() const
    {
        return std::any_of(begin(), end(), [&](const KV& kv) {
                    return kv.isPtr;
                });
    }

    bool hasValue() const
    {
        return std::any_of(begin(), end(), [&](const KV& kv) {
                    return !kv.isPtr;
                });
    }

    KeyFragment commonPrefix() const
    {
        if (empty()) return KeyFragment();

        KeyFragment prefix = front().key;
        for (auto it = begin() + 1, end = this->end(); it != end; ++it) {
            prefix = prefix.commonPrefix(it->key);
        }

        return prefix;
    }

    KVList trim(int32_t startBit) const
    {
        KVList result;

        for (auto it = begin(), end = this->end(); it != end; ++it)
            result.push_back({it->key.suffix(startBit), it->value, it->isPtr });

        return result;
    }

    KVList prefixKeys(const KeyFragment& prefix) const
    {
        KVList result;

        for (auto it = begin(), end = this->end(); it != end; ++it)
            result.push_back({ prefix + it->key, it->value, it->isPtr });

        return result;
    }

    KVList narrow(const KeyFragment& prefix) const
    {
        auto first = begin();
        while (first != end() && first->key.commonPrefixLen(prefix) != prefix.bits)
            ++first;

        auto last = first;
        while (last != end() && last->key.commonPrefixLen(prefix) == prefix.bits)
            ++last;

        return KVList(first, last);
    }

    bool operator= (const KVList& other) const
    {
        return std::equal(begin(), end(), other.begin());
    }
};


inline std::ostream& operator<< (std::ostream & stream, const KVList & kvs)
{
    stream << "[ ";
    for (const KV& kv : kvs) stream << kv << " ";
    stream << "]";
    return stream;
}


inline size_t insertKv(KVList& kvs, const KV& kv)
{
    auto isSmaller = [&] (const KV& aKv) { return aKv.key < kv.key; };
    auto it = std::find_if_not(kvs.begin(), kvs.end(), isSmaller);

    size_t pos = std::distance(kvs.begin(), it);
    kvs.insert(it, kv);

    return pos;
}

} // namespace MMap
} // Datacratic

#endif // __mmap__kv_h__
