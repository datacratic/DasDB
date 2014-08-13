/* trie_key.h                                                     -*- C++ -*-
   Mathieu Stefani, 22 April 2013
   Copyright (c) 2013 Datacratic.  All rights reserved.

    TrieKey struct that can be used to convert back and worth between an
    user's key to a trie's key.
*/

#ifndef __mmap_trie_key_h__
#define __mmap_trie_key_h__

#include "bin_string.h"
#include "key_fragment.h"
#include "jml/utils/compact_vector.h"
#include "jml/arch/bitops.h"
#include "jml/arch/bit_range_ops.h"

namespace Datacratic {
namespace MMap {

struct TrieKey;

/*****************************************************************************/
/* TRIE KEY                                                                  */
/*****************************************************************************/

/**
Class used to convert back and worth between a user's key to a trie's key.

The goal of this class is that the conversion should be completely transparent
to the user. Note that in order for that to work, the provided key should only
be a POD. Anything else will (probably) result in undefined behaviour.
*/


template<typename T> T triekey_cast(const TrieKey &key);


struct TrieKey {

    explicit TrieKey(): key(), bytes(0) {}
    explicit TrieKey(const KeyFragment& frag);

    /** Primitive constructors. Implicitly invoked. */

    /* Keep this disabled for now to avoid unfortunate accidents.
    template<typename T>
    TrieKey(const T& key) {
        store(&key, sizeof(T));
    }
    */

    TrieKey(const uint64_t& key) {
        store(&key, sizeof(uint64_t));
    }

    TrieKey(const std::string& key) {
        store((uint8_t*)key.c_str(), key.size());
    }

    // Needed because otherwise it gets confused with the template constructor.
    TrieKey(const char* ckey) {
        std::string key(ckey);
        store((uint8_t*)key.c_str(), key.size());
    }

    TrieKey(const BinString& key) {
        store(&key[0], key.size());
    }


    /** Converts the key into something usable by the user. */

    /* Keep this disabled for now to avoid unfortunate accidents.
    template<typename T>
    operator T () const {
        T dest;
        load(&dest, sizeof(T));
        return dest;
    }
    */


    /** Explicitly converts the key into something usable by the user.
     *
     *  For now, since the Trie does not carry any type information, we can not
     *  type check. Thus, the following will throw :
     *
     *  \code{.cpp}
     *      TrieKey key(std::string("meow"));
     *      key.value<uint64_t>();
     *  \endcode
     *
     *  \pre T must be the same type than the type of the key being stored.
     */
    template<typename T> T cast() const {
        return triekey_cast<T>(*this);
    }

    /** Converts the key into something usable by the Trie. */
    KeyFragment getFragment() const;

private:
    template<typename T> friend struct TrieKeyCastImpl;

    template<typename T>
    void store(const T* src, size_t srcBytes)
    {
        const size_t wordSize = sizeof(uint64_t);
        const size_t chunkSize = sizeof(T);
        const size_t chunks = srcBytes / chunkSize;

        // \todo Would be nice if we didn't have this restriction.
        ExcAssertLessEqual(chunkSize, 64);

        bytes = srcBytes;
        if (!bytes) return;

        key.resize(ceilDiv(bytes, wordSize));

        ML::Bit_Buffer<T> reader(src);
        ML::Bit_Writer<uint64_t> writer(&key[0]);

        for (int i = 0; i < chunks; ++i)
            writer.rwrite(reader.rextract(chunkSize*8), chunkSize*8);
    }

    template<typename T>
    void load(T* dest, size_t destBytes) const
    {
        if (!bytes) return;
        ExcCheckEqual(bytes, destBytes, "Converting to incorrect type.");

        const size_t chunkSize = sizeof(T);
        const size_t chunks = bytes / chunkSize;

        // \todo Would be nice if we didn't have this restriction.
        ExcAssertLessEqual(chunkSize, 64);

        ML::Bit_Buffer<uint64_t> reader(&key[0]);
        ML::Bit_Writer<T> writer(dest);

        for (int i = 0; i < chunks; ++i)
            writer.rwrite((T)reader.rextract(chunkSize*8), chunkSize*8);
    }

    // same typedef as KeyFragment::KeyVec (should be unified somehow)
    typedef ML::compact_vector<uint64_t, KeyFragmentCompactSize> KeyVec;

    KeyVec key;
    size_t bytes;
};

/* Helper class for TrieKey::cast conversion member. This
 * is needed to successfully handle all types of supported
 * conversions. Each type-conversion must fully specialize
 * this template
 */

template<typename T> struct TrieKeyCastImpl {
};

template <> struct TrieKeyCastImpl<std::string> {
    static std::string cast(const TrieKey &key) {
        char buf[key.bytes + 1];
        buf[key.bytes] = 0;
        key.load(buf, key.bytes);

        return std::string(buf);
    }
};

template<> struct TrieKeyCastImpl<uint64_t> {
    static uint64_t cast(const TrieKey &key) {
        uint64_t dst = 0;
        key.load(&dst, sizeof(uint64_t));

        return dst;
    }
};

template<> struct TrieKeyCastImpl<BinString> {
    static BinString cast(const TrieKey &key) {
        BinString dst(key.bytes);
        key.load(&dst[0], key.bytes);

        return dst;
    }
};

/** Equivalent to TrieKey::cast<T>() function, except that this version
 *  is non-member.
 */

template<typename T> T triekey_cast(const TrieKey &key) {
    return TrieKeyCastImpl<T>::cast(key);
}

}
}

#endif
