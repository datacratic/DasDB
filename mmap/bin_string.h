/** bin_string.h                                 -*- C++ -*-
    Rémi Attab, 04 Oct 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Raw binary string suitable for key string.

    \todo try a compact vector instead of vector.
*/

#ifndef __mmap__bin_string_h__
#define __mmap__bin_string_h__

#include "jml/utils/exc_assert.h"

#include <vector>
#include <functional>
#include <algorithm>

namespace Datacratic {
namespace MMap {


/******************************************************************************/
/* BSWAP                                                                      */
/******************************************************************************/
/** Utility to switch between little and big endian.

    Need something to detect endianese and and turn into a noop if we're at the
    expected endianese.
 */

template<typename T> struct BSwap_ {};

template<>
struct BSwap_<bool>
{
    static bool bswap(bool v) { return v; }
};

template<>
struct BSwap_<uint8_t>
{
    static uint8_t bswap(uint8_t v) { return v; }
};

template<>
struct BSwap_<uint16_t>
{
    static uint16_t bswap(uint16_t v) {
        return ((v & 0x00FF) << 8) | ((v & 0xFF00) >> 8);
    }
};

template<>
struct BSwap_<uint32_t>
{
    static uint32_t bswap(uint32_t v) { return __builtin_bswap32(v); }
};

template<>
struct BSwap_<uint64_t>
{
    static uint64_t bswap(uint64_t v) { return __builtin_bswap64(v); }
};


template<typename T>
T bswap(T v) { return BSwap_<T>::bswap(v); }


/******************************************************************************/
/* BIN STRING                                                                 */
/******************************************************************************/

/** Binary string for use where string is not appropriate. For example if you
    need null bytes then a string will be all kinds of bad. Printing will also
    be a mess.

    This class ensures that the binary string is stored in big-endian to
    preserve the intended ordering of keys within the mmap file. Note that
    currently this assumes that all given values are in little endian.

    \todo Remove the little endian assumption.
 */
struct BinString : public std::vector<uint8_t>
{
    BinString() : vector() {}
    explicit BinString(size_t count) : vector(count) {}
    explicit BinString(size_t count, uint8_t value) : vector(count, value) {}
    BinString(std::initializer_list<uint8_t> init) : vector(init) {}

    template<typename It>
    BinString(const It& first, const It& last) : vector(first, last) {}

    /** Only unsigned ints are supported. */
    template<typename T>
    void append(T v)
    {
        Converter<T> conv;
        conv.value = bswap(v);
        insert(end(), conv.bits, conv.bits + sizeof(T));
    }

    void append(const std::string& v)
    {
        insert(end(), v.begin(), v.end());
        insert(end(), '\0');
    }

    /** Only unsigned ints are supported. */
    template<typename T>
    std::pair<T, const_iterator> extract(const_iterator pos) const
    {
        auto endIt = pos + sizeof(T);
        ExcAssert(endIt <= end());

        Converter<T> conv;
        std::copy(pos, endIt, conv.bits);
        return std::make_pair(bswap(conv.value), endIt);
    }

    std::pair<std::string, const_iterator>
    extractString(const_iterator& pos) const
    {
        auto it = std::find(pos, cend(), '\0');
        ExcAssert(it != end());
        return std::make_pair(std::string(pos, it), it + 1);
    }

private:

    template<typename T>
    struct Converter {
        union {
            struct { T value; };
            uint8_t bits[sizeof(T)];
        };
    };

};

} // namespace MMap
} // Datacratic

namespace std {

/** Hash function for the BinString type.

    \todo dbj2 is just the first hash I could think of. Might be a better idea
          to use city hash or murmur hash 3. That would require an extra
          dependency though.
 */
template<>
struct hash<Datacratic::MMap::BinString>
{
    size_t operator() (const Datacratic::MMap::BinString& bin) const
    {
        // djb2 - bernstein hash
        uint64_t hash = 5381;
        for (auto it = bin.begin(), end = bin.end(); it != end; ++it)
            hash = ((hash << 5) + hash) + *it;
        return hash;
    }
};

} // namespace std

#endif // __mmap__bin_string_h__
