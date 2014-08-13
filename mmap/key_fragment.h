/* key_fragment.h                                                  -*- C++ -*-
   Jeremy Barnes, 8 September 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Class to deal with fragments of keys.
*/

#ifndef __mmap__key_fragment_h__
#define __mmap__key_fragment_h__

#include "soa/gc/gc_lock.h"
#include "mmap_const.h"
#include "sync_stream.h"
#include "mmap_trie_stats.h"
#include "jml/arch/exception.h"
#include "jml/arch/bitops.h"
#include "jml/arch/bit_range_ops.h"
#include "jml/utils/compact_vector.h"
#include "jml/utils/exc_assert.h"
#include <stdint.h>
#include <string>

#include <iostream>

namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* KEY FRAGMENT REPR                                                         */
/*****************************************************************************/

struct KeyFragmentRepr;

std::ostream& operator << (std::ostream & stream, const KeyFragmentRepr & frag);

/** Representation of the key fragment that can be saved in the mmap. */
struct KeyFragmentRepr {
    uint64_t data; //< Either the bits of the fragment or an offset to the bits.
    int32_t bits;  //< Number of bits in the fragment

    KeyFragmentRepr() : bits(-1) {}

    bool isValid() const {return bits >= 0; }
    bool isInline() const { return bits <= 64; }

    uint64_t offset() const {
        ExcAssert(isValid() && !isInline());
        return data;
    }

    uint64_t key() const {
        ExcAssert(isValid() && isInline());
        return data;
    }

    size_t directMemUsage() const {
        return isInline() ? 0 : ceilDiv(bits, 8) + 2; // 2 -> refCount
    }

    NodeStats nodeStats() const {
        NodeStats stats;

        stats.totalBytes = directMemUsage();
        stats.bookeepingBytes = sizeof(bits);

        if (!isInline()) {
            stats.bookeepingBytes += sizeof(data);
            stats.externalKeyBytes = directMemUsage();
        }

        return stats;
    }

    std::string print() const;
} JML_PACKED;


/*****************************************************************************/
/* KEY FRAGMENT                                                              */
/*****************************************************************************/

struct KeyFragment;
struct MemoryAllocator;

std::ostream & operator << (std::ostream & stream, const KeyFragment & frag);

/** Represents a fragment of a key to be matched.  The matching is done from
    the most significant bit to the least significant bit.
*/
struct KeyFragment
{
    typedef uint64_t Word;
    enum { bitsInWord = sizeof(Word) * 8 };

    typedef ML::compact_vector<Word, KeyFragmentCompactSize> KeyVec;
    KeyVec key;

    int32_t bits;
    int32_t startBit;

    explicit KeyFragment(uint64_t keyBits = 0, int32_t bits = 0)
        : key(), bits(bits), startBit(0)
    {
        if (bits)
            key.push_back(keyBits << (bitsInWord - bits));

        ExcCheckEqual(getBits(bits), keyBits,
                "Creating KeyFragment with invalid bits");
    }

    KeyFragment(const KeyVec& keyVec, int32_t bits) :
        key(keyVec), bits(bits), startBit(0)
    {
        ExcCheckEqual(getBitVec(bits), keyVec,
                "Creating KeyFragment with invalid bits");
    }


    /** Loads a key fragment from it's mmap representation. */
    static KeyFragment loadRepr(
            const KeyFragmentRepr& repr, 
            MemoryAllocator& area, 
            GcLock::ThreadGcInfo* info = 0);

    /** 
    Creates a representation of a key fragment that can be stored in the mmap.

    Note that if the repr.isInline() is false then this function will allocate 
    additional space in the mmap to store the key. In order to properly free 
    this additional space, you must call deallocRepr.
    */
    KeyFragmentRepr allocRepr(
            MemoryAllocator& area, GcLock::ThreadGcInfo* info = 0) const;

    /**
    Deallocates any additional space that may have been allocated by storeRepr()

    This is a no-op if repr.isInline() evaluates to true.
    */
    static void deallocRepr(
            const KeyFragmentRepr& repr, 
            MemoryAllocator& area, 
            GcLock::ThreadGcInfo* info = 0);

    /** Convenience function to copy a key fragment. */
    static KeyFragmentRepr copyRepr(
            const KeyFragmentRepr& repr, 
            MemoryAllocator& area, 
            GcLock::ThreadGcInfo* info = 0);


    bool empty() const { return bits == 0; }

    /** \deprecated Need to fix every function that accesses this. */
    uint64_t getKey() const
    {
        return getBits(bits);
    }


    /** Returns the first n bits from this key. */
    uint64_t getBits(int nbits, int doneBits = 0) const
    {
        ExcCheckLessEqual(nbits, 64, 
                "Too many bits to extract (see getBitVec)");

        ExcCheckGreaterEqual(nbits, 0, "Invalid number of bits");
        ExcCheckLessEqual(nbits + doneBits, bits, 
                "Not enough bits left to extract");

        if (nbits == 0) return 0;

        ML::Bit_Buffer<Word> buffer(&key[0]);
        buffer.advance(doneBits + startBit);

        return buffer.rextract(nbits);
    }

    KeyVec getBitVec(int nbits, int doneBits = 0) const
    {
        ExcCheckGreaterEqual(nbits, 0, "Invalid number of bits");
        ExcCheckLessEqual(nbits + doneBits, bits, 
                "Not enough bits to extract");

        if (nbits == 0) return KeyVec();

        KeyVec vec(ceilDiv(nbits, bitsInWord));
        
        SeqBitBuffer buffer(&key[0]);
        buffer.advance(doneBits + startBit);
        copyBits(vec, 0, buffer, nbits);

        return vec;
    }

    /** Removes and returns the first n bits from this key. */
    uint64_t removeBits(int nbits)
    {
        uint64_t result = getBits(nbits);
        startBit += nbits;
        bits -= nbits;
        return result;
    }

    KeyVec removeBitVec(int nbits)
    {
        KeyVec result = getBitVec(nbits);
        startBit += nbits;
        bits -= nbits;
        return result;
    }

    /** Removes the bits of other from this key only if they match the prefix */
    bool consume(const KeyFragment & other)
    {
        if (empty()) return other.empty();
        if (other.empty()) return true;

        if (bits < other.bits) return false;
        if (getBitVec(other.bits) != other.getBitVec(other.bits)) return false;

        removeBitVec(other.bits);
        return true;
    }

    /** Return the prefix with the given length. */
    KeyFragment prefix(int nbits) const
    {
        return KeyFragment(getBitVec(nbits), nbits);
    }

    /** Return the suffix that's left over when we remove the given number
        of bits.
    */
    KeyFragment suffix(int nbits) const
    {
        int32_t suffixBits = bits - nbits;
        return KeyFragment(getBitVec(suffixBits, nbits), suffixBits);
    }

    /** Returns the length of the longest common prefix of two keys. */
    int32_t commonPrefixLen(const KeyFragment & k2) const
    {
        // using namespace std;

        if (bits == 0 || k2.bits == 0)
            return 0;

        // 1.  Find the shortest length
        int shortestLen = std::min(bits, k2.bits);
        int lastIndex = ceilDiv(shortestLen, bitsInWord);

        // To make things simpler, trim the keys before doing anything.
        // \todo Avoid the copy by using Bit_Reader to do the extracting.
        trim();
        k2.trim();

        // 2. Look for the first word that is different.
        int diffPos = -1;
        for (int i = 0; i < lastIndex; ++i) {
            if (key[i] != k2.key[i]) {
                diffPos = i;
                break;
            }
        }

        if (diffPos < 0) return shortestLen;

        // 3.  Extract the two prefixes
        int doneBits = diffPos * bitsInWord;
        int leftoverBits = shortestLen - doneBits;
        if (leftoverBits > 64) leftoverBits = 64;

        uint64_t p1 = getBits(leftoverBits, doneBits);
        uint64_t p2 = k2.getBits(leftoverBits, doneBits);

        // 4.  Find the highest bit that's different between the two
        //     returns zero if the two are identical
        int suffixLen = ML::highest_bit(p1 ^ p2, -1) + 1;

        return doneBits + leftoverBits - suffixLen;
    }

    /** Returns the longest common prefix of two keys. */
    KeyFragment commonPrefix(const KeyFragment & k2) const
    {
        return prefix(commonPrefixLen(k2));
    }

    bool operator == (const KeyFragment & other) const
    {
        trim();
        other.trim();
        return bits == other.bits && (empty() || key == other.key);
    }

    bool operator != (const KeyFragment & other) const
    {
        return !operator==(other);
    }

    bool operator < (const KeyFragment & other) const
    {
        int shortestLen = std::min(bits, other.bits);
        KeyVec p1 = getBitVec(shortestLen);
        KeyVec p2 = other.getBitVec(shortestLen);
        
        if (p1 < p2) return true;
        if (p1 == p2 && bits < other.bits) return true;
        return false;
    }

    KeyFragment operator + (const KeyFragment & other) const
    {
        if (empty()) return other;
        else if (other.empty()) return *this;

        KeyFragment result;

        result.bits = bits + other.bits;
        result.key.resize(ceilDiv(result.bits, bitsInWord));

        SeqBitBuffer thisSrc(&key[0]);
        thisSrc.advance(startBit);
        copyBits(result.key, 0, thisSrc, bits);

        SeqBitBuffer otherSrc(&other.key[0]);
        otherSrc.advance(other.startBit);
        copyBits(result.key, bits, otherSrc, other.bits);

        return result;
    }

    void operator += (const KeyFragment & other)
    {
        if (other.empty()) return;

        key.resize(ceilDiv(bits + other.bits, bitsInWord));

        SeqBitBuffer otherSrc(&other.key[0]);
        otherSrc.advance(other.startBit);
        copyBits(key, startBit + bits, otherSrc, other.bits);

        bits += other.bits;
    }

    std::string print() const;

private:

    typedef ML::Bit_Buffer<Word, ML::Simple_Mem_Buffer<Word> > SeqBitBuffer;

public:
    /** Physically removes the bits that were marked as removed by the startBit
        var. Should be used if when we want to access the key directly without
        having to deal with the startBit (complicates things).

        Note that if startBit isn't zero then this function will trigger a copy
        which is what we were trying to avoid by using startBit. Use sparingly.
    */
    void trim() const
    {
        if (!startBit) return;

        // While not very pretty, this op won't meaningfully change the KF
        // and we need to use it in const functions (for now anyway).
        KeyFragment* mutableThis = const_cast<KeyFragment*>(this);

        mutableThis->key = mutableThis->getBitVec(bits);
        mutableThis->startBit = 0;
    }

public:

    // Internal utility that is public only so we can test it.
    static void copyBits(
            KeyVec& dest, int32_t start, SeqBitBuffer& src, int32_t nbits)
    {
        ML::Bit_Writer<Word> writer(&dest[0]);
        writer.skip(start);

        int32_t bitsLeft = nbits;
        for (; bitsLeft > bitsInWord; bitsLeft -= bitsInWord)
            writer.rwrite(src.rextract(bitsInWord), bitsInWord);

        if (bitsLeft)
            writer.rwrite(src.rextract(bitsLeft), bitsLeft);
    }
};

} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__key_fragment_h__ */
