/* full_bitmap.h                                                   -*- C++ -*-
   Jeremy Barnes, 6 October 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   A bitmap that knows information about whether something is full or not.
*/

#ifndef __mmap__full_bitmap_h__
#define __mmap__full_bitmap_h__

#include <cstring>
#include "jml/arch/atomic_ops.h"
#include "jml/arch/bitops.h"
#include "jml/utils/exc_assert.h"
#include "jml/compiler/compiler.h"
#include <boost/tuple/tuple.hpp>
#include <iostream>

namespace Datacratic {
namespace MMap {

template<typename T>
JML_ALWAYS_INLINE
int findClearBit(T bitmap, T fullMask, int startAt)
{
    if ((bitmap & fullMask) == fullMask) return -1;

#if 0

    int numBits = ML::highest_bit(fullMask, -1) + 1;
    int start = startAt % numBits;
    int bitNum = -1;

    //using namespace std;

    //cerr << "looking for clear bit in " << bitmap << " with "
    //     << numBits << " bits " << endl;

    for (int i = 0;  bitNum == -1 && i < numBits;  ++i) {
        int bit = (i + start) % numBits;
        //cerr << "i = " << i << " start = " << start << " bitNum = "
        //     << bit << " numBits = " << numBits
        //     << " value " << (bitmap & (1ULL << bit))
        //     << endl;
            
        if (bitmap & (1ULL << bit)) continue;
        bitNum = bit;
    }

    //cerr << "bitmap = " << bitmap << " bitNum = " << bitNum
    //     << " start = " << start << endl;

#else
    // Try to get a free bit
    int numBits = ML::highest_bit(fullMask, -1) + 1;
    int totalBits = 8 * sizeof(T);
    int toRotate = startAt % numBits;  // number of bits to rotate
    T masked = ~bitmap & fullMask;
    T rotated = (masked >> toRotate) | (masked << (totalBits - toRotate));
    int bitNum = ML::lowest_bit(rotated, -1);
    if (bitNum != -1) bitNum += toRotate;
#endif
    if (bitNum == -1) return -1;

    ExcAssertEqual(bitmap & (1ULL << bitNum), 0);
    
    return bitNum;
}

// Set a bit to empty, and return a bit that tells us whether or not the
// bitmap moved from full to not full.
template<typename T>
std::pair<int, bool> allocateClearBit(T & bitmap, T fullMask, int startAt)
{
    T currentValue = bitmap, newValue;

    for (;;) {
        newValue = currentValue;

        if (currentValue & ~fullMask)
            throw ML::Exception("non-full bit set in bitmap");

        // Try to get a free bit
        int bitNum = findClearBit<T>(currentValue, fullMask, startAt);
        
        // Full?  No can alloc...
        if (bitNum == -1)
            return std::make_pair(-1, false);

        // TODO: this doesn't need to be atomic...
        bool wasSet = ML::atomic_test_and_set(newValue, bitNum);
        if (wasSet) continue;  // something raced with us
        
        if (ML::cmp_xchg(bitmap, currentValue, newValue))
            return std::make_pair(bitNum, newValue == fullMask);
    }

}

enum TryBitmapResult {
    TBM_FAILED,   ///< The operation had already been done (no change)
    TBM_SUCCESS,  ///< The operation succeeded but no recursion needed
    TBM_RECURSE   ///< Succeeded and recursion needed
};

/** Try to clear a bit in a bitmap.

    Return value:
    TBM_FAILED:  the bit was already cleared
    TBM_SUCCESS: the bit was modified from true to false but the bitmap
                 did not move from full to not full as a result
    TBM_RECURSE: the bit was modified from true to false and the bitmap
                 moved from full to not full as a result; a higher level
                 bitmap probably needs to be modified.
*/
template<typename T>
TryBitmapResult
tryMarkBitmapNotFull(T & bitmap, int bitNum, T fullMask)
{
    T currentValue = bitmap, newValue, oldValue;

    for (;;) {
        newValue = oldValue = currentValue;

        if (currentValue & ~fullMask)
            throw ML::Exception("non-full bit set in bitmap");

        // TODO: this doesn't need to be atomic...
        bool wasSet = ML::atomic_test_and_clear(newValue, bitNum);

        if (!wasSet) return TBM_FAILED;  // already was reset
        
        if (ML::cmp_xchg(bitmap, currentValue, newValue)) break;
    }

    return oldValue == fullMask ? TBM_RECURSE : TBM_SUCCESS;
}

/** Try to set a bit in a bitmap.

    Return value:
    TBM_FAILED:  the bit was already set
    TBM_SUCCESS: the bit was modified from false to true but the bitmap
                 did not move from not full to full as a result
    TBM_RECURSE: the bit was modified from false to true and the bitmap
                 moved from not full to full as a result; a higher level
                 bitmap probably needs to be modified.
*/
template<typename T>
TryBitmapResult
tryMarkBitmapFull(T & bitmap, int bitNum, T fullMask)
{
    T currentValue = bitmap, newValue;

    for (;;) {
        newValue = currentValue;

        if (currentValue & ~fullMask)
            throw ML::Exception("non-full bit set in bitmap");

        // TODO: this doesn't need to be atomic...
        bool wasSet = ML::atomic_test_and_set(newValue, bitNum);

        // If it was set then we've succeeded
        if (wasSet) return TBM_FAILED;
        
        if (ML::cmp_xchg(bitmap, currentValue, newValue)) break;
    }

    return newValue == fullMask ? TBM_RECURSE : TBM_SUCCESS;
}

// Set a bit to empty, and return a bit that tells us whether or not the
// bitmap moved from full to not full.
template<typename T>
bool markBitmapFull(T & bitmap, int bitNum, T fullMask)
{
    TryBitmapResult result;
    while ((result = tryMarkBitmapFull(bitmap, bitNum, fullMask)) == TBM_FAILED);
    return result == TBM_RECURSE;
}

// Set a bit to empty, and return a bit that tells us whether or not the
// bitmap moved from full to not full.
template<typename T>
bool markBitmapNotFull(T & bitmap, int bitNum, T fullMask)
{
    TryBitmapResult result;
    while ((result = tryMarkBitmapNotFull(bitmap, bitNum, fullMask))
           == TBM_FAILED);
    return result == TBM_RECURSE;
}

/** A bitmap with one bit for each page saying whether or not the page
    is full.  Has space for 1024 entries and is entirely
    lock free and thread safe.
*/
template<uint32_t NumEntries>
struct FullBitmap 
{

    enum {
        minNumEntries = 1,
        maxNumEntries = 32*64,

        numChunks = (NumEntries-1)/64 +1, //ceil(NumEntris/64)
        hasIndex = numChunks > 1,
        leftoverEntries = NumEntries%64
    };
    BOOST_STATIC_ASSERT(hasIndex == 0 || hasIndex == 1);
    BOOST_STATIC_ASSERT(
            NumEntries >= minNumEntries &&
            NumEntries <= maxNumEntries);


    uint64_t entries[numChunks];
    // one bit saying whether each of the others is full
    uint32_t fullEntries_[hasIndex];


    std::string print () {
        using namespace std;

        stringstream ss;

        if (hasIndex)
            ss << "index=" << hex << fullEntries() << dec << ", ";

        ss << "entries={ " << hex;
        for (int i = 0; i < numChunks; ++i) {
            if (!hasIndex || !(fullEntries() & (1ULL << i)))
                ss << i << ":" << entries[i] << " ";
        }
        ss << dec << "}";

        return ss.str();
    }

    uint32_t& fullEntries()
    {
        ExcAssertEqual(hasIndex, 1);
        return fullEntries_[0];
    }
    const uint32_t& fullEntries() const
    {
        ExcAssertEqual(hasIndex, 1);
        return fullEntries_[0];
    }
    uint32_t fullEntriesMask() const
    {
        ExcAssertEqual(hasIndex, 1);
        return (1ULL << numChunks) -1;
    }


    uint64_t entriesMask(unsigned index) const
    {
        if (leftoverEntries > 0 && index == numChunks -1) {
            return (1ULL << leftoverEntries) -1;
        }
        else {
            return (uint64_t) -1;
        }
    }

    void init(bool value)
    {
        if (value) {
            for (unsigned i = 0;  i < numChunks;  ++i)
                entries[i] = entriesMask(i);
            if (hasIndex)
                fullEntries() = fullEntriesMask();
        }
        else {
            for (unsigned i = 0;  i < numChunks;  ++i)
                entries[i] = 0;
            if (hasIndex)
                fullEntries() = 0;
        }
    }

    int getNonFullEntry(int startAt = 0)
    {
        for (unsigned attempt = 0;  attempt < 10;  ++attempt) {

            int entry = 0;
            if (hasIndex) {
                entry = findClearBit<uint32_t>(
                        fullEntries(), fullEntriesMask(), startAt);
                if (entry == -1) return -1;  // completely full
            }

            int bitNumInEntry
                = findClearBit<uint64_t>(
                        entries[entry], entriesMask(entry), startAt);

            if (bitNumInEntry == -1) {
                if (hasIndex) continue;  // we had a race
                else return -1;
            }

            return entry << 6 | bitNumInEntry;
        }
        return -1;
    }

    std::pair<int, bool> allocate(int startAt = 0)
    {
        for (unsigned attempt = 0;  attempt < 5;  ++attempt) {
        
            int entry = 0;
            if (hasIndex) {
                // TODO: random
                entry =
                    ML::lowest_bit((~fullEntries()) & fullEntriesMask(), -1);

                // All full...
                if (entry == -1)
                    return std::make_pair(-1, false);
            }
            
            int bitNumInEntry;
            bool entryIsNowFull;

            boost::tie(bitNumInEntry, entryIsNowFull)
                = allocateClearBit<uint64_t>(
                        entries[entry],
                        entriesMask(entry),
                        startAt);

            if (bitNumInEntry == -1) {
                if (hasIndex) continue; // we had a race, try again.
                else return std::make_pair(-1, false);
            }

            bool allFull = entryIsNowFull;

            if (hasIndex && entryIsNowFull) {
                //using namespace std;
                //cerr << "entryIsNowFull" << endl;
                //cerr << "fullEntries = " << fullEntries << endl;
                //cerr << "entry = " << entry << endl;
                allFull = markBitmapFull<uint32_t>(
                        fullEntries(), entry, fullEntriesMask());
                //cerr << "fullEntries now = " << fullEntries << endl;
            }
            
            return std::make_pair(entry << 6 | bitNumInEntry, allFull);
        }

        return std::make_pair(-1, false);
    }
 
    /** Mark the entry empty.  Returns true if the bitmap was previously
        full but is now not-full (so there's a free entry).

    Possible race condition:

    Initial State
      => index=[01] entries=[[1111],[0000]]

    Thread A - markDeallocated() -> markBitmapNotFull(entries)
      => index=[01] entries=[[0111],[0000]]

    Thread A - falls asleep

    Thread B - markDeallocated() * 2
      => index=[01] entries=[[0101],[0000]] - index bit still set!!!
      => index=[01] entries=[[0100],[0000]] - index bit still set!!!

    Thread B - allocate() * 4
      => index=[01] entries=[[0100],[0001]]
      => index=[01] entries=[[0100],[0011]]
      => index=[01] entries=[[0100],[0111]]
      => index=[11] entries=[[0100],[1111]] - is marked as full but it isn't!!!

    Thread A - wakes up

    Thread A - markDeallocated() -> markBitmapNotFull(fullEntries())
      => index=[10] entries=[[0100],[1111]] - Back to normal

    The bad news is that there's not much we can do about this race condition.
    The good news is that it doesn't really matter.
    It doesn't matter because the places that curently uses FBM in mmap can
    tolerate a spurious return of isFull. There also won't be any issues with
    memory leaks because the FBM eventually becomes consistent.
    */
    bool markDeallocated(unsigned entryNum)
    {
        ExcAssertLess(entryNum,NumEntries);

        int entry = entryNum >> 6;
        int bitInEntry = entryNum & 63;

        if (leftoverEntries > 0 && entry == numChunks-1) {
            ExcAssertLess(bitInEntry, leftoverEntries);
        }
        
        //using namespace std;
        //cerr << "entry = " << entry << " bitInEntry = " << bitInEntry
        //     << " fullEntries = " << fullEntries << endl;

        if (!markBitmapNotFull<uint64_t>(
                        entries[entry], bitInEntry, entriesMask(entry)))
        {
            return false;
        }
        
        // This entry is no longer full... reflect that status in the
        // entry for the full bitmap.
        if  (hasIndex) {
            return markBitmapNotFull<uint32_t>(
                    fullEntries(), entry, fullEntriesMask());
        }
        else return true;
    }

    /** Mark the entry as allocated.  This will throw an exception if it
        fails (ie, it was already allocated).

        Returns true if the bitmap is now full.
    */
    bool markAllocated(unsigned entryNum)
    {
        ExcAssertLess(entryNum, NumEntries);

        int entry = entryNum >> 6;
        int bitInEntry = entryNum & 63;

        if (leftoverEntries > 0 && entry == numChunks-1) {
            ExcAssertLess(bitInEntry, leftoverEntries);
        }

        //if (entries[entry] & (1ULL << bitInEntry))
        //    throw ML::Exception("markAllocated: bit was already allocated");
        
        if (!markBitmapFull<uint64_t>(
                        entries[entry], bitInEntry, entriesMask(entry)))
        {
            return false;
        }
        
        // This entry is now full... reflect that status in the
        // entry for the full bitmap.
        if (hasIndex) {
            return markBitmapFull<uint32_t>(
                    fullEntries(), entry, fullEntriesMask());
        }
        else return true;
    }

    /** Returns true if the entry was allocated and false otherwise. 

        Note that even if this function returns true for a given entry, there
        is no guarantee that a subsequent call to markAllocated for the same
        entry will succeed.
    */
    bool isAllocated(unsigned entryNum)
    {
        unsigned entry = entryNum >> 6;
        unsigned bitInEntry = entryNum & 63;

        return entries[entry] & (1ULL << bitInEntry);
    }

    void clear()
    {
        memset(this, 0, sizeof(*this));
    }

    /** Estimate the number of full entries.  Note that concurrent changes may
        mean this estimate is out of date before it's even returned.
    */
    size_t numFull() const
    {
        size_t result = 0;
        for (unsigned i = 0; i < numChunks;  ++i)
            result += ML::num_bits_set(entries[i]);
        return result;
    }

    bool isFull(int entryNum) const
    {
        ExcAssertLess(entryNum, NumEntries);

        int entry = entryNum >> 6;
        int bitInEntry = entryNum & 63;

        if (leftoverEntries > 0 && entry == numChunks-1) {
            ExcAssertLess(bitInEntry, leftoverEntries);
        }

        return entries[entry] & (1ULL << bitInEntry);
    }
};

template<typename Int, Int MASK_T = (Int)-1>
struct HierarchicalBitmap {
    static const Int MASK = (Int)-1;

    typedef Int v2qi __attribute__((__vector_size__(2 * sizeof(Int))));

    // 64 allocated bits
    // one lock word (currently only one bit used but later it will
    // include information to allow us to recover from a process crashing)
    struct Data {
        Data()
            : bits(0), lock(0)
        {
        }

        Data(const Data & other)
        {
            __asm__ ("movdqa %[other], %[me]\n\t"
                     : [me] "=x" (all)
                     : [other] "m" (other.all)
                     : );
        }

        bool update(Data & oldData, const Data & newData)
        {
            return ML::cmp_xchg(*this, oldData, newData);
        }

        void operator = (const Data & other)
        {
            __asm__ ("movdqa %[other], %[me]\n\t"
                     : [me] "=x" (all)
                     : [other] "m" (other.all)
                     : );
        }
        
        union {
            struct {
                Int bits;
                Int lock;
            };
            v2qi all;
        };

    } JML_ALIGNED(16);
    
    Data data;

    int findClearBit(int startAt = 0)
    {
        return MMap::findClearBit(data.bits, MASK, startAt);
    }

    std::pair<int, bool>
    allocate(int startAt = 0)
    {
        Data current = data, newData;

        using std::make_pair;

        for (;;) {
            newData = current;
            
            if (current.lock) return make_pair(-1, false);

            // Try to get a free bit
            int bitNum = MMap::findClearBit<Int>(current.bits, MASK,
                                                 startAt);
            
            //cerr << "got bit " << bitNum << endl;

            // Full?  No can alloc...
            if (bitNum == -1)
                return std::make_pair(-1, false);

            ML::atomic_test_and_set<Int>(newData.bits, bitNum);

            if (newData.bits == MASK) {
                //cerr << "locking " << this << endl;
                newData.lock = 1;
            }
                
            if (data.update(current, newData))
                return make_pair(bitNum, newData.lock);
        }
    }

    bool deallocate(int bitNum)
    {
        return markNotFull(bitNum);
    }

    bool markNotFull(int bitNum)
    {
        Data current = data, newData;

        for (;;) {
            newData = current;
            
            // Spin until it's not locked any more
            if (current.lock) {
                current = data;
                continue;
            }

            // Clear the bit
            bool wasSet = ML::atomic_test_and_clear(newData.bits, bitNum);

            if (!wasSet)
                throw ML::Exception("markNotFull: bit wasn't set");

            if (current.bits == MASK)
                newData.lock = 1;
            
            if (data.update(current, newData))
                return newData.lock;
        }
    }

    bool markFull(int bitNum)
    {
        Data current = data, newData;

        for (;;) {
            newData = current;
            
            // Spin until it's not locked any more
            if (current.lock) {
                current = data;
                continue;
            }

            // Set the bit
            bool wasSet = ML::atomic_test_and_set(newData.bits, bitNum);

            if (wasSet)
                throw ML::Exception("markFull: bit was already set");

            if (newData.bits == MASK)
                newData.lock = 1;
            
            if (data.update(current, newData))
                return newData.lock;
        }
    }

    /** Called all the way up to finish the allocation. */
    void unlock()
    {
        if (!data.lock)
            throw ML::Exception("invalid current lock");

        data.lock = 0;
    }

    int numAllocated() const
    {
        return ML::num_bits_set(data.bits);
    }

    operator Int () const { return data.bits; }
} JML_ALIGNED(16);

struct LeafBitmap : public HierarchicalBitmap<uint64_t> {
} JML_ALIGNED(16);

#if 0
std::ostream & operator << (std::ostream & stream,
                            const LeafBitmap::Data & data)
{
    return stream << "(" << data.bits << "," << data.lock << ")" << endl;
}
#endif



} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__full_bitmap_h__ */
