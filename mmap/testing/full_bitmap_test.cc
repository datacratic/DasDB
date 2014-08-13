/* full_bitmap_test.cc
   Jeremy Barnes, 17 October 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the full bitmap.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "jml/utils/smart_ptr_utils.h"
#include "jml/utils/string_functions.h"
#include "jml/utils/pair_utils.h"
#include "jml/arch/atomic_ops.h"
#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/function.hpp>
#include <iostream>
#include "mmap/full_bitmap.h"
#include <set>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;

template<uint32_t N>
struct SeqFullBitmapTest {

    static void exec() {
        FullBitmap<N> bitmap;

        bitmap.init(true);
        BOOST_REQUIRE_EQUAL(bitmap.numFull(), N);

        bitmap.init(false);
        BOOST_REQUIRE_EQUAL(bitmap.numFull(), 0);

        set<int> allocated;

        for (unsigned i = 0;  i < N;  ++i) {

            int entryNum;
            bool full;
            boost::tie(entryNum, full) = bitmap.allocate();

            //cerr << "i = " << i << " entryNum = " << entryNum << " full = "
            //     << full << endl;

            BOOST_CHECK_NE(entryNum, -1);
            BOOST_CHECK_EQUAL(allocated.count(entryNum), 0);
            BOOST_CHECK(!full || i == N-1);
            BOOST_CHECK(full || i < N-1);

            BOOST_CHECK_EQUAL(bitmap.numFull(), i + 1);

            allocated.insert(entryNum);
        }

        // Test that we can mark things full
        for (unsigned i = 0;  i < N;  ++i) {
            //cerr << "i = " << i << endl;

            bool notFull = bitmap.markDeallocated(i);

            BOOST_CHECK(notFull || i > 0);
            BOOST_CHECK_EQUAL(bitmap.numFull(), N-1-i);
        }
    }
};

BOOST_AUTO_TEST_CASE( test_full_bitmap_seq )
{
    // These numbers come from the number of nodes contained in a 
    // GenericNodePage for a sample of our supported sizes.
    SeqFullBitmapTest<16>::exec();
    SeqFullBitmapTest<21>::exec();
    SeqFullBitmapTest<64>::exec();
    SeqFullBitmapTest<341>::exec();

    SeqFullBitmapTest<1024>::exec();
}


template<uint32_t NumEntries>
struct MTFullBitmapTest 
{
    
    static void exec() {
        uint64_t num_allocated = 0, num_deallocated = 0, num_alloc_failed = 0;
        uint64_t num_errors = 0;

        int nthreads = 4;
        int n = 10000;
        // The division by 2 here is VERY important.
        // See the FullBitmap::markDeallocated() doc for more details.
        int nallocs = (NumEntries/nthreads/2)-1;
    
        boost::barrier barrier(nthreads);
    
        int allocValues[NumEntries];
        std::fill(allocValues, allocValues + NumEntries, -1);

        FullBitmap<NumEntries> bitmap;
        bitmap.clear();

        BOOST_CHECK_EQUAL(bitmap.numFull(), 0);

        auto testThread = [&] (int threadNum)
            {
                //ThreadGuard threadGuard;

                for (unsigned i = 0;  i < n;  ++i) {
                    int myAlloc[nallocs];

                    for (unsigned j = 0;  j < nallocs;  ++j) {
                        bool full;
                        boost::tie(myAlloc[j], full)
                            = bitmap.allocate();
                        
                        if (full) {
                            cerr << "was marked as full\n";
                            ML::atomic_add(num_errors, 1);
                        }
                        if (myAlloc[j] == -1) {
                            ML::atomic_add(num_alloc_failed, 1);
                        }
                        else {
                            ML::atomic_add(num_allocated, 1);
                            if (allocValues[myAlloc[j]] != -1) {
                                cerr << "already alloc'd (not -1): "
                                    << allocValues[myAlloc[j]]
                                    << endl;
                            }
                            allocValues[myAlloc[j]] = threadNum;
                        }
                    }

                    for (unsigned j = 0;  j < nallocs;  ++j) {
                        if (myAlloc[j] == -1) continue;
                        if (allocValues[myAlloc[j]] != threadNum) {
                            cerr << "wrong thread number\n";
                            ML::atomic_add(num_errors, 1);
                        }
                    }

                    for (unsigned j = 0;  j < nallocs;  ++j) {
                        if (myAlloc[j] == -1) continue;

                        if (allocValues[myAlloc[j]] != threadNum) {
                            cerr << "wrong thread number\n";
                            ML::atomic_add(num_errors, 1);
                        }

                        allocValues[myAlloc[j]] = -1;

                        bool notFull = bitmap.markDeallocated(myAlloc[j]);

                        if (notFull) {
                            cerr << "was marked as no longer full\n";
                            ML::atomic_add(num_errors, 1);
                        }

                        ML::atomic_add(num_deallocated, 1);
                    }
                }
            };
    
        boost::thread_group tg;
        
        for (unsigned i = 0;  i < nthreads;  ++i)
            tg.create_thread(boost::bind<void>(testThread, i));
    
        tg.join_all();

        BOOST_CHECK_EQUAL(bitmap.numFull(), 0);
        BOOST_CHECK_EQUAL(num_allocated, num_deallocated);

        //BOOST_CHECK_EQUAL(num_alloc_failed, 0);
        BOOST_CHECK_EQUAL(num_errors, 0);

        //rcu_defer_barrier();
    }
};

BOOST_AUTO_TEST_CASE( test_full_bitmap_multithreaded )
{
    MTFullBitmapTest<42>::exec();
    MTFullBitmapTest<64>::exec();
    MTFullBitmapTest<341>::exec();
    MTFullBitmapTest<1024>::exec();
}





template<typename T>
bool
markBitmapNotFull2(T & bitmap, int bitNum, T fullMask)
{
    T oldBitmap = bitmap;
    bool oldValue = ML::atomic_test_and_clear(bitmap, bitNum);
    return oldBitmap == fullMask && oldValue;


    return oldBitmap != fullMask;
#if 0
    T currentValue = bitmap, newValue, oldValue;

    // 1.  We always toggle the bit
    for (;;) {
        newValue = oldValue = currentValue;

        if (currentValue & ~fullMask)
            throw ML::Exception("non-full bit set in bitmap");

        // TODO: this doesn't need to be atomic...
        ML::atomic_test_and_toggle(newValue, bitNum);
        
        if (ML::cmp_xchg(bitmap, currentValue, newValue)) break;
    }

    // 2.  If we moved from full to not full then we recurse
    return oldValue == fullMask;
#endif
}

template<typename T>
bool
markBitmapFull2(T & bitmap, int bitNum, T fullMask)
{
    bool oldValue = ML::atomic_test_and_set(bitmap, bitNum);
    return bitmap == fullMask && !oldValue;
#if 0
    T currentValue = bitmap, newValue, oldValue;

    for (;;) {
        newValue = oldValue = currentValue;

        if (currentValue & ~fullMask)
            throw ML::Exception("non-full bit set in bitmap");

        // TODO: this doesn't need to be atomic...
        ML::atomic_test_and_toggle(newValue, bitNum);

        //cerr << "currentValue = " << currentValue << " newValue = "
        //     << newValue << endl;

        if (ML::cmp_xchg(bitmap, currentValue, newValue)) break;
    }

    // If we moved from not full to full then we recurse
    return newValue == fullMask;
#endif
}

#if 0
struct LeafBitmap {
    static const uint64_t MASK = (uint64_t)-1;

    uint64_t bits;

    int findClearBit()
    {
        return MMap::findClearBit(data.bits, MASK);
    }

    std::pair<int, bool>
    allocate(uint32_t & lock)
    {
        uint64_t current = data, newData;

        for (;;) {
            newData = current;
            
            // Try to get a free bit
            int bitNum = MMap::findClearBit<uint64_t>(bits, MASK);
            
            //cerr << "got bit " << bitNum << endl;

            // Full?  No can alloc...
            if (bitNum == -1 || lock)
                return std::make_pair(-1, false);

            ML::atomic_test_and_set<uint64_t>(newData, bitNum);
            
            if (newData == MASK) {
                if (!lock()) return make_pair(-1, false);
            }
                
            if (ML::cmp_xchg(bits, current, newData))
                return make_pair(bitNum, newData.lock);
        }
    }

    bool deallocate(int bitNum)
    {
        return markNotFull(bitNum);
    }

    bool markNotFull(int bitNum)
    {
        uint64_t current = data, newData;

        for (;;) {
            newData = current;
            
            // Spin until it's not locked any more
            if (current.lock) {
                current = data;
                continue;
            }

            // Clear the bit
            bool wasSet = ML::atomic_test_and_clear(newData, bitNum);

            if (!wasSet)
                throw ML::Exception("markNotFull: bit wasn't set");

            if (bits == MASK)
                newData.lock = 1;
            
            if (data.update(current, newData))
                return newData.lock;
        }
    }

    bool markFull(int bitNum)
    {
        uint64_t current = data, newData;

        for (;;) {
            newData = current;
            
            // Spin until it's not locked any more
            if (current.lock) {
                current = data;
                continue;
            }

            // Set the bit
            bool wasSet = ML::atomic_test_and_set(newData, bitNum);

            if (wasSet)
                throw ML::Exception("markFull: bit was already set");

            if (newData == MASK)
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

    operator uint64_t () const { return bits; }

} JML_ALIGNED(16);

#endif

std::ostream & operator << (std::ostream & stream,
                            const LeafBitmap::Data & data)
{
    return stream << "(" << data.bits << "," << data.lock << ")" << endl;
}


BOOST_AUTO_TEST_CASE( test_leaf_bitmap )
{
    LeafBitmap::Data d1;
    BOOST_CHECK_EQUAL(d1.bits, 0); 
    BOOST_CHECK_EQUAL(d1.lock, 0);
   
    LeafBitmap::Data d2 = d1;
    BOOST_CHECK_EQUAL(d2.bits, 0); 
    BOOST_CHECK_EQUAL(d2.lock, 0);
    
    d1.bits = 12345;
    d1.lock = 56789;

    d2 = d1;
    BOOST_CHECK_EQUAL(d2.bits, 12345); 
    BOOST_CHECK_EQUAL(d2.lock, 56789);

    LeafBitmap::Data d3 = d1;
    BOOST_CHECK_EQUAL(d3.bits, 12345);
    BOOST_CHECK_EQUAL(d3.lock, 56789);

    LeafBitmap bmap;
    BOOST_CHECK_EQUAL(bmap.numAllocated(), 0);
    BOOST_CHECK_EQUAL(bmap, 0);
    BOOST_CHECK_EQUAL(bmap.allocate(), make_pair(0, false));
    BOOST_CHECK_EQUAL(bmap, 1);
    BOOST_CHECK_EQUAL(bmap.data.lock, 0);

    for (int i = 1;  i < 63;  ++i) {
        BOOST_CHECK_EQUAL(bmap.numAllocated(), i);
        BOOST_CHECK_EQUAL(bmap.allocate(), make_pair(i, false));
        BOOST_CHECK_EQUAL(bmap.data.lock, 0);
    }

    BOOST_CHECK_EQUAL(bmap.allocate(), make_pair(63, true));
    BOOST_CHECK_EQUAL(bmap.data.lock, 1);
}

struct Bitmap64 {
    static const uint64_t MASK = (uint64_t)-1;

    Bitmap64(uint64_t bits = 0)
        : bits(bits)
    {
        std::fill(extra, extra + 64, 0);
    }

    uint64_t bits;
    int extra[64];

    int findClearBit()
    {
        return MMap::findClearBit(bits, MASK, 0);
    }

    std::pair<int, bool>
    allocate()
    {
        return allocateClearBit(bits, MASK, 0);
    }

    static const int MARK_TRIES = 10;

    bool markFull(int bit, uint64_t & failed)
    {
        return markBitmapFull(bits, bit, MASK);

        TryBitmapResult result;
        for (unsigned i = 0;  i < MARK_TRIES;  ++i) {
            result = tryMarkBitmapFull(bits, bit, MASK);
            if (result != TBM_FAILED) return result == TBM_RECURSE;
        }

        return bits == MASK;

        // At this point, we tried to set the full bit but it was
        // already set.  This is because something came in at the
        // same time, deallocated a page which was then re-allocated,
        // and it got in front of us to set the full bit.
        //
        // If we keep on spinning, eventually we'll be able to get
        // in line.  However, what we can do is simply set a bit
        // saying that something else should spin for us and
        // be done with it.

        ++failed;
        
        if (extra[bit] > 0) {
            cerr << "full failed with negative extra" << endl;
        }

        int current = extra[bit];

        for (;;) {
            int newExtra;

            if (current == 0)
                newExtra = 1;
            else if (current == 1) {
                return false;  // already an exta full is pending
            }
            else if (current == -1) {
                newExtra = 0;
            }

            if (cmp_xchg(extra[bit], current, newExtra)) break;
        }

        return false;
        //
        //return markBitmapFull(bits, bit, MASK);
    }

    bool markNotFull(int bit, uint64_t & failed)
    {
        return markBitmapNotFull(bits, bit, MASK);

        TryBitmapResult result;
        for (unsigned i = 0;  i < MARK_TRIES;  ++i) {
            result = tryMarkBitmapNotFull(bits, bit, MASK);
            if (result != TBM_FAILED) return result == TBM_RECURSE;
        }

        return false;

        ++failed;

        //cerr << "not full failed; extra = " << (int)extra[bit] << endl;

        //atomic_dec(extra[bit]);


        return markBitmapNotFull(bits, bit, MASK);
    }

    operator uint64_t () const { return bits; }

} JML_ALIGNED(8);


BOOST_AUTO_TEST_CASE( test_full_bitmap_hierarchical_multithreaded_bare )
{
    uint64_t num_allocated = 0, num_deallocated = 0, num_alloc_failed = 0;
    uint64_t num_errors = 0;

    int nthreads = 8;
    int nallocs = 5000;

    volatile bool finished = false;
    
    boost::barrier barrier(nthreads);
    
    int allocValues[64 * 64 * 64];
    std::fill(allocValues, allocValues + 64*64*64, -1);

    uint64_t totalFailed = 0;

    LeafBitmap l1Entry;
    LeafBitmap l2[64];
    LeafBitmap l3[64 * 64];

    auto allocBit = [&] (uint64_t & failed) -> int
        {
            for (;;) {
                int l1Bit = l1Entry.findClearBit();
                if (l1Bit == -1) {
                    cerr << "invalid l1 bit" << endl;
                    ML::atomic_add(num_errors, 1);
                    return -2;
                }

                LeafBitmap & l2Entry = l2[l1Bit];
                int l2Bit = l2Entry.findClearBit();

                if (l2Bit == -1) continue;

                LeafBitmap & l3Entry = l3[l1Bit * 64 + l2Bit];

                int l3Bit;
                bool nowFull3 = false, nowFull2 = false, nowFull1 = false;
                boost::tie(l3Bit, nowFull3) = l3Entry.allocate();

                if (l3Bit == -1) continue;

                // Propagate fullness down...
                if (nowFull3) {
                    nowFull2 = l2Entry.markFull(l2Bit);
                    l3Entry.unlock();
                }
                if (nowFull2) {
                    nowFull1 = l1Entry.markFull(l1Bit);
                    l2Entry.unlock();
                }
                if (nowFull1)
                    throw ML::Exception("way tooo full");

                return (l1Bit << 12) | (l2Bit << 6) | l3Bit;
            }
        };

    auto deallocBit = [&] (int bit, uint64_t & failed)
        {
            int l1Bit = (bit >> 12) & 63;
            int l2Bit = (bit >> 6) & 63;
            int l3Bit = (bit >> 0) & 63;

            if (l1Bit < 0 || l1Bit > 63)
                throw ML::Exception("bad l1bit");
            if (l2Bit < 0 || l2Bit > 63)
                throw ML::Exception("bad l2bit");
            if (l3Bit < 0 || l3Bit > 63)
                throw ML::Exception("bad l2bit");

            LeafBitmap & l2Entry = l2[l1Bit];
            LeafBitmap & l3Entry = l3[l1Bit * 64 + l2Bit];

            bool noLongerFull = l3Entry.deallocate(l3Bit);
            if (noLongerFull) {
                noLongerFull = l2Entry.markNotFull(l2Bit);
                l3Entry.unlock();
            }
            if (noLongerFull) {
                noLongerFull = l1Entry.markNotFull(l1Bit);
                l2Entry.unlock();
            }
            if (noLongerFull)
                throw ML::Exception("too much no longer full");
        };

    auto testThread = [&] (int threadNum)
        {
            uint64_t numFailed = 0;

            while (!finished) {

                int myAlloc[nallocs];

                for (unsigned j = 0;  j < nallocs;  ++j) {

                    myAlloc[j] = allocBit(numFailed);

                    if (myAlloc[j] == -1) {
                        ML::atomic_add(num_alloc_failed, 1);
                    }
                    else if (myAlloc[j] == -2) {
                        ML::atomic_add(num_errors, 1);
                        cerr << "bombing out" << endl;
                        return;
                    }
                    else {
                        ML::atomic_add(num_allocated, 1);
                        if (allocValues[myAlloc[j]] != -1) {
                            cerr << "already alloc'd (not -1): "
                                 << allocValues[myAlloc[j]]
                                 << endl;
                        }
                        allocValues[myAlloc[j]] = threadNum;
                    }
                }

                for (unsigned j = 0;  j < nallocs;  ++j) {
                    if (myAlloc[j] == -1) continue;
                    if (allocValues[myAlloc[j]] != threadNum) {
                        cerr << "wrong thread number" << endl;
                        ML::atomic_add(num_errors, 1);
                    }
                }

                for (unsigned j = 0;  j < nallocs;  ++j) {
                    if (myAlloc[j] == -1) continue;

                    if (allocValues[myAlloc[j]] != threadNum) {
                        cerr << "wrong thread number" << endl;
                        ML::atomic_add(num_errors, 1);
                    }

                    allocValues[myAlloc[j]] = -1;

                    deallocBit(myAlloc[j], numFailed);

                    ML::atomic_add(num_deallocated, 1);
                }
            }

            ML::atomic_add(totalFailed, numFailed);
        };
    
    boost::thread_group tg;
        
    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.create_thread(boost::bind<void>(testThread, i));
    
    sleep(1);
    finished = true;

    tg.join_all();
    
    cerr << "num_allocated = " << num_allocated << " failures "
         << num_alloc_failed << endl;

    cerr << "num failed = " << totalFailed << endl;

    BOOST_CHECK_EQUAL(num_allocated, num_deallocated);

    //BOOST_CHECK_EQUAL(num_alloc_failed, 0);
    BOOST_CHECK_EQUAL(num_errors, 0);

    BOOST_CHECK_EQUAL(l1Entry, 0);
    for (unsigned i = 0;  i < 64;  ++i)
        BOOST_CHECK_EQUAL(l2[i], 0);
    for (unsigned i = 0;  i < 64;  ++i)
        for (unsigned j = 0;  j < 64;  ++j)
            BOOST_CHECK_EQUAL(l3[i * 64 + j], 0);
    
    //rcu_defer_barrier();
}

#if 0

BOOST_AUTO_TEST_CASE( test_full_bitmap_hierarchical_multithreaded )
{
    uint64_t num_allocated = 0, num_deallocated = 0, num_alloc_failed = 0;
    uint64_t num_errors = 0;

    int nthreads = 16;
    int nallocs = 5000;

    volatile bool finished = false;
    
    boost::barrier barrier(nthreads);
    
    int allocValues[1024 * 1024];
    std::fill(allocValues, allocValues + 1024 * 1024, -1);

    FullBitmap<1024> l1;
    l1.clear();

    FullBitmap<1024> l2[1024];
    for (unsigned i = 0;  i < 1024;  ++i)
        l2[i].clear();

    auto allocBit = [&] () -> int
        {
            for (;;) {
                int l1Bit = l1.getNonFullEntry();
                if (l1Bit == -1) {
                    cerr << "invalid l1 bit" << endl;
                    ML::atomic_add(num_errors, 1);
                    return -2;
                }

                FullBitmap<1024> & bm = l2[l1Bit];
                int l2Bit ;
                bool full;
                boost::tie(l2Bit, full) = bm.allocate();
                if (l2Bit == -1) continue;  // filled up
                
                if (full) {
                    // Now we need to mark the l1 bit full
                    l1.markAllocated(l1Bit);
                }
                return l1Bit << 10 | l2Bit;
            }
        };

    auto deallocBit = [&] (int bit)
        {
            int l1Bit = bit >> 10;
            int l2Bit = bit & 1023;

            if (l1Bit < 0 || l1Bit > 1023)
                throw ML::Exception("bad l1bit");
            if (l2Bit < 0 || l2Bit > 1023)
                throw ML::Exception("bad l2bit");

            FullBitmap<1024> & bm = l2[l1Bit];
            
            if (!bm.isFull(l2Bit))
                throw ML::Exception("allocated bit should be full");

            bool noLongerFull = bm.markDeallocated(l2Bit);

            if (noLongerFull)
                l1.markDeallocated(l1Bit);
        };

    auto testThread = [&] (int threadNum)
        {
            while (!finished) {

                int myAlloc[nallocs];

                for (unsigned j = 0;  j < nallocs;  ++j) {

                    myAlloc[j] = allocBit();

                    if (myAlloc[j] == -1) {
                        ML::atomic_add(num_alloc_failed, 1);
                    }
                    else if (myAlloc[j] == -2) {
                        ML::atomic_add(num_errors, 1);
                        cerr << "bombing out" << endl;
                        return;
                    }
                    else {
                        ML::atomic_add(num_allocated, 1);
                        if (allocValues[myAlloc[j]] != -1) {
                            cerr << "already alloc'd (not -1): "
                                 << allocValues[myAlloc[j]]
                                 << endl;
                        }
                        allocValues[myAlloc[j]] = threadNum;
                    }
                }

                for (unsigned j = 0;  j < nallocs;  ++j) {
                    if (myAlloc[j] == -1) continue;
                    if (allocValues[myAlloc[j]] != threadNum) {
                        cerr << "wrong thread number" << endl;
                        ML::atomic_add(num_errors, 1);
                    }
                }

                for (unsigned j = 0;  j < nallocs;  ++j) {
                    if (myAlloc[j] == -1) continue;

                    if (allocValues[myAlloc[j]] != threadNum) {
                        cerr << "wrong thread number" << endl;
                        ML::atomic_add(num_errors, 1);
                    }

                    allocValues[myAlloc[j]] = -1;

                    deallocBit(myAlloc[j]);

                    ML::atomic_add(num_deallocated, 1);
                }
            }
        };
    
    boost::thread_group tg;
        
    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.create_thread(boost::bind<void>(testThread, i));
    
    sleep(1);
    finished = true;

    tg.join_all();
    
    cerr << "num_allocated = " << num_allocated << " failures "
         << num_alloc_failed << endl;

    BOOST_CHECK_EQUAL(num_allocated, num_deallocated);

    //BOOST_CHECK_EQUAL(num_alloc_failed, 0);
    BOOST_CHECK_EQUAL(num_errors, 0);

    BOOST_CHECK_EQUAL(l1.numFull(), 0);
    for (unsigned i = 0;  i < 1024;  ++i) {
        BOOST_CHECK_EQUAL(l2[i].numFull(), 0);
    }
    
    //rcu_defer_barrier();
}
#endif
