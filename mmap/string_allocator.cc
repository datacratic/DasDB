/* string_allocator.h                                              -*- C++ -*-
   Rémi Attab, 26 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   String allocator that uses a trie for it's free list.
*/

#include "mmap_const.h"
#include "string_allocator.h"
#include "trie_allocator.h"
#include "page_table.h"
#include "page_table_allocator.h"
#include "node_allocator.h"
#include "node_page.h"
#include "mmap_trie.h"
#include "sync_stream.h"

using namespace std;
using namespace ML;

namespace Datacratic {
namespace MMap {

/*****************************************************************************/
/* HELPERS                                                                   */
/*****************************************************************************/


/* Keep the smallest order that can accomodate a given size. */
static inline int
orderForSize(uint64_t size)
{
    int order = 1;
    size /= page_size;

    while (size) {
        size /= 1024;
        order++;
    }

    ExcAssertLessEqual(order, 5);
    return order;
}


/* Get the page size for an order. */
static inline uint64_t
sizeForOrder(int order)
{
    return page_size << (10 * (order-1));
}


/* Get the size required to fit the string and it's metadata. */
static inline pair<uint64_t, int>
adjustStringSize(uint64_t size)
{
    uint64_t adjSize = size + 8; // 8 bytes to keep the string size.

    int sentinelBytes = adjSize & 1 ? 1 : 2;
    adjSize += sentinelBytes;

    return make_pair(adjSize, sentinelBytes);
}


/*
Before returning to the user, setup the string metadata and adjust the
offset.
*/
static uint64_t
packString(
        MemoryRegion& region,
        uint64_t offset,
        uint64_t size,
        int sentinelBytes)
{
    // Shove the size before the address so we don't have to provide it when
    // deallocating (it's safer too).
    RegionPtr<uint64_t> sizePtr(region, offset);
    *sizePtr = size;

    offset += sizeof(uint64_t);

    // Add the sentinel bytes for overflow detection.
    // They also act as padding to avoid weird sizes.
    uint64_t sentinelOffset = offset + size;
    RegionPtr<char> sentinelPtr(region, sentinelOffset, sentinelBytes);

    for (int i = 0; i < sentinelBytes; ++i) {
        sentinelPtr[i] = 0x5A;
    }

    return offset;
}


/* Extract the string's metadata and adjust the offset. */
static pair<uint64_t, uint64_t>
unpackString(MemoryRegion& region, uint64_t offset, uint64_t expectedSize)
{
    offset -= sizeof(uint64_t);
    uint64_t size = *RegionPtr<uint64_t>(region, offset);

    if (expectedSize)
        ExcAssertEqual(size, expectedSize);

    uint64_t adjSize;
    int sentinelBytes;
    tie(adjSize, sentinelBytes) = adjustStringSize(size);

    uint64_t sentinelOffset = offset + adjSize - sentinelBytes;
    RegionPtr<char> sentinelPtr(region, sentinelOffset, sentinelBytes);

    for (int i = 0; i < sentinelBytes; ++i) {
        ExcCheckEqual(sentinelPtr[i], 0x5A, "Overflow detected");
    }

    return make_pair(offset, adjSize);
}


/*****************************************************************************/
/* FREE VALUE                                                                */
/*****************************************************************************/

/** Represents the size of a free block along with whether it's a page marker
    or not.
*/
struct FreeValue
{
    FreeValue() : order(0), size(0)  {}
    FreeValue(uint64_t size) : order(0), size(size) {}
    FreeValue(uint64_t size, unsigned order) : order(order), size(size) {}

    static FreeValue fromBits(uint64_t bits)
    {
        FreeValue value;
        value.bits = bits;
        return value;
    }

    union {
        struct {
            unsigned order:3;
            uint64_t size:61;
        };
        uint64_t bits;
    };

    bool isPageMarker() const { return order != 0; }

    string print() {
        stringstream ss;
        ss << "{ ";
        ss << "bits=" << bits;
        ss << ", size=" << size;
        if (isPageMarker()) ss << ", order=" << order;
        ss << " }";
        return ss.str();
    }
};


/*****************************************************************************/
/* STRING ALLOCATOR                                                          */
/*****************************************************************************/

StringAllocator::
StringAllocator(
        PageTableAllocator & pageAlloc,
        NodeAllocator & nodeAlloc,
        Trie freeList) :
    bytesAllocated_(0), bytesDeallocated_(0), bytesPrivate_(0),
    pageAlloc_(pageAlloc),
    nodeAlloc_(nodeAlloc),
    region_(pageAlloc.memoryRegion()),
    freeList_(make_shared<Trie>(freeList)) // a bit silly but required.
{
}

uint64_t
StringAllocator::
stringSize(uint64_t offset)
{
    return *RegionPtr<uint64_t>(region_, offset - sizeof(uint64_t));
}


/*
\todo Optimize for allocations not deallocations!
Current problem is that we're indexing by offsets which is good to have when
we're freeing space. For allocating, we need to index by size. Problem
is that the trie can't hold multiple values for a key which makes indexing by
size problematic.
*/
uint64_t
StringAllocator::
allocate(uint64_t size, int64_t hint) {
    uint64_t adjSize;
    int sentinelBytes;
    tie(adjSize, sentinelBytes) = adjustStringSize(size);

    // If the string is small, just use the node allocator.
    if (adjSize < NodeAllocator::MaxNodeSize) {
        uint64_t offset = nodeAlloc_.allocate(adjSize, hint);
        return packString(region_, offset, size, sentinelBytes);
    }

    // sync_cerr()
    //     << "\tallocate("
    //         << "size=" << size
    //         << ") : {"
    //         << "adjSize=" << adjSize
    //         << ", sentinel=" << sentinelBytes
    //         << "}" << endl << sync_dump;

    // Try to find a big enough entry in the free list.
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        auto current = freeList_->current();

        TrieIterator begin, end;
        tie(begin, end) = current.beginEnd();

        TrieIterator firstFit = end;
        TrieIterator it = end;

        // Scan the free list for a first fit entry.
        // Starting from the back has a better chance of finding newer pages.
        // \todo replace by a call to lowerBound.
        while (it != begin) {
            --it;

            uint64_t freeSize = FreeValue::fromBits(it.value()).size;
            if (freeSize < adjSize) continue;

            firstFit = it;
            break;
        }

        if (firstFit == end) break;

        uint64_t freeOffset = firstFit.key().cast<uint64_t>();
        FreeValue freeValue = FreeValue::fromBits(firstFit.value());

        do {
            // We alocate from the back because we can't easily change the
            //   offset (it's the key to the trie).
            FreeValue newValue(freeValue);
            newValue.size -= adjSize;

            bool keyFound;
            FreeValue oldValue;
            tie(keyFound, oldValue.bits) = current.compareAndSwap(
                    freeOffset, freeValue.bits, newValue.bits);

            // The block might have been merged with another block. Retry.
            if (!keyFound) break;

            // If the CAS worked, then we have our block.
            if (oldValue.bits == freeValue.bits) {
                // sync_cerr()
                //     << "\tswap("
                //         << "offset=" << freeOffset
                //         << ", old=" << freeValue.print()
                //         << ", new=" << newValue.print()
                //         << ") -> "
                //         << "offset=" << (freeOffset + newValue.size)
                //         << ", adjSize=" << adjSize
                //         << endl << sync_dump;

                ML::atomic_add(bytesAllocated_, adjSize);
                ML::atomic_add(bytesPrivate_, -adjSize);

                return packString(
                        region_,
                        freeOffset + newValue.size,
                        size,
                        sentinelBytes);
            }

            // Someone beat us to the replacement,
            // try again in case there's still enough space left-over.
            freeValue = oldValue;
        } while (freeValue.size >= adjSize);
    }

    // No space available, create a new free list entry.
    int order = orderForSize(adjSize);

    FreeValue freeValue (sizeForOrder(order), order);
    uint64_t freeOffset = pageAlloc_.allocatePage(order).offset;

    freeValue.size -= adjSize;

    auto current = freeList_->current();

    TrieIterator it;
    bool inserted;
    tie(it, inserted) = current.insert(freeOffset, freeValue.bits);

    ExcAssertEqual(it.value(), freeValue.bits);
    ExcAssert(inserted);

    // sync_cerr()
    //     << "\tnew("
    //         << "order=" << order
    //         << ", freeOffset=" << freeOffset
    //         << ", freeValue=" << freeValue.print()
    //         << ") -> "
    //         << "offset=" << (freeOffset + freeValue.size)
    //         << ", adjSize=" << adjSize
    //         << endl << sync_dump;

    ML::atomic_add(bytesAllocated_, adjSize);
    ML::atomic_add(bytesPrivate_, freeValue.size);
    return packString(
            region_, freeOffset + freeValue.size, size, sentinelBytes);
}

/**
Note that this algo is not optimal because when we're sweeping we're only seeing
one version of the trie. So while we're sweeping this one version, other free
blocks might be added around us and we won't be able to see them. Even after we
add our block, it's possible that the processes that are acting around us won't
see our addition for the same reason.

What this means is that, without doing multiple sweeps, a page that is fully
freed might not be merged into a single free block and therefor won't be
deallocated.

Note that all this sweeping could be avoided by adding a upper_bound and
lower_bound op to the trie. We could also more easily recover from invalidated
iterators.
*/
void
StringAllocator::
deallocate(uint64_t offset, uint64_t expectedSize)
{
    uint64_t adjSize;
    tie(offset, adjSize) = unpackString(region_, offset, expectedSize);

    if (adjSize < NodeAllocator::MaxNodeSize) {
        nodeAlloc_.deallocate(offset, adjSize);
        return;
    }

    // sync_cerr()
    //     << "\tdeallocate("
    //         << "offset=" << offset
    //         << ", expected=" << expectedSize
    //         << ") -> "
    //         << "adjSize=" << adjSize
    //         << endl << sync_dump;

    auto current = freeList_->current();

    uint64_t newSize = adjSize;


    // --- Sweep right ---
    // Check whether there are free blocks on our right that we can merge with.

    auto it = current.find(offset + adjSize);
    auto end = current.end();

    while (it != end) {
        // Is the next block adjacent to our block?
        uint64_t nextOffset = offset + newSize;
        if (it.key().cast<uint64_t>() != nextOffset) break;

        // Don't try and merge beyond our page.
        FreeValue freeValue = FreeValue::fromBits(it.value());
        if (freeValue.isPageMarker()) break;

        // remove is safe because even if someone modified the value via a
        // CAS, the offset is still the same so we're still ok.
        bool keyFound;
        tie(keyFound, freeValue.bits) = current.remove(nextOffset);

        // If someone beat us to the block then we're done.
        if (!keyFound) break;

        newSize += freeValue.size;

        // sync_cerr()
        //     << "\tmerge-right("
        //         << "nextOffset=" << nextOffset
        //         << ") -> "
        //         << "freeValue=" << freeValue.print()
        //         << ", newSize=" << newSize
        //         << endl << sync_dump;

        it++;
    }

    // --- Sweep left ---
    // Check whether there's a free entry on our left that we can merge with.

    uint64_t newOffset = offset;

    auto begin = current.begin();
    it = current.end();

    uint64_t pageOffset = 0;
    FreeValue pageValue;

    // \todo Replace by lower bounds and a backwards scan from there.
    while (it != begin) {
        --it;

        uint64_t freeOffset = it.key().cast<uint64_t>();
        FreeValue freeValue = FreeValue::fromBits(it.value());

        // ignore irrelevant blocks.
        if (freeOffset > newOffset) continue;

        // Stop if the block isn't adjacent to our block.
        if (freeOffset + freeValue.size != newOffset) break;

        // We'll need to know about the page marker later.
        if (freeValue.isPageMarker()) {
            pageOffset = freeOffset;
            pageValue = freeValue;
        }

        // We can merge; remove it so that nobody else can mess with it.
        bool keyFound;
        FreeValue oldValue;
        tie(keyFound, oldValue.bits) =
            current.compareAndRemove(freeOffset, freeValue.bits);

        // Someone beat us to it, so just stop.
        if (!keyFound || freeValue.bits != oldValue.bits)
            break;

        newOffset -= freeValue.size;
        newSize += freeValue.size;

        // sync_cerr()
        //     << "\tmerge-left("
        //         << "freeOffset=" << freeOffset
        //         << ", freeValue=" << freeValue.print()
        //         << ") -> "
        //         << "pageOffset=" << pageOffset
        //         << ", pageValue=" << pageValue.print()
        //         << ", newOffset=" << newOffset
        //         << ", newSize=" << newSize
        //         << endl << sync_dump;


        // Don't go past the page marker.
        if (pageOffset) break;

        // Iterators have been invalidated by compareAndRemove(). gotta stop.
        // \todo reset the iterators so we can continue.
        break;
    }

    // --- Make our modifications visible ---

    uint64_t pageOrderSize = sizeForOrder(pageValue.order);
    if (pageOffset && newSize == pageOrderSize) {
        ML::atomic_add(bytesPrivate_, -(pageOrderSize - adjSize));
        pageAlloc_.deallocatePage(Page(pageOffset, pageValue.order));

        // sync_cerr()
        //     << "\tdealloc-page("
        //         << "pageOffset=" << pageOffset
        //         << ", pageValue=" << pageValue.print()
        //         << ", pageOrderSize=" << pageOrderSize
        //         << ")" << endl << sync_dump;

    }
    else {
        FreeValue newValue(newSize);
        if (pageOffset)
            newValue = FreeValue(newSize, pageValue.order);

        bool inserted;
        tie(it, inserted) = current.insert(newOffset, newValue.bits);
        ExcAssert(inserted);

        FreeValue value;
        value.bits = it.value();

        // sync_cerr()
        //     << "\tinsert-fb("
        //         << "newOffset=" << newOffset
        //         << ", newValue=" << newValue.print()
        //         << ", newSize=" << newSize
        //         << ")" << endl << sync_dump;

        ML::atomic_add(bytesPrivate_, adjSize);
    }

    ML::atomic_add(bytesDeallocated_, adjSize);
}

void
StringAllocator::
dumpFreeList(std::ostream& stream)
{
    auto current = freeList_->current();
    current.dump(0, 0, stream);
}

} // namespace MMap
} // namespace Datacratic
