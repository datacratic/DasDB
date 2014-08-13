/* node_allocator.cc                                              -*- C++ -*-
   Rémi Attab, 26 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Lock-free allocator for fixed size nodes.
*/

#include "node_allocator.h"
#include "page_table.h"
#include "page_table_allocator.h"
#include "node_page.h"

#include <valgrind/valgrind.h>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/seq/for_each_i.hpp>


using namespace std;
using namespace ML;

namespace Datacratic {
namespace MMap {

/*****************************************************************************/
/* HELPERS PROTOTYPE                                                         */
/*****************************************************************************/
// These functions hide the nasty details related to the sizes of the nodes
// to allocate and free.

/*
Returns an ordinal associated with the size (8 = 0, 12 = 1, 16 = 2...)
This is useful to access the fullNodes array in PageTable or to calculate the
PT_ARENA_ARENA_XXB associated with a size.
*/
static int
getSizeOrdinal(uint64_t size);

static uint64_t
adjustSize (uint64_t size);

static pair<int64_t, bool>
allocateNodeForSize(
        PageTableAllocator& pageAlloc, uint64_t size, const Page& page);

static pair<int64_t, bool>
allocateAndInitNodeForSize(
        PageTableAllocator& pageAlloc, uint64_t size, const Page& page);

static bool
deallocateNodeForSize(
        PageTableAllocator& pageAlloc,
        uint64_t offset,
        uint64_t size,
        const Page& page);


/*****************************************************************************/
/* NODE ALLOCATOR                                                            */
/*****************************************************************************/

NodeAllocator::
NodeAllocator(PageTableAllocator & pageAlloc) :
    bytesAllocated_(0), bytesDeallocated_(0),
    pageAlloc_(pageAlloc), region_(pageAlloc.memoryRegion())
{
}

uint64_t
NodeAllocator::
allocate(uint64_t size, int64_t hint)
{
    std::function<uint64_t(uint64_t)> prepareBlock;

    if (!NodeAllocSentinels) {
        size = adjustSize(size);
        prepareBlock = [](uint64_t offset) -> uint64_t { return offset; };
    }
    else {
        uint64_t origSize = size;
        size = adjustSize(size * 3);

        prepareBlock = [&, origSize](uint64_t offset) -> uint64_t {
            RegionPtr<uint8_t> regionPtr(region_, offset);

            uint8_t* ptr = regionPtr.get();
            std::fill(ptr, ptr + origSize, 0xA5);

            ptr += origSize * 2;
            std::fill(ptr, ptr + origSize, 0x5A);

            return offset + origSize;
        };
    }

    const int sizeOrdinal = getSizeOrdinal(size);

    int startAt = 0;

    for (unsigned attempt = 0;  attempt < 3;  ++attempt) {
        Page foundPage(0, 5);

        // Walk the page tables looking for a free page
        for (int order = 4;  order > 0;  --order) {
            //cerr << "order = " << order << " foundPage = " << foundPage
            //     << endl;

            auto pt = pageAlloc_.getPageTableForPage(foundPage.subpage(0));

            int subpage = pt->fullNodes[sizeOrdinal].getNonFullEntry(startAt);

            //cerr << "  subpage = " << subpage << endl;

            if (subpage == -1) break;
            foundPage = foundPage.subpage(subpage);

            //cerr << "  foundPage = " << foundPage << endl;

            if (order == 1) {
                int64_t offset;
                bool needUpdate;

                boost::tie(offset, needUpdate) =
                    allocateNodeForSize(pageAlloc_, size, foundPage);

                if (offset == -1) break;

                //cerr << "allocating at offset " << offset << " in page "
                //     << foundPage << endl;

                Page page = foundPage;

                for (unsigned o = 1;  o < 5 && needUpdate;  ++o) {
                    ExcAssertEqual(o, page.order);
                    int index = PageTableAllocator::getPageTableIndex(page);
                    auto pt = pageAlloc_.getPageTableForPage(page);
                    needUpdate = pt->fullNodes[sizeOrdinal].markAllocated(index);
                    page = page.superpage();
                }

                ML::atomic_add(bytesAllocated_, size);

                //VALGRIND_MALLOCLIKE_BLOCK(pageAlloc_.memoryRegion().start()
                //                          + foundPage.offset + offset,
                //                          size, 0, 0);


                return prepareBlock(foundPage.offset + offset);
            }

            ExcAssertEqual(foundPage.order, order);
        }
    }

    // Failure to find a free block?  Allocate a new page to contain 64 byte
    // chunks
    Page allocated =
        pageAlloc_.allocatePageOfType(1, PT_ARENA_8B + sizeOrdinal);

    //cerr << "no subpage found; allocating a new one at "
    //     << allocated << endl;

    // TODO: don't lose it on failure...

    // Make sure that it stays pinned

    // Set up the metadata
    uint64_t offset =
        allocateAndInitNodeForSize(pageAlloc_, size, allocated).first;

    // Now set the bits to show the rest of the world that it's there
    Page page = allocated;
    bool needUpdate = true;

    for (unsigned o = 1;  o < 5 && needUpdate;  ++o) {
        ExcAssertEqual(o, page.order);
        int index = PageTableAllocator::getPageTableIndex(page);
        auto pt = pageAlloc_.getPageTableForPage(page);
        needUpdate = pt->fullNodes[sizeOrdinal].markDeallocated(index);
        page = page.superpage();
    }

    ML::atomic_add(bytesAllocated_, size);

    //VALGRIND_MALLOCLIKE_BLOCK(pageAlloc_.memoryRegion().start()
    //                          + allocated.offset + offset,
    //                          size, 0, 0);

    return prepareBlock(allocated.offset + offset);
}

void
NodeAllocator::
deallocate(uint64_t offset, uint64_t size)
{
    if (!NodeAllocSentinels) {
        size = adjustSize(size);
    }
    else {
        checkSentinels(offset, size);

        uint64_t origSize = size;
        size = adjustSize(size * 3);
        offset -= origSize;
    }

    const int sizeOrdinal = getSizeOrdinal(size);

    // Find the 4k page that contains it
    Page page = Page::containing(offset, 1);

    int64_t localOffset = (offset - page.offset);

    //cerr << "localOffset = " << localOffset << endl;

#if 1 // test
    char * cp = pageAlloc_.memoryRegion().start() + offset;
    std::fill(cp, cp + size, 255);
#endif // test

    bool needUpdate =
        deallocateNodeForSize(pageAlloc_, localOffset, size, page);

    // TODO: if the page is no longer full can we deallocate it...

    // TODO: copy and paste from above
    for (unsigned o = 1;  o < 5 && needUpdate;  ++o) {
        ExcAssertEqual(o, page.order);
        int index = PageTableAllocator::getPageTableIndex(page);
        auto pt = pageAlloc_.getPageTableForPage(page);
        needUpdate = pt->fullNodes[sizeOrdinal].markDeallocated(index);
        page = page.superpage();
    }

    //VALGRIND_FREELIKE_BLOCK(pageAlloc_.memoryRegion().start()
    //                        + offset, 0);

    ML::atomic_add(bytesDeallocated_, size);
}


void
NodeAllocator::
checkSentinels(uint64_t offset, uint64_t size)
{
    if (!NodeAllocSentinels) return;

    uint64_t origSize = size;
    size = adjustSize(size * 3);
    uint64_t origOffset = offset;
    offset -= origSize;

    RegionPtr<uint8_t> regionPtr(region_, offset);

    auto isMagicValueFront = [](uint8_t v) -> bool { return v == 0xA5; };
    auto isMagicValueBack  = [](uint8_t v) -> bool { return v == 0x5A; };

    uint8_t* ptr = regionPtr.get();
    bool front = std::all_of(ptr, ptr + origSize, isMagicValueFront);

    ptr += origSize * 2;
    bool back = std::all_of(ptr, ptr + origSize, isMagicValueBack);

    if (!front || !back) {
        cerr << "CHECK: " << hex
            << "origSize=" << origSize
            << ", size=" << size
            << ", offset=" << origOffset
            << ", adjOffset=" << offset
            << "\n\tfront: { ";
        for (uint8_t* pi = regionPtr.get(); pi < regionPtr.get() + origSize; ++pi)
            cerr << ((uint64_t) *pi) << " ";
        cerr << "}\n\tback: { ";
        for (uint8_t* pi = regionPtr.get() + origSize * 2;
             pi < regionPtr.get() + origSize * 3; ++pi)
            cerr << ((uint64_t) *pi) << " ";
        cerr << "}" << endl << endl;
        abort();
    }

    ExcCheck(front, "underflow detected");
    ExcCheck(back, "overflow detected");
}


uint64_t
NodeAllocator::
usedSize()
{
    return pageAlloc_.memoryRegion().length();
}




/*****************************************************************************/
/* Helpers                                                                   */
/*****************************************************************************/

template<uint64_t Size>
struct NodeAllocatorHelper
{
    static pair<int64_t, bool>
    allocateNode(PageTableAllocator& pageAlloc, const Page& page)
    {
        auto nodes = pageAlloc.mapPage<GenericNodePage<Size> >(page);
        return nodes->allocate();
    }

    static pair<int64_t, bool>
    allocateAndInitNode(PageTableAllocator& pageAlloc, const Page& page)
    {
        auto nodes = pageAlloc.mapPage<GenericNodePage<Size> >(page);
        nodes->init();
        return nodes->allocate();
    }

    static bool
    deallocateNode(
            PageTableAllocator& pageAlloc, int64_t offset, const Page& page)
    {
        auto nodes = pageAlloc.mapPage<GenericNodePage<Size> >(page);
        return nodes->deallocate(offset);
    }
};




/*
The following mess of unreadable code is used to map the run-time size to a
compile-time size used for the GenericNodePage template.

SIZES_SEQ is a boost::preproc sequence with the current sizes we support
(in bytes).
*/
#define SIZES_SEQ (8)(12)(16)(24)(32)(48)(64)(96)(128)(192)(256)


static int
getSizeOrdinal(uint64_t size)
{
    // _i_ here is the index in SIZES_SEQ.
    #define ORDINAL_CASE_MACRO(r, _, _i_, _size_)       \
        case _size_ : return _i_;

    switch (size) {
        BOOST_PP_SEQ_FOR_EACH_I(ORDINAL_CASE_MACRO, _, SIZES_SEQ)
    };
    throw ML::Exception("Invalid chunk size: %lld", (long long) size);

    #undef ORDINAL_CASE_MACRO
}


uint64_t
adjustSize(uint64_t size)
{
    #define CHECKSIZE_CASE_MACRO(r, _, _size_)  \
        if (size <= _size_) return _size_;

    BOOST_PP_SEQ_FOR_EACH(CHECKSIZE_CASE_MACRO, _, SIZES_SEQ)

    throw ML::Exception(
            "Invalid chunk size: 256 < size=%lld", (long long) size);

    #undef CHECKSIZE_CASE_MACRO
}


pair<int64_t, bool>
allocateNodeForSize(
        PageTableAllocator& pageAlloc, uint64_t size, const Page& page)
{
    #define ALLOC_CASE_MACRO(r, _, _size_)                              \
        case _size_ :                                                   \
            return NodeAllocatorHelper<_size_>::allocateNode(pageAlloc, page);

    switch (size) {
        BOOST_PP_SEQ_FOR_EACH(ALLOC_CASE_MACRO, _, SIZES_SEQ)
    };

    throw ML::Exception(
            "Can't allocate a chunk of size %lld.", (long long) size);

    #undef ALLOC_CASE_MACRO
}


pair<int64_t, bool>
allocateAndInitNodeForSize(
        PageTableAllocator& pageAlloc, uint64_t size, const Page& page)
{
    #define ALLOCI_CASE_MACRO(r, _, _size_)                             \
        case _size_ :                                                   \
            return NodeAllocatorHelper<_size_>::allocateAndInitNode(    \
                    pageAlloc, page);

    switch (size) {
        BOOST_PP_SEQ_FOR_EACH(ALLOCI_CASE_MACRO, _, SIZES_SEQ)
    };

    throw ML::Exception(
            "Can't allocate and init a chunk of size %lld.", (long long) size);

    #undef ALLOCI_CASE_MACRO
}


bool
deallocateNodeForSize(
        PageTableAllocator& pageAlloc,
        uint64_t offset,
        uint64_t size,
        const Page& page)
{
    #define DEALLOC_CASE_MACRO(r, _, _size_)                            \
        case _size_ :                                                   \
            return NodeAllocatorHelper<_size_>::deallocateNode(         \
                    pageAlloc, offset, page);

    switch (size) {
        BOOST_PP_SEQ_FOR_EACH(DEALLOC_CASE_MACRO, _, SIZES_SEQ)
    };

    throw ML::Exception(
            "Can't deallocate a chunk of size %lld.", (long long) size);

    #undef DEALLOC_CASE_MACRO
}



} // namespace MMap
} // namespace Datacratic
