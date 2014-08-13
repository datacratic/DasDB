/* node_page.h                                                     -*- C++ -*-
   Jeremy Barnes, 23 October 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Page that contains nodes.
*/

#ifndef __mmap__node_page_h__
#define __mmap__node_page_h__

#include "mmap_const.h"
#include "full_bitmap.h"
#include "jml/utils/exc_assert.h"

#include <boost/tuple/tuple.hpp>
#include <boost/static_assert.hpp>
#include <stdint.h>
#include <algorithm>


namespace Datacratic {
namespace MMap {

#if 0

// Orders:
//
// even = power of 2
// odd = 3 * power of 2
// 0 = 2 * 2^2 = 8 bytes
// 1 = 3 * 2^2 = 12 bytes
// 2 = 2 * 2^3 = 16 bytes
// 3 = 3 * 2^3 = 24 bytes
// 4 = 2 * 2^4 = 32 bytes
// 5 = 3 * 2^4 = 48 bytes
// 6 = 2 * 2^5 = 64 bytes
// 7 = 3 * 2^5 = 96 bytes
// 8 = 2 * 2^6 = 128 bytes
// 9 = 3 * 2^6 = 192 bytes


/** A 4096 byte page containing 128 or less nodes of 32 or more bytes
    (so that the bitmap can fit in one atomic settable 128 bit value).
*/
struct LargeNodePage {

    struct Metadata {
        uint32_t magic;     ///< Magic to show what page we are
        uint32_t order;     ///< Gives the size of the nodes on this page
        
        uint64_t full;      ///< Bitmap saying whether each one is full
    };

    struct Data {
    };
};

/** A 4096 byte page containing 128 to 1024 nodes of 24, 16, 12, 8, 6, 4,
    3 or 2 bytes.

    There are up to 2048 of them.
*/
struct SmallNodePage {
    struct Metadata {
        uint32_t magic;     ///< Magic to show what page we are
        uint32_t order;     ///< Gives the size of the nodes on this page
        
        uint64_t full1;
        uint64_t full2[];    ///< Bitmap saying whether each one is full
    };

};

#endif


/*
Adapts the number of available nodes within the page to the size of the node.
*/
template<uint32_t NodeSize> 
struct GenericNodePage 
{

    enum {
        minNodeSize = 8,
        maxNodeSize = 2048,
        numNodes = page_size / NodeSize,

        metadataSize = 8 + sizeof(FullBitmap<numNodes>), // magic + order = 8
        metadataNodes = (metadataSize-1)/NodeSize +1, // ceil(mdSize/NodeSize)
        metadataPadding = (metadataNodes * NodeSize) - metadataSize,

        magicNum = 0xDC0B8326,
    };
    BOOST_STATIC_ASSERT(NodeSize >= minNodeSize && NodeSize <= maxNodeSize);


    struct Metadata {
        uint32_t magic;     ///< Magic to show what page we are
        uint32_t order;     ///< Gives the size of the nodes on this page
        FullBitmap<numNodes> full;
        uint8_t padding[metadataPadding];
    };
    BOOST_STATIC_ASSERT(sizeof(Metadata) == metadataNodes * NodeSize);


    Metadata metadata;
    char data[numNodes-metadataNodes][NodeSize];


    void init() 
    {
        metadata.magic = magicNum;
        metadata.order = NodeSize;

        metadata.full.init(false);

        // first pages are full since it's the header blocks
        for (int i = 0; i < metadataNodes; ++i) {
            metadata.full.markAllocated(i);
        }
    }

    /** Allocate a new page.  Returns the offset of the page and a boolean that
        tells whether full information needs to be propagated.
     
        Returns (-1, false) if the allocation failed.
    */
    std::pair<int64_t, bool> allocate()
    {
        ExcAssertEqual(metadata.order, NodeSize);
        
        int entry;
        bool isNowFull;

        boost::tie(entry, isNowFull) = metadata.full.allocate();

        if (entry < 0) {
            return std::make_pair(-1, false);
        }
        else if (entry < metadataNodes) {
            throw ML::Exception(
                    "Attempted to allocate a header node (%d < %d)", 
                    entry, metadataNodes);
        }

        int64_t offset = entry * NodeSize;
        return std::make_pair(offset, isNowFull);
    }

    /** Marks the entry as allocated. Returns the offset of the page and a 
        boolean that tells whether full information needs to be propagated.
    */
    std::pair<int64_t, bool> markAllocated(unsigned entryNum)
    {
        bool isNowFull = metadata.full.markAllocated(entryNum);

        int64_t offset = entryNum * NodeSize;
        return std::make_pair(offset, isNowFull);
    }

    /** Returns true if the entry was allocated and false otherwise. 

        Note that even if this function returns true for a given entry, there
        is no guarantee that a subsequent call to markAllocated for the same
        entry will succeed.
    */
    bool isAllocated(unsigned entryNum)
    {
        return metadata.full.isAllocated(entryNum);
    }

    /** Returns the offset of a given entry. */
    int64_t getEntryOffset(unsigned entryNum) const
    { 
        return entryNum * NodeSize; 
    }

    /** Deallocate an allocated page.  Returns a boolean that tells whether
        full information needs to be propagated.
    */
    bool deallocate(uint64_t offset)
    {
        ExcAssertEqual(metadata.order, NodeSize);

        int entryNum = offset / NodeSize;

        ExcAssertEqual(entryNum * NodeSize, offset);
        ExcAssertGreaterEqual(entryNum, metadataNodes);
        ExcAssertLess(entryNum, numNodes);

        return metadata.full.markDeallocated(entryNum);
    }

};


} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__node_page_h__ */
