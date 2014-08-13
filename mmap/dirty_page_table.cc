/** dirty_page_table.cc                                 -*- C++ -*-
    Rémi Attab, 29 Aug 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Experimental implementation of the dirty page table.

*/

#include "dirty_page_table.h"
#include "jml/arch/cmp_xchg.h"
#include "jml/arch/bitops.h"
#include "jml/utils/exc_check.h"
#include "jml/utils/exc_assert.h"

#include <iostream>
#include <sstream>
#include <algorithm>
#include <memory>
#include <cstring>

using namespace std;
using namespace ML;

namespace Datacratic {
namespace MMap {


/******************************************************************************/
/* PAGE GROUP                                                                 */
/******************************************************************************/

/** Bitset that can be manipulated atomically.

    Might be useful to turn this into a generic library.
    Does about 512 pages per cache line which is equivalent to 2Mb of memory.
 */
struct DirtyPageGroup
{
    uint64_t pages;
    vector<uint64_t> table;

    DirtyPageGroup(uint64_t pages) :
        pages(pages),
        table(pages / 64, 0ULL)
    {}

    void markPage(uint64_t page)
    {
        ExcAssertLess(page, pages);

        uint64_t index = page / 64;
        uint64_t bit = 1ULL << (page % 64);

        uint64_t oldWord = table[index];
        uint64_t newWord;

        do {
            if (oldWord & bit) return;
            newWord = oldWord | bit;
        } while (!cmp_xchg(table[index], oldWord, newWord));
    }

    int64_t nextPage(uint64_t start)
    {
        uint64_t index = start / 64;
        uint64_t mask = ~((1ULL << (start % 64)) - 1);

        for (; index < pages / 64; ++index) {
            uint64_t word = table[index] & mask;

            if (word) {
                uint64_t bit = ML::lowest_bit(word);
                return index * 64 + bit;
            }
            mask = -1;
        }

        return -1;
    }

    bool clearPage(uint64_t page)
    {
        ExcAssertLess(page, pages);

        uint64_t index = page / 64;
        uint64_t bit = 1ULL << (page % 64);

        uint64_t oldWord = table[index];
        uint64_t newWord;

        do {
            if (!(oldWord & bit)) return false;
            newWord = oldWord & ~bit;
        } while (!cmp_xchg(table[index], oldWord, newWord));

        return true;
    }

    string print() {
        stringstream ss;
        ss << "{ ";

        for (uint64_t page = 0; page < pages; ++page) {
            uint64_t index = page / 64;
            uint64_t bit = 1ULL << (page % 64);

            if (table[index] & bit)
                ss << page << " ";
        }

        ss << "}";
        return ss.str();
    }
};


/******************************************************************************/
/* DIRTY PAGE TABLE                                                           */
/******************************************************************************/
/** Details for the data structure:

    The whole thing is based around a given offset which is parsed like so:

    uint64_t offset:

    Highest Bit                               MinGroupBits
    +---------------------------------------------------------------+
    |  ...0001|                                | 9 bits | 12 bits   |
    +---------------------------------------------------------------+
              |->            subindex                 <-| Page Offset

    We don't need the the least-significant 12 bits because we're only only
    dealing with pages.

    We when find the position of the most significant bit in the offset which
    then becomes our group number. All positions within MinGroup are assigned to
    group 0. So the groups can be between 0 and 43 but that would assume we can
    index the whole 64 bits. Realistically, only about 20 groups are ever going
    to be used which is why we have a constant cap of 20 memory allocations.

    MinGroupBits is set so that group 0 uses an entire cache line. This
    corresponds to 512 pages (about 2Mb worth of memory).

    If we mask out the group bit and shift out the page offset bit, what we're
    left with is the index of the page within the DirtyPageGroup bitfield. This
    means that the size of each group is a function of the position of each bis.

    The whole point of this is to have a constant cap on memory allocations
    while still having a page table to grows with the mmap. Otherwise we could
    just allocate a gigantic 1Gb bit-field but that would be no fun :)

    The rest is fairly easy to figure out.
 */

DirtyPageTable::
DirtyPageTable(size_t initialSize)
{
    fill(groups.begin(), groups.end(), (DirtyPageGroup*)0);

    int maxGroup;
    uint64_t subindex;
    tie(maxGroup, subindex) = addrToIndex(initialSize);

    for (int group = 0; group <= maxGroup; ++group)
        getGroup(group);
}


DirtyPageTable::
~DirtyPageTable()
{
    for (size_t i = 0; i < GroupCount; ++i) {
        if (!groups[i]) continue;
        delete groups[i];
    }
}

void
DirtyPageTable::
markPages(uint64_t start, uint64_t length)
{
    for (uint64_t end = start + length; start < end; start += page_size)
        markPage(start);
}

void
DirtyPageTable::
markPage(uint64_t addr)
{
    int group;
    uint64_t subindex;
    tie(group, subindex) = addrToIndex(addr);

    return getGroup(group)->markPage(subindex);
}


int64_t
DirtyPageTable::
nextPage(uint64_t start)
{
    int group;
    uint64_t subindex;
    tie(group, subindex) = addrToIndex(start);

    for (; group < GroupCount; ++group) {
        if (!groups[group]) continue;

        int64_t page = groups[group]->nextPage(subindex);
        if (page >= 0) return indexToAddr(group, page);

        subindex = 0ULL;
    }

    return -1;
}


bool
DirtyPageTable::
clearPage(uint64_t addr)
{
    int group;
    uint64_t subindex;
    tie(group, subindex) = addrToIndex(addr);

    if (!groups[group]) return false;
    return groups[group]->clearPage(subindex);
}


std::pair<int, uint64_t>
DirtyPageTable::
addrToIndex(uint64_t addr) const
{
    uint64_t page = addr >> page_shift;

    int group = ML::highest_bit(page) - MinGroupBits + 1;
    if (group <= 0) return make_pair(0, page);

    uint64_t subindex = page & ((1ULL << (group + MinGroupBits - 1)) - 1);
    return make_pair(group, subindex);
}

uint64_t
DirtyPageTable::
indexToAddr(int group, uint64_t subindex) const
{
    uint64_t head = !group ? 0ULL : 1ULL << (group + MinGroupBits - 1);
    return (head | subindex) << page_shift;
}

DirtyPageGroup*
DirtyPageTable::
getGroup(int group)
{
    ExcAssertLess(group, GroupCount);
    if (groups[group]) return groups[group];

    uint64_t groupSize = (1ULL << group) << 16;
    unique_ptr<DirtyPageGroup> newGroup(new DirtyPageGroup(groupSize));

    // We need properly typed l-values for the cmp_xchg call.
    DirtyPageGroup* newGroupTmp = newGroup.get();
    DirtyPageGroup* oldGroup = nullptr;

    if (cmp_xchg(groups[group], oldGroup, newGroupTmp))
        newGroup.release();

    return groups[group];
}

string
DirtyPageTable::
print() const
{
    stringstream ss;
    ss << "PT< ";

    for (size_t i = 0; i < GroupCount; ++i) {
        if (!groups[i]) continue;
        ss << i << ":" << groups[i]->print() << " ";
    }

    ss << ">";
    return ss.str();
}

} // namespace MMap
} // namepsace Datacratic
