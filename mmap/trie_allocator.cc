/* trie_allocator.cc                                              -*- C++ -*-
   Rémi Attab, 26 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Lock-free allocator for trie roots.
*/


#include "trie_allocator.h"
#include "node_page.h"
#include "mmap_trie.h"

#include "jml/arch/exception.h"
#include "jml/compiler/compiler.h"

#include <cstddef>


using namespace std;
using namespace ML;

namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* BLOCK STRUCT                                                              */
/*****************************************************************************/

/** Defined as macros in sys/sysmacros.h for backwards compatibility reasons.
    Should Probably use something else buuut... Laziness wins this round...
*/
#undef major
#undef minor

struct Version {
    union {
        struct {
            uint32_t major;
            uint32_t minor;
        };
        uint64_t data;
    };

    Version() {}
    Version(uint32_t major_, uint32_t minor_) :
        major(major_), minor(minor_)
    {}

    string print() {
        stringstream ss;
        ss << 'v' << major << '.' << minor;
        return ss.str();
    }
};


/** Blocks that holds information related to a trie with a given id */
struct TrieBlock {
    static const uint64_t MagicNum;
    static const Version CurVersion;

    Version version;
    uint64_t magic;

    struct {
        uint64_t type;         ///< Type of structure at root
        uint64_t root;         ///< Root of structure; opaque (depends on type)
    } JML_ALIGNED(16);

    // char name[16];          ///< Symbolic name of structure (TODO)
    // GcLock::Data gcData;    ///< GC data for structure for CS detection (TODO)

    void init(uint64_t type)
    {
        magic = MagicNum;
        version.data = CurVersion.data;
        type = type;
        root = 0x0ULL;
    }

    void trash()
    {
        magic = ~MagicNum;
        version.data = ~version.data;
        type = ~type;
        root = 0x0ULL;
    }

    void validate()
    {
        ExcCheckEqual(magic, MagicNum, "Corrupted trie block");
        ExcCheckEqual(version.major, CurVersion.major, "Invalid major version");
        ExcCheckEqual(version.minor, CurVersion.minor, "Invalid minor version");
    }

    string print()
    {
        stringstream ss;
        ss << "Trie: "
            << version.print()
            << ", type=" << type
            << ", root=" << root;
        return ss.str();
    }

} JML_ALIGNED(64);
static_assert(sizeof(TrieBlock) == 64, "TrieBlock must fit in 64 bytes");


const uint64_t TrieBlock::MagicNum = 0xF07111AA110A62A6ULL;
const Version TrieBlock::CurVersion = {1, 0};


/*****************************************************************************/
/* ROOT ALLOCATOR                                                            */
/*****************************************************************************/

static RegionPtr<GenericNodePage<64> >
rootPage(MemoryRegion& region, uint64_t offset)
{
    // We use a full cache line per root to avoid false-sharing.
    return RegionPtr<GenericNodePage<64> >(region, offset);
}

static RegionPtr<TrieBlock>
trieBlock(MemoryRegion& region, uint64_t offset)
{
    return RegionPtr<TrieBlock>(region, offset);
}

TrieAllocator::
TrieAllocator(MemoryRegion& region, bool init):
    bytesAllocated_(0), bytesDeallocated_(0),
    region_(region), offset_(StartOffset)
{
    if (init) {
        MMAP_PIN_REGION(region)
        {
            auto page = rootPage(region_, offset_);
            page->init();

            ExcAssert(page->isAllocated(0));
            for (unsigned i = 1; i <= ReservedBlocks; ++i)
                page->markAllocated(i);
        }
        MMAP_UNPIN_REGION;
    }
}

static void
checkId(unsigned id)
{
    ExcCheckLessEqual(id, TrieAllocator::MaxTrieId, "id out of bounds");
    ExcCheckGreaterEqual(id, TrieAllocator::MinTrieId, "id out of bounds");
}

static unsigned
trieIndex(unsigned id)
{
    checkId(id);
    return (TrieAllocator::HeadBlocks + TrieAllocator::ReservedBlocks + id) - 1;
}

uint64_t
TrieAllocator::
trieOffset(unsigned id)
{
    unsigned index = trieIndex(id);

    MMAP_PIN_REGION(region_)
    {
        auto page = rootPage(region_, offset_);
        ExcCheck(page->isAllocated(index), "Trie must be allocated first.");

        uint64_t blockOffset = offset_ + page->getEntryOffset(index);
        trieBlock(region_, blockOffset)->validate();

        return blockOffset + offsetof(TrieBlock, root);
    }
    MMAP_UNPIN_REGION;
}

bool
TrieAllocator::
isAllocated(unsigned id)
{
    unsigned index = trieIndex(id);

    MMAP_PIN_REGION(region_)
    {
        return rootPage(region_, offset_)->isAllocated(index);
    }
    MMAP_UNPIN_REGION;
}

void
TrieAllocator::
allocate(unsigned id)
{
    unsigned index = trieIndex(id);

    MMAP_PIN_REGION(region_)
    {
        auto page = rootPage(region_, offset_);
        ExcCheck(!page->isAllocated(index), "Trie is already allocated.");

        int64_t rootOffset;
        bool isFull;
        tie(rootOffset, isFull) = page->markAllocated(index);
        ExcCheckGreaterEqual(rootOffset, 0, "allocation failed.");

        trieBlock(region_, offset_ + rootOffset)->init(0);
    }
    MMAP_UNPIN_REGION;

    region_.allocateGcLock(id);

    ML::atomic_add(bytesAllocated_, sizeof(TrieBlock));
}

void
TrieAllocator::
deallocate(unsigned id)
{
    unsigned index = trieIndex(id);

    MMAP_PIN_REGION(region_)
    {
        auto page = rootPage(region_, offset_);
        ExcCheck(page->isAllocated(index), "Double free!");

        uint64_t blockOffset = offset_ + page->getEntryOffset(index);
        trieBlock(region_, blockOffset)->trash();

        (void)page->deallocate(blockOffset);
    }
    MMAP_UNPIN_REGION;

    region_.gcLock(id)->deferBarrier();
    region_.unlinkGcLock(id);

    ML::atomic_add(bytesDeallocated_, sizeof(TrieBlock));
}


void
TrieAllocator::
dumpAllocatedTries(ostream& stream) const
{
    MMAP_PIN_REGION(region_)
    {
        auto page = rootPage(region_, offset_);

        stream << "Trie roots: ";

        if (page->metadata.full.numFull() == 0) {
            stream << "EMPTY" << endl;
            return;
        }

        for (unsigned id = MinTrieId; id <= MaxTrieId; ++id) {
            if (!page->isAllocated(id)) continue;

            unsigned index = trieIndex(id);
            uint64_t blockOffset = offset_ + page->getEntryOffset(index);

            stream << "\n    " << id << ": "
                << trieBlock(region_, blockOffset)->print();
        }
        stream << endl;
    }
    MMAP_UNPIN_REGION;
}



} // namespace MMap
} // namespace Datacratic
