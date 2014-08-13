/* key_fragment.cc
   Jeremy Barnes, 9 September 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   
*/

#include <inttypes.h>

#include "debug.h"
#include "key_fragment.h"

#include "soa/gc/gc_lock.h"
#include "memory_allocator.h"
#include "memory_tracker.h"
#include "jml/arch/format.h"

using namespace std;


namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* KEY FRAGMENT REPR                                                         */
/*****************************************************************************/

std::string
KeyFragmentRepr::
print() const
{
    stringstream ss;
    ss << bits << ":";

    if (!isValid())
        ss << "Inv";
    else
        ss << data;

    return ss.str();
}

std::ostream&
operator << (std::ostream & stream,
             const KeyFragmentRepr & frag)
{
    return stream << frag.print();
}


/*****************************************************************************/
/* KEY FRAGMENT                                                              */
/*****************************************************************************/

/** Portion of the Keyfragment that can't be stored inline with KeyFragmentRepr
*/
struct KeyFragmentStorage
{
    int16_t refCount;
    uint8_t* data() { return &data_; }

private:
    uint8_t data_;
};

KeyFragment
KeyFragment::
loadRepr(
        const KeyFragmentRepr& repr,
        MemoryAllocator& area,
        GcLock::ThreadGcInfo* info)
{
    ExcAssert(repr.isValid());

    KeyFragment frag;
    frag.bits = repr.bits;

    if (repr.isInline()) {
        frag.key.push_back(repr.data);
        return frag;
    }

    if (kfMemoryCheck) kfMemoryTracker.checkRead(repr.data);

    frag.key.resize(ceilDiv(repr.bits, KeyFragment::bitsInWord));

    RegionPtr<KeyFragmentStorage> ptr(area.region(), repr.data);
    ExcCheckGreater(ptr->refCount, 0, "Accessing deallocated key fragment!");

    ML::Bit_Buffer<uint8_t> reader(ptr->data());
    ML::Bit_Writer<uint64_t> writer(&frag.key[0]);

    size_t bitsLeft = frag.bits;
    const size_t chunkSize = 8;

    for (; bitsLeft >= chunkSize; bitsLeft -= chunkSize)
        writer.rwrite(reader.rextract(chunkSize), chunkSize);

    if (bitsLeft > 0)
        writer.rwrite(reader.rextract(bitsLeft), bitsLeft);

    return frag;
}

KeyFragmentRepr
KeyFragment::
allocRepr(MemoryAllocator& area, GcLock::ThreadGcInfo* info) const
{
    KeyFragmentRepr repr;
    repr.bits = bits;

    ExcAssert(repr.isValid());

    if (repr.isInline()) {
        if (bits) {
            trim();
            repr.data = key[0];
        }
        return repr;
    }

    const size_t bytesToCopy = ceilDiv(bits, 8);
    repr.data = area.stringAlloc.allocate(bytesToCopy + sizeof(int16_t));

    if (kfMemoryCheck) kfMemoryTracker.trackAlloc(repr.data);

    RegionPtr<KeyFragmentStorage> ptr(area.region(), repr.data);
    ptr->refCount = 1;

    ML::Bit_Writer<uint8_t> writer(ptr->data());
    ML::Bit_Buffer<uint64_t> reader(&key[0]);
    reader.advance(startBit);

    size_t bitsLeft = bits;
    const size_t chunkSize = 8;

    for (; bitsLeft >= chunkSize; bitsLeft -= chunkSize)
        writer.rwrite(reader.rextract(chunkSize), chunkSize);

    if (bitsLeft > 0)
        writer.rwrite(reader.rextract(bitsLeft), bitsLeft);

    return repr;
}

void
KeyFragment::
deallocRepr(
        const KeyFragmentRepr& repr,
        MemoryAllocator& area,
        GcLock::ThreadGcInfo* info)
{
    if (!repr.isValid() || repr.isInline())
        return;

    RegionPtr<KeyFragmentStorage> ptr(area.region(), repr.data);

    int16_t count = __sync_add_and_fetch(&ptr->refCount, -1);
    ExcCheckGreaterEqual(count, 0, "Double deallocation of key fragment");

    if (count == 0) {
        if (kfMemoryCheck) kfMemoryTracker.trackDealloc(repr.data);
        area.stringAlloc.deallocate(repr.data);
    }
}

KeyFragmentRepr
KeyFragment::
copyRepr(
        const KeyFragmentRepr& repr,
        MemoryAllocator& area,
        GcLock::ThreadGcInfo* info)
{
    ExcAssert(repr.isValid());

    if (repr.isInline())
        return repr;

    RegionPtr<KeyFragmentStorage> ptr(area.region(), repr.data);

    int16_t count = __sync_fetch_and_add(&ptr->refCount, 1);

    ExcCheckGreater(count, 0, "Copying deallocated key fragment");
    ExcCheckNotEqual(count, numeric_limits<int16_t>::max(), "Out of integers");

    return repr;
}




std::string
KeyFragment::
print() const
{
    if (bits <= 64)
        return ML::format("%d:%llx", bits, getKey());

    if (bits % 8) {
        //  Enable to get the hex repr of the key (kinda spammy).
        // stringstream ss;
        // ss << bits << ":";
        // for (int i = 0; i < bits /8; ++i)
        //     ss << hex << ((unsigned) getBits(8, i*8)) << " ";
        // ss << hex << ((unsigned) getBits(bits % 8, bits / 8));
        // return ss.str();

        return ML::format("%d:<...>", bits);
    }

    stringstream ss;
    ss << bits << ":" << hex;

    // Interpret the key as a string
    for (int i = 0; i < bits / 8; ++i)
        ss << ((char)getBits(8, i*8));

    return ss.str();
}

std::ostream &
operator << (std::ostream & stream,
             const KeyFragment & frag)
{
    return stream << frag.print();
}

} // namespace MMap
} // namespace Datacratic
