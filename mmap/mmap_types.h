/** mmap_types.h                                 -*- C++ -*-
    Rémi Attab, 27 Jul 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    High level types that reside within the mmap.

    \todo THIS HAS NOT BEEN TESTED AND PROBABLY DOESN'T EVEN COMPILE!!!!

*/

#ifndef __mmap__mmap_utils_h__
#define __mmap__mmap_utils_h__


#include "mmap_file.h"

#include <string>
#include <algorithm>


namespace Datacratic {
namespace MMap {
namespace Types {



/******************************************************************************/
/* NODE ALLOCATOR                                                             */
/******************************************************************************/

template<size_t Size>
struct NodeAlloc
{
    static uint64_t alloc(MemoryAllocator& area)
    {
        MMAP_PIN_REGION(area.region())
        {
            return area.nodeAlloc.allocate(Size);
        }
        MMAP_UNPIN_REGION;
    }

    static void dealloc(MemoryAllocator& area, uint64_t offset)
    {
        MMAP_PIN_REGION(area.region())
        {
            area.nodeAlloc.deallocate(offset, Size);
        }
        MMAP_UNPIN_REGION;
    }
};


/******************************************************************************/
/* STRING ALLOCATOR                                                           */
/******************************************************************************/

struct StringAlloc
{
    static uint64_t alloc(MemoryAllocator& area, size_t size)
    {
        MMAP_PIN_REGION(area.region())
        {
            return area.stringAlloc.allocate(size);
        }
        MMAP_UNPIN_REGION;
    }

    static void dealloc(MemoryAllocator& area, uint64_t offset)
    {
        MMAP_PIN_REGION(area.region())
        {
            return area.stringAlloc.deallocate(offset);
        }
        MMAP_UNPIN_REGION;
    }

    static size_t size(MemoryAllocator& area, uint64_t offset)
    {
        MMAP_PIN_REGION(area.region())
        {
            return area.stringAlloc.stringSize(offset);
        }
        MMAP_UNPIN_REGION;

    }
};


/******************************************************************************/
/* VALUE                                                                      */
/******************************************************************************/

template<typename T>
struct Value
{
    Value(MemoryAllocator& area, uint64_t offset) :
        area(area), offset_(offset)
    {}

    static Value<T>
    alloc(MemoryAllocator& area, const T& value = T())
    {
        MMAP_PIN_REGION(area.region())
        {
            Value<T> obj(area, NodeAlloc<sizeof(T)>::alloc(area));

            auto ptr = obj.get();
            new (ptr.get()) T(value); // Initialize the memory location.

            return obj;
        }
        MMAP_UNPIN_REGION;
    }

    void dealloc()
    {
        NodeAlloc<sizeof(T)>::dealloc(area, offset_);
        offset_ = -1LL;
    }

    uint64_t offset() const { return offset_; }

    void store(T value)
    {
        MMAP_PIN_REGION(area.region())
        {
            (*get()) = value;
        }
        MMAP_UNPIN_REGION;
    }

    T load()
    {
        if (!offset_) return T();

        MMAP_PIN_REGION(area.region())
        {
            return *get();
        }
        MMAP_UNPIN_REGION;
    }

private:
    MemoryAllocator& area;
    uint64_t offset_;

    RegionPtr<T>
    get()
    {
        return RegionPtr<T>(area.region(), offset_);
    }
};


/******************************************************************************/
/* STRING                                                                     */
/******************************************************************************/

/** Immutable string implementation. */
template<>
struct Value<std::string>
{
    Value(MemoryAllocator& area, uint64_t offset) :
        area(area), offset_(offset)
    {}

    uint64_t offset() const { return offset_; }

    static Value<std::string>
    alloc(MemoryAllocator& area, const std::string& value)
    {
        MMAP_PIN_REGION(area.region())
        {
            uint64_t offset = StringAlloc::alloc(area, value.size());
            Value<std::string> obj(area, offset);

            auto rawPtr = obj.get();
            std::copy(value.begin(), value.end(), rawPtr.get());

            return obj;
        }
        MMAP_UNPIN_REGION;
    }

    void dealloc()
    {
        StringAlloc::dealloc(area, offset_);
        offset_ = -1LL;
    }

    std::string load()
    {
        if (!offset_) return std::string();

        MMAP_PIN_REGION(area.region())
        {
            size_t size = StringAlloc::size(area, offset_);

            auto rawPtr = get();
            return std::string(rawPtr.get(), size);
        }
        MMAP_UNPIN_REGION;
    }


private:
    MemoryAllocator& area;
    uint64_t offset_;

    RegionPtr<char> get()
    {
        return RegionPtr<char>(area.region(), offset_);
    }
};

typedef Value<std::string> String;


/******************************************************************************/
/* UTILS                                                                      */
/******************************************************************************/

/** Shorthand to allocate a value of type T and initialize it with the given
    value T.

    Note that if no initialization value is provided then the type T will have
    to be passed as a template arguement.
 */
template<typename T>
Value<T> alloc(MemoryAllocator& area, const T& value = T())
{
    return Value<T>::alloc(area, value);
}

template<typename T>
void dealloc(MemoryAllocator& area, uint64_t offset)
{
    Value<T>(area, offset).dealloc();
}

/** Shorthand to load the value out of a given offset.

    Note that since the params to the function don't provide a way to induce the
    template parameter it must be given explictly. It can also be given through
    the t parameter which is only present to differentiate various instantiation
    of the function template. Any given value will be ignored.
 */
template<typename T>
T load(MemoryAllocator& area, uint64_t offset, T* noop = nullptr)
{
    return Value<T>(area, offset).load();
}


/** Shorthand to store a value to the given offset. */
template<typename T>
void store(MemoryAllocator& area, uint64_t offset, const T& value)
{
    Value<T>(area, offset).store(value);
}

} // namespace Types;
} // namespace MMap
} // namespace Datacratic

#endif // __mmap__mmap_utils_h__
