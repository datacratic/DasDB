/** mmap_trie_ptr.h                                 -*- C++ -*-
    Rémi Attab, 15 Nov 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Composite pointer for trie nodes.

*/

#ifndef __mmap__mmap_trie_ptr_h__
#define __mmap__mmap_trie_ptr_h__


#include "memory_allocator.h"

#include <iostream>
#include <string>


namespace Datacratic {
namespace MMap {

/******************************************************************************/
/* NODE TYPE                                                                  */
/******************************************************************************/

/** Changing the value of existing node types breaks binary compatibility. */
enum NodeType
{
    NullTerm       = 0,

    BinaryBranch   = 1,
    DenseBranch    = 7,
    SparseBranch   = 8,

    InlineTerm     = 2,
    BasicKeyedTerm = 3,
    SparseTerm     = 4,
    CompressedTerm = 5,
    LargeKeyTerm   = 6,
};

/** Returns a stringified version of a NodeType entry. */
std::string nodeTypeName(NodeType type);

/*****************************************************************************/
/* TRIE PTR                                                                  */
/*****************************************************************************/

/** Represents a pointer to a node of a memory mapped trie.  Mostly opaque;
    the only thing needed is the type which allows the correct decoding
    function to be used.
*/

struct TriePtr
{

    /** Indicates the state of the object being pointed to. */
    enum State {
        COPY_ON_WRITE = 0,  //< Object should be copied before writing
        IN_PLACE = 1,       //< Object can be modified in place
    };


    /** Changing any of these values breaks binary compatibility between mmap
        versions.
    */
    enum {
        TypeBits = 6,
        StateBits = 1,
        MetaBits = TypeBits + StateBits,
        DataBits = 64 - MetaBits,
    };

    explicit TriePtr(NodeType type = NullTerm, uint64_t data = 0)
        : type(type), state(COPY_ON_WRITE), data(data)
    {
    }

    TriePtr(NodeType type, State state, uint64_t data)
        : type(type), state(state), data(data)
    {
    }

    static TriePtr fromBits(uint64_t bits)
    {
        TriePtr result;
        result.bits = bits;
        return result;
    }

    union {
        struct {
            NodeType type :TypeBits;  //< What type of pointer is it?
            State state   :StateBits;
            uint64_t data :DataBits; //< pointer data
        };
        uint64_t bits;
    };

    operator uint64_t () const { return bits; }

    /** Print out a string representation of what it points to */
    std::string print() const;

    /** Print out a better string representation of what it points to. */
    std::string print(MemoryAllocator & area) const;

    std::string print(MemoryAllocator * area) const
    {
        if (area) return print(*area);
        return print();
    }
};

static_assert(sizeof(TriePtr) == sizeof(uint64_t), "TriePtr must be uint64_t" );

std::ostream & operator << (std::ostream & stream, const TriePtr & ptr);


} // namespace MMap
} // Datacratic

#endif // __mmap__mmap_trie_ptr_h__
