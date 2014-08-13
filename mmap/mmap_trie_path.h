/* mmap_trie_iterator.h                                            -*- C++ -*-
   Jeremy Barnes, 8 September 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Iterator class for the MMap Trie.
*/

#pragma once

#include "memory_allocator.h"
#include <iostream>
#include <iterator>
#include "jml/utils/compact_vector.h"
#include "jml/utils/unnamed_bool.h"
#include "key_fragment.h"
#include "mmap_trie_ptr.h"


namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* TRIE PATH ENTRY                                                           */
/*****************************************************************************/

/** This is a single component of a mmap trie path.

    It represents that:
    - within the given node,
    - we matched bits to get to (bitnum)
    - and in doing so, we got to entry number (entrynum)

    In essence, it's an iterator within a trie node.

    TODO: make it able to represent lower bounds
*/

struct TriePathEntry {
    TriePathEntry()
        : data_(0), entryNum(0), bitNum(0), type_(INVALID),
          relativity_(ABSOLUTE)
    {
    }

    enum Type {
        INVALID,      ///< Entry was never initialized
        TERMINAL,     ///< Entry is a valid terminal node
        NONTERMINAL,  ///< Entry is a valid non-terminal node
        OFFTHEEND,    ///< Entry is an invalid off-the-end entry
    };

    enum Relativity {
        ABSOLUTE,     ///< Offsets are absolute
        RELATIVE      ///< Offsets are relative
    };

    uint64_t data_ JML_PACKED;    //< either a value, a TriePtr or unused
    struct {
        uint64_t entryNum:38;     //< Entry number within this node
        uint64_t bitNum:23;       //< Total bits matched after this node
        Type type_:2;             //< either terminal, nonterminal or offtheend
        Relativity relativity_:1; //< either ABSOLUTE or RELATIVE
    } JML_PACKED;

    bool valid() const
    {
        return (type_ == TERMINAL || type_ == NONTERMINAL);
    }

    /** This is the node reached after this value. */
    TriePtr node() const
    {
        ExcCheckEqual(type_, NONTERMINAL,
                "Attempt to extract node from non-NONTERMINAL");
        return TriePtr::fromBits(data_);
    }

    uint64_t value() const
    {
        ExcCheckEqual(type_, TERMINAL,
                "Attempt to extract value from non-TERMINAL");
        return data_;
    }

    bool isRelative() const { return relativity_ == RELATIVE; }
    bool isAbsolute() const { return relativity_ == ABSOLUTE; }
    bool isTerminal() const { return type_ == TERMINAL; }
    bool isNonTerminal() const { return type_ == NONTERMINAL; }
    bool isOffTheEnd() const { return type_ == OFFTHEEND; }

    std::string print(MemoryAllocator * area = 0) const;

    bool operator == (const TriePathEntry & other) const
    {
        return data_ == other.data_ && entryNum == other.entryNum
            && bitNum == other.bitNum;
    }

    static TriePathEntry offTheEnd(size_t skipped)
    {
        TriePathEntry result;
        result.entryNum = skipped;
        result.bitNum = 0;
        result.type_ = OFFTHEEND;
        result.relativity_ = RELATIVE;
        return result;
    }

    static TriePathEntry nonTerminal(int bitsMatched,
                                     TriePtr next,
                                     size_t skipped,
                                     bool exactMatch = true)
    {
        TriePathEntry result;
        result.entryNum = skipped;
        result.bitNum = bitsMatched;
        result.type_ = NONTERMINAL;
        result.relativity_ = RELATIVE;
        result.data_ = next.bits;
        return result;
    }

    static TriePathEntry terminal(int bitsMatched,
                                  uint64_t value,
                                  size_t skipped,
                                  bool exactMatch = true)
    {
        TriePathEntry result;
        result.entryNum = skipped;
        result.bitNum = bitsMatched;
        result.type_ = TERMINAL;
        result.relativity_ = RELATIVE;
        result.data_ = value;
        return result;
    }

} JML_PACKED;

std::ostream &
operator << (std::ostream & stream, const TriePathEntry & entry);


/*****************************************************************************/
/* TRIE PATH                                                                 */
/*****************************************************************************/

/** This structure contains a list of trie path entries that describes
    between them the path taken through a trie from the root to a given
    point.
*/

typedef ML::compact_vector<TriePathEntry, 2, uint32_t> TriePathBase;

struct TriePath: public TriePathBase {

    TriePath()
    {
    }

    TriePath(TriePtr root)
        : root_(root)
    {
    }

    TriePath(TriePtr root, const TriePathEntry & entry1)
        : root_(root)
    {
        *this += entry1;
    }

    TriePath(TriePtr root, const TriePathEntry & entry1,
             const TriePathEntry & entry2)
        : root_(root)
    {
        *this += entry1;
        *this += entry2;
    }

    TriePath(TriePtr root, const TriePathEntry & start,
             const TriePath & rest)
        : root_(root)
    {
        *this += start;
        *this += rest;
    }

    KeyFragment key(MemoryAllocator & area, GcLock::ThreadGcInfo * info) const;

    TriePtr root() const
    {
        return root_;
    }

    TriePtr lastNode() const
    {
        if (empty()) return root_;

        int i = size() - 1 - !back().isNonTerminal();
        if (i == -1) return root_;

        return at(i).node();
    }

    TriePtr getNode(int pathIndex) const
    {
        if (pathIndex == 0) return root();
        return at(pathIndex - 1).node();
    }

    size_t entryNumStart(int pathIndex) const;
    std::pair<size_t, size_t>
    entryNumRange(int pathIndex,
                  MemoryAllocator & area,
                  GcLock::ThreadGcInfo * info) const;

    size_t entryNum() const
    {
        return empty() ? 0 : back().entryNum;
    }

    uint64_t value() const
    {
        return back().value();
    }

    bool valid() const
    {
        return !empty() && back().isTerminal();
    }

    bool equal(const TriePath & other) const
    {
        if (root() != other.root()) return false;
        if (size() != other.size())
            return false;
        ExcCheck(!empty(), "invalid TrieIterator in comparison");

        return back() == other.back();
    }

    size_t totalBits() const
    {
        if (empty()) return 0;
        return getAbsolute(size() - 1).bitNum;
    }

    /** Advance by the given amount. */
    void advance(ssize_t n,
                 MemoryAllocator & area,
                 GcLock::ThreadGcInfo * info);

    /** Append to the other path. */
    void operator += (const TriePath & other)
    {
        if (lastNode() != other.root()) {
            std::stringstream ss;
            ss << "Attempt to append invalid TriePath { "
                << "lastNode=" << lastNode()
                << ", other.root=" << other.root()
                << " }";
            ExcCheckEqual(lastNode(), other.root(), ss.str());
        }
        int oldsz = size();
        reserve(oldsz + other.size());

        size_t entryNum = 0;
        int bitNum = 0;
        if (oldsz > 0) {
            const TriePathEntry & oldBack = back();
            entryNum = oldBack.entryNum;
            bitNum = oldBack.bitNum;
        }

        for (unsigned i = 0;  i < other.size();  ++i) {
            TriePathEntry newEntry = other[i];
            newEntry.entryNum += entryNum;
            newEntry.bitNum += bitNum;
            push_back(newEntry);
        }
    }

    void operator += (const TriePathEntry & other)
    {
        if (empty()) {
            push_back(other);
            back().relativity_ = TriePathEntry::ABSOLUTE;
        }
        else {
            if (other.relativity_ != TriePathEntry::RELATIVE)
                throw ML::Exception("can't append non-relative TriePathEntry");
            TriePathEntry old = at(size() - 1);

            push_back(other);
            TriePathEntry & pushed = back();
            pushed.bitNum += old.bitNum;
            pushed.entryNum += old.entryNum;
            pushed.relativity_ = TriePathEntry::ABSOLUTE;
        }
    }

    /** Concatenate the two paths. */
    TriePath operator + (const TriePath & other) const
    {
        TriePath path = *this;
        path += other;
        return path;
    }

    TriePathEntry getRelative(int element) const
    {
        TriePathEntry prev;
        if (element > 0) prev = at(element - 1);
        TriePathEntry result = at(element);
        result.entryNum -= prev.entryNum;
        result.bitNum -= prev.bitNum;
        result.relativity_ = TriePathEntry::RELATIVE;
        return result;
    }

    TriePathEntry getAbsolute(int element) const
    {
        return at(element);
    }

    /**
    Returns an iterator to the entry that contains the given node.
    Returns end() if the node is not contained in this path.
    */
    iterator find(const TriePtr& node)
    {
        for (auto it = begin(), itEnd = end(); it != itEnd; ++it) {
            if (!it->isNonTerminal())
                continue;
            if (it->node() == node)
                return it;
        }
        return end();
    }

    /**
    Returns an iterator in \c this where both \c this and \c other point to the same node.
    Returns end() if there are no common sub paths.
    */
    iterator commonSubPath(const TriePath& other)
    {
        for (auto it = other.begin(), itEnd = other.end(); it != itEnd; ++it) {
            if (!it->isNonTerminal())
                continue;

            iterator pos = std::find(begin(), end(), *it);
            if (pos == end())
                continue;

            return pos;
        }
        return end();
    }


    /**
    Returns a copy of the path split off at the given iterator.
    Note that the bitnum of each entry is copied as is.
    return {topHalf, bottomHalf};
    */
    std::pair<TriePath, TriePath> splitAt(const_iterator pos) const
    {
        TriePath topHalf (root_);
        std::copy(begin(), pos, std::back_inserter(topHalf));

        if (pos == end())
            return std::make_pair(topHalf, TriePath());

        TriePath bottomHalf (pos->node());
        std::copy(pos+1, end(), std::back_inserter(bottomHalf));

        for (auto it = bottomHalf.begin(), itEnd = bottomHalf.end();
             it != itEnd; ++it)
        {
            it->bitNum -= pos->bitNum;
            it->entryNum -= pos->entryNum;
        }

        return std::make_pair(topHalf, bottomHalf);
    }

    std::ostream & dump(std::ostream & stream,
                        MemoryAllocator * area = 0,
                        GcLock::ThreadGcInfo * info = 0) const;

    std::ostream & dump(std::ostream & stream,
                        MemoryAllocator & area,
                        GcLock::ThreadGcInfo * info) const
    {
        return dump(stream, &area, info);
    }

    void broaden(int level);

    /** Run an internal consistency check.  The finshed parameter tells
        us whether or not the path should be finished (ie, part of a valid
        TrieIterator) and extra checks should be run, or if it's just
        "under construction".
    */
    void validate(MemoryAllocator & area,
                  GcLock::ThreadGcInfo * info,
                  bool finished = true) const;

private:
    /** Root node. */
    TriePtr root_;
};

std::ostream & operator << (std::ostream & stream, const TriePath & path);

} // namespace Datacratic
} // namespace MMap

