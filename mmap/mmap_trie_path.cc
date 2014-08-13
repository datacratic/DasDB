/* mmap_trie_path.cc
   Jeremy Barnes, 9 September 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Trie path for mmapped trie.
*/

#include "debug.h"
#include "mmap_trie_path.h"
#include "mmap_trie_node.h"
#include "jml/arch/format.h"
#include "jml/arch/exception_handler.h"
#include <iostream>

using namespace ML;
using namespace std;

namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* TRIE PATH ENTRY                                                           */
/*****************************************************************************/

std::string
TriePathEntry::
print(MemoryAllocator * area) const
{
    if (type_ == INVALID) return "INVALID";

    const char * rel = (relativity_ == ABSOLUTE ? "" : "+");

    switch (type_) {
    case INVALID: return "INVALID";
    case TERMINAL:
        return format("TERMINAL: entry %s%lld, bits %s%d, value %lld",
                      rel, (long long)entryNum,
                      rel, (int)bitNum,
                      (unsigned long long)data_);
    case NONTERMINAL:
        return format("NONTERMINAL: entry %s%lld, bits %s%d, node: %s",
                      rel, (long long)entryNum,
                      rel, (int)bitNum,
                      node().print(area).c_str());
    case OFFTHEEND:
        return format("OFFTHEEND: entry %s%lld", rel, (long long)entryNum);
    default:
        throw ML::Exception("invalid type_");
    }
}

std::ostream &
operator << (std::ostream & stream, const TriePathEntry & entry)
{
    return stream << entry.print();
}

BOOST_STATIC_ASSERT(sizeof(TriePathEntry) == 16);


/*****************************************************************************/
/* TRIE PATH                                                                 */
/*****************************************************************************/

std::ostream &
TriePath::
dump(std::ostream & stream, MemoryAllocator * area,
     GcLock::ThreadGcInfo * info) const
{
    stream << "TriePath of length " << (int)size()
        << " valid " << valid() << " with root ";

    if (area) {
        MMAP_PIN_REGION(area->region())
        {
            stream << NodeOps::print(root_, area, info) << endl;
        }
        MMAP_UNPIN_REGION;
    }
    else stream << root_ << endl;

    for (unsigned i = 0;  i < size();  ++i) {
        stream << "  " << i << ": " << at(i).print(area) << endl;
    }

    return stream;
}

KeyFragment
TriePath::
key(MemoryAllocator & area, GcLock::ThreadGcInfo * info) const
{
    KeyFragment result;
    
    //cerr << "key(): this = " << *this << endl;

    TriePtr current = root();
    size_t start = 0;

    for (unsigned i = 0;  i < size();  ++i) {
        const TriePathEntry & entry = at(i);

        KeyFragment extracted
            = NodeOps::extractKey(current, area, info,
                                  entry.entryNum - start);
        result += extracted;
        
        if (!entry.isNonTerminal())
            break;

        current = entry.node();
        start = entry.entryNum;
    }

    return result;
}

void
TriePath::
advance(ssize_t n, MemoryAllocator & area, GcLock::ThreadGcInfo * info)
{
    bool debug = false;//false;//(n < 0);

    if (debug) {
        cerr << "entryNum_ = " << entryNum() << endl;
        cerr << "n = " << n << endl;
        cerr << *this << endl;
    }

    if (n == 0) return;

    ssize_t targetNum = (ssize_t)entryNum() + n;

    if (debug)
        cerr << "moving from " << entryNum() << " to " << targetNum << endl;

    // Step 1: go back up the trie until the wanted element is somewhere
    // within the given element's range.

    int i = size();
    for (; i >= 0; --i) {
        size_t start, end;
        boost::tie(start, end) = entryNumRange(i, area, info);

        // Allow one-past-the-end to be accessed
        if (i == 0 && targetNum == end) {
            broaden(0);
            push_back(NodeOps::offTheEnd(root_, area, info));
            return;
        }

        if (targetNum >= start && targetNum < end) break;
    }
    
    if (debug)
        cerr << "broadening to " << i << endl;
    
    if (i == -1) {
        cerr << "moving from " << entryNum() << " to " << targetNum
             << " n = " << n << endl;
        cerr << *this << endl;
        throw ML::Exception("attempt to access invalid element number %lld "
                            "valid: 0-%lld", targetNum,
                            NodeOps::size(root_, area, info));
    }

    broaden(i);

    if (debug)
        cerr << *this << endl;

    // Step 2: go down the trie until we get the desired element
    for (int i = 0;  empty() || !back().isTerminal();  ++i) {
        if (debug)
            cerr << "iteration " << i << ": " << *this << endl;

        TriePtr node = getNode(size());
        size_t start = 0;
        if (!empty()) start = back().entryNum;
        size_t end = start + NodeOps::size(node, area, info);
        
        ExcAssert(start <= targetNum);
        ExcAssert(end > targetNum);

        // The given index within that node corresponds to entry entryNum.
        // First, subtract the start to get the relative position within
        // the node.

        size_t localTarget = targetNum - start;

        // Now do the match
        TriePathEntry entry = NodeOps::matchIndex(node, area, info,
                                                  localTarget);
        *this += entry;
        ExcAssert(back().entryNum <= targetNum);
        
        if (entry.isTerminal()) break;
    }

    if (back().entryNum != targetNum || debug) {
        cerr << *this << endl;
        cerr << "targetNum = " << targetNum << endl;
    }

    ExcAssertEqual(back().entryNum, targetNum);

    if (!valid()) {
        cerr << "trie: " << endl;
        NodeOps::dump(root(), area, info);
        cerr << "iterator: " << endl;
        dump(cerr, area, info);
        cerr << endl;
        cerr << "n = " << n << endl;
        cerr << "targetNum = " << targetNum << endl;
    }

    if (trieValidate) {
        ExcAssert(valid());
        validate(area, info, true);
    }
}

void
TriePath::
broaden(int level)
{
    if (level > size() || level < 0)
        throw ML::Exception("broadening too far");

    resize(level);
}

inline size_t
TriePath::
entryNumStart(int pathIndex) const
{
    return pathIndex == 0
        ? 0
        : at(pathIndex - 1).entryNum;
}

inline
std::pair<size_t, size_t>
TriePath::
entryNumRange(int pathIndex, MemoryAllocator & area,
              GcLock::ThreadGcInfo * info) const
{
    if (pathIndex < 0 || pathIndex > size())
        throw ML::Exception("entryNumRange: index %d out of range 0-%d",
                            pathIndex, (int)size());
    size_t start = entryNumStart(pathIndex);
    size_t end;
    if (pathIndex == size())
        end = start + 1;
    else {
        TriePtr ptr = (pathIndex == 0 ? root() : at(pathIndex - 1).node());
        size_t sz = NodeOps::size(ptr, area, info);
        end = start + sz;
    }
    return std::make_pair(start, end);
}

void
TriePath::
validate(MemoryAllocator & area, GcLock::ThreadGcInfo * info,
         bool finished) const
{
    std::string context;
    try {
        JML_TRACE_EXCEPTIONS(false);

        // There should be at least one
        ExcAssert(!empty());

        TriePtr current = root_;
        size_t currentSize = NodeOps::size(current, area, info);
        size_t start = 0, end = currentSize;
        size_t bitsDone = 0;

        for (unsigned i = 0;  i < size();  ++i) {
            try {
                const TriePathEntry & entry = at(i);

                // Entry should be valid
                ExcAssert(entry.type_ != TriePathEntry::INVALID);

                // Check that the entry number is in range
                ExcAssertGreaterEqual(entry.entryNum, start);
                ExcAssertLessEqual(entry.entryNum, end);

                // Off the end should only happen at the top level if finished
                if (i != 0 && finished)
                    ExcAssert(entry.entryNum < end);
        
                // Trie path should only contain absolute entries
                ExcAssertEqual(entry.relativity_, TriePathEntry::ABSOLUTE);

                size_t localEntryNum = entry.entryNum - start;
                TriePathEntry found1, found2;

                if (entry.type_ != TriePathEntry::OFFTHEEND) {
                    // Bit number should always increase
                    ExcAssertGreaterEqual(entry.bitNum, bitsDone);

                    KeyFragment key
                        = NodeOps::extractKey(current, area, info,
                                              localEntryNum);

                    // Searching for the key should yield the node
                    found1 = NodeOps::matchKey(current, area, info, key);
                    ExcAssertEqual(found1.entryNum, localEntryNum);
                    ExcAssertEqual(found1.bitNum, entry.bitNum - bitsDone);

                    // Searching for the entry num should yield the node
                    found2 = NodeOps::matchIndex(current, area, info,
                                                 localEntryNum);
                    ExcAssertEqual(found2.entryNum, localEntryNum);
                    ExcAssertEqual(found2.bitNum, entry.bitNum - bitsDone);
                }

                if (entry.type_ == TriePathEntry::TERMINAL) {
                    // Terminal match should only be on the last level
                    ExcAssertEqual(i, size() - 1);

                    ExcAssertEqual(found1.value(), entry.value());
                    ExcAssertEqual(found2.value(), entry.value());
                }
                else if (entry.type_ == TriePathEntry::NONTERMINAL) {
                    // Non-terminal match should not be on the last level
                    if (finished)
                        ExcAssertNotEqual(i, size() - 1);

                    ExcAssertEqual(found1.node(), entry.node());
                    ExcAssertEqual(found2.node(), entry.node());
                    
                    // New node should fit within the current (start, end)
                    // range
                    ExcAssertLessEqual(entry.entryNum
                                       + NodeOps::size(entry.node(), area,
                                                       info),
                                       end);

                    current = entry.node();
                    start = entry.entryNum;
                    end = entry.entryNum + NodeOps::size(current, area, info);
                    currentSize = NodeOps::size(current, area, info);
                    bitsDone = entry.bitNum;
                }
                else if (entry.type_ == TriePathEntry::OFFTHEEND) {

                    // Should really be off the end
                    ExcAssertEqual(entry.entryNum, end);

                    // Should e the last one in the stack
                    ExcAssertEqual(i, size() - 1);
                }
                else ExcAssert(false);
            } catch (const std::exception & exc) {
                context = ML::format("validating entry %d", i);
                throw;
            }
        }
    } catch (const std::exception & exc) {
        stringstream ss;
        ss << "TriePath didn't validate: " << context << endl;
        dump(ss, area, info);
        ss << "message: " << exc.what() << endl;
        cerr << ss.str();
        throw ML::Exception("TriePath didn't validate");
        //throw;
    }
}


std::ostream & operator << (std::ostream & stream, const TriePath & path)
{
    path.dump(stream);
    return stream;
}


} // namespace MMap
} // namespace Datacratic
