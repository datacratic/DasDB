/** mmap_trie_ptr.cc                                 -*- C++ -*-
    Rémi Attab, 15 Nov 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Composite pointer for trie nodes.

*/

#include "mmap_trie_ptr.h"
#include "mmap_trie_node.h"

using namespace std;

namespace Datacratic {
namespace MMap {


/******************************************************************************/
/* NODE TYPE                                                                  */
/******************************************************************************/

std::string nodeTypeName(NodeType type)
{
#define DASDB_NODETYPE_STRING_CASE(_name_) \
    case _name_: return #_name_

    switch(type) {
        DASDB_NODETYPE_STRING_CASE(NullTerm);

        DASDB_NODETYPE_STRING_CASE(BinaryBranch);
        DASDB_NODETYPE_STRING_CASE(DenseBranch);
        DASDB_NODETYPE_STRING_CASE(SparseBranch);

        DASDB_NODETYPE_STRING_CASE(InlineTerm);
        DASDB_NODETYPE_STRING_CASE(BasicKeyedTerm);
        DASDB_NODETYPE_STRING_CASE(SparseTerm);
        DASDB_NODETYPE_STRING_CASE(CompressedTerm);
        DASDB_NODETYPE_STRING_CASE(LargeKeyTerm);

    default: return "Unknown";
    }
#undef DASDB_NODETYPE_STRING_CASE
}

/*****************************************************************************/
/* TRIE PTR                                                                  */
/*****************************************************************************/

static string
getStateString(TriePtr::State state) {
    return string(state == TriePtr::IN_PLACE ? "I" : "C") + "-";
}

std::string
TriePtr::
print() const
{
    return
        getStateString(state) +
        NodeOps::print(*this, 0, GcLock::GcInfo::getThisThread());
}

std::string
TriePtr::
print(MemoryAllocator & area) const
{
    return
        getStateString(state) +
        NodeOps::print(*this, &area, GcLock::GcInfo::getThisThread());
}

std::ostream &
operator << (std::ostream & stream, const TriePtr & ptr)
{
    return stream << ptr.print();
}


} // namespace MMap
} // namepsace Datacratic
