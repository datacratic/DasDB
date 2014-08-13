/* merge_test.h                                               -*- C++ -*-
   Rémi Attab, 14 June 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Testing utilities for the merge tests.
*/


#include "merge_test.h"

#include "mmap/mmap_trie_node_impl.h"
#include "mmap/mmap_trie_dense_branching_node.h"


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;


namespace Datacratic {
namespace MMap {
namespace Merge {

bool
TrieBuilder::
isTerm(TriePtr node)
{
    return !NodeOps::isBranchingNode(node, area, 0);
}

bool
TrieBuilder::
isBranch(TriePtr node)
{
    return !isTerm(node);
}

TriePtr
TrieBuilder::
term(KVList kvs)
{
    sort(kvs.begin(), kvs.end());
    TriePtr node = makeMultiLeafNode(area, 0, kvs, kvs.size(), state, gc).root();
    ExcAssert(isTerm(node));
    return node;
}

TriePtr
TrieBuilder::
branch(const KeyFragment& prefix, TriePtr b0, TriePtr b1)
{
    KVList kvs;
    if (b0) kvs.emplace_back(KeyFragment(0, 1), b0);
    if (b1) kvs.emplace_back(KeyFragment(1, 1), b1);
    return branch(1, prefix, kvs);
}

TriePtr
TrieBuilder::
branch(const KeyFragment& prefix, uint64_t value, TriePtr b0, TriePtr b1)
{
    KVList kvs;
    if (b0) kvs.emplace_back(KeyFragment(0, 1), b0);
    if (b1) kvs.emplace_back(KeyFragment(1, 1), b1);
    return branch(1, prefix, value, kvs);
}

TriePtr
TrieBuilder::
branch( unsigned branchBits,
        const KeyFragment& prefix,
        uint64_t value,
        const KVList& branches)
{
    ExcAssertLessEqual(branchBits, 4);
    for (const KV& kv : branches) ExcAssertEqual(kv.key.bits, branchBits);

    KVList kvs = branches.prefixKeys(prefix);
    return DenseBranchingNodeOps::allocBranchingNode(
            area, 0, prefix.bits, branchBits, kvs, true, value, state, gc);
}

TriePtr
TrieBuilder::
branch(unsigned branchBits, const KeyFragment& prefix, const KVList& branches)
{
    ExcAssertLessEqual(branchBits, 4);
    for (const KV& kv : branches) ExcAssertEqual(kv.key.bits, branchBits);

    KVList kvs = branches.prefixKeys(prefix);
    return DenseBranchingNodeOps::allocBranchingNode(
            area, 0, prefix.bits, branchBits, kvs, false, 0, state, gc);
}

KVList
TrieBuilder::
gather(TriePtr root)
{
    return NodeOps::gatherKV(root, area, 0);
}

KVList
TrieBuilder::
gatherValues(TriePtr root)
{
    KVList values;
    auto onValue = [&](const KeyFragment& frag, uint64_t value) {
        values.push_back({frag, value});
    };
    NodeOps::forEachValue(root, area, 0, onValue);
    return values;
}


TriePtr
TrieBuilder::
inplace(TriePtr node)
{
    ExcAssertNotEqual(node.state, TriePtr::IN_PLACE);
    return NodeOps::changeState(node, area, 0, TriePtr::IN_PLACE, gc);
}

TriePtr
TrieBuilder::
replaceBranch(TriePtr node, int branch, TriePtr newBranch)
{
    KVList kvs = NodeOps::gatherKV(node, area, 0);
    KeyFragment cp = kvs.commonPrefix();

    TriePathEntry entry = NodeOps::matchKey(
            node, area, 0, cp + KeyFragment(branch, 1));

    return NodeOps::replace(node, area, 0, entry, newBranch, state, gc).root();
}

TriePtr
TrieBuilder::
replaceValue(TriePtr node, const KeyFragment& key, uint64_t value)
{
    return insert(remove(node, key), key, value);
}


TriePtr
TrieBuilder::
insert(TriePtr node, const KeyFragment& key, uint64_t value)
{
    return NodeOps::insertLeaf(node, area, 0, key, value, state, gc).root();
}


TriePtr
TrieBuilder::
remove(TriePtr node, const KeyFragment& key)
{
    return NodeOps::removeLeaf(node, area, 0, key, state, gc);
}


void
TrieBuilder::
dump(TriePtr root)
{
    NodeOps::dump(root, area, 0, 4);
    cerr << endl << endl;
}


} // namespace Merge
} // namespace MMap
} // namespace Datacratic

