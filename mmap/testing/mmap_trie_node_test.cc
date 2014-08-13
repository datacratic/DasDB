/* mmap_trie_node_test.cc
   Jeremy Barnes, 16 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the mmap trie node classes.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap/mmap_trie_null_node.h"
#include "mmap/mmap_trie_terminal_nodes.h"
//#include "mmap_trie_dense_node.h"
#include "mmap/mmap_trie_binary_nodes.h"
#include "mmap/mmap_trie_inline_nodes.h"
#include "mmap/mmap_trie_sparse_nodes.h"
#include "mmap/mmap_trie_compressed_nodes.h"
#include "mmap/mmap_trie_large_key_nodes.h"
#include "mmap/mmap_file.h"

#include <boost/test/unit_test.hpp>
#include "jml/utils/smart_ptr_utils.h"
#include "jml/utils/string_functions.h"
#include "jml/arch/atomic_ops.h"
#include "jml/utils/pair_utils.h"
#include "jml/utils/vector_utils.h"
#include <boost/thread.hpp>
#include <boost/function.hpp>
#include <iostream>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;

KVList
extractValues(MMapFile & area, TriePtr ptr)
{
    return NodeOps::gatherKV(ptr, area, 0);
}

KVList
extractValues(const std::vector<uint64_t> & values)
{
    if (values.size() % 2 != 0)
        throw Exception("extractValues: needs an even number of values");

    KVList result;
    for (unsigned i = 0;  i < values.size();  i += 2)
        result.push_back({ KeyFragment(values[i], 64), values[i + 1] });

    //cerr << endl << endl << endl << endl;
    //cerr << "result = " << result << endl;

    return result;
}

bool isEquivalent(const KVList & vals1, const KVList & vals2)
{
    auto it1 = vals1.begin(), end1 = vals1.end();
    auto it2 = vals2.begin(), end2 = vals2.end();

    bool result = true;

    for (; it1 != end1 && it2 != end2;) {
        if (*it1 == *it2) {
            ++it1;
            ++it2;
            continue;
        }

        // Different; check how
        result = false;
        cerr << "different at position (" << (it1 - vals1.begin())
             << "," << (it2 - vals2.begin()) << "): "
             << *it1 << " vs " << *it2 << endl;

        if (it1->key == it2->key) {
            ++it1;
            ++it2;
        }
        else if (it1->key < it2->key) ++it1;
        else ++it2;
    }

    for (; it1 != end1;  ++it1) {
        result = false;
        cerr << "first sequence has extra entry: " << *it1 << endl;
    }

    for (; it2 != end2;  ++it2) {
        result = false;
        cerr << "first sequence has extra entry: " << *it2 << endl;
    }

    return result;
}

bool isEquivalent(MMapFile & area, TriePtr ptr1, TriePtr ptr2)
{
    auto vals1 = extractValues(area, ptr1);
    auto vals2 = extractValues(area, ptr2);

    bool result = isEquivalent(vals1, vals2);

    if (!result) {
        cerr << "node: ";
        NodeOps::dump(ptr1, area, 0, 6);
        cerr << endl;

        cerr << "expected: " << endl;
        NodeOps::dump(ptr2, area, 0, 6);
        cerr << endl;
    }

    return result;
}

bool isEquivalent(
        MMapFile & area,
        TriePtr ptr1,
        const std::initializer_list<uint64_t> & values)
{
    auto vals = extractValues(area, ptr1);
    auto exp = extractValues(values);
    bool result = isEquivalent(vals, exp);

    if (!result) {
        cerr << "node: ";
        NodeOps::dump(ptr1, area, 0, 6) << endl;
        cerr << vals << endl;

        cerr << "expected: " << endl;
        cerr << exp << endl;
    }

    return result;
}

bool isEquivalent(MMapFile & area, TriePtr ptr1, const KVList & exp)
{
    auto vals = extractValues(area, ptr1);
    bool result = isEquivalent(vals, exp);

    if (!result) {
        cerr << "node: ";
        NodeOps::dump(ptr1, area, 0, 6) << endl;
        cerr << vals << endl;

        cerr << "expected: " << endl;
        cerr << exp << endl;
    }

    return result;
}

#define CHECK_EQUIVALENT(area, x, y) \
    BOOST_CHECK(isEquivalent(area, x, y))

// We're using a define instead of a function because it's easier to figure out
// which line triggered the error.
#define CHECK_BOUNDS(node, kf, start, end, area)                \
    do {                                                        \
    TriePathEntry lb = NodeOps::lowerBound(node, area, 0, kf);  \
    BOOST_CHECK_EQUAL(lb.entryNum, start);                      \
                                                                \
    TriePathEntry ub = NodeOps::upperBound(node, area, 0, kf);  \
    BOOST_CHECK_EQUAL(ub.entryNum, end);                        \
    } while(false);


BOOST_AUTO_TEST_CASE( test_null_node )
{
    TriePtr ptr(NullTerm);  // null node...
    MMapFile area(RES_CREATE);

    // Null node doesn't match
    MMAP_PIN_REGION(area.region())
    {
        CHECK_EQUIVALENT(area, ptr, ptr);
        CHECK_EQUIVALENT(area, ptr, TriePtr(NullTerm));

        KeyFragment key(123, 64);
        BOOST_CHECK(!NodeOps::matchKey(ptr, area, 0, key).valid());

        // Copying and inserting a leaf leads to a single leaf node
        GCList gc(area);
        TriePath res = NodeOps::insertLeaf(
                ptr, area, 0, key, 9874, TriePtr::COPY_ON_WRITE, gc);

        BOOST_CHECK(res.valid());
        BOOST_CHECK_EQUAL(res.value(), 9874);

        BOOST_CHECK_EQUAL(NodeOps::size(res.root(), area, 0), 1);
        BOOST_CHECK_EQUAL(NodeOps::matchKey(
                        res.root(), area, 0, key).value(), 9874);

        BOOST_CHECK(isEquivalent(area, res.root(), {123, 9874}));

        BOOST_CHECK_EQUAL(NodeOps::findKey(
                        res.root(), area, 0, key).value(), 9874);
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_terminal_node )
{
    MMapFile area(RES_CREATE);

    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        // One with no key has a value
        TriePtr node1 =
            BasicKeyedTerminalOps::alloc(
                    area, 0, KeyFragment(0, 0), 1234,
                    TriePtr::COPY_ON_WRITE, gc);

        // Is equivalent
        BOOST_CHECK(isEquivalent(area, node1,
                        {{ KeyFragment(0, 0), 1234 }}));

        // And it matches the empty key
        TriePathEntry matched = NodeOps::matchKey(
                node1, area, 0, KeyFragment(0, 0));
        BOOST_CHECK(matched.isTerminal());
        BOOST_CHECK_EQUAL(matched.value(), 1234);

        // Now try one with a prefix (key is 0x2692)
        TriePtr node2 = BasicKeyedTerminalOps::alloc(
                area, 0, KeyFragment(9874, 64), 1234,
                TriePtr::COPY_ON_WRITE, gc);

        // Check the lower and upper bound functions.
        CHECK_BOUNDS(node2, KeyFragment(0xFFFF, 64), 1, 1, area);
        CHECK_BOUNDS(node2, KeyFragment(0x26, 56), 0, 1, area);
        CHECK_BOUNDS(node2, KeyFragment(0x0000, 64), 0, 0, area);

        KeyFragment kf3(0x2692, 64); kf3 += KeyFragment(0x00, 8);
        CHECK_BOUNDS(node2, kf3, 1, 1, area);

        // Is equivalent
        BOOST_CHECK(isEquivalent(area, node2, {9874, 1234}));

        // Doesn't match empty
        matched = NodeOps::matchKey(node2, area, 0, KeyFragment(0, 0));
        BOOST_CHECK(matched.isOffTheEnd());

        // Does match the key
        matched = NodeOps::matchKey(node2, area, 0, KeyFragment(9874, 64));
        BOOST_CHECK_EQUAL(matched.value(), 1234);

        // Now add in another value
        TriePath res = NodeOps::insertLeaf(
                node2, area, 0, KeyFragment(1, 64), 5678,
                TriePtr::COPY_ON_WRITE, gc);

        // Is equivalent
        BOOST_CHECK(isEquivalent(area, res.root(), {1, 5678,   9874, 1234}));

        BOOST_CHECK_EQUAL(NodeOps::findKey(
                res.root(), area, 0, KeyFragment(9874, 64)).value(), 1234);
        BOOST_CHECK_EQUAL(NodeOps::findKey(
                res.root(), area, 0, KeyFragment(1, 64)).value(), 5678);
        BOOST_CHECK(!NodeOps::findKey(
                res.root(), area, 0, KeyFragment(1111, 64)).valid());
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_inplace_terminal_node )
{
    enum {
        key1 = 0x1234567890ABCDEF,
        key2 = 0xFEDCBA0987654321
    };

    MMapFile area(RES_CREATE);

    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        // One with no key has a value
        TriePtr node1 = BasicKeyedTerminalOps::alloc(
                area, 0, KeyFragment(key1, 64), 0, TriePtr::COPY_ON_WRITE, gc);
        auto entry1 = TriePathEntry::terminal(64, 0, 0, true);

        // Transition into an in place state using a copy on write.
        TriePtr node2 = NodeOps::replaceValue(
                node1, area, 0, entry1, 42, TriePtr::IN_PLACE, gc).root();
        auto entry2 = TriePathEntry::terminal(64, 42, 0, true);

        BOOST_CHECK_NE(node1, node2);
        BOOST_CHECK_EQUAL(node2.state, TriePtr::IN_PLACE);
        BOOST_CHECK(isEquivalent(area, node2, {key1, 42}));

        // Modify the node in place.
        TriePtr node3 = NodeOps::replaceValue(
                node2, area, 0, entry2, key1, TriePtr::IN_PLACE, gc).root();

        BOOST_CHECK_EQUAL(node2, node3);
        BOOST_CHECK(isEquivalent(area, node3, {key1, key1}));

        // Change it's prefix inplace
        node3 = NodeOps::prefixKeys(
                node3, area, 0, KeyFragment(), TriePtr::IN_PLACE, gc);

        BOOST_CHECK_EQUAL(node2, node3);
        BOOST_CHECK(isEquivalent(area, node3, {key1, key1}));

        // Burst and make sure the in place state remains.
        TriePtr node4 = NodeOps::insertLeaf(
                node3, area, 0, KeyFragment(key2, 64), key2,
                TriePtr::IN_PLACE, gc).root();

        BOOST_CHECK_NE(node3, node4);
        BOOST_CHECK_EQUAL(node4.state, TriePtr::IN_PLACE);
        BOOST_CHECK(isEquivalent(area, node4, { key1, key1,   key2, key2 }));
        // \todo Should also make sure that node2/3 is deallocated.
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_inline_node )
{
    MMapFile area(RES_CREATE);

    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        // One with no key has a value
        TriePtr node1 = InlineNode::encode(
                KeyFragment(0, 0), 1234, 0,
                TriePtr::COPY_ON_WRITE, gc).first.toPtr();

        // Is equivalent
        BOOST_CHECK(isEquivalent(area, node1,
                        {{ KeyFragment(0, 0), 1234 }}));

        // And it matches the empty key
        TriePathEntry matched = NodeOps::matchKey(
                node1, area, 0, KeyFragment(0, 0));
        BOOST_CHECK(matched.isTerminal());
        BOOST_CHECK_EQUAL(matched.value(), 1234);

        // Now try one with a prefix
        TriePtr node2 = InlineNode::encode(
                KeyFragment(9874, 64), 1234, 0,
                TriePtr::COPY_ON_WRITE, gc).first.toPtr();

        // Check the lower and upper bound functions.
        CHECK_BOUNDS(node2, KeyFragment(0xFFFF, 64), 1, 1, area);
        CHECK_BOUNDS(node2, KeyFragment(0x26, 56), 0, 1, area);
        CHECK_BOUNDS(node2, KeyFragment(0x0000, 64), 0, 0, area);

        KeyFragment kf3(0x2692, 64); kf3 += KeyFragment(0x00, 8);
        CHECK_BOUNDS(node2, kf3, 1, 1, area);

        // Is equivalent
        BOOST_CHECK(isEquivalent(area, node2, {9874, 1234}));

        // Doesn't match empty
        matched = NodeOps::matchKey(node2, area, 0, KeyFragment(0, 0));
        BOOST_CHECK(matched.isOffTheEnd());

        // Does match the key
        matched = NodeOps::matchKey(node2, area, 0, KeyFragment(9874, 64));
        BOOST_CHECK_EQUAL(matched.value(), 1234);

        // Now add in another value
        TriePath res = NodeOps::insertLeaf(
                node2, area, 0, KeyFragment(1, 64), 5678,
                TriePtr::COPY_ON_WRITE, gc);
        // Is equivalent
        BOOST_CHECK(isEquivalent(area, res.root(), {1, 5678,   9874, 1234}));

        BOOST_CHECK_EQUAL(NodeOps::findKey(
                 res.root(), area, 0, KeyFragment(9874, 64)).value(), 1234);
        BOOST_CHECK_EQUAL(NodeOps::findKey(
                 res.root(), area, 0, KeyFragment(1, 64)).value(), 5678);
        BOOST_CHECK(!NodeOps::findKey(
                 res.root(), area, 0, KeyFragment(1111, 64)).valid());
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_inplace_inline_node )
{
    enum {
        key1 = 0x12,
        key2 = 0xFE
    };

    MMapFile area(RES_CREATE);

    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        // One with no key has a value
        auto res = InlineNode::encode(
                KeyFragment(key1, 64), 0, 0, TriePtr::IN_PLACE, gc);
        TriePtr node1 = res.first.toPtr();
        auto entry1 = TriePathEntry::terminal(64, 0, 0, true);

        // Transition into an in place state using a copy on write.
        TriePtr node2 = NodeOps::replaceValue(
                node1, area, 0, entry1, 42, TriePtr::IN_PLACE, gc).root();
        auto entry2 = TriePathEntry::terminal(64, 42, 0, true);

        BOOST_CHECK_EQUAL(node2.state, TriePtr::IN_PLACE);
        BOOST_CHECK(isEquivalent(area, node2, {key1, 42}));

        // Modify the node in place.
        TriePtr node3 = NodeOps::replaceValue(
                node2, area, 0, entry2, key1, TriePtr::IN_PLACE, gc).root();

        BOOST_CHECK(isEquivalent(area, node3, {key1, key1}));

        // Change it's prefix inplace
        node3 = NodeOps::prefixKeys(
                node3, area, 0, KeyFragment(), TriePtr::IN_PLACE, gc);
        BOOST_CHECK(isEquivalent(area, node3, {key1, key1}));

        // Burst and make sure the in place state remains.
        TriePtr node4 = NodeOps::insertLeaf(
                node3, area, 0, KeyFragment(key2, 64), key2,
                TriePtr::IN_PLACE, gc).root();

        BOOST_CHECK_EQUAL(node4.state, TriePtr::IN_PLACE);
        BOOST_CHECK(isEquivalent(area, node4, { key1, key1,   key2, key2 }));
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_sparse_node )
{
    MMapFile area(RES_CREATE);

    MMAP_PIN_REGION(area.region())
    {
        GCList gc (area);

        // One with no key has a value
        KVList kv1 = { {KeyFragment(0, 0), 1234} };
        TriePtr node1 = SparseNodeOps::allocMultiLeaf(
                area, 0, kv1, TriePtr::COPY_ON_WRITE, gc);

        // Is equivalent
        BOOST_CHECK(isEquivalent(area, node1,
                        {{ KeyFragment(0, 0), 1234 }}));

        // And it matches the empty key
        TriePathEntry matched =
            NodeOps::matchKey(node1, area, 0, KeyFragment(0, 0));
        BOOST_CHECK(matched.isTerminal());
        BOOST_CHECK_EQUAL(matched.value(), 1234);

        // Now try one with a prefix
        KVList kv2 = { {KeyFragment(9874, 64), 1234} };
        TriePtr node2 = SparseNodeOps::allocMultiLeaf(
                area, 0, kv2, TriePtr::COPY_ON_WRITE, gc);

        // Is equivalent
        BOOST_CHECK(isEquivalent(area, node2, {9874, 1234}));

        // Doesn't match empty
        matched = NodeOps::matchKey(node2, area, 0, KeyFragment(0, 0));
        BOOST_CHECK(matched.isOffTheEnd());

        // Does match the key
        matched = NodeOps::matchKey(node2, area, 0, KeyFragment(9874, 64));
        BOOST_CHECK_EQUAL(matched.value(), 1234);

        // Now add in another value
        TriePath res = NodeOps::insertLeaf(
                node2, area, 0, KeyFragment(1, 64), 5678,
                TriePtr::COPY_ON_WRITE, gc);

        // Is equivalent
        BOOST_CHECK(isEquivalent(area, res.root(), {1, 5678,   9874, 1234}));

        BOOST_CHECK_EQUAL(NodeOps::findKey(
                res.root(), area, 0, KeyFragment(9874, 64)).value(),1234);
        BOOST_CHECK_EQUAL(NodeOps::findKey(
                res.root(), area, 0, KeyFragment(1, 64)).value(),5678);
        BOOST_CHECK(!NodeOps::findKey(
                res.root(), area, 0, KeyFragment(1111, 64)).valid());
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_sparse_node_bounds )
{
    enum {
        key1 = 0x441111,
        key2 = 0x442222,
        key3 = 0x442233,
        key4 = 0x444444,
    };

    MMapFile area(RES_CREATE);

    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        KVList kv = {
            { KeyFragment(key1, 64), key1 },
            { KeyFragment(key2, 64), key2 },
            { KeyFragment(key3, 64), key3 },
            { KeyFragment(key4, 64), key4 },
        };

        TriePtr node1 = SparseNodeOps::allocMultiLeaf(
                area, 0, kv, TriePtr::COPY_ON_WRITE, gc);

        CHECK_BOUNDS(node1, KeyFragment(0x44, 48),     0, 4, area);
        CHECK_BOUNDS(node1, KeyFragment(0x4422, 56),   1, 3, area);
        CHECK_BOUNDS(node1, KeyFragment(0x444444, 64), 3, 4, area);
        CHECK_BOUNDS(node1, KeyFragment(0x00, 64),     0, 0, area);
        CHECK_BOUNDS(node1, KeyFragment(0xFFFFFF, 64), 4, 4, area);
        CHECK_BOUNDS(node1, KeyFragment(0x442223, 64), 2, 2, area);

        KeyFragment kf7(0x442233, 64);
        kf7 += KeyFragment(0x00, 8);
        CHECK_BOUNDS(node1, kf7, 3, 3, area);
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_inplace_sparse_node )
{
    enum {
        key1 = 0x4141414141414141,
        key2 = 0x4242424242424242,
        key3 = 0x4343434343434343,
        key4 = 0x4444444444444444,
    };


    MMapFile area(RES_CREATE);

    auto toNode = [&](const TriePtr& ptr) -> SparseNodeOps::Node {
        return SparseNodeOps::encode(ptr, area, 0);
    };


    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        KVList kv = {
            { KeyFragment(key1, 64), key1 },
            { KeyFragment(key2, 64), key2 },
            { KeyFragment(key3, 64), key3 },
            { KeyFragment(key4, 64), key4 },
        };

        TriePtr node2 = SparseNodeOps::allocMultiLeaf(
                area, 0, kv, TriePtr::IN_PLACE, gc);

        // remove a leaf inplace
        TriePtr node3 = NodeOps::removeLeaf(
                node2, area, 0, KeyFragment(key2, 64), TriePtr::IN_PLACE, gc);

        BOOST_CHECK_EQUAL(toNode(node2).offset, toNode(node3).offset);
        BOOST_CHECK_EQUAL(node3.state, TriePtr::IN_PLACE);
        BOOST_CHECK(isEquivalent(area, node3,
                        { key1,key1, key3,key3, key4,key4 }));

        // insert a leaf inplace
        TriePtr node4 = NodeOps::insertLeaf(
                node3, area, 0, KeyFragment(key2, 64), key2,
                TriePtr::IN_PLACE, gc).root();
        auto entry4 = TriePathEntry::terminal(64, key1, 0, true);

        BOOST_CHECK_EQUAL(node2, node4); // metadata should be equal

        // replace a value inplace
        TriePtr node5 = NodeOps::replaceValue(
                node4, area, 0, entry4, key1 + 1, TriePtr::IN_PLACE, gc).root();

        BOOST_CHECK_EQUAL(node2, node5); // metadata should be equal
        BOOST_CHECK(isEquivalent(area, node5,
                        { key1,key1+1, key2,key2, key3,key3, key4,key4 }));

        // Burst the node.
        TriePtr node6 = NodeOps::prefixKeys(
                node5, area, 0, KeyFragment(0x45, 8), TriePtr::IN_PLACE, gc);
        BOOST_CHECK_EQUAL(node6.state, TriePtr::IN_PLACE);
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_large_key_node )
{
    MMapFile area(RES_CREATE);

    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        const uint64_t BaseValue = 0x123456789ABCDEF0;

        // Create a null prefix node.
        KVList kv1 = { { KeyFragment(0, 0), BaseValue + 1} };
        TriePtr node1 = LargeKeyNodeOps::allocMultiLeaf(
                area, 0, kv1, TriePtr::COPY_ON_WRITE, gc);
        CHECK_EQUIVALENT(area, node1, { kv1[0] });

        // Can we find our value?
        TriePathEntry matched =
            NodeOps::matchKey(node1, area, 0, KeyFragment(0, 0));
        BOOST_CHECK(matched.isTerminal());
        BOOST_CHECK_EQUAL(matched.value(), BaseValue + 1);

        // Create a semi-realistic node.
        KVList kv2 = {
            { KeyFragment(BaseValue + 0, 64), BaseValue + 0},
            { KeyFragment(BaseValue + 5, 64), BaseValue + 5},
            { KeyFragment(BaseValue + 9, 64), BaseValue + 9},
        };
        TriePtr node2 = LargeKeyNodeOps::allocMultiLeaf(
                area, 0, kv2, TriePtr::COPY_ON_WRITE, gc);

        BOOST_CHECK(isEquivalent(area, node2, kv2));

        // Can we still find all our values?
        for (int i = 0; i < 3; ++i) {
            matched = NodeOps::matchKey(node2, area, 0, kv2[i].key);
            BOOST_CHECK(matched.isTerminal());
            BOOST_CHECK_EQUAL(matched.value(), kv2[i].value);
        }

        // Remove the middle value.
        TriePtr res = NodeOps::removeLeaf(
                node2, area, 0, kv2[1].key, TriePtr::COPY_ON_WRITE, gc);
        BOOST_CHECK(isEquivalent(area, res, { kv2[0], kv2[2] }));

        // Add a new value
        KV newPair(KeyFragment(BaseValue + 6, 64), BaseValue + 6);
        res = NodeOps::insertLeaf(
                res, area, 0, newPair.key, newPair.getValue(),
                TriePtr::COPY_ON_WRITE, gc).root();

        // Check the order and the presence of the key.
        BOOST_CHECK(isEquivalent(area, res, { kv2[0], newPair, kv2[2] }));

        // We shouldn't be able to find the value that we removed.
        matched = NodeOps::matchKey(res, area, 0, kv2[1].key);
        BOOST_CHECK(matched.isOffTheEnd());
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_large_key_node_bounds )
{
    enum {
        key1 = 0x442222,
        key2 = 0x442233,
        key3 = 0x444444,
    };

    MMapFile area(RES_CREATE);

    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        KVList kv = {
            { KeyFragment(key1, 64), key1 },
            { KeyFragment(key2, 64), key2 },
            { KeyFragment(key3, 64), key3 },
        };

        TriePtr node1 = LargeKeyNodeOps::allocMultiLeaf(
                area, 0, kv, TriePtr::COPY_ON_WRITE, gc);

        CHECK_BOUNDS(node1, KeyFragment(0x44, 48),     0, 3, area);
        CHECK_BOUNDS(node1, KeyFragment(0x4422, 56),   0, 2, area);
        CHECK_BOUNDS(node1, KeyFragment(0x444444, 64), 2, 3, area);
        CHECK_BOUNDS(node1, KeyFragment(0x00, 64),     0, 0, area);
        CHECK_BOUNDS(node1, KeyFragment(0xFFFFFF, 64), 3, 3, area);
        CHECK_BOUNDS(node1, KeyFragment(0x442223, 64), 1, 1, area);

        KeyFragment kf7(0x442233, 64);
        kf7 += KeyFragment(0x00, 8);
        CHECK_BOUNDS(node1, kf7, 2, 2, area);
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_inplace_large_key_node )
{
    enum {
        key1 = 0x4141414141414141,
        key2 = 0x4242424242424242,
        key3 = 0x4343434343434343,
    };


    MMapFile area(RES_CREATE);

    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        KVList kv = {
            { KeyFragment(key1, 64), key1 },
            { KeyFragment(key2, 64), key2 },
            { KeyFragment(key3, 64), key3 },
        };

        for (int i = 0; i < 3; ++i)
            kv[i].key = KeyFragment('F',8) + kv[i].key;

        TriePtr node2 = LargeKeyNodeOps::allocMultiLeaf(
                area, 0, kv, TriePtr::IN_PLACE, gc);

        // remove a leaf inplace
        TriePtr node3 = NodeOps::removeLeaf(
                node2, area, 0, kv[1].key, TriePtr::IN_PLACE, gc);

        BOOST_CHECK_EQUAL(node2, node3);
        BOOST_CHECK_EQUAL(node3.state, TriePtr::IN_PLACE);

        // insert a leaf inplace
        TriePtr node4 = NodeOps::insertLeaf(
                node3, area, 0, kv[1].key, key2, TriePtr::IN_PLACE, gc).root();
        auto entry4 = TriePathEntry::terminal(64, key1, 0, true);

        BOOST_CHECK_EQUAL(node2, node4); // metadata should be equal

        // replace a value inplace
        TriePtr node5 = NodeOps::replaceValue(
                node4, area, 0, entry4, key1 + 1, TriePtr::IN_PLACE, gc).root();

        BOOST_CHECK_EQUAL(node2, node5); // metadata should be equal

        // Burst the node.
        TriePtr node6 = NodeOps::insertLeaf(
                node5, area, 0, KeyFragment(0x45, 8), 0,
                TriePtr::IN_PLACE, gc).root();
        BOOST_CHECK_EQUAL(node6.state, TriePtr::IN_PLACE);
    }
    MMAP_UNPIN_REGION;
}


BOOST_AUTO_TEST_CASE( test_binary_node_bounds )
{
    enum { key = 0x77 };

    MMapFile area(RES_CREATE);

    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        TriePtr child = makeLeaf(
                area, 0, KeyFragment(), 0, TriePtr::COPY_ON_WRITE, gc).root();

        KeyFragment prefix(key, 8);

        KVList kvs(2);
        kvs[0] = KV(prefix, key);
        kvs[1] = KV(prefix + KeyFragment(1,1), child);

        TriePtr node1 = makeBranchingNode(
                area, 0, kvs, TriePtr::COPY_ON_WRITE, gc);

        // Check the lower and upper bound functions.
        CHECK_BOUNDS(node1, KeyFragment(0xFF, 8), 2, 2, area);
        CHECK_BOUNDS(node1, KeyFragment(0x77, 8), 0, 2, area);
        CHECK_BOUNDS(node1, KeyFragment(0x00, 8), 0, 0, area);

        // Left branch shouldn't exist so attempt to match it.
        CHECK_BOUNDS(node1, prefix + KeyFragment(0,1), 1, 1, area);
    }
    MMAP_UNPIN_REGION;
}
