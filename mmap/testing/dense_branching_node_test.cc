/** dense_branching_node_test.cc                                 -*- C++ -*-
    Rémi Attab, 06 Dec 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Tests for DenseBranchingNode

    Deserves its own file because it forms the backbone of the entire trie and
    is by far one of the most complicated node type. We should probably also
    include some perf test in here as well since it's perf characteristics are
    crucial.


    \todo Compile time for this test is HORRIBLE. Need to do something about it.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "mmap/mmap_trie_node.cc"
#include "mmap/mmap_trie_dense_branching_node.h"
#include "mmap/mmap_trie_terminal_nodes.h"

#include <boost/test/unit_test.hpp>
#include <bitset>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;

enum {
    MaxBits = 4,
    MaxBranches = 1 << MaxBits,
};

template<typename T>
uint64_t p(T v) { return static_cast<uint64_t>(v); }

void print(const KVList& kvs, std::string prefix = "")
{
    for (size_t i = 0; i < kvs.size(); ++i)
        cerr << prefix << i << ": " << kvs[i] << endl;
}


/******************************************************************************/
/* DENSE BRANCHING NODE REPR TESTS                                            */
/******************************************************************************/

BOOST_AUTO_TEST_CASE( test_repr )
{
    cerr << endl
        << "REPR TEST ========================================================"
        << endl;

    cerr << "init-test --------------------------------------------------------"
        << endl;

    for (uint8_t bits = MaxBits; bits > 0; --bits) {
        cerr << p(bits) << "-bit-test: " << endl;

        DenseBranchingNodeRepr repr(bits);
        BOOST_CHECK_EQUAL(repr.isInline(), bits == 1);
        BOOST_CHECK_EQUAL(repr.numBranches(), 1 << bits);
        BOOST_CHECK_EQUAL(repr.countBranches(), 0);
        BOOST_CHECK_EQUAL(repr.nextBranch(), -1);
        BOOST_CHECK_EQUAL(repr.prevBranch(1 << bits), -1);

        for(uint8_t branch = 0; branch < repr.numBranches(); ++branch)
            BOOST_CHECK(!repr.isBranchMarked(branch));
    }

    cerr << endl;


    cerr << "bitfield-test ----------------------------------------------------"
        << endl;

    for (uint8_t bits = MaxBits; bits > 0; --bits) {
        cerr << p(bits) << "-bit-test: " << endl;

        DenseBranchingNodeRepr repr(bits);

        for(uint8_t i = 0; i < repr.numBranches(); ++i)
            repr.markBranch(i);

        BOOST_CHECK_EQUAL(repr.countBranches(), repr.numBranches());

        cerr << "\tnextBranch no bounds" << endl;
        {
            std::bitset<MaxBranches> bits;

            int8_t branch = -1;
            while ((branch = repr.nextBranch(branch + 1)) >= 0)
                bits[branch] = true;

            BOOST_CHECK_EQUAL(bits.count(), repr.numBranches());
            for (uint8_t i = 0; i < MaxBranches; ++i)
                BOOST_CHECK_EQUAL(bits[i], i < repr.numBranches());
        }

        cerr << "\tnextBranch bounded" << endl;
        {
            std::bitset<MaxBranches> bits;

            uint8_t start = repr.numBranches() / 4;
            uint8_t end = repr.numBranches() * 3 / 4;

            cerr << "\t\tstart=" << p(start) << ", end=" << p(end) << endl;

            int8_t branch = start - 1;
            while ((branch = repr.nextBranch(branch + 1, end)) >= 0)
                bits[branch] = true;

            BOOST_CHECK_EQUAL(bits.count(), end - start);
            for (uint8_t i = 0; i < MaxBranches; ++i)
                BOOST_CHECK_EQUAL(bits[i], i >= start && i < end);
        }

        cerr << "\tprevBranch" << endl;
        {
            std::bitset<MaxBranches> bits;

            int8_t branch = repr.numBranches() + 1;
            while ((branch = repr.prevBranch(branch - 1)) >= 0)
                bits[branch] = true;

            BOOST_CHECK_EQUAL(bits.count(), repr.numBranches());
            for (uint8_t i = 0; i < MaxBranches; ++i)
                BOOST_CHECK_EQUAL(bits[i], i < repr.numBranches());
        }
    }
}


/******************************************************************************/
/* UTILITIES                                                                  */
/******************************************************************************/

void checkEntry(const TriePathEntry& entry, const KV& kv)
{
    BOOST_CHECK(entry.valid());
    BOOST_CHECK_EQUAL(entry.isTerminal(), !kv.isPtr);

    if (entry.isTerminal())
        BOOST_CHECK_EQUAL(entry.value(), kv.getValue());
    else
        BOOST_CHECK_EQUAL(entry.node(), kv.getPtr());
};

void checkNode(MemoryAllocator& area, TriePtr node, const KVList& expectedKvs)
{
    KVList actualKvs = NodeOps::gatherKV(node, area, 0);
    if (actualKvs != expectedKvs) {
        cerr << "\tACTUAL: " << endl;
        print(actualKvs, "\t\t");
        cerr << "\tEXPECTED: " << endl;
        print(expectedKvs, "\t\t");
    }
    BOOST_CHECK_EQUAL(actualKvs, expectedKvs);

    size_t actualSize = NodeOps::size(node, area, 0);
    BOOST_CHECK_EQUAL(actualSize, expectedKvs.size());

    for (size_t i = 0; i < expectedKvs.size(); ++i) {
        TriePathEntry entry = NodeOps::matchKey(
                node, area, 0, expectedKvs[i].key);
        checkEntry(entry, expectedKvs[i]);
    }

    for (size_t i = 0; i < expectedKvs.size(); ++i) {
        TriePathEntry entry = NodeOps::matchIndex(node, area, 0, i);
        checkEntry(entry, expectedKvs[i]);
    }

    for (size_t i = 0; i < expectedKvs.size(); ++i) {
        KeyFragment kf = NodeOps::extractKey(node, area, 0, i);
        BOOST_CHECK_EQUAL(kf, expectedKvs[i].key);
    }
}

void checkPath(
        MemoryAllocator& area,
        TriePtr root,
        size_t index,
        const TriePath& path,
        const KV& kv)
{
    path.validate(area, 0, true);
    BOOST_CHECK(path.valid());
    BOOST_CHECK_EQUAL(path.root(), root);
    BOOST_CHECK_EQUAL(path.entryNum(), index);
    BOOST_CHECK_EQUAL(path.key(area, 0), kv.key);
    BOOST_CHECK_EQUAL(path.value(), kv.getValue());
}


void checkNodeDeep(
        MemoryAllocator& area, TriePtr node, const KVList& expectedKvs)
{
    KVList actualKvs;

    auto onValue = [&](const KeyFragment& key, uint64_t value) {
        actualKvs.push_back({ key, value });
    };
    NodeOps::forEachValue(node, area, 0, onValue);
    if (actualKvs != expectedKvs) {
        cerr << endl << "actual:" << endl;
        print(actualKvs, "\t");
        cerr << endl << "excpected:" << endl;
        print(expectedKvs, "\t");
        cerr << endl;
    }
    BOOST_CHECK_EQUAL(actualKvs, expectedKvs);

    size_t actualSize = NodeOps::size(node, area, 0);
    BOOST_CHECK_EQUAL(actualSize, expectedKvs.size());

    for (size_t i = 0; i < expectedKvs.size(); ++i) {
        TriePath path = NodeOps::findKey(node, area, 0, expectedKvs[i].key);
        checkPath(area, node, i, path, expectedKvs[i]);
    }

    for (size_t i = 0; i < expectedKvs.size(); ++i) {
        TriePath path = NodeOps::findIndex(node, area, 0, i);
        checkPath(area, node, i, path, expectedKvs[i]);
    }
}


TriePtr makeAndCheck(
        MemoryAllocator& area,
        GCList& gc,
        const KVList& kvs,
        const KVList& expected)
{
    TriePtr node;

    MMAP_PIN_REGION(area.region())
    {
        node = makeBranchingNode(area, 0, kvs, TriePtr::COPY_ON_WRITE, gc);
        checkNode(area, node, expected);
    }
    MMAP_UNPIN_REGION;

    return node;
};

TriePtr makeChild(
        MemoryAllocator& area,
        const KV& kv,
        GCList& gc,
        TriePtr::State state = TriePtr::COPY_ON_WRITE)
{
    return makeLeaf(area, 0, kv.key, kv.value, state, gc).root();
}

TriePtr makeChild(
        MemoryAllocator& area,
        const KVList& kvs,
        GCList& gc,
        TriePtr::State state = TriePtr::COPY_ON_WRITE)
{
    return makeMultiLeafNode(area, 0, kvs, kvs.size(), state, gc).root();
}


/******************************************************************************/
/* MAKE BRANCHING NODE TEST                                                   */
/******************************************************************************/

BOOST_FIXTURE_TEST_CASE( test_alloc, MMapAnonFixture )
{
    cerr << endl
        << "ALLOC TEST ======================================================="
        << endl;

    auto trackerGuard = enableTrieMemoryTracker();
    auto dbgGuard = enableTrieDebug();

    const uint64_t value = 0xA5A5A5A5A5A5A5A5;
    const KeyFragment suffix(0x5, 4);

    for (uint8_t bits = MaxBits; bits > 0; --bits) {
        cerr << p(bits) << "-bit-test: " << endl;

        GCList gc(area);

        int startBit = 4;
        KeyFragment prefix(0xA, startBit);

        MMAP_PIN_REGION(area.region())
        {
            KVList kvs;
            for (size_t branch = 0; branch < (1 << bits); branch++) {
                KeyFragment key = prefix + KeyFragment(branch, bits);
                TriePtr child = makeChild(area, { suffix, branch }, gc);
                kvs.push_back({ key, child });
            }

            TriePtr node = DenseBranchingNodeOps::allocBranchingNode(
                    area, 0, startBit, bits, kvs, true, value,
                    TriePtr::COPY_ON_WRITE, gc);

            KVList expectedKvs = {{ prefix, value }};
            expectedKvs.insert(expectedKvs.end(), kvs.begin(), kvs.end());

            checkNode(area, node, expectedKvs);
        }
        MMAP_UNPIN_REGION;

        // GCList will revert our allocations here because we didn't commit it.
    }
}

// Test for the getArity function used by makeBranchingNode.
BOOST_FIXTURE_TEST_CASE( test_get_arity, MMapAnonFixture )
{
    cerr << endl
        << "MAKE-ARITY ======================================================="
        << endl;

    auto trackerGuard = enableTrieMemoryTracker();
    auto dbgGuard = enableTrieDebug();

    const KeyFragment nilKf;

    // Ensures that we're dealing with variable length keys.
    auto branchingBits = [] (int minBits, int i, int branchBits) {
        KeyFragment key(i, branchBits);
        // We'll generate 16 so chances are pretty good that we'll generate at
        // least one minBits with a % 4.
        return key + KeyFragment(0, random() % 4 + (minBits - branchBits));
    };

    cerr << "with-value -------------------------------------------------------"
        << endl;

    // With value:
    // - minBits > MaxBits || minBits < MaxBits -> can loop this.
    for (size_t bits = MaxBits * 2; bits > 0; --bits) {
        cerr << bits << "-bit-test: " << endl;

        GCList gc(area);

        const KeyFragment prefix(0xAA, 8);

        KVList kvs;
        KVList expected;

        KV value = { prefix, 0xA5 };
        kvs.push_back(value); // Value will disable adjustments.
        expected.push_back(value);

        int branchBits = std::min<int>(bits, MaxBits);
        int branchCount = 1 << branchBits;

        for (size_t i = 0; i < branchCount; ++i) {
            KeyFragment key = prefix + branchingBits(bits, i, branchBits);
            TriePtr child = makeChild(area, { nilKf, i }, gc);
            kvs.push_back({ key, child });

            unsigned splitBit = prefix.bits + branchBits;
            TriePtr exChild = makeChild(area, { key.suffix(splitBit), i }, gc);
            expected.push_back({ key.prefix(splitBit), exChild });
        }

        sort(kvs.begin(), kvs.end());
        // print(kvs, "\t");
        makeAndCheck(area, gc, kvs, expected);
    }


#if 0
    // \todo The check on these are a little akward to do because they may
    // increase the depth of the trie. Revisit them later.

    cerr << "minBits-adj ------------------------------------------------------"
        << endl;

    // Without value: minBits - cpLen < MaxBits -> maximize
    for (size_t bits = MaxBits * 2; bits > 0; --bits) {
        cerr << bits << "-bit-test: " << endl;

        GCList gc(area);

        KVList kvs;
        KVList expected;

        int branchBits = std::min<int>(bits, MaxBits);
        int branchCount = 1 << branchBits;

        for (size_t i = 0; i < branchCount; ++i) {
            KeyFragment key =
                KeyFragment(0xA, 4) + branchingBits(bits, i, branchBits);
            TriePtr child = makeChild(area, { nilKf, i }, gc);
            kvs.push_back({ key, child });

            unsigned splitBit = 4 + branchBits;
            if (bits < 4) splitBit -= 4 - bits;
            TriePtr exChild = makeChild(area, { key.suffix(splitBit), i }, gc);
            expected.push_back({ key.prefix(splitBit), exChild });
        }

        sort(kvs.begin(), kvs.end());
        print(kvs, "\t");
        makeAndCheck(area, gc, kvs, expected);
    }


    cerr << "alignment --------------------------------------------------------"
        << endl;

    // Without value:
    // - Play with alignment.
    for (size_t bits = MaxBits * 2; bits > 0; --bits) {
        cerr << bits << "-bit-test: " << endl;

        GCList gc(area);

        KVList kvs;
        KVList expected;

        int branchBits = std::min<int>(bits, MaxBits);
        int branchCount = 1 << branchBits;

        for (size_t i = 0; i < branchCount; ++i) {

            // These bits will throw off the alignment
            KeyFragment extra((1 << bits) - 1, bits);

            KeyFragment key = extra + branchingBits(4, i, branchBits);
            TriePtr child = makeChild(area, { nilKf, i }, gc);
            kvs.push_back({ key, child });

            unsigned splitBit = extra.bits + branchBits;
            TriePtr exChild = makeChild(area, { key.suffix(splitBit), i}, gc);
            expected.push_back({ key.prefix(splitBit), exChild });
        }

        sort(kvs.begin(), kvs.end());
        print(kvs, "\t");
        makeAndCheck(area, gc, kvs, expected);
    }

#endif

}


/******************************************************************************/
/* ITERATOR TESTS                                                             */
/******************************************************************************/

BOOST_FIXTURE_TEST_CASE( test_match_index, MMapAnonFixture )
{
    auto trackerGuard = enableTrieMemoryTracker();
    auto dbgGuard = enableTrieDebug();

    cerr << endl
        << "MATCH-INDEX ======================================================="
        << endl;

    // This will mostly tests the DenseBranchingNodeOps::getIndex() function.

    for(unsigned bits = MaxBits; bits > 0; --bits) {
        const unsigned branchCount = 1 << bits;

        for (unsigned gap = 0; gap < branchCount; ++gap) {
            cerr << bits << "-bit-" << gap << "-gap-test: " << endl;

            GCList gc(area);
            KVList kvs;

            KeyFragment prefix(0xA, 4);
            kvs.push_back({ prefix, 0 });

            for (unsigned i = 0; i < branchCount; i += (gap + 1)) {
                KeyFragment key = prefix + KeyFragment(i, bits);
                TriePtr child = makeChild(area, { KeyFragment(), i }, gc);
                kvs.push_back({ key, child });
            }

            makeAndCheck(area, gc, kvs, kvs);
        }
    }
}

BOOST_FIXTURE_TEST_CASE( test_match_key, MMapAnonFixture )
{
    auto trackerGuard = enableTrieMemoryTracker();
    auto dbgGuard = enableTrieDebug();

    cerr << endl
        << "MATCH-KEY ========================================================="
        << endl;

    for (unsigned bits = MaxBits; bits > 0; --bits) {
        const unsigned branchCount = 1 << bits;
        cerr << bits << "-bit-test: " << endl;

        GCList gc(area);

        KVList kvs;
        KeyFragment prefix(0xA, 4);

        // We can't have a single branch in a DBN so add a value if that becomes
        // the case.
        bool hasValue = bits == 1;

        for (unsigned i = 0; i < branchCount; i += 2) {
            KeyFragment key = prefix + KeyFragment(i, bits);
            TriePtr child = makeChild(area, { KeyFragment(), i }, gc);
            kvs.push_back({ key, child });
        }

        MMAP_PIN_REGION(area.region())
        {
            // Don't use makeBranchingNodes because it does adjustments that we
            // don't want for this test.
            TriePtr node = DenseBranchingNodeOps::allocBranchingNode(
                    area, 0, 4, bits, kvs, hasValue, 0, TriePtr::COPY_ON_WRITE, gc);

            if (hasValue) insertKv(kvs, { prefix, 0 });

            // This will test all the ok cases.
            checkNode(area, node, kvs);

            KeyFragment kf0 = KeyFragment(2, 2);
            TriePathEntry e0 = NodeOps::matchKey(node, area, 0, kf0);
            BOOST_CHECK(e0.isOffTheEnd());

            if (!hasValue) {
                TriePathEntry e1 = NodeOps::matchKey(node, area, 0, prefix);
                BOOST_CHECK(e1.isOffTheEnd());
            }

            for (unsigned i = 1; i < branchCount; i += 2) {
                KeyFragment key = prefix + KeyFragment(i, bits);
                TriePathEntry entry = NodeOps::matchKey(node, area, 0, key);
                BOOST_CHECK(entry.isOffTheEnd());
            }

            for (unsigned j = 1; j < bits; ++j) {
                for (unsigned i = 1; i <= j; ++i) {
                    KeyFragment key = prefix + KeyFragment(i, j);
                    TriePathEntry entry = NodeOps::matchKey(node, area, 0, key);
                    BOOST_CHECK(entry.isOffTheEnd());
                }
            }
        }
        MMAP_UNPIN_REGION;
    }
}

BOOST_FIXTURE_TEST_CASE( test_bounds, MMapAnonFixture )
{
    auto trackerGuard = enableTrieMemoryTracker();
    auto dbgGuard = enableTrieDebug();

    cerr << endl
        << "BOUNDS ============================================================"
        << endl;

    auto checkBounds = [&](
            TriePtr node, const KeyFragment& key, uint64_t start, uint64_t end)
    {
        TriePathEntry lb = NodeOps::lowerBound(node, area, 0, key);
        BOOST_CHECK_EQUAL(lb.entryNum, start);

        TriePathEntry ub = NodeOps::upperBound(node, area, 0, key);
        BOOST_CHECK_EQUAL(ub.entryNum, end);
    };


    for (unsigned bits = MaxBits; bits > 0; --bits) {
        const unsigned branchCount = 1 << bits;
        cerr << bits << "-bit-test: " << endl;

        GCList gc(area);

        KVList kvs;

        KeyFragment prefix(0xA, 4);

        for (unsigned i = 0; i < branchCount; i += 2) {
            KeyFragment key = prefix + KeyFragment(i, bits);
            TriePtr child = makeChild(area, { KeyFragment(), i }, gc);
            kvs.push_back({ key, child });
        }

        MMAP_PIN_REGION(area.region())
        {
            // Don't use makeBranchingNodes because it does adjustments that we
            // don't want for this test.
            TriePtr node = DenseBranchingNodeOps::allocBranchingNode(
                    area, 0, 4, bits, kvs, true, 0, TriePtr::COPY_ON_WRITE, gc);

            cerr << "\tfull-prefix:" << endl;
            checkBounds(node, prefix.prefix(2), 0, kvs.size() + 1);

            cerr << "\tleft-prefix:" << endl;
            checkBounds(node, KeyFragment(0,4), 0, 0);

            cerr << "\tright-prefix:" << endl;
            checkBounds(node, KeyFragment(0xF,4), kvs.size()+1, kvs.size()+1);

            cerr << "\ton-prefix:" << endl;
            checkBounds(node, prefix, 0, kvs.size()+1);

            // Worked it out with purty picture. Just assume magic
            auto end = [] (uint64_t i) { return (i / 2) + 1; };

            // Empty branches should have the same start and end.
            // Non-empty branches should differ by one.
            auto start = [&] (uint64_t i) {
                return !(i % 2) ? end(i) - 1 : end(i);
            };


            cerr << "\tfull:" << endl;
            for (unsigned i = 0; i < branchCount; ++i) {
                KeyFragment key = prefix + KeyFragment(i, bits);

                cerr << "\t\t" << i << ": "
                    << "[" << start(i) << ", " << end(i) << "]"
                    << endl;

                checkBounds(node, key, start(i) + 1, end(i) + 1);
            }
            cerr << endl;

            cerr << "\tpartial:" << endl;
            for (unsigned branchBit = 1; branchBit < bits; ++branchBit) {
                for (unsigned branch = 0; branch < (1 << branchBit); ++branch) {
                    KeyFragment key = prefix + KeyFragment(branch, branchBit);

                    // Bounds consists of every branch that can be formed with
                    // key as a prefix. So figure out how they divide the node
                    // and take the LB of the first branch in the range and the
                    // UB of the last branch in the range.
                    unsigned n = branchCount / (1 << branchBit);
                    unsigned i = n * branch;

                    cerr << "\t\t(" << branchBit << ", " << branch << "): "
                        << "{" << n << ", " << i << "} "
                        << "[" << start(i) << ", " << start(i + n) << "]"
                        << endl;

                    checkBounds(node, key, start(i) + 1, start(i + n) + 1);
                }
            }

        }
        MMAP_UNPIN_REGION;
    }
}


/******************************************************************************/
/* INSERT TEST                                                                */
/******************************************************************************/

KeyFragment kf(uint64_t bits, unsigned numBits)
{
    return KeyFragment(bits, numBits);
}

void insertTest(MemoryAllocator& area, TriePtr::State state)
{
    auto trackerGuard = enableTrieMemoryTracker();
    auto dbgGuard = enableTrieDebug();

    for (unsigned bits = MaxBits; bits > 0; --bits) {
        const unsigned branchCount = 1 << bits;
        cerr << bits << "-bit-test: " << endl;

        GCList gc(area);

        // We can't have a single branch in a DBN so add a value if that becomes
        // the case.
        bool hasValue = bits <= 2;

        auto insertAndCheck = [&] (
                unsigned bits, const KV& kv, const KVList& kvs, KVList expected)
        {
            // Don't use makeBranchingNodes because it does adjustments that we
            // don't want for this test.
            TriePtr node = DenseBranchingNodeOps::allocBranchingNode(
                    area, 0, 4, bits, kvs, hasValue, 0, state, gc);

            TriePath path = NodeOps::insertLeaf(
                    node, area, 0, kv.key, kv.getValue(), state, gc);

            // cerr << "result -> ";
            // NodeOps::dump(path.root(), area, 0, 4);
            // cerr << endl << endl;

            uint64_t i = insertKv(expected, kv);

            checkPath(area, path.root(), i, path, kv);
            checkNodeDeep(area, path.root(), expected);
        };

        KVList kvs;
        KVList expected;

        KeyFragment prefix(0xA, 4);
        if (hasValue) expected.push_back({ prefix, 0 });

        for (unsigned i = branchCount / 2; i < branchCount; i += 2) {
            KeyFragment key = prefix + KeyFragment(i, bits);
            TriePtr child = makeChild(area, { KeyFragment(), i }, gc, state);
            kvs.push_back({ key, child });
            expected.push_back({ key, i });
        }

        MMAP_PIN_REGION(area.region())
        {
            cerr << "\tinsert-break-prefix:" << endl;
            insertAndCheck(bits, { kf(0x2, 2), 100 }, kvs, expected);
            insertAndCheck(bits, { kf(0x9, 4), 100 }, kvs, expected);
            insertAndCheck(bits, { kf(0x6, 3), 100 }, kvs, expected);

            if (!hasValue) {
                cerr << "\tinsert-into-value:" << endl;
                insertAndCheck(bits, { prefix, 100 }, kvs, expected);
            }

            cerr << "\tinsert-break-branches:" << endl;
            for (unsigned keyBits = 1; keyBits < bits; ++keyBits) {
                for (unsigned branch = 0; branch < (1 << keyBits); ++branch) {
                    cerr << "\t\t" << keyBits << "-bits-"
                        << branch << "-branch-test:" << endl;

                    KeyFragment key = prefix + KeyFragment(branch, keyBits);
                    insertAndCheck(bits, { key, 100 }, kvs, expected);
                }
            }

            cerr << "\tinsert-into-child:" << endl;
            // This includes inserting in an existing branch or an empty branch.
            for (unsigned i = 0; i < branchCount; ++i) {
                KeyFragment key =
                    prefix + KeyFragment(i, bits) + KeyFragment(0xA, 4);
                insertAndCheck(bits, { key, 100 }, kvs, expected);
            }
        }
        MMAP_UNPIN_REGION;

        cerr << "\tgc:" << endl;
    }
}

BOOST_FIXTURE_TEST_CASE( test_insert, MMapAnonFixture )
{
    cerr << endl
        << "INSERT(CoW) ======================================================="
        << endl;

    insertTest(area, TriePtr::COPY_ON_WRITE);


    cerr << endl
        << "INSERT(IP) ========================================================"
        << endl;

    insertTest(area, TriePtr::IN_PLACE);
}


/******************************************************************************/
/* REMOVE TESTS                                                               */
/******************************************************************************/

pair<TriePtr, KVList>
removeAndCheck(
        TriePtr node,
        MemoryAllocator& area,
        GCList& gc,
        TriePtr::State state,
        const KeyFragment& key,
        KVList expected)
{
    TriePtr newNode = NodeOps::removeLeaf(node, area, 0, key, state, gc);

    // cerr << "result -> ";
    // NodeOps::dump(newNode, area, 0, 4);
    // cerr << endl << endl;

    auto isKey = [=] (const KV& kv) { return kv.key == key; };
    expected.erase(find_if(expected.begin(), expected.end(), isKey));

    checkNodeDeep(area, newNode, expected);

    return make_pair(newNode, expected);
};

TriePtr
makeRemoveTestNode(
        unsigned bits,
        const KVList& kvs,
        MemoryAllocator& area,
        GCList& gc,
        TriePtr::State state)
{
    // Don't use makeBranchingNodes because it does adjustments that we
    // don't want for this test.
    return DenseBranchingNodeOps::allocBranchingNode(
            area, 0, 4, bits, kvs, true, 0, state, gc);
};


void removeTest(MemoryAllocator& area, TriePtr::State state)
{
    auto trackerGuard = enableTrieMemoryTracker();
    auto dbgGuard = enableTrieDebug();

    for (unsigned bits = MaxBits; bits > 0; --bits) {
        const unsigned branchCount = 1 << bits;
        cerr << bits << "-bit-test: " << endl;

        GCList gc(area);

        KVList kvs;
        KVList expected;

        KeyFragment prefix(0xA, 4);
        expected.push_back({ prefix, 0 });

        for (unsigned i = 0; i < branchCount; i += 2) {
            KeyFragment key = prefix + KeyFragment(i, bits);
            TriePtr child = makeChild(area, { KeyFragment(), i }, gc, state);
            kvs.push_back({ key, child });
            expected.push_back({ key, i });
        }

        auto makeNode = [&]() {
            return makeRemoveTestNode(bits, kvs, area, gc, state);
        };

        MMAP_PIN_REGION(area.region())
        {

            cerr << "\tremove-value:" << endl;
            removeAndCheck(makeNode(), area, gc, state, prefix, expected);

            cerr << "\tremove-child:" << endl;
            for (unsigned i = 0; i < branchCount; i += 2) {
                KeyFragment key = prefix + KeyFragment(i, bits);
                removeAndCheck(makeNode(), area, gc, state, key, expected);
            }

            cerr << "\tremove-simplify-subtree:" << endl;
            TriePtr node = makeNode();
            KVList tmpExp = expected;
            for (unsigned i = 0; i < branchCount; i += 2) {
                KeyFragment key = prefix + KeyFragment(i, bits);
                tie(node, tmpExp) =
                    removeAndCheck(node, area, gc, state, key, tmpExp);
            }
            BOOST_CHECK_EQUAL(node.type, InlineTerm);
        }
        MMAP_UNPIN_REGION;

    }
}

BOOST_FIXTURE_TEST_CASE( test_remove, MMapAnonFixture )
{
    cerr << endl
        << "REMOVE(CoW) ======================================================="
        << endl;

    removeTest(area, TriePtr::COPY_ON_WRITE);


    cerr << endl
        << "REMOVE(IP) ========================================================"
        << endl;

    removeTest(area, TriePtr::IN_PLACE);
}

void removeSimplifyNodeTest(MemoryAllocator& area, TriePtr::State state)
{
    auto trackerGuard = enableTrieMemoryTracker();

    for (unsigned bits = MaxBits; bits > 0; --bits) {
        cerr << bits << "-bit-test: " << endl;

        GCList gc(area);

        MMAP_PIN_REGION(area.region())
        {
            KVList expected;

            KeyFragment prefix(0xA, 4);

            // We need 4 to prevent subtree simplification from kicking in.
            KVList childKvs = {
                { KeyFragment(0,2), 0 },
                { KeyFragment(1,2), 1 },
                { KeyFragment(2,2), 2 },
                { KeyFragment(3,2), 3 }};

            KV valueKv { prefix, 0 };
            expected.push_back(valueKv);

            KeyFragment branchesKey[2];
            for (unsigned i = 0; i < 2; ++i) {
                branchesKey[i] = prefix + KeyFragment(i, bits);

                for (size_t j = 0; j < 4; ++j) {
                    KeyFragment expKey = branchesKey[i] + childKvs[j].key;
                    expected.push_back({ expKey, childKvs[j].value });
                }
            }

            auto makeNode = [&] {
                KVList kvs;

                for (unsigned i = 0; i < 2; ++i) {
                    TriePtr childPtr = makeChild(area, childKvs, gc, state);
                    kvs.push_back({ branchesKey[i], childPtr });
                }

                return makeRemoveTestNode(bits, kvs, area, gc, state);
            };

            auto removeBranchAndCheck = [&](
                    TriePtr node, KVList exp, unsigned branch)
            {
                for (unsigned i = 0; i < 4; ++i) {
                    KeyFragment key = branchesKey[branch] + childKvs[i].key;
                    tie(node, exp) =
                        removeAndCheck(node, area, gc, state, key, exp);
                }
                return make_pair(node, exp);
            };

            // This is actually a subset of subtree simplication.
            {
                cerr << "\tsimplify-to-terminal:" << endl;
                TriePtr ptr = makeNode();
                KVList exp = expected;

                tie(ptr, exp) = removeBranchAndCheck(ptr, exp, 0);
                BOOST_CHECK_EQUAL(ptr.type, DenseBranch);

                tie(ptr, exp) = removeBranchAndCheck(ptr, exp, 1);
                BOOST_CHECK_EQUAL(ptr.type, InlineTerm);
            }

            {
                cerr << "\tsimplify-to-branch-left:" << endl;

                TriePtr ptr = makeNode();
                KVList exp = expected;

                tie(ptr, exp) = removeAndCheck(ptr, area, gc, state, prefix, exp);
                BOOST_CHECK_EQUAL(ptr.type, DenseBranch);

                tie(ptr, exp) = removeBranchAndCheck(ptr, exp, 1);
                BOOST_CHECK_EQUAL(ptr.type, SparseTerm);
            }

            {
                cerr << "\tsimplify-to-branch-right:" << endl;

                TriePtr ptr = makeNode();
                KVList exp = expected;

                tie(ptr, exp) = removeAndCheck(ptr, area, gc, state, prefix, exp);
                BOOST_CHECK_EQUAL(ptr.type, DenseBranch);

                tie(ptr, exp) = removeBranchAndCheck(ptr, exp, 0);
                BOOST_CHECK_EQUAL(ptr.type, SparseTerm);
           }

            {
                cerr << "\tsimplify-remove-value:" << endl;

                TriePtr ptr = makeNode();
                KVList exp = expected;

                tie(ptr, exp) = removeBranchAndCheck(ptr, exp, 1);
                BOOST_CHECK_EQUAL(ptr.type, DenseBranch);

                tie(ptr, exp) = removeAndCheck(ptr, area, gc, state, prefix, exp);
                BOOST_CHECK_EQUAL(ptr.type, SparseTerm);
            }

        }
        MMAP_UNPIN_REGION;

    }
}

BOOST_FIXTURE_TEST_CASE( test_remove_simplify_node, MMapAnonFixture )
{
    cerr << endl
        << "REMOVE SIMPLIFY NODE(CoW) ========================================="
        << endl;

    removeSimplifyNodeTest(area, TriePtr::COPY_ON_WRITE);

    cerr << endl
        << "REMOVE SIMPLIFY NODE(IP) =========================================="
        << endl;

    removeSimplifyNodeTest(area, TriePtr::IN_PLACE);

}
