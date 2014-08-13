/* merge_utils_test.cc
   Rémi Attab, 14 June 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Tests for Cursor, BranchingPoint and MergeBase.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "merge_test.h"

#include "mmap/mmap_file.h"

#include <boost/test/unit_test.hpp>
#include <set>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace Datacratic::MMap::Merge;
using namespace ML;

// Sandbox for the test helpers.
BOOST_AUTO_TEST_CASE( test_utils )
{
    MMapFile area(RES_CREATE);

    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        TrieBuilder bc(area, gc, TriePtr::COPY_ON_WRITE);

        TriePtr sub;
        TriePtr root = bc.branch(frag(0x12, 8),
                bc.term({   { frag(0x34, 64), 1 },
                            { frag(0x5678, 16), 2 }}),
                sub = bc.branch(frag(0x34, 8), 3,
                        bc.term({   { frag(0x56, 8), 4 },
                                    { frag(0x1234, 16), 6 },
                                    { frag(0x7890ABCD, 32), 7 }}),
                        bc.term({{ frag(0x7890ABCD, 32), 5 }})));

        bc.dump(root);

        TrieBuilder bi(area, gc, TriePtr::IN_PLACE);

        TriePtr ipRoot = bi.inplace(root);
        bi.replaceBranch(ipRoot, 1,
                bi.replaceValue(sub, key(frag(0x34, 8), 0, frag(0x56, 8)), 10));

        bi.dump(ipRoot);
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE(test_bp_branching)
{
    TriePtr root(BinaryBranch, TriePtr::IN_PLACE, 0xFFFF00);
    TriePtr ptr0(SparseTerm,   TriePtr::IN_PLACE, 0xFFFF01);
    TriePtr ptr1(InlineTerm,   TriePtr::COPY_ON_WRITE, 0xFFFF02);

    {
        KeyFragment prefix = frag(0xFFFF, 16);
        KeyFragment prefix0 = prefix + frag(0, 1);
        KeyFragment prefix1 = prefix + frag(1, 1);

        KVList kvs = { {prefix, 1}, {prefix0, ptr0}, {prefix1, ptr1} };
        Cursor c({ frag(0x1234, 32) , root}, kvs);

        BOOST_CHECK_EQUAL(c.commonPrefixLen(), c.prefix().bits + prefix.bits);

        BranchingPoint bp0(c, c.commonPrefixLen());
        BOOST_CHECK_EQUAL(bp0.value(), kvs[0].value);
        BOOST_CHECK_EQUAL(bp0.branch(0), kvs[1]);
        BOOST_CHECK_EQUAL(bp0.branch(1), kvs[2]);

        for (int i = 0; i < prefix.bits; ++i) {
            BranchingPoint bp1(c, c.prefix().bits + i);
            BOOST_CHECK(!bp1.hasValue());
            BOOST_CHECK(!bp1.hasBranch(0));
            BOOST_CHECK(bp1.hasBranch(1));
            BOOST_CHECK_EQUAL(bp1.branchSize(1), kvs.size());

            BranchingPoint::iterator it, end;
            auto itKvs = kvs.begin();
            for (tie(it, end) = bp1.branchItPair(1); it != end; ++it, ++itKvs)
                BOOST_CHECK_EQUAL(*it, *itKvs);
        }
    }

    {
        KeyFragment prefix = frag(0x0000, 16);
        KeyFragment prefix0 = prefix + KeyFragment(0, 1);
        KeyFragment prefix1 = prefix + KeyFragment(1, 1);

        KVList kvs = { {prefix, 1}, {prefix0, ptr0}, {prefix1, ptr1} };
        Cursor c({ frag(0x1234, 32) , root}, kvs);

        BOOST_CHECK_EQUAL(c.commonPrefixLen(), c.prefix().bits + prefix.bits);

        BranchingPoint bp0(c, c.commonPrefixLen());
        BOOST_CHECK_EQUAL(bp0.value(), kvs[0].value);
        BOOST_CHECK_EQUAL(bp0.branch(0), kvs[1]);
        BOOST_CHECK_EQUAL(bp0.branch(1), kvs[2]);

        for (int i = 0; i < prefix.bits; ++i) {
            BranchingPoint bp1(c, c.prefix().bits + i);
            BOOST_CHECK(!bp1.hasValue());
            BOOST_CHECK(!bp1.hasBranch(1));
            BOOST_CHECK(bp1.hasBranch(0));
            BOOST_CHECK_EQUAL(bp1.branchSize(0), kvs.size());

            BranchingPoint::iterator it, end;
            auto itKvs = kvs.begin();
            for (tie(it, end) = bp1.branchItPair(0); it != end; ++it, ++itKvs)
                BOOST_CHECK_EQUAL(*it, *itKvs);
        }
    }
}

// Note that nary nodes are just terminal nodes with ptr instead of values.
// There's no real need to add extra tests for that.
BOOST_AUTO_TEST_CASE(test_bp_terminal)
{
    typedef BranchingPoint::iterator It;

    auto checkBranch = [&] (
            const Cursor& c, const BranchingPoint& bp, int branch,
            It itKvs, size_t size)
    {
        if (size == 0) {
            BOOST_CHECK(!bp.hasBranch(branch));
            return;
        }

        BOOST_CHECK(bp.hasBranch(branch));
        BOOST_CHECK_EQUAL(bp.branchSize(branch), size);

        It it, end;
        for (tie(it, end) = bp.branchItPair(branch); it != end; ++it, ++itKvs)
            BOOST_CHECK_EQUAL(*it, *itKvs);
    };
    auto checkValue = [&] (
            const Cursor& c, const BranchingPoint& bp, uint64_t value)
    {
        BOOST_CHECK(bp.hasValue());
        BOOST_CHECK_EQUAL(value, bp.value());
    };

    enum { n = 8 };

    {
        KeyFragment prefix = frag(0xFF, 8);

        for (unsigned i = 0; i < n; ++i) {
            KVList kvs;
            for (unsigned j = 1; j < n; ++j) {
                kvs.push_back({
                            prefix + frag(j > i, 1) + frag(j+0xF0, 8), j });
            }

            Cursor c({ frag(0x1234, 32), TriePtr()}, kvs);
            BranchingPoint bp(c, c.prefix().bits + prefix.bits);

            BOOST_CHECK(!bp.hasValue());
            checkBranch(c, bp, 0, kvs.begin(), i);
            checkBranch(c, bp, 1, kvs.begin() + i, (n - i - 1));
        }
    }

    {
        KeyFragment prefix = frag(0xFF, 8);

        for (unsigned i = 0; i < n; ++i) {
            KVList kvs;
            kvs.push_back({ prefix, 42 });

            for (unsigned j = 1; j < n; ++j) {
                kvs.push_back({
                            prefix + frag(j > i, 1) + frag(j+0xF0, 8), j });
            }

            Cursor c({ frag(0x1234, 32), TriePtr()}, kvs);
            BranchingPoint bp(c, c.prefix().bits + prefix.bits);

            checkValue(c, bp, 42);
            checkBranch(c, bp, 0, kvs.begin() + 1, i);
            checkBranch(c, bp, 1, kvs.begin() + i + 1, (n - i - 1));
        }
    }
}

BOOST_AUTO_TEST_CASE(test_cursor_advanceToBranch)
{
    MMapFile area(RES_CREATE);
    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);
        TrieBuilder b(area, gc, TriePtr::COPY_ON_WRITE);

        TriePtr subBranch, subTerm;
        TriePtr root = b.branch(frag(0, 1), 0,
                subBranch = b.branch(frag(1, 1),
                        subTerm = b.term({
                                    { frag(0xFF,        8), 2 },
                                    { frag(0xFF00,     16), 3 },
                                    { frag(0xFFFF0000, 32), 4 }}),
                        b.term({{ frag(0x7890ABCD, 32), 10 }})),
                b.term({{ frag(0x34, 64), 11 }}));

        Cursor c0(root, NodeOps::gatherKV(root, area, 0));
        BOOST_CHECK_EQUAL(root, c0.node());
        BOOST_CHECK(c0.isBranching());
        BOOST_CHECK_EQUAL(c0.kvs.size(), 3);

        BranchingPoint b0(c0, 1);
        BOOST_CHECK_EQUAL(b0.value(), 0);
        BOOST_CHECK_EQUAL(b0.branch(0).getPtr(), subBranch);
        BOOST_CHECK(b0.hasBranch(1));

        Cursor c1 = c0.advanceToValue(b0, area);
        BOOST_CHECK_EQUAL(c1.node(), c0.node());
        BOOST_CHECK_EQUAL(c1.kvs.size(), 1);
        BOOST_CHECK_EQUAL(c1.kvs.front().value, 0);
        BOOST_CHECK_EQUAL(c1.kvsCP, frag(0, 1));

        Cursor c2 = c0.advanceToBranch(b0, 1, area);
        BOOST_CHECK(c2.isTerminal());
        BOOST_CHECK_EQUAL(c2.kvs.size(), 1);
        BOOST_CHECK_EQUAL(c2.kvs.front().value, 11);

        KeyFragment cp = frag(0, 2) + frag(1, 1);

        Cursor c3 = c0.advanceToBranch(b0, 0, area);
        BOOST_CHECK_EQUAL(c3.node(), subBranch);
        BOOST_CHECK_EQUAL(c3.parent().node(), c0.node());
        BOOST_CHECK_EQUAL(c3.kvs.size(), 2);
        BOOST_CHECK_EQUAL(c3.kvsCP, frag(1, 1));

        BranchingPoint b1(c3, 3);
        BOOST_CHECK(!b1.hasValue());
        BOOST_CHECK_EQUAL(b1.branch(0).getPtr(), subTerm);
        BOOST_CHECK(b1.hasBranch(1));

        cp += frag(0,1);

        Cursor c4 = c3.advanceToBranch(b1, 0, area);
        BOOST_CHECK(c4.isTerminal());
        BOOST_CHECK_EQUAL(c4.node(), subTerm);
        BOOST_CHECK_EQUAL(c4.parent().node(), subBranch);
        BOOST_CHECK_EQUAL(c4.kvs.size(), 3);
        BOOST_CHECK_EQUAL(c4.kvsCP, frag(0xFF, 8));

        BranchingPoint b2(c4, c4.prefix().bits + 8);
        BOOST_CHECK_EQUAL(b2.value(), 2);
        BOOST_CHECK_EQUAL(b2.branchSize(0), 1);
        BOOST_CHECK_EQUAL(b2.branchSize(1), 1);

        Cursor cTermVal = c4.advanceToValue(b2, area);
        BOOST_CHECK(cTermVal.isTerminal());
        BOOST_CHECK_EQUAL(cTermVal.node(), subTerm);
        BOOST_CHECK_EQUAL(cTermVal.parent().node(), subBranch);
        BOOST_CHECK_EQUAL(cTermVal.kvs.size(), 1);
        BOOST_CHECK_EQUAL(cTermVal.kvs.front().value, 2);

        Cursor cTerm0 = c4.advanceToBranch(b2, 0, area);
        BOOST_CHECK(cTerm0.isTerminal());
        BOOST_CHECK_EQUAL(cTerm0.node(), subTerm);
        BOOST_CHECK_EQUAL(cTerm0.parent().node(), subBranch);
        BOOST_CHECK_EQUAL(cTerm0.kvs.size(), 1);
        BOOST_CHECK_EQUAL(cTerm0.kvs.front().value, 3);

        Cursor cTerm1 = c4.advanceToBranch(b2, 1, area);
        BOOST_CHECK(cTerm1.isTerminal());
        BOOST_CHECK_EQUAL(cTerm1.node(), subTerm);
        BOOST_CHECK_EQUAL(cTerm1.parent().node(), subBranch);
        BOOST_CHECK_EQUAL(cTerm1.kvs.size(), 1);
        BOOST_CHECK_EQUAL(cTerm1.kvs.front().value, 4);
    }
    MMAP_UNPIN_REGION;
}


BOOST_AUTO_TEST_CASE(test_advanceToBranch_node_split)
{
    MMapFile area(RES_CREATE);
    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);
        TrieBuilder b(area, gc, TriePtr::COPY_ON_WRITE);

        TriePtr nil, mid0, mid1, term0, term1;

        // Well... this is unreadable...
        TriePtr root = b.branch(frag(0xF, 4),
                mid0 = b.branch(frag(0x1, 1),
                        mid1 = b.branch(2, frag(1,1), {
                                    { frag(0, 2), term0 = b.term({{ frag(0xF, 4), 1 }}) },
                                    { frag(2, 2), term1 = b.term({{ frag(0xF, 4), 2 }}) }
                                }),
                        nil),
                nil);

        // This simulates a Cursor who's not aware that its underlining node was
        // broken up. Can happen in the insert phase on an n-ary node.
        KeyFragment prefix = key(key(frag(0xF, 4), 0, frag(1, 1)), 0, frag(1,1));
        KVList kvs = {{ prefix + frag(0x0, 2), term0 }};


        cerr << endl << endl
            << "---------------------------------------------------------------"
            << endl << endl;

        b.dump(root);

        Cursor c0({ frag(0,0), root }, kvs);
        BranchingPoint p0(c0, kvs.front().key.bits - 1);

        Cursor c1 = c0.advanceToBranch(p0, 0, area);
        cerr << c1.print() << endl;

        BOOST_CHECK_EQUAL(c1.node(), mid1);
        BOOST_CHECK_EQUAL(c1.kvs.size(), 1);
        BOOST_CHECK_EQUAL(c1.kvs.front(), KV(frag(0x1, 1) + frag(0, 2), term0));
        BOOST_CHECK_EQUAL(c1.parent().node(), mid0);
        BOOST_CHECK_EQUAL(c1.parent().parent().node(), root);
    }
    MMAP_UNPIN_REGION;

}


struct MergeTest : public MergeBase {
    typedef std::function<void(
                    Cursor&, const BranchingPoint&,
                    Cursor&, const BranchingPoint&, Cursor&)> TestFn;
    TestFn test;

    MergeTest(TestFn test, MemoryAllocator& area, GCList& gc) :
        MergeBase(area, gc), test(test)
    {}

    void mergeBranchingPoints(
        Cursor& a, const BranchingPoint& aPoint,
        Cursor& b, const BranchingPoint& bPoint,
        Cursor& nil)
    {
        test(a, aPoint, b, bPoint, nil);
    }

    static void run (TriePtr a, TriePtr b, TestFn test, MemoryAllocator& area)
    {
        Cursor aCursor(a, NodeOps::gatherKV(a, area, 0));
        Cursor bCursor(b, NodeOps::gatherKV(b, area, 0));
        Cursor nil;

        GCList gc(area);
        MergeTest merger(test, area, gc);
        merger.merge(aCursor, bCursor, nil);
    }
};


BOOST_AUTO_TEST_CASE(test_merge_base)
{
    MMapFile area(RES_CREATE);
    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);
        TrieBuilder b(area, gc, TriePtr::COPY_ON_WRITE);

        TriePtr nil;

        TriePtr aTerm0, aTerm1;
        TriePtr aRoot = b.branch(frag(0xFF, 8),
                aTerm0 = b.term({
                            {frag(0x00, 8), 11},
                            {frag(0x0F, 8), 12},
                            {frag(0xF0, 8), 13},
                            {frag(0xFF, 8), 14}}),
                aTerm1 = b.term({
                            {frag(0xFF, 8), 19}}));

        TriePtr bBranch0, bBranch1, bTerm0, bTerm1;
        TriePtr bRoot = b.branch(frag(0xF, 4), 29,
                nil,
                bBranch0 = b.branch(frag(0x7, 3), 28,
                        bBranch1 = b.branch(frag(0,0), 20,
                                bTerm0 = b.term({
                                            {frag(0x0FF, 11), 21}}),
                                bTerm1 = b.term({
                                            {frag(0x7F ,  7), 22}})),
                        nil));

        cerr << endl << endl
            << "---------------------------------------------------------------"
            << endl << endl;

        b.dump(aRoot);
        b.dump(bRoot);

        bool sawARoot = false;
        set<uint64_t> valuesSeen;

        auto testFn = [&] (
                Cursor& a, const BranchingPoint& aPoint,
                Cursor& b, const BranchingPoint& bPoint,
                Cursor& nil)
        {
            BOOST_CHECK_EQUAL(aPoint.bitNum, bPoint.bitNum);

            // Walking down to bBranch0
            if (b.node() == bRoot) {
                BOOST_CHECK_EQUAL(aPoint.bitNum, 4);

                BOOST_CHECK(!aPoint.hasValue());
                BOOST_CHECK(!aPoint.hasBranch(0));
                BOOST_CHECK(aPoint.hasBranch(1));

                valuesSeen.insert(bPoint.value());
                BOOST_CHECK(!bPoint.hasBranch(0));
                BOOST_CHECK(bPoint.hasBranch(1));
            }

            // Walking down to bBranch1
            else if (b.node() == bBranch0) {
                BOOST_CHECK_EQUAL(a.node(), aRoot);
                BOOST_CHECK_EQUAL(aPoint.bitNum, 8);

                BOOST_CHECK(aPoint.hasBranch(0));
                BOOST_CHECK(aPoint.hasBranch(1));
                BOOST_CHECK(bPoint.hasBranch(0));
                BOOST_CHECK(!bPoint.hasBranch(1));

                sawARoot = true;
            }

            // We're diffing the a terminal node.
            else if (a.node() == aTerm0) {
                // Walking into one of the 2 b branches.
                if (aPoint.bitNum == 9) {
                    BOOST_CHECK_EQUAL(aPoint.branchSize(0), 2);
                    BOOST_CHECK_EQUAL(aPoint.branchSize(1), 2);

                    valuesSeen.insert(bPoint.value());
                    BOOST_CHECK(bPoint.hasBranch(0));
                    BOOST_CHECK(bPoint.hasBranch(1));
                }

                // Diff-ing terminal values with terminal values.
                else {
                    if (aPoint.hasValue())
                        valuesSeen.insert(aPoint.value());

                    if (bPoint.hasValue())
                    valuesSeen.insert(bPoint.value());

                    for (int branch = 0; branch < 2; ++branch) {
                        if (aPoint.hasBranch(branch) == bPoint.hasBranch(branch))
                            continue;

                        BranchingPoint::iterator it, end;
                        if (aPoint.hasBranch(branch))
                            tie(it, end) = aPoint.branchItPair(branch);
                        if (bPoint.hasBranch(branch))
                            tie(it, end) = bPoint.branchItPair(branch);

                        for(; it != end; ++it) valuesSeen.insert(it->value);
                    }
                }
            }

            else ExcAssert(false);
        };

        MergeTest::run(aRoot, bRoot, testFn, area);

        BOOST_CHECK(sawARoot);
        BOOST_CHECK(valuesSeen.count(11));
        BOOST_CHECK(valuesSeen.count(12));
        BOOST_CHECK(valuesSeen.count(13));
        BOOST_CHECK(valuesSeen.count(14));
        BOOST_CHECK(valuesSeen.count(20));
        BOOST_CHECK(valuesSeen.count(21));
        BOOST_CHECK(valuesSeen.count(22));
        BOOST_CHECK(valuesSeen.count(29));
    }
    MMAP_UNPIN_REGION;

}
