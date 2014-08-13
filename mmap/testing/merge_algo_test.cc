/* merge_algo_test.cc
   Rémi Attab, 18 June 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Tests for MergeDiff, MergeRemove and MergeInsert.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "merge_test.h"

#include "mmap/mmap_file.h"

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace Datacratic::MMap::Merge;
using namespace ML;


bool count(const KVList& kvs, uint64_t value)
{
    auto it = find_if(kvs.begin(), kvs.end(), [&](const KV& kv) -> bool {
                return value == kv.value;
            });
    return it != kvs.end();
};

bool count(const KVList& kvs, const KV& kv)
{
    auto it = find_if(kvs.begin(), kvs.end(), [&](const KV& otherKv) -> bool {
                return otherKv == kv;
            });
    return it != kvs.end();
};

bool count(const NodeList& nl, TriePtr ptr)
{
    auto it = find(nl.begin(), nl.end(), ptr);
    return it != nl.end();
}

bool nilConflict2(const TrieKey&, uint64_t, uint64_t)
{
    ExcAssert(false);
    return false;
};

uint64_t nilConflict3(const TrieKey&, uint64_t, uint64_t, uint64_t)
{
    ExcAssert(false);
    return false;
};


void printTitle(const std::string& title)
{
    cerr << endl << "[" << title << "]"
        << "-------------------------------------------------------------------"
        << endl;
}

BOOST_AUTO_TEST_CASE( test_remove_cutoff )
{
    MMapFile area(RES_CREATE);


    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        TrieBuilder b(area, gc, TriePtr::COPY_ON_WRITE);
        TrieBuilder bi(area, gc, TriePtr::IN_PLACE);

        TriePtr base = b.term({
                    { frag(0xFF, 8), 1 },
                    { frag(0x0F, 8), 2 }});

        {
            TriePtr dest = bi.branch(frag(0,0), 11, base,
                    b.term({    { frag(0x0F, 8), 12 },
                                { frag(0xFF, 8), 13 }}));

            printTitle("Cutoff - Remove Branch - Original");
            b.dump(dest);

            Cursor baseCursor({frag(0,1), base}, b.gather(base));
            Cursor destCursor(dest, b.gather(dest));

            KVList removedValues;
            MergeRemove::exec(
                    baseCursor, destCursor, nilConflict2,
                    area, gc, removedValues);

            BOOST_CHECK_EQUAL(destCursor.node(), dest);

            KVList kvs = b.gatherValues(dest);
            BOOST_CHECK(count(kvs, { frag(0,0), 11 }));
            BOOST_CHECK(count(kvs, { frag(1,1) + frag(0x0F,8), 12 }));
            BOOST_CHECK(count(kvs, { frag(1,1) + frag(0xFF,8), 13 }));
            BOOST_CHECK(!count(kvs, 1));
            BOOST_CHECK(!count(kvs, 2));

            BOOST_CHECK_EQUAL(removedValues.size(), 2);
            BOOST_CHECK(count(removedValues, { frag(0,1) + frag(0xFF,8), 1 }));
            BOOST_CHECK(count(removedValues, { frag(0,1) + frag(0x0F,8), 2 }));

            printTitle("Cutoff - Remove Branch");
            b.dump(dest);

            for (int i = 0; i < gc.oldNodes.size(); ++i)
                BOOST_CHECK_EQUAL(gc.oldNodes[i], base);
            gc.oldNodes.clear();
        }

        {
            TriePtr dest = bi.branch(frag(0,0), base,
                    b.term({{ frag(0xFF, 8), 12 }}));

            printTitle("Cutoff - Simplify Branch - Original ");
            b.dump(dest);

            Cursor baseCursor({frag(0,1), base}, b.gather(base));
            Cursor destCursor(dest, b.gather(dest));
            KVList removedValues;
            MergeRemove::exec(
                    baseCursor, destCursor,
                    nilConflict2, area, gc, removedValues);

            dest = destCursor.node();

            KVList kvs = b.gatherValues(dest);
            BOOST_CHECK(count(kvs, { frag(1,1) + frag(0xFF,8), 12 }));
            BOOST_CHECK(!count(kvs, 1));
            BOOST_CHECK(!count(kvs, 2));

            BOOST_CHECK_EQUAL(removedValues.size(), 2);
            BOOST_CHECK(count(removedValues, { frag(0,1) + frag(0xFF,8), 1 }));
            BOOST_CHECK(count(removedValues, { frag(0,1) + frag(0x0F,8), 2 }));

            printTitle("Cutoff - Simplify Branch");
            b.dump(dest);

            for (int i = 0; i < gc.oldNodes.size(); ++i)
                BOOST_CHECK_EQUAL(gc.oldNodes[i], base);
            gc.oldNodes.clear();
        }
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_remove_deep )
{
    MMapFile area(RES_CREATE);
    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        TrieBuilder b(area, gc, TriePtr::COPY_ON_WRITE);
        TrieBuilder bi(area, gc, TriePtr::IN_PLACE);

        TriePtr base = b.term({
                    {frag(0x0F, 8), 11},
                    {frag(0x0F0F, 16), 12},
                    {frag(0x0FFF, 16), 13}});

        TriePtr dest = bi.branch(frag(0x0F, 8), 21, // <- Conflict with 11
                b.branch(frag(0, 3), // <- To Simplify
                        b.term({{ frag(0x7, 3), 22 }}),
                        b.term({{ frag(0x7, 3), 12 }})), // <- To delete
                b.term({    { frag(0x0F, 7), 23 },
                            { frag(0x7F, 7), 13 }})); // <- To Delete


        printTitle("Deep - Original");
        b.dump(dest);

        auto conflict = [&](
                const TrieKey& key, uint64_t baseVal, uint64_t destVal)
        {
            KeyFragment kf = key.getFragment();
            BOOST_CHECK_EQUAL(kf , frag(0x0F, 8));
            BOOST_CHECK_EQUAL(baseVal, 11);
            BOOST_CHECK_EQUAL(destVal, 21);
            return false;
        };

        KVList removedValues;
        Cursor baseCursor(base, b.gather(base));
        Cursor destCursor(dest, b.gather(dest));
        MergeRemove::exec(
                baseCursor, destCursor, conflict, area, gc, removedValues);

        // This can vary depending on how well we simplify the trie.
        if (destCursor.node() != dest)
            dest = destCursor.node();

        KVList kvs = b.gatherValues(dest);

        KeyFragment prefix = frag(0x0F, 8);
        BOOST_CHECK(count(kvs, { prefix, 21 }));

        KeyFragment prefix0 = key(prefix, 0, frag(0,3));
        BOOST_CHECK(count(kvs, { key(prefix0, 0, frag(0x7,3)), 22 }));
        BOOST_CHECK(count(kvs, { key(prefix, 1, frag(0x0F, 7)), 23 }));

        BOOST_CHECK(!count(kvs, 11));
        BOOST_CHECK(!count(kvs, 12));
        BOOST_CHECK(!count(kvs, 13));

        BOOST_CHECK_EQUAL(removedValues.size(), 2);
        BOOST_CHECK(count(removedValues, { key(prefix0, 1, frag(0x7, 3)), 12 }));
        BOOST_CHECK(count(removedValues, { key(prefix, 1, frag(0x7F, 7)), 13 }));

        printTitle("Deep - Result");
        b.dump(dest);
    }
    MMAP_UNPIN_REGION;
}


BOOST_AUTO_TEST_CASE( test_insert_cutoff )
{
    MMapFile area(RES_CREATE);
    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        TrieBuilder b(area, gc, TriePtr::COPY_ON_WRITE);
        TrieBuilder bi(area, gc, TriePtr::IN_PLACE);

        TriePtr nil;

        TriePtr baseBranch, baseTerm0, baseTerm1;
        TriePtr base = b.branch(frag(0xFF,8), 20,
                baseBranch = b.branch(frag(0xFF, 8),
                        baseTerm0 = b.term({
                                    { frag(0x0F, 8), 11 },
                                    { frag(0xF0, 8), 12 }}),
                        baseTerm1 = b.term({
                                    { frag(0x0F, 8), 13 },
                                    { frag(0xF0, 8), 14 }})),
                nil);

        TriePtr dest = bi.branch(frag(0xFF, 8), 20,
                baseBranch,
                nil);

        TriePtr srcBranch, srcTerm;
        TriePtr src = bi.branch(frag(0xFF, 8), 20,
                srcBranch = bi.branch(frag(0xFF, 8),
                        baseTerm0,
                        srcTerm = bi.term({ // <- To insert
                                    { frag(0x0F, 8), 31 },
                                    { frag(0xFF, 8), 32 }})),
                nil);

        printTitle("Insert Cutoff - src");
        b.dump(src);

        printTitle("Insert Cutoff - dest");
        b.dump(dest);

        Cursor baseCursor(base, b.gather(base));
        Cursor srcCursor (src,  b.gather(src));
        Cursor destCursor(dest, b.gather(dest));

        KVList removedValues;
        NodeList srcKeepers;
        MergeInsert::exec(
                baseCursor, srcCursor, destCursor,
                nilConflict3, area, gc, srcKeepers, removedValues);


        printTitle("Insert Cutoff - Result");
        b.dump(dest);

        BOOST_CHECK_EQUAL(destCursor.node(), dest);

        KVList kvs = b.gatherValues(dest);

        KeyFragment prefix = frag(0xFF,8);
        BOOST_CHECK(count(kvs, { prefix, 20 }));

        prefix = key(prefix, 0, frag(0xFF,8));
        BOOST_CHECK(count(kvs, { key(prefix, 0, frag(0x0F,8)), 11 }));
        BOOST_CHECK(count(kvs, { key(prefix, 0, frag(0xF0,8)), 12 }));
        BOOST_CHECK(count(kvs, { key(prefix, 1, frag(0x0F,8)), 31 }));
        BOOST_CHECK(count(kvs, { key(prefix, 1, frag(0xFF,8)), 32 }));

        BOOST_CHECK(!count(kvs, 13));
        BOOST_CHECK(!count(kvs, 14));

        BOOST_CHECK_EQUAL(srcKeepers.size(), 2);
        BOOST_CHECK(count(srcKeepers, srcBranch));
        BOOST_CHECK(count(srcKeepers, srcTerm));
        // baseTerm should not be in here because it's part of base

        BOOST_CHECK_EQUAL(removedValues.size(), 2);
        BOOST_CHECK(count(removedValues, { key(prefix, 1, frag(0x0F,8)), 13 }));
        BOOST_CHECK(count(removedValues, { key(prefix, 1, frag(0xF0,8)), 14 }));

        // Should be marked for GC by MergeGC.
        BOOST_CHECK(
                find(gc.oldNodes.begin(), gc.oldNodes.end(), baseTerm1)
                !=  gc.oldNodes.end());
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_insert_deep )
{
    MMapFile area(RES_CREATE);
    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        TrieBuilder b(area, gc, TriePtr::COPY_ON_WRITE);
        TrieBuilder bi(area, gc, TriePtr::IN_PLACE);

        TriePtr nil;

        TriePtr srcTerm; // Should be in keeper
        TriePtr src = bi.branch(frag(0xF, 4), 10,
                bi.term({{ frag(0xF, 4), 11 }}), // splitDest
                bi.branch(frag(0x7, 3), 12, // conflicts with 22
                        srcTerm = bi.term({{ frag(0xFF, 8), 13 }}), // insertBranch
                        bi.branch(frag(0xFF, 8),
                                bi.term({{ frag(0x0F, 8), 14 }}), // Term -> Branch
                                bi.branch( frag(0xFF, 8), // Branch -> Term
                                        bi.term({{ frag(0xFF, 8), 15 }}),
                                        bi.term({{ frag(0xFF, 8), 16 }})))));

        TriePtr dest = bi.branch(frag(0xFF, 8), 22, // <- Conflict with 12
                nil,  // <- insert srcTerm1 here.
                b.branch(frag(0xFF, 8),
                        b.branch(frag(0xFF, 8), // Term -> Branch
                                b.term({{ frag(0xFF, 8), 23 }}),
                                b.term({{ frag(0xFF, 8), 24 }})),
                        b.term({{ frag(0xFF, 8), 25 }}))); // Branch -> Term

        printTitle("Insert Deep - src");
        b.dump(src);

        printTitle("Insert Deep - dest");
        b.dump(dest);

        auto conflict = [&] (
                const TrieKey& key,
                uint64_t baseVal, uint64_t srcVal, uint64_t destVal)
        {
            KeyFragment kf = key.getFragment();
            BOOST_CHECK_EQUAL(kf , frag(0xFF, 8));
            BOOST_CHECK_EQUAL(srcVal, 12);
            BOOST_CHECK_EQUAL(destVal, 22);
            return 42;
        };


        Cursor baseCursor;
        Cursor srcCursor (src,  b.gather(src));
        Cursor destCursor(dest, b.gather(dest));

        NodeList srcKeepers;
        KVList removedValues;
        MergeInsert::exec(
                baseCursor, srcCursor, destCursor,
                conflict, area, gc, srcKeepers, removedValues);

        BOOST_CHECK_NE(destCursor.node(), dest);
        dest = destCursor.node();

        printTitle("Insert Deep - Result");
        b.dump(dest);

        KVList kvs = b.gatherValues(dest);

        KeyFragment prefix = frag(0xF,4);
        BOOST_CHECK(count(kvs, { prefix, 10 }));
        BOOST_CHECK(count(kvs, { key(prefix, 0, frag(0xF,4)), 11 }));

        prefix = frag(0xFF,8);
        BOOST_CHECK(count(kvs, { key(prefix, 0, frag(0xFF,8)), 13 }));

        prefix = key(prefix, 1, frag(0xFF,8));
        BOOST_CHECK(count(kvs, { key(prefix, 0, frag(0x0F,8)), 14 }));

        prefix = key(prefix, 1, frag(0xFF, 8));
        BOOST_CHECK(count(kvs, { key(prefix, 0, frag(0xFF,8)), 15 }));
        BOOST_CHECK(count(kvs, { key(prefix, 1, frag(0xFF,8)), 16 }));

        prefix = key(frag(0xFF,8), 1, frag(0xFF,8));
        BOOST_CHECK(count(kvs, { key(prefix, 1, frag(0xFF,8)), 25 }));

        prefix = key(prefix, 0, frag(0xFF,8));
        BOOST_CHECK(count(kvs, { key(prefix, 0, frag(0xFF,8)), 23 }));
        BOOST_CHECK(count(kvs, { key(prefix, 1, frag(0xFF,8)), 24 }));

        // Conflicts were replaced by 42
        BOOST_CHECK(count(kvs, { frag(0xFF, 8), 42 }));
        BOOST_CHECK(!count(kvs, 12));
        BOOST_CHECK(!count(kvs, 22));

        BOOST_CHECK_EQUAL(srcKeepers.size(), 1);
        BOOST_CHECK(count(srcKeepers, srcTerm));

        BOOST_CHECK_EQUAL(removedValues.size(), 1);
        BOOST_CHECK(count(removedValues, { frag(0xFF, 8), 22 }));
    }
    MMAP_UNPIN_REGION;

}


BOOST_AUTO_TEST_CASE( test_insert_shrink_src )
{
    MMapFile area(RES_CREATE);
    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        TrieBuilder b(area, gc, TriePtr::COPY_ON_WRITE);
        TrieBuilder bi(area, gc, TriePtr::IN_PLACE);

        TriePtr nil;

        TriePtr dest = bi.branch(frag(0xFF,8), 10,
                b.term({{ frag(0xFF,8), 11 }}), nil);

        TriePtr src = bi.branch(frag(0xFFFF,16), 20,
                bi.term({{ frag(0xFF,8), 21 }}),
                bi.term({{ frag(0xFF,8), 22 }}));

        printTitle("Insert ShrinkSrc - src");
        b.dump(src);

        printTitle("Insert ShrinkSrc - dest");
        b.dump(dest);

        Cursor baseCursor;
        Cursor srcCursor (src,  b.gather(src));
        Cursor destCursor(dest, b.gather(dest));

        NodeList srcKeepers;
        KVList removedValues;
        MergeInsert::exec(
                baseCursor, srcCursor, destCursor,
                nilConflict3, area, gc, srcKeepers, removedValues);

        BOOST_CHECK_EQUAL(destCursor.node(), dest);

        printTitle("Insert ShrinkSrc - Result");
        b.dump(dest);

        KVList kvs = b.gatherValues(dest);

        KeyFragment prefix = frag(0xFF,8);
        BOOST_CHECK(count(kvs, { prefix, 10 }));
        BOOST_CHECK(count(kvs, { key(prefix, 0, frag(0xFF,8)), 11 }));

        prefix = frag(0xFFFF,16);
        BOOST_CHECK(count(kvs, { key(prefix, 0, frag(0xFF,8)), 21 }));
        BOOST_CHECK(count(kvs, { key(prefix, 1, frag(0xFF,8)), 22 }));

        BOOST_CHECK_EQUAL(srcKeepers.size(), 0);
        BOOST_CHECK(!count(srcKeepers, src)); // was recreated from scratch.

        BOOST_CHECK_EQUAL(removedValues.size(), 0);
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_diff_cutoff )
{
    MMapFile area(RES_CREATE);
    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        TrieBuilder b(area, gc, TriePtr::COPY_ON_WRITE);
        TrieBuilder bi(area, gc, TriePtr::IN_PLACE);

        TriePtr nil;

        TriePtr baseSub;
        TriePtr base = b.branch(frag(0xFF,8), 10,
                nil,
                baseSub = b.branch(frag(0xFF,8),
                        b.term({{ frag(0xFF,8), 11 }}),
                        b.term({{ frag(0xFF,8), 12 }})));

        TriePtr dest = b.branch(frag(0xFF,8), 10,
                b.term({{ frag(0xFF,8), 23 }}),
                baseSub);

        TriePtr srcBranch, srcTerm0, srcTerm1;
        TriePtr src = bi.branch(frag(0xFF,8), 10,
                nil,
                srcBranch = bi.branch(frag(0xFF,8), 31,
                        srcTerm0 = bi.term({{ frag(0xFF,8), 32 }}),
                        srcTerm1 = bi.term({{ frag(0xFF,8), 33 }})));


        printTitle("Diff Cutoffs - dest");
        b.dump(dest);

        printTitle("Diff Cutoffs - base");
        b.dump(base);

        printTitle("Diff Cutoffs - src");
        b.dump(src);

        NodeList srcKeepers;
        KVList removedValues;
        TriePtr newDest = MergeDiff::exec(
                base, src, dest,
                nilConflict3, nilConflict2, area, gc,
                srcKeepers, removedValues);

        BOOST_CHECK_NE(dest, newDest);

        printTitle("Diff Cutoffs - Result");
        b.dump(dest);

        KVList kvs = b.gatherValues(newDest);

        KeyFragment prefix = frag(0xFF,8);
        BOOST_CHECK(count(kvs, { prefix, 10 }));
        BOOST_CHECK(count(kvs, { key(prefix, 0, frag(0xFF,8)), 23 }));

        prefix = key(prefix, 1, frag(0xFF,8));
        BOOST_CHECK(count(kvs, { prefix, 31 }));
        BOOST_CHECK(count(kvs, { key(prefix, 0, frag(0xFF,8)), 32 }));
        BOOST_CHECK(count(kvs, { key(prefix, 1, frag(0xFF,8)), 33 }));

        BOOST_CHECK(!count(kvs, 11));
        BOOST_CHECK(!count(kvs, 12));

        BOOST_CHECK_EQUAL(srcKeepers.size(), 3);
        BOOST_CHECK(count(srcKeepers, srcBranch));
        BOOST_CHECK(count(srcKeepers, srcTerm0));
        BOOST_CHECK(count(srcKeepers, srcTerm1));

        BOOST_CHECK_EQUAL(removedValues.size(), 2);
        BOOST_CHECK(count(removedValues, { key(prefix, 0, frag(0xFF,8)), 11 }));
        BOOST_CHECK(count(removedValues, { key(prefix, 1, frag(0xFF,8)), 12 }));
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_diff_branches )
{
    MMapFile area(RES_CREATE);
    MMAP_PIN_REGION(area.region())
    {
        GCList gc(area);

        TrieBuilder b(area, gc, TriePtr::COPY_ON_WRITE);
        TrieBuilder bi(area, gc, TriePtr::IN_PLACE);

        TriePtr nil;

        TriePtr base = b.branch(frag(0xFF,8), 10, // value removed
                b.branch(frag(0xF0,8), // diff in prefix
                        b.term({{ frag(0xFF,8), 11 }}),
                        b.term({{ frag(0xFF,8), 12 }})),
                b.branch(frag(0x7F,7), 13, // replace (aka insert) by 23
                        b.term({{ frag(0xFF,8), 14 }}),
                        nil)); // branch 24 inserted

        TriePtr src = bi.branch(frag(0xFF,8),
                bi.branch(frag(0xFF,8), 20, // value inserted + diff in prefix
                        bi.term({{ frag(0xFF,8), 21 }}),
                        bi.term({{ frag(0xFF,8), 22 }})),
                bi.branch(frag(0x7F,7), 23, // replace (aka insert) from 13
                        nil, // branch 14 removed
                        bi.term({{ frag(0xFF, 8), 24 }})));

        TriePtr dest;
        {
            KeyFragment prefix = frag(0xFF,8);
            dest = b.insert(dest, prefix, 10);

            KeyFragment prefix0 = key(prefix, 0, frag(0xF0,8));
            dest = b.insert(dest, key(prefix0, 0, frag(0xFF,8)), 11);
            dest = b.insert(dest, key(prefix0, 1, frag(0xFF,8)), 12);

            KeyFragment prefix1 = key(prefix, 1, frag(0x7F,7));
            dest = b.insert(dest, prefix1, 13);
            dest = b.insert(dest, key(prefix1, 0, frag(0xFF,8)), 14);
        }

        printTitle("Diff Branches - src");
        b.dump(src);

        printTitle("Diff Branches - base");
        b.dump(base);

        printTitle("Diff Branches - dest");
        b.dump(dest);


        auto insConflict = [&] (
                const TrieKey& key,
                uint64_t baseVal, uint64_t srcVal, uint64_t destVal)
        {
            KeyFragment kf = key.getFragment();

            BOOST_CHECK_EQUAL(kf, frag(0xFF,8) + frag(1,1) + frag(0x7F,7));
            BOOST_CHECK_EQUAL(srcVal, 23);
            BOOST_CHECK_EQUAL(destVal, 13);

            return 42;
        };

        NodeList srcKeepers;
        KVList removedValues;
        TriePtr newDest = MergeDiff::exec(
                base, src, dest,
                insConflict, nilConflict2, area, gc, srcKeepers, removedValues);

        BOOST_CHECK_NE(dest, newDest);

        printTitle("Diff Branches - Result");
        b.dump(newDest);

        KVList kvs = b.gatherValues(newDest);

        KeyFragment prefix0 = key(frag(0xFF,8), 0, frag(0xFF,8));
        BOOST_CHECK(count(kvs, { prefix0, 20 }));
        BOOST_CHECK(count(kvs, { key(prefix0, 0, frag(0xFF,8)), 21 }));
        BOOST_CHECK(count(kvs, { key(prefix0, 1, frag(0xFF,8)), 22 }));

        KeyFragment prefix1 = key(frag(0xFF,8), 1, frag(0x7F,7));
        BOOST_CHECK(count(kvs, { prefix1, 42 }));
        BOOST_CHECK(count(kvs, { key(prefix1, 1, frag(0xFF,8)), 24 }));

        BOOST_CHECK(!count(kvs, 10));
        BOOST_CHECK(!count(kvs, 11));
        BOOST_CHECK(!count(kvs, 12));
        BOOST_CHECK(!count(kvs, 13));
        BOOST_CHECK(!count(kvs, 14));
        BOOST_CHECK(!count(kvs, 23));

        BOOST_CHECK_EQUAL(srcKeepers.size(), 0);

        BOOST_CHECK_EQUAL(removedValues.size(), 5);
        BOOST_CHECK(count(removedValues, 10));
        BOOST_CHECK(count(removedValues, 11));
        BOOST_CHECK(count(removedValues, 12));
        BOOST_CHECK(count(removedValues, 13));
        BOOST_CHECK(count(removedValues, 14));
    }
    MMAP_UNPIN_REGION;
}
