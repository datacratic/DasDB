/** dirty_page_table_test.cc                                 -*- C++ -*-
    Rémi Attab, 29 Aug 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Tests for the dirty page table thingy.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap/dirty_page_table.h"

#include <boost/test/unit_test.hpp>
#include <iostream>
#include <vector>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;


uint64_t makeAddr(int group, int64_t page)
{
    uint64_t word = 1ULL << (group + 9);
    word |= word + page;
    return (word << page_shift) | ((1ULL << page_shift) - 1);
};


BOOST_AUTO_TEST_CASE( test_table_basics )
{
    DirtyPageTable table;

    auto markGroup = [&](int group) {
        // cerr << "--- MARK " << group << " ---" << endl;

        table.markPage(makeAddr(group, 0));
        table.markPage(makeAddr(group, -1));

        // cerr << "=> " << table.print() << endl << endl;
    };

    auto clearGroup = [&](int group) {
        // cerr << "--- CLEAR " << group << " ---" << endl;

        BOOST_CHECK(table.clearPage(makeAddr(group, 0)));
        BOOST_CHECK(!table.clearPage(makeAddr(group, 0)));

        BOOST_CHECK(!table.clearPage(makeAddr(group, -2)));
        BOOST_CHECK(table.clearPage(makeAddr(group, -1)));
        BOOST_CHECK(!table.clearPage(makeAddr(group, -1)));

        // cerr << "=> " << table.print() << endl << endl;
    };

    auto testGroup = [&](int group) {
        markGroup(group);
        markGroup(group+1);
        markGroup(group-1);

        clearGroup(group);
        clearGroup(group+1);
        clearGroup(group-1);
    };

    for (int i = 0; i < 10; ++i)
        testGroup(i);
}


BOOST_AUTO_TEST_CASE( test_table_find )
{
    DirtyPageTable table;

    BOOST_CHECK_LT(table.nextPage(0ULL), 0);
    BOOST_CHECK_LT(table.nextPage(makeAddr(10, 0xFF)), 0);

    vector<uint64_t> addr = {
        makeAddr(-1, 0),   makeAddr(-1, 1),   makeAddr(-1, 0xFF),

        makeAddr(0, 0),    makeAddr(0, 1),    makeAddr(0, 2),
        makeAddr(0, 0xAA), makeAddr(0, 0xAB), makeAddr(0, 0xAD),
        makeAddr(0, 0xFE), makeAddr(0, 0xFF),

        makeAddr(1, 0),    makeAddr(1, 1),    makeAddr(1, 2),
        makeAddr(1, 0xCB), makeAddr(1, 0xCC), makeAddr(1, 0xCD),
        makeAddr(1, 0xFE), makeAddr(1, 0xFF),

        makeAddr(4, 0),    makeAddr(4, 1),    makeAddr(4, 2),
        makeAddr(4, 0xCB), makeAddr(4, 0xCC), makeAddr(4, 0xCD),
        makeAddr(4, 0xFE), makeAddr(4, 0xFF),

        makeAddr(10, 0xFF)
    };

    for (int i = 0; i < addr.size(); ++i)
        table.markPage(addr[i]);

    int64_t page = table.nextPage(0);
    for (int i = 0; i < addr.size(); ++i) {
        BOOST_CHECK_GE(page, 0);
        BOOST_CHECK_EQUAL(page, addr[i] >> page_shift << page_shift);

        page = table.nextPage(page + page_size);
    }

    BOOST_CHECK_LT(page, 0);
}
