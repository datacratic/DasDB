/* mmap_trie_iterator_test.cc
   Jeremy Barnes, 2 September 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the mmap trie class.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "jml/utils/smart_ptr_utils.h"
#include "jml/utils/string_functions.h"
#include "jml/arch/atomic_ops.h"
#include <boost/thread.hpp>
#include <boost/function.hpp>
#include <iostream>
#include "mmap/mmap_trie.h"
#include "mmap/mmap_trie_node.h"
#include "mmap/mmap_file.h"

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;

BOOST_AUTO_TEST_CASE( test_iterator_basics )
{
    enum { TrieId = 1 };

    cerr << "sizeof(TrieIterator) = "
         << sizeof(TrieIterator) << endl;
    cerr << "sizeof(TriePathEntry) = " << sizeof(TriePathEntry) << endl;
    cerr << "sizeof(TriePath) = " << sizeof(TriePath) << endl;

    MMapFile area(RES_CREATE);
    area.trieAlloc.allocate(TrieId);

    Trie trie = area.trie(TrieId);
    auto current = *trie;

    current.insert(123, 456);

    current.dump();

    BOOST_CHECK_EQUAL(current.begin().entryNum(), 0);
    BOOST_CHECK_EQUAL(current.end().entryNum(), 1);
    cerr << current.begin() << endl;
    BOOST_CHECK_EQUAL(current.begin().valid(), true);
    BOOST_CHECK_EQUAL(current.end().valid(), false);
    BOOST_CHECK_EQUAL(current.begin() + 1, current.end());
    BOOST_CHECK_NE(current.begin(), current.end());
    BOOST_CHECK_LT(current.begin(), current.end());
    BOOST_CHECK_EQUAL(current.begin().value(), 456);

    current.reset();

    area.trieAlloc.deallocate(TrieId);
}

#if 1
BOOST_AUTO_TEST_CASE( test_sequential_insert )
{
    enum { TrieId = 42 };

    MMapFile area(RES_CREATE);
    area.trieAlloc.allocate(TrieId);

    Trie trie = area.trie(TrieId);
    auto current = *trie;

    BOOST_CHECK_EQUAL(current.size(), 0);

    //current.dump();

    int n = 1000;

    BOOST_CHECK_EQUAL(current.begin(), current.end());
    cerr << current.begin() << endl;
    BOOST_CHECK_EQUAL(current.begin().valid(), false);
    BOOST_CHECK_EQUAL(current.end().valid(), false);
    BOOST_CHECK_EQUAL(current.end() - current.begin(), 0);
    BOOST_CHECK_EQUAL(std::distance(current.begin(), current.end()), 0);

    for (unsigned i = 0;  i < n;  ++i) {
        //cerr << endl;
        //cerr << "inserting " << i << endl;

        current.insert(i, i);

        //current.dump();
        
        BOOST_CHECK_EQUAL(current.begin().entryNum(), 0);
        BOOST_CHECK_EQUAL(current.end().entryNum(), i + 1);
        BOOST_CHECK_EQUAL(current.begin().valid(), true);
        BOOST_CHECK_EQUAL(current.end().valid(), false);
        BOOST_CHECK_EQUAL(current.begin() + current.size(), current.end());
        BOOST_CHECK_EQUAL(current.end() - current.size(), current.begin());
        BOOST_CHECK_NE(current.begin(), current.end());
        BOOST_CHECK_LT(current.begin(), current.end());

        BOOST_CHECK_EQUAL(std::distance(current.begin(), current.end()), current.size());
        BOOST_CHECK_EQUAL(std::distance(current.end(), current.begin()),
                          -current.size());
        
        BOOST_CHECK_EQUAL(current.find(i).value(), i);
        BOOST_CHECK_EQUAL(current.size(), i + 1);

        int i2 = 0;
        for (TrieIterator it = current.begin(), end = current.end();
             it != end;  ++it, ++i2) {
            BOOST_CHECK_EQUAL(it.value(), i2);
        }
        
        BOOST_CHECK_EQUAL(i2, i + 1);

        for (unsigned j = 0;  j <= i;  ++j) {
            TrieIterator it = current.begin() + j;
            BOOST_CHECK_EQUAL(it.value(), j);
            BOOST_CHECK_EQUAL((unsigned)it.key().cast<uint64_t>(), j);
            it = current.end() - (current.size() - j);
            BOOST_CHECK_EQUAL(it.value(), j);
            BOOST_CHECK_EQUAL((unsigned)it.key().cast<uint64_t>(), j);
        }
    }

    //current.dump();

    BOOST_CHECK_EQUAL(current.size(), n);

    for (unsigned i = 0;  i < n;  ++i) {
        BOOST_CHECK_EQUAL(current.count(i), 1);
    }

    current.clear();
    BOOST_CHECK_EQUAL(current.size(), 0);
    BOOST_CHECK_EQUAL(current.begin(), current.end());
    BOOST_CHECK_EQUAL(current.end() - current.begin(), 0);
    BOOST_CHECK_EQUAL(std::distance(current.begin(), current.end()), 0);
    current.reset();

    area.trieAlloc.deallocate(TrieId);
}
#endif
