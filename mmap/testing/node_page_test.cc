/* node_page_test.cc
   Rémi Attab, 20 January 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Tests for node_page.h
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap/node_page.h"
#include "soa/utils/threaded_test.h"

#include <boost/test/unit_test.hpp>
#include <iostream>
#include <thread>
#include <future>
#include <stack>
#include <array>

using namespace std;
using namespace boost;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;

// Testing utilities used in both para and seq tests.
template <uint32_t size> struct TestUtil {
    typedef GenericNodePage<size> NodeT;

    static void checkEmpty (NodeT& nodes) {
        BOOST_REQUIRE_EQUAL(nodes.metadata.magic, NodeT::magicNum);
        BOOST_REQUIRE_EQUAL(
                nodes.metadata.full.numFull(), NodeT::metadataNodes);
    }

    static void checkSize () {
        //Not all NodePages uses the full Page
        if (sizeof(NodeT) >= page_size-size && sizeof(NodeT) <= page_size)
            return;

        cerr << "sizeof(GenericNodePage<" << size << ">)="
            << sizeof(NodeT) << endl;

        cerr << "{ numNodes = " << NodeT::numNodes
            << ", sizeof(FullBitmap) = " << sizeof(FullBitmap<NodeT::numNodes>)
            << ", metadataSize = " << NodeT::metadataSize
            << ", metadataNodes = " << NodeT::metadataNodes
            << ", metadataPadding = " << NodeT::metadataPadding
            << "}" << endl;

        BOOST_REQUIRE(sizeof(NodeT) == page_size);
    }
};


// Test various behaviours when allocating and deallocating in a sequential test
template<uint32_t size> struct SequentialTest {
    static void exec () {
        typedef GenericNodePage<size> NodeT;
        NodeT nodes;
        nodes.init();

        int64_t offset;
        bool needUpdate;

        TestUtil<size>::checkSize();

        for (int k = 0; k < 3; ++k) {

// Exceptions are thrown but it pollutes the console output so disable it.
#if 0
            // Try deallocating metadata nodes
            for (int i = 0; i < NodeT::metadataNodes; ++i) {
                BOOST_CHECK_THROW(
                        nodes.deallocate(i*size), ML::Assertion_Failure);
            }
#endif

            // Fully allocate the page.
            for (int i = NodeT::metadataNodes; i < NodeT::numNodes; ++i) {
                tie(offset, needUpdate) = nodes.allocate();
                BOOST_REQUIRE_EQUAL(needUpdate, i == NodeT::numNodes-1);
                BOOST_REQUIRE_EQUAL(offset, i*size);
            }

            // Over allocate the page.
            for(int i = 0; i < 3; ++i) {
                tie(offset, needUpdate) = nodes.allocate();
                BOOST_REQUIRE_LE(offset, -1);
            }

            // De-allocate and reallocate a random node.
            int64_t newOffset = (NodeT::numNodes/2) * size;
            needUpdate = nodes.deallocate(newOffset);
            BOOST_REQUIRE_EQUAL(needUpdate, true);

            tie(offset, needUpdate) = nodes.allocate();
            BOOST_REQUIRE_EQUAL(needUpdate, true);
            BOOST_REQUIRE_EQUAL(offset, newOffset);

            // Fully de-allocate the page.
            for (int i = NodeT::metadataNodes; i < NodeT::numNodes; ++i) {
                bool needUpdate = nodes.deallocate(i*size);
                BOOST_REQUIRE_EQUAL(needUpdate, i == NodeT::metadataNodes);
            }

// Exceptions are thrown but it pollutes the console output so disable it.
#if 0
            // Over de-allocate the page
            for (int i = NodeT::metadataNodes; i < NodeT::numNodes; ++i) {
                BOOST_CHECK_THROW(nodes.deallocate(i*size), ML::Exception);
            }
#endif

            // Make sure everything is properly deallocated.
            TestUtil<size>::checkEmpty(nodes);

        }
    }
};

BOOST_AUTO_TEST_CASE(test_seq) {
    SequentialTest<8>::exec();
    SequentialTest<12>::exec();
    SequentialTest<32>::exec();
    SequentialTest<64>::exec();
    SequentialTest<96>::exec();
    SequentialTest<192>::exec();
    SequentialTest<256>::exec();
}


// Starts a truck load of tests that randomly allocates and frees nodes.
template<uint32_t size>
struct ParallelTest
{

    enum {
        threadCount = 4,
        iterationCount = 10000
    };

    typedef GenericNodePage<size> NodeT;
    NodeT nodes;

    bool allocateNode (stack<int64_t>& s) {
        int64_t offset = nodes.allocate().first;
        if (offset >= 0) {
            s.push(offset);
            return true;
        }
        return false;
    }

    void deallocateHead(stack<int64_t>& s) {
        nodes.deallocate(s.top());
        s.pop();
    }

    void runThread (int id) {
        mt19937 engine(id);
        uniform_int_distribution<int> opDist(0, 1);
        uniform_int_distribution<int> numDist(0, size);
        stack<int64_t> allocatedOffsets;

        for (int i = 0; i < iterationCount; ++i) {

            if (allocatedOffsets.empty()) {
                if (!allocateNode(allocatedOffsets)) {
                    // nothing to allocate or deallocate,
                    //    take a nap and try again.
                    std::this_thread::yield();
                }
                continue;
            }
            else if (opDist(engine) && allocateNode(allocatedOffsets)) {
                for (int j = numDist(engine); j > 0; --j) {
                    if (!allocateNode(allocatedOffsets)) {
                        break;
                    }
                }
                continue;
            }

            // De-allocate if space is full or we're randomly chosen.
            for (int j = numDist(engine);
                 !allocatedOffsets.empty() && j > 0;
                 --j)
            {
                deallocateHead(allocatedOffsets);
            }
         }

        // We're done, cleanup.
        while (!allocatedOffsets.empty()) {
            deallocateHead(allocatedOffsets);
        }
    }

    void exec() {
        nodes.init();

        auto runFn = [&] (int id) {
            this->runThread(id);
            return 0;
        };

        ThreadedTest test;
        test.start(runFn, threadCount);
        test.joinAll(10000);

        // Make sure everything is properly deallocated.
        TestUtil<size>::checkEmpty(nodes);
    }

};


BOOST_AUTO_TEST_CASE(test_para) {
    ParallelTest<8>().exec();
    ParallelTest<48>().exec();
    ParallelTest<64>().exec();
    ParallelTest<192>().exec();
    ParallelTest<256>().exec();
}

