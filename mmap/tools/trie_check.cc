/** trie_check.cc                                 -*- C++ -*-
    Mathieu Stefani, 16 May 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Trie checking and repair logic
*/

#include "mmap/mmap_trie.h"
#include "mmap/trie_key.h"
#include "mmap/mmap_file.h"
#include "mmap/tools/trie_check.h"
#include "mmap/trie_allocator.h"
#include "mmap/memory_allocator.h"
#include "mmap/mmap_trie_node.h"
#include "mmap/mmap_trie_node_impl.h"
#include "jml/arch/exception_handler.h"
#include "mmap/mmap_trie_dense_branching_node.h"
#include "testing/mmap_check_test_utils.h"

using namespace std;

namespace Datacratic {
namespace MMap {

TrieChecker::TrieChecker(bool verbose) :
    verbose(verbose) {

}

bool TrieChecker::operator()(MutableTrieVersion &trie) {
    JML_TRACE_EXCEPTIONS(false)

    auto valid = bool { true };
    make_pin_guard(&trie, [&]()
    {
        auto checkPathEntries = [&](TriePath::const_iterator first,
                                    TriePath::const_iterator last) {
            for (auto it = first; it != last; ++it) {
                if (it->isNonTerminal()) {
                    const auto list = NodeOps::gatherKV(it->node(),
                            area(trie), 0);
                }
            }
        };

        auto checkPath = [&](const TriePath &path) {
            path.validate(area(trie), 0);
            checkPathEntries(std::begin(path), std::end(path));
        };

        auto checkDeltaPath = [&](TriePath &oldPath,
                                  TriePath &newPath) {
            const TriePath::iterator commonPathIt = newPath.commonSubPath(oldPath);
            checkPathEntries(commonPathIt, newPath.end());
        };


        TriePath leftPath = NodeOps::begin(trie.root, area(trie), 0);
        TriePath rightPath = NodeOps::end(trie.root, area(trie), 0);
        mCorruption.leftValidPath = leftPath;
        mCorruption.rightValidPath = rightPath;
        try {
            rightPath.advance(-1, area(trie), 0);

            mCorruption.rightValidPath = rightPath;

            try {
                checkPath(leftPath);
                ExcAssertEqual(leftPath.key(area(trie), 0).bits % 8, 0);
                leftPath.advance(1, area(trie), 0);

                while (leftPath.valid()) {
                    ExcAssertEqual(leftPath.key(area(trie), 0).bits % 8, 0);
                    checkDeltaPath(mCorruption.leftValidPath, leftPath);
                    mCorruption.leftValidPath = leftPath;
                    leftPath.advance(1, area(trie), 0);
                }
                mCorruption.leftValidPath = leftPath;


            } catch (const std::exception &e) {
                mCorruption.leftInvalidPath = leftPath;
            }

            try {
                while (rightPath.valid()) {
                    ExcAssertEqual(rightPath.key(area(trie), 0).bits % 8, 0);
                    checkDeltaPath(mCorruption.rightValidPath, rightPath);
                    mCorruption.rightValidPath = rightPath;
                    if (rightPath.entryNum() == 0) {
                        break;
                    }
                    rightPath.advance(-1, area(trie), 0);
                }
            } catch (const std::exception &e) {
                mCorruption.rightInvalidPath = rightPath;
            }

            const size_t size = NodeOps::size(trie.root, area(trie), 0);
            /* Yipi ! */
            if (mCorruption.leftValidPath.entryNum() == size && 
                mCorruption.rightValidPath.entryNum() == 0) {
            }
            /* Ups */
            else {
                valid = false;

            }
        } catch (const std::exception &e) {
            mCorruption.leftInvalidPath = rightPath;
            mCorruption.rightInvalidPath = rightPath;
            valid = false;
        }

    });

    return valid;

}

bool TrieRepair::operator()(MutableTrieVersion &trie, CorruptionArea &corruption) {
    JML_TRACE_EXCEPTIONS(false)
    const auto debug = bool { false };

    if (debug) {
        TrieChecker checker(false);
        cout << boolalpha;
        cout << "------- BEFORE -------\n";
        cout << "Valid = " << checker(trie) << endl;
    }

    return make_pin_guard(&trie, [&]() {
        auto mostCommonDepthBranch = [&](TriePath &lhs, TriePath &rhs) 
                            -> TriePath::iterator {
            for (auto it = end(rhs) - 2; it->valid(); --it) {
                if (!it->isNonTerminal()) {
                    continue;
                }
                if (!NodeOps::isBranchingNode(it->node(), area(trie), 0)) {
                    continue;
                }
                auto pos = find(begin(lhs), end(lhs), *it);
                if (pos == end(lhs)) {
                    continue;
                }

                return pos;

            }

            return end(lhs);
        };
        auto commonInvalidPath = mostCommonDepthBranch(corruption.leftInvalidPath,
                                    corruption.rightInvalidPath);
        auto commonValidPath = corruption.leftValidPath.commonSubPath(
                                    corruption.rightValidPath);
        auto commonValidInvalidPath = corruption.leftValidPath.commonSubPath(
                                    corruption.rightInvalidPath);

        /* First step, we identify the branch where the corruption is */
        GCList gc(area(trie));

        TriePtr parent;
        if (commonInvalidPath == end(corruption.leftInvalidPath)) {
            parent = corruption.leftInvalidPath.root();
        }
        else if (!commonInvalidPath->isTerminal()) {
            const auto node = commonInvalidPath->node();
            parent = node;
        }

        /* Second step, we gather all the { Key, Ptr } from that branch */
        auto kvs = NodeOps::gatherKV(parent, area(trie), 0);
        auto oldKvs { kvs };

        /* Third step, we remove the corrupted ptrs from the list */

        /* Soon to come: polymorphic lambdas */
        kvs.erase(remove_if(begin(kvs), end(kvs), [&](const KV &kv) {
            return kv.getPtr() == corruption.leftInvalidPath.lastNode(); 
        }), end(kvs));
        kvs.erase(remove_if(begin(kvs), end(kvs), [&](const KV &kv) {
            return kv.getPtr() == corruption.rightInvalidPath.lastNode();
        }), end(kvs));

        if (oldKvs.size() == kvs.size()) {
            return false;
        }

        /* Fourth step, we create a new DenseBranchingNode by calling makeNode */
        TriePtr newNode = makeNode(area(trie), 0, kvs, TriePtr::COPY_ON_WRITE,
                gc);

        /* Last step, we replace the old subtree by the new clean node we just
         * created
         */

        const TriePath newTree { newNode };
        TriePath subTree;
        if (parent == corruption.leftInvalidPath.root()) {
            subTree = parent;
        } else {
            copy(begin(corruption.leftInvalidPath), 
                    commonInvalidPath + 1, back_inserter(subTree));
        }
        const TriePath newRoot { replaceSubtreeRecursive(
                area(trie), 0, subTree, newTree, gc, trie.root, 0) };
        const TriePtr newRootPtr = newRoot.root();
        auto rootPtr = trie.trie->getRootPtr();
        *rootPtr = newRootPtr;

        /* Garbage collect the old path */
        gc.commit(subTree, trie.trie->gc());

        trie.root = newRoot.root();

        if (debug) {
            TrieChecker checker(false);
            cout << endl;
            cout << "------- AFTER -------\n";
            cout << "Valid = " << checker(trie) << endl;
            cout << endl;
        }
        return true;
    });

}


} // namespace MMap
} // namespace Datacratic
