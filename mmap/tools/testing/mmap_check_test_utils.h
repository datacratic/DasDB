/** mmap_check.cc                                 -*- C++ -*-
    Mathieu Stefani, 05 June 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Utils for mmap checking tests
*/

#ifndef MMAP_CHECK_TEST_UTILS_H
#define MMAP_CHECK_TEST_UTILS_H

#include "mmap/memory_region.h"
#include "mmap/mmap_trie.h" 
#include "mmap/testing/mmap_test.h"
#include "mmap/mmap_trie_node_impl.h"
#include "mmap/mmap_trie_large_key_nodes.h"
#include "mmap/mmap_trie_sparse_nodes.h"
#include "mmap/mmap_trie_terminal_nodes.h"
#include "mmap/mmap_trie_null_node.h"
#include "mmap/mmap_trie_inline_nodes.h"
#include "mmap/mmap_trie_compressed_nodes.h"
#include "mmap/mmap_trie_dense_branching_node.h"
#include "mmap/mmap_trie_ptr.h"

#define NS_PREFIX Datacratic::MMap
#define ADD_NS(Identifier) NS_PREFIX::Identifier 

/* Boilerplate but eh, worth it */
namespace internal {
    template<typename T> struct is_trie {
        static constexpr bool value = false;
    };
    
#define DECLARE_TRIE(TrieVersion) \
    template<> struct is_trie<TrieVersion> { \
        static constexpr bool value = true; \
    } 

    DECLARE_TRIE(ADD_NS(ConstTrieVersion));
    DECLARE_TRIE(ADD_NS(MutableTrieVersion));
    DECLARE_TRIE(ADD_NS(TransactionalTrieVersion));
}

template<typename TrieVersion> 
Datacratic::MMap::MemoryAllocator &area(const TrieVersion &trie) {
    static_assert(internal::is_trie<TrieVersion>::value,
            "The argument must either be a ConstTrieVersion, "
            "MutableTrieVersion or TransactionalTrieVersion");
    return trie.trie->area();
}

/* Unfortunately, gcc 4.6 does not support this kind of template pack
 * expansion and says he is sorry not to implement that :
 *
 * sorry, unimplemented: cannot expand ‘Bits ...’ into a fixed-length argument list
 *
 * Works with gcc >= 4.7
 */

#if 0
template<typename Data, size_t Bit, size_t ...Rest>
struct FlipBitsImpl {
    static void flip(Data &data) {
        flip_bit<Bit>(data);
        FlipBitsImpl<Data, Rest...>::flip(data);
    }
};

template<typename Data, size_t Bit>
struct FlipBitsImpl<Data, Bit> {
    static void flip(Data &data) {
        flip_bit<Bit>(data);
    }
};

template<typename Data, size_t ...Bits> 
using FlipBits = FlipBitsImpl<Data, Bits...>;

template<size_t ...Bits, typename Data>
void
flip_bits(Data &data) {
   FlipBits<Data, Bits...>::flip(data); 
}
#endif

template<size_t Bit, typename Data>
inline typename std::enable_if<std::is_integral<Data>::value, void>::type
flip_bit(Data &data) {
    static_assert(Bit < sizeof(Data) * CHAR_BIT,
            "Invalid bit number");
    data ^= (1 << Bit);
}

/* Where in the trie the corruption should occur */
enum PositionHint {
    Begin,
    Middle,
    End
};

template<int Node> struct node_traits {
};

#define DECLARE_NODE_TRAITS(NodeType, NodeRepr, NodeOps) \
    template<> struct node_traits<NodeType> { \
        typedef NodeRepr repr_type; \
        typedef NodeOps ops_type; \
    }


DECLARE_NODE_TRAITS(
    ADD_NS(SparseTerm), 
    ADD_NS(SparseNodeRepr), 
    ADD_NS(SparseNodeOps)
);
DECLARE_NODE_TRAITS(
    ADD_NS(CompressedTerm), 
    ADD_NS(CompressedNodeRepr), 
    ADD_NS(CompressedNodeOps)
);
DECLARE_NODE_TRAITS(
    ADD_NS(BasicKeyedTerm), 
    ADD_NS(BasicKeyedTerminalNodeRepr), 
    ADD_NS(BasicKeyedTerminalOps)
);
DECLARE_NODE_TRAITS(
    ADD_NS(LargeKeyTerm), 
    ADD_NS(LargeKeyNodeRepr), 
    ADD_NS(LargeKeyNodeOps)
);
DECLARE_NODE_TRAITS(
    ADD_NS(InlineTerm), 
    ADD_NS(InlineNode), 
    ADD_NS(InlineNodeOps)
);


#if 0
    template<int NodeType> using integral_node = std::integral_constant<int, NodeType>;
#endif
template<int NodeType> struct constant_node : std::integral_constant<int, NodeType> { };

template<int Node> struct NodeCorruption {
    static void corrupt(Datacratic::MMap::MemoryRegion &region, 
                        const Datacratic::MMap::TriePtr &ptr) {
        using namespace Datacratic::MMap;
        typedef typename node_traits<Node>::repr_type ReprType;
        const size_t size = sizeof(ReprType);
        const uint64_t offset = decode_offset(ptr);
        RegionPtr<ReprType> node_ptr(region, offset);
        char *raw = reinterpret_cast<char *>(node_ptr.get());
        for (size_t i = 0; i < size; ++i, ++raw) {
            flip_bit<3>(*raw);
            
        }
    }

private:
    static uint64_t offset_helper(const Datacratic::MMap::TriePtr &ptr, 
                                  std::true_type) {
        return ptr.data;
    }

    static uint64_t offset_helper(const Datacratic::MMap::TriePtr &ptr, 
                                  std::false_type) {
        typedef typename node_traits<Node>::ops_type OpsType;
        return OpsType::Node::decodeOffset(ptr.data);
    }

    static uint64_t decode_offset(const Datacratic::MMap::TriePtr &ptr) {
        return offset_helper(ptr, std::is_same<
                constant_node<ADD_NS(InlineTerm)>, 
                constant_node<Node>>());
    }
};


template<> struct NodeCorruption<ADD_NS(DenseBranch)> {
    static void corrupt(Datacratic::MMap::MemoryRegion &region, 
                        const Datacratic::MMap::TriePtr &ptr) {
        using namespace Datacratic::MMap;
        const auto offset = DenseBranchingNodeOps::Node::decodeOffset(ptr.data);
        RegionPtr<DenseBranchingNodeRepr> repr(region, offset);
        repr->numBits = 0;
    }
};

template<int Node> void corrupt_node(
        Datacratic::MMap::MemoryRegion &region, 
        const Datacratic::MMap::TriePtr &ptr) {
    NodeCorruption<Node>::corrupt(region, ptr);
}

template<int N> struct is_null_node : std::false_type { };

template<> struct is_null_node<ADD_NS(NullTerm)> : std::true_type { };

template<bool Cond, typename T, T A, T B> struct constant_if {
    static constexpr T value = A;
};

template<typename T, T A, T B> struct constant_if<false, T, A, B> {
    static constexpr T value = B;
};

static inline int randrange(int lower, int upper) {
    /* Could use C++11 new random facilities here but nope */ 
    return lower + (rand() % (int)(upper - lower + 1));
}

/* We don't do anything for NullNode */
template<int Node> inline bool do_corrupt(
        const Datacratic::MMap::ConstTrieVersion &version,
        PositionHint hint,
        std::true_type)  {
    /* Should return false instead but mmap_check_test run_test function
     * will fail for NullNode otherwise */
    return true;
}

template<int Node> inline bool do_corrupt(
        const Datacratic::MMap::ConstTrieVersion &version,
        PositionHint hint,
        std::false_type) 
{
    using namespace Datacratic::MMap;
    using namespace std;
    return make_pin_guard(&version, [&]() {
        vector<TriePtr> ptrs;
        const size_t size = NodeOps::size(version.root, area(version), 0);
        for (size_t i = 0; i < size; ++i) {
             const auto path = NodeOps::findIndex(version.root, area(version), 
                                                  0, i);
             for (const auto &entry: path) {
                 if (!entry.isNonTerminal()) {
                    continue;
                 }
                 const auto node = entry.node();
                 if (node.type == static_cast<NodeType>(Node)) {
                     if (find(begin(ptrs), end(ptrs), node) == end(ptrs)) {
                         ptrs.push_back(node);
                     }
                 }
             }
        }

        auto getPtr = [&]() {
            if (ptrs.empty()) {
                return TriePtr();
            }
            typedef vector<TriePtr>::size_type size_type;
            const auto size = ptrs.size();
            if (hint == Begin) {
                const auto upper = max(size_type(3), size);
                return ptrs[randrange(1, upper)];
            }
            else if (hint == Middle) {
                return ptrs[size / 2];
            }
            else {
                const auto lower = max(size_type(0), size - 4);
                return ptrs[randrange(lower, size - 1)];
            }
        };
        const TriePtr ptr { getPtr() }; 
        if (!ptr) {
            return false;
        }
        corrupt_node<Node>(area(version).region(), ptr);
        return true;
    });

}

template<int Node>
bool inline corrupt(const Datacratic::MMap::ConstTrieVersion &current, 
                    PositionHint hint = Begin) {
    return do_corrupt<Node>(current, hint, is_null_node<Node>());
}

#endif
