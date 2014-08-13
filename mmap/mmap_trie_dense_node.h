/* mmap_trie_dense_node.h                                      -*- C++ -*-
   Jeremy Barnes, 16 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the null trie node.
*/

#ifndef __mmap__trie_dense_node_h__
#define __mmap__trie_dense_node_h__

namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* DENSE INTERNAL NODE                                                       */
/*****************************************************************************/

/** A node that matches a prefix, then 2 bits, and has a dense list of the
    16 child nodes.
*/

struct DenseInternalNodeRepr {
    DenseInternalNodeRepr()
        : hasValue(false), bits(0), value(0)
    {
    }

    KeyFragment prefix;     ///< Node key prefix
    unsigned hasValue:1;    ///< Does the value exist?
    unsigned bits:2;        ///< How many bits to match (1 or 2);
    uint64_t value;         ///< Value at the node, if it has one

    TriePtr children[4];   ///< List of child nodes
};

// Decoded structure to pass around
struct DenseInternalNode {
    DenseInternalNodeRepr * operator -> () const
    {
        return node;
    }

    DenseInternalNodeRepr & operator * () const
    {
        return *node;
    }

    DenseInternalNodeRepr * node;
    TriePtr ptr;
};

// Operations structure
struct DenseInternalOps {

    typedef DenseInternalNode Node;
    enum { isTerminal = false };

    // Allocate empty
    static TriePtr alloc(MMapArea & area)
    {
        char * p = area.allocate(sizeof(DenseInternalNodeRepr), 0);
        DenseInternalNodeRepr * p2
            = reinterpret_cast<DenseInternalNodeRepr *>(p);
        new (p2) DenseInternalNodeRepr;

        TriePtr ptr;
        ptr.type = 2;
        ptr.data = area.encode(p);
        
        return ptr;
    }

    // Allocate to hold two leaves
    static TriePtr alloc(MMapArea & area, KeyFragment prefix,
                         KeyFragment suffix1, uint64_t value1,
                         KeyFragment suffix2, uint64_t value2)
    {
        using namespace std;

        char * p = area.allocate(sizeof(DenseInternalNodeRepr), 0);
        DenseInternalNodeRepr * node
            = reinterpret_cast<DenseInternalNodeRepr *>(p);
        new (node) DenseInternalNodeRepr;

        int bits = std::min(2, std::min(suffix1.bits, suffix2.bits));

        if (bits == 0) {
            cerr << "suffix1 = " << suffix1
                 << " suffix2 = " << suffix2
                 << endl;
        }


        int i1 = suffix1.removeBits(bits);
        int i2 = suffix2.removeBits(bits);

        if (i1 == i2) {
            cerr << "prefix = " << prefix << endl;
            cerr << "suffix1 = " << suffix1 << endl;
            cerr << "suffix2 = " << suffix2 << endl;
            cerr << "bits = " << bits << endl;
            throw ML::Exception("constructing two-leaf dense node with "
                                "common prefix; bits = %d", bits);
        }

        TriePtr p1 = makeLeaf(area, suffix1, value1);
        TriePtr p2 = makeLeaf(area, suffix2, value2);

        node->prefix = prefix;
        node->bits = bits;
        node->children[i1] = p1;
        node->children[i2] = p2;

        TriePtr ptr;
        ptr.type = 2;
        ptr.data = area.encode(p);
        
        return ptr;
    }

    // Allocate and add an extra leaf
    static TriePtr alloc(MMapArea & area,
                         const DenseInternalNode & nodeToCopy,
                         KeyFragment suffix, uint64_t value)
    {
        using namespace std;

        char * cp = area.allocate(sizeof(DenseInternalNodeRepr), 0);
        DenseInternalNodeRepr * node
            = reinterpret_cast<DenseInternalNodeRepr *>(cp);
        new (node) DenseInternalNodeRepr(*nodeToCopy);

        if (suffix.bits < nodeToCopy->bits)
            throw ML::Exception("new leaf with less bits");

        int i = suffix.removeBits(nodeToCopy->bits);
        TriePtr p = addLeaf(area, node->children[i], suffix, value);
        node->children[i] = p;

        TriePtr ptr;
        ptr.type = 2;
        ptr.data = area.encode(cp);
        
        return ptr;
    }

    static DenseInternalNode encode(const TriePtr & ptr, MMapArea & area)
    {
        DenseInternalNode result;
        result.node = reinterpret_cast<DenseInternalNodeRepr *>
            (area.dereference(ptr.data));
        result.ptr = ptr;
        return result;
    }

    static TriePtr decode(const DenseInternalNode & node, MMapArea & area)
    {
        throw ML::Exception("DenseInternalNode::decode()");
    }

    static size_t size(const DenseInternalNode & node,
                       MMapArea & area)
    {
        size_t result = node->hasValue;
        for (unsigned i = 0;  i < node->bits * 2;  ++i) {
            if (!node->children[i]) continue;
            NodeAccessor child(node->children[i], area);
            result += child.size();
        }
        return result;
    }

    static void
    forEachEntry(const DenseInternalNode & node, MMapArea & area,
                 const NodeOps::OnEntryFn & fn, int what)
    {
        if ((what & NodeOps::VALUE) && node->hasValue)
            fn(node->prefix, 0, node->value);

        if (what & NodeOps::CHILD) {
            for (unsigned i = 0;  i < node->bits * 2;  ++i) {
                if (node->children[i])
                    fn(node->prefix + KeyFragment(i, node->bits),
                       node->children[i], 0);
            }
        }
    }

    static TriePtr
    match(const DenseInternalNode & node, MMapArea & area,
          AccessorKey & key)
    {
        using namespace std;
        cerr << "***** match ******" << endl;
        cerr << "key before prefix " << node->prefix << ": " << key << endl;

        // 1.  Match the prefix
        if (!key.match(node->prefix)) return false;

        cerr << "prefix matches" << endl;

        // 2.  Match the value if we got to it
        if (key.finished()) {
            if (node->hasValue)
                key.finish(node->value);
            return TriePtr();
        }
        
        // 3.  If not... consume the next 2 bits to find which branch to
        //     take...

        cerr << "key before branch: " << key << endl;
        
        int branch = key.getBitsAndIncrement(node->bits);

        cerr << "key after branch: " << key << endl;

        cerr << "branch " << branch << " child = " << node->children[branch]
             << endl;

        return node->children[branch];
    }
    
    static TriePtr
    copyAndReplace(const DenseInternalNode & node,
                   MMapArea & area,
                   const KeyFragment & key,
                   const TriePtr & replaceWith)
    {
        throw ML::Exception("copyAndReplace on DenseInternalNode");
    }

    /** Make a copy of the current node.  The extraValues parameter
        is a hint that the node will soon be expanded to include
        the given number of extra values in addition to the ones
        currently in the node.
    */
    static TriePtr copyAndInsertLeaf(const DenseInternalNode & node,
                                     MMapArea & area,
                                     const KeyFragment & key,
                                     uint64_t value)
    {
        return alloc(area, node, key, value);
    }

    static void dump(const DenseInternalNode & node,
                     MMapArea & area,
                     int indent,
                     int maxDepth,
                     std::ostream & stream)
    {
        using namespace std;

        stream << "DenseInternal: offset " << node.ptr.data
               << " prefix: " << node->prefix << " bits: "
               << node->bits;
        if (node->hasValue)
            stream << "value: " << node->value;
        stream << endl;

        string id(indent, ' ');

        for (unsigned i = 0;  i < node->bits * 2;  ++i) {
            stream << id << i << ": ";
            NodeAccessor child(node->children[i], area);
            child.dump(indent + 4, maxDepth, stream);
            stream << endl;
        }
    }
};

} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__trie_dense_node_h__ */

