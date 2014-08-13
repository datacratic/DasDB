/* mmap_trie_basics_test.cc
   Jeremy Barnes, 10 August 2011
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
#include "mmap/mmap_file.h"
#include "mmap/mmap_trie.h"
#include "mmap/mmap_trie_node.h"
#include "mmap/bin_string.h"

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;

BOOST_AUTO_TEST_CASE( test_trie_ptr )
{
    // Make sure that a null pointer has a zero integer representation.
    TriePtr p;
    BOOST_CHECK(!p);
    BOOST_CHECK_EQUAL(p, 0);

    TriePtr p2(BinaryBranch);
    BOOST_CHECK(p2);
    BOOST_CHECK_EQUAL(p2, 1ULL);

    // Make sure that one with a low value in data has a small integer
    // representation
    TriePtr p3(BinaryBranch, 1);
    BOOST_CHECK(p3);
    BOOST_CHECK_EQUAL(p3, (1 << TriePtr::MetaBits) | 1);

    TriePtr p4(BinaryBranch, 1);
    p4.state = TriePtr::IN_PLACE;
    BOOST_CHECK_EQUAL(p4,
            (1 << TriePtr::MetaBits) | (1 << TriePtr::TypeBits) | 1);
}

BOOST_AUTO_TEST_CASE( test_trie_key)
{
    auto toFrag = [](const TrieKey& key) -> KeyFragment {
        return key.getFragment();
    };

    auto toFragAndBack = [](const TrieKey& key) -> TrieKey {
        return TrieKey(key.getFragment());
    };

    const uint64_t key64 = 0x123456789ABCDEF0;
    BOOST_CHECK_EQUAL(toFrag(key64).getKey(), key64);
    BOOST_CHECK_EQUAL((uint64_t)toFragAndBack(key64).cast<uint64_t>(), key64);

    const uint32_t key32 = 0x12345678;
    BOOST_CHECK_EQUAL(toFrag(key32).getKey(), key32);
    BOOST_CHECK_EQUAL((uint32_t)toFragAndBack(key32).cast<uint64_t>(), key32);

    const uint16_t key16 = 0x1234;
    BOOST_CHECK_EQUAL(toFrag(key16).getKey(), key16);
    BOOST_CHECK_EQUAL((uint16_t)toFragAndBack(key16).cast<uint64_t>(), key16);

    const uint8_t key8 = 0x12;
    BOOST_CHECK_EQUAL(toFrag(key8).getKey(), key8);
    BOOST_CHECK_EQUAL((uint8_t)toFragAndBack(key8).cast<uint64_t>(), key8);

    const string keyStr = "I'm too tired to be clever... Try again later.";
    string resultStr = toFragAndBack(keyStr).cast<string>();
    BOOST_CHECK_EQUAL(keyStr, resultStr);

    const char* keyChar = "Insert interesting quote here.";
    string resultChar = toFragAndBack(keyChar).cast<string>();
    BOOST_CHECK_EQUAL(string(keyChar), resultChar);

    toFrag("Pass a string literal directly to see if it works.");

    // \todo This kinda complicates things...
    // The bit range ops stuff simply can't deal with this...

    // struct keyStruct_t {
    //     uint16_t a;
    //     uint64_t b;
    //     uint32_t c;

    //     keyStruct_t():
    //         a(0x1234), b(0x123456789ABCDEF0), c(0x89ABCDEF)
    //     {}

    //     bool operator==(const keyStruct_t& rhs) {
    //         return a == rhs.a && b == rhs.b && c == rhs.c;
    //     }

    // } keyStruct;

    // keyStruct_t resultStruct = toFragAndBack(keyStruct);
    // BOOST_CHECK(resultStruct == keyStruct);

    // typedef pair<uint64_t, uint16_t> pair_t;
    // pair_t keyPair = make_pair(
    //         uint64_t(0x123456789ABCDEF0),
    //         uint32_t(0x89ABCDEF));
    // pair_t resultPair = toFragAndBack(keyPair);
    // BOOST_CHECK(resultPair == keyPair);
}

BOOST_AUTO_TEST_CASE( test_trie_key_explicit )
{
    const uint64_t small_key64 = 0x123;
    BOOST_CHECK_EQUAL(TrieKey(small_key64).cast<uint64_t>(), small_key64);
    
    const uint64_t big_key64 = 0x1234567890ABCDEF;
    BOOST_CHECK_EQUAL(TrieKey(big_key64).cast<uint64_t>(), big_key64);

    const std::string strKey("meow");
    BOOST_CHECK_EQUAL(TrieKey(strKey).cast<std::string>(), strKey);

    const std::string nullString;
    BOOST_CHECK_EQUAL(TrieKey(nullString).cast<std::string>(), nullString);

    const BinString binString_uint({1, 2, 3, 4, 5});
    BOOST_CHECK_EQUAL(TrieKey(binString_uint).cast<BinString>(), binString_uint);

    BinString binString_str;
    binString_str.append(std::string("meow"));
    BOOST_CHECK_EQUAL(TrieKey(binString_str).cast<BinString>(), binString_str);
}

BOOST_AUTO_TEST_CASE( test_key_fragment )
{
    KeyFragment frag(1, 16);  // one in 16 bits
    BOOST_CHECK_EQUAL(frag.getBits(1), 0);
    BOOST_CHECK_EQUAL(frag.getBits(16), 1);
    BOOST_CHECK_EQUAL(frag.bits, 16);
    BOOST_CHECK_EQUAL(frag.removeBits(1), 0);
    BOOST_CHECK_EQUAL(frag.bits, 15);
    BOOST_CHECK_EQUAL(frag.removeBits(15), 1);
    BOOST_CHECK_EQUAL(frag.bits, 0);
    BOOST_CHECK(frag.empty());
}

BOOST_AUTO_TEST_CASE( test_key_fragment_common_prefix )
{
    KeyFragment frag0(0, 0);
    KeyFragment frag1(1, 1);
    KeyFragment frag2(-1ULL, 64);
    KeyFragment frag3(65535, 16);
    KeyFragment frag4(65536, 64);
    
    BOOST_CHECK_EQUAL(frag3.getBits(16), 65535);
    BOOST_CHECK_EQUAL(frag3.getBits(15), 32767);
    BOOST_CHECK_EQUAL(frag3.getBits(0), 0);

    // Check that the common prefix of something and itself is itself
    BOOST_CHECK_EQUAL(frag0.commonPrefix(frag0), frag0);
    BOOST_CHECK_EQUAL(frag1.commonPrefix(frag1), frag1);
    BOOST_CHECK_EQUAL(frag2.commonPrefix(frag2), frag2);
    BOOST_CHECK_EQUAL(frag3.commonPrefix(frag3), frag3);
    BOOST_CHECK_EQUAL(frag4.commonPrefix(frag4), frag4);

    // Check that the common prefix of something and an empty prefix is
    // the empty prefix
    BOOST_CHECK_EQUAL(frag1.commonPrefix(frag0), frag0);
    BOOST_CHECK_EQUAL(frag2.commonPrefix(frag0), frag0);
    BOOST_CHECK_EQUAL(frag3.commonPrefix(frag0), frag0);
    BOOST_CHECK_EQUAL(frag4.commonPrefix(frag0), frag0);

    BOOST_CHECK_EQUAL(KeyFragment(0, 64).commonPrefix(KeyFragment(1, 64)).bits,
                      63);
    BOOST_CHECK_EQUAL(KeyFragment(0, 64).commonPrefix(KeyFragment(3, 64)).bits,
                      62);
    BOOST_CHECK_EQUAL(KeyFragment(0, 64).commonPrefix(KeyFragment(255, 64)).bits,
                      56);

    KeyFragment frag5(1, 64);
    KeyFragment frag6(9874, 64);

    KeyFragment prefix = frag5.commonPrefix(frag6);

    cerr << "prefix = " << prefix << endl;

    BOOST_CHECK_EQUAL(prefix, KeyFragment(0, 50));

    KeyFragment suffix5 = frag5.suffix(prefix.bits);
    KeyFragment suffix6 = frag6.suffix(prefix.bits);

    cerr << "suffix5 = " << suffix5 << endl;
    cerr << "suffix6 = " << suffix6 << endl;

    BOOST_CHECK_EQUAL(prefix + suffix5, frag5);
    BOOST_CHECK_EQUAL(prefix + suffix6, frag6);


    KeyFragment frag7(0, 63);
    KeyFragment frag8(2, 63);

    KeyFragment prefix78 = frag7.commonPrefix(frag8);
    BOOST_CHECK_EQUAL(prefix78, frag8.commonPrefix(frag7));
    BOOST_CHECK_EQUAL(prefix78, KeyFragment(0, 61));
    
    KeyFragment suffix7 = frag7.suffix(prefix78.bits);
    KeyFragment suffix8 = frag8.suffix(prefix78.bits);

    BOOST_CHECK_EQUAL(prefix78 + suffix7, frag7);
    BOOST_CHECK_EQUAL(prefix78 + suffix8, frag8);
}

BOOST_AUTO_TEST_CASE( test_key_fragment_copy_bits )
{
    typedef 
        ML::Bit_Buffer<uint64_t, ML::Simple_Mem_Buffer<uint64_t> >
        SeqBitBuffer;

    {
        KeyFragment::KeyVec src;
        src.resize(1, -1ULL);
        SeqBitBuffer srcBuf(&src[0]);

        KeyFragment::KeyVec dest;
        dest.resize(1, 0);

        KeyFragment::copyBits(dest, 0, srcBuf, 64);

        BOOST_CHECK_EQUAL(src, dest);

    }

    {
        KeyFragment::KeyVec src;
        src.push_back(0xAAAAAAAAAAAAAAAA);
        src.push_back(0x5555555555555555);
        SeqBitBuffer srcBuf(&src[0]);

        KeyFragment::KeyVec dest;
        dest.resize(2, 0);

        KeyFragment::copyBits(dest, 0, srcBuf, 32);
        KeyFragment::copyBits(dest, 32, srcBuf, 64);
        KeyFragment::copyBits(dest, 96, srcBuf, 32);

        BOOST_CHECK_EQUAL(src, dest);
    }


    {
        KeyFragment::KeyVec src;
        src.push_back(0xAAAABBBBCCCCDDDD);
        src.push_back(0x5555555555555555);
        src.push_back(0x6666666666666666);
        src.push_back(0x1111222233334444);
        SeqBitBuffer srcBuf(&src[0]);

        KeyFragment::KeyVec dest;
        dest.resize(4, 0);

        KeyFragment::copyBits(dest, 0, srcBuf, 16);
        KeyFragment::copyBits(dest, 16, srcBuf, 32);
        KeyFragment::copyBits(dest, 48, srcBuf, 128);
        KeyFragment::copyBits(dest, 176, srcBuf, 32);
        KeyFragment::copyBits(dest, 208, srcBuf, 16);
        KeyFragment::copyBits(dest, 224, srcBuf, 32);

        BOOST_CHECK_EQUAL(src, dest);
    }
}


// \todo Could be a little more elaborate.
BOOST_AUTO_TEST_CASE( test_key_fragment_consume )
{
    KeyFragment frag0(1, 64);
    KeyFragment frag1(2, 64);

    KeyFragment temp = frag0;
    BOOST_CHECK(!temp.consume(frag1));
    BOOST_CHECK(!temp.empty());

    BOOST_CHECK(!(frag1.consume(frag0) && frag1.empty()));
    BOOST_CHECK(!(frag0.consume(frag1) && frag0.empty()));

}

BOOST_AUTO_TEST_CASE( test_large_keys )
{
    MMapFile area(RES_CREATE);
    MemoryRegion& region = area.region();

    const string str("0123456789876543210");
    KeyFragment frag0 = TrieKey(str).getFragment();

    KeyFragmentRepr repr0 = MMAP_PIN_REGION_RET(region, frag0.allocRepr(area));
    BOOST_CHECK(repr0.isValid());
    BOOST_CHECK(!repr0.isInline());

    KeyFragment frag1 =
        MMAP_PIN_REGION_RET(region, KeyFragment::loadRepr(repr0, area));
    MMAP_PIN_REGION_INL(region, KeyFragment::deallocRepr(repr0, area));

    // Check op on 2 equal KF.
    BOOST_CHECK_EQUAL(frag0, frag1);
    BOOST_CHECK_EQUAL(frag0.commonPrefix(frag1).bits, frag0.bits);

    KeyFragment frag2 = frag0 + frag0;
    KeyFragmentRepr repr2 = MMAP_PIN_REGION_RET(region, frag2.allocRepr(area));

    // check op on different KFs.
    BOOST_CHECK_EQUAL(frag2.bits, frag0.bits + frag1.bits);
    BOOST_CHECK_EQUAL(frag2.commonPrefix(frag0), frag0);
    BOOST_CHECK_EQUAL(frag2.suffix(frag0.bits), frag0);

    // Modify frag2.
    BOOST_CHECK(frag2.consume(frag0));
    BOOST_CHECK_EQUAL(frag2, frag0);

    // Check to see that nothing got mangled.
    for (int i = 0; i < str.size(); ++i)
        BOOST_CHECK_EQUAL(frag2.removeBits(8), str[i]);

    // repr2 should still contain frag0 + frag0
    KeyFragmentRepr repr3 = 
        MMAP_PIN_REGION_RET(region, KeyFragment::copyRepr(repr2, area));
    BOOST_CHECK(repr3.isValid());
    BOOST_CHECK(!repr3.isInline());

    KeyFragment frag3 = 
        MMAP_PIN_REGION_RET(region, KeyFragment::loadRepr(repr3, area));
    MMAP_PIN_REGION_INL(region, KeyFragment::deallocRepr(repr3, area));

    KeyFragment frag4 = 
        MMAP_PIN_REGION_RET(region, KeyFragment::loadRepr(repr2, area));
    MMAP_PIN_REGION_INL(region, KeyFragment::deallocRepr(repr2, area));

    BOOST_CHECK_EQUAL(frag3, frag4);
    BOOST_CHECK_EQUAL(frag3, frag0 + frag0);
    BOOST_CHECK_NE(frag3, frag2);

    // Check to see that we can still revert automatically to an inline mode.
    auto bitVec4 = frag4.removeBitVec(frag4.bits - 64);

    KeyFragmentRepr repr4 = MMAP_PIN_REGION_RET(region, frag4.allocRepr(area));
    BOOST_CHECK(repr4.isValid());
    BOOST_CHECK(repr4.isInline());

    MMAP_PIN_REGION_INL(region, KeyFragment::deallocRepr(repr4, area));

    // Check the memory to make sure everything was properly freed.
    BOOST_CHECK_EQUAL(area.nodeAlloc.bytesOutstanding(), 0);

    // commonPrefix check
    KeyFragment frag5 = TrieKey("01").getFragment();
    BOOST_CHECK_EQUAL(frag0.commonPrefix(frag5), frag5);
    BOOST_CHECK_EQUAL(frag5.commonPrefix(frag0), frag5);

    KeyFragment frag6 = TrieKey("01FFFFFFFFFFF").getFragment();
    frag0.commonPrefix(frag6);
    frag6.commonPrefix(frag0);

    // Check that the startBit is properly handled.
    KeyFragment frag7 = TrieKey("0123456789sjklafhasfhjsdhfk").getFragment();
    frag7.removeBitVec(77);
    KeyFragmentRepr repr7 = MMAP_PIN_REGION_RET(region, frag7.allocRepr(area));
    KeyFragment frag8 = MMAP_PIN_REGION_RET(region, KeyFragment::loadRepr(repr7, area));

    BOOST_CHECK_EQUAL(frag7, frag8);
}


/*
\todo This test fails when JML_ALWAYS_INLINE is commented out.

If this is fixed then we can go back to using Bit_Writer in KeyFragment.

The issue is sadly somewhat tricky and probably requires looking at the
generated assembly.
*/
#if 0

typedef uint64_t Word;
enum { bitsInWord = sizeof(Word) * 8 };
typedef ML::compact_vector<Word, 1> KeyVec;
typedef ML::Bit_Buffer<Word, ML::Simple_Mem_Buffer<Word> > SeqBitBuffer;


namespace this_is_a_test {

//JML_ALWAYS_INLINE
void copyBits(
        ML::Bit_Writer<Word>& dest, SeqBitBuffer& src, int32_t nbits)
{
    using namespace std;

    cerr << "N" << nbits << " W" << bitsInWord << " ";

    // Grab whatever we can in bulk.
    for(int32_t i = 0, bitsLeft = nbits; 
        bitsLeft >= bitsInWord; 
        ++i, bitsLeft -= bitsInWord)
    {
        dest.rwrite(src.rextract(bitsInWord), bitsInWord);
    }

    // Grab any leftover.
    uint32_t leftoverBits = nbits % bitsInWord;
    if (leftoverBits)
        dest.rwrite(src.rextract(leftoverBits), leftoverBits);
}

};


BOOST_AUTO_TEST_CASE( test_key_fragment_misc )
{

    cerr << endl << "------------------------------------------------" << endl;

    KeyVec dest;
    dest.resize(1, -1ULL);
    ML::Bit_Writer<Word> writer(&dest[0]);

    KeyVec src;
    src.resize(1, 1);
    SeqBitBuffer buffer(&src[0]);


    cerr << "old=" << dest[0] << endl;
    this_is_a_test::copyBits(writer, buffer, bitsInWord);
    cerr << "new=" << dest[0] << endl;

    BOOST_CHECK_EQUAL(dest[0], 1);
}
#endif
