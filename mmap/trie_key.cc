/* trie_key.cc                                               -*- C++ -*-
   Mathieu Stefani, 22 April 2013
   Copyright (c) 2013 Datacratic.  All rights reserved.


*/

#include "trie_key.h"
#include "key_fragment.h"
#include "jml/utils/exc_check.h"


namespace Datacratic {
namespace MMap {

/*****************************************************************************/
/* TRIE KEY                                                                  */
/*****************************************************************************/

TrieKey::
TrieKey(const KeyFragment& frag):
    key(frag.key), bytes(frag.bits / 8)
{
    ExcCheckEqual(bytes * 8, frag.bits, "Incomplete key");
}

KeyFragment
TrieKey::
getFragment() const
{
    return KeyFragment(key, bytes*8);
}

}
}
