/* mmap_js.cc                                                 -*- C++ -*-
   Rémi Attab, 19 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   JS mmap module definition
*/

#include "v8.h"
#include "mmap_js.h"
#include "mmap_file_js.h"
#include "mmap/mmap_const.h"
#include "soa/js/js_registry.h"

using namespace v8;
using namespace std;

namespace Datacratic {
namespace JS {

const char * const mmapModule = "mmap";
const char * const mmapCreate = "create";
const char * const mmapOpen = "open";

// Node.js initialization function; called to set up the mmap object
extern "C" void
init(Handle<v8::Object> target)
{
    Datacratic::JS::registry.init(target, mmapModule);


    static Persistent<FunctionTemplate> atn
        = v8::Persistent<FunctionTemplate>::New(
                v8::FunctionTemplate::New(cleanupMMapFileJS));

    // Forward declaration for cleanMMapFile()
    target->Set(String::NewSymbol("cleanup"), atn->GetFunction());

    // RES_CREATE and RES_OPEN for js
    target->Set(String::NewSymbol("Create"), String::New(mmapCreate));
    target->Set(String::NewSymbol("Open"), String::New(mmapOpen));

    // Permissions enum
    target->Set(String::NewSymbol("Read"), Int32::New(MMap::PERM_READ));
    target->Set(String::NewSymbol("Write"), Int32::New(MMap::PERM_WRITE));
    target->Set(String::NewSymbol("ReadWrite"),
            Int32::New(MMap::PERM_READ_WRITE));

    // Useful to indicate the initial size to allocate
    target->Set(String::NewSymbol("PageSize"), Uint32::New(MMap::page_size));
}

} // namespace JS
} // namespace Datacratic

