/* mmap_file_js.cc                                                 -*- C++ -*-
   Rémi Attab, 19 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   JS wrapper for the mmap file handling mechanism.
*/

#include "mmap_file_js.h"

#include "soa/js/js_call.h"
#include "soa/sigslot/slot.h"

#include "jml/utils/exc_check.h"


using namespace std;
using namespace v8;
using namespace node;
using namespace Datacratic::MMap;

namespace Datacratic {
namespace JS {


/*****************************************************************************/
/* MMAP FILE                                                                 */
/*****************************************************************************/

extern const char* const MMapFileName;
const char * const MMapFileName = "MMapFile";

struct MMapFileJS :
    public JSWrapped2<MMapFile, MMapFileJS, MMapFileName, mmapModule>
{

    MMapFileJS(
            v8::Handle<v8::Object> This,
            const std::shared_ptr<MMapFile>& mmapFile =
                std::shared_ptr<MMapFile>())
    {
        HandleScope scope;
        wrap(This, mmapFile);
    }


    static Handle<v8::Value>
    New(const Arguments& args)
    {
        try {

            ExcCheck(args.Length() > 0 && !args[0]->IsUndefined(),
                    "opType required (see mmap.Create or mmap.Open)");

            string opType = getArg<string>(args, 0, "", "opType");
            ExcCheck(opType == mmapCreate || opType == mmapOpen,
                    "Invalid operation type (see mmap.Create or mmap.Open)");

            string mapFile = getArg<string>(args, 1, "PRIVATE", "mapFile");

            Permissions perm = getArg<Permissions>(
                    args, 2, PERM_READ_WRITE, "permissions");
            ExcCheck(!(perm & ~PERM_READ_WRITE),
                    "Invalid permissions (see mmap.Read or mmap.Write)");

            if (opType == mmapCreate) {
                size_t initialSize =
                    getArg<size_t>(args, 3, page_size * 64, "initialSize");

                ExcCheckLessEqual(args.Length(), 4, "Too many arguments");

                auto obj = ML::make_std_sp(new MMapFile(
                                RES_CREATE, mapFile, perm, initialSize));
                new MMapFileJS(args.This(), obj);
            }
            else {
                ExcCheckLessEqual(args.Length(), 3, "Too many arguments");

                auto obj = ML::make_std_sp(new MMapFile(RES_OPEN, mapFile, perm));
                new MMapFileJS(args.This(), obj);
            }

            return args.This();
        }
        HANDLE_JS_EXCEPTIONS;
    }

    static void
    Initialize()
    {
        Persistent<FunctionTemplate> t = Register(New);

        registerMemberFn(&MMapFile::unlink, "unlink");
        registerMemberFn(&MMapFile::snapshot, "snapshot");

        NODE_SET_PROTOTYPE_METHOD(t, "allocateTrie", allocateTrie);
        NODE_SET_PROTOTYPE_METHOD(t, "deallocateTrie", deallocateTrie);
    }

    static Handle<Value>
    allocateTrie(const Arguments& args)
    {
        try {
            ExcCheck(args.Length() == 1 && !args[0]->IsUndefined(),
                    "id argument required");
            unsigned id = getArg<unsigned>(args, 0, 0, "id");

            getShared(args)->trieAlloc.allocate(id);

            return Handle<Value>();
        } HANDLE_JS_EXCEPTIONS;
    }


    static Handle<Value>
    deallocateTrie(const Arguments& args)
    {
        try {
            ExcCheck(args.Length() == 1 && !args[0]->IsUndefined(),
                    "id argument required");
            unsigned id = getArg<unsigned>(args, 0, 0, "id");

            getShared(args)->trieAlloc.deallocate(id);

            return Handle<Value>();
        } HANDLE_JS_EXCEPTIONS;
    }
};


std::shared_ptr<MMapFile>
from_js(const JSValue& value, std::shared_ptr<MMapFile>*)
{
    return MMapFileJS::fromJS(value);
}

std::shared_ptr<MMapFile>
from_js_ref(const JSValue& value, std::shared_ptr<MMapFile>*)
{
    return MMapFileJS::fromJS(value);
}

MMapFile*
from_js(const JSValue& value, MMapFile**)
{
    return MMapFileJS::fromJS(value).get();
}

void
to_js(JS::JSValue& value, const std::shared_ptr<MMapFile>& mmapFile)
{
    value = MMapFileJS::toJS(mmapFile);
}

std::shared_ptr<MMapFile>
getMMapFileSharedPointer(const JS::JSValue & value)
{
    if(MMapFileJS::tmpl->HasInstance(value))
    {
        std::shared_ptr<MMapFile> mmapFile = MMapFileJS::getSharedPtr(value);
        return mmapFile;
    }
    std::shared_ptr<MMapFile> mmapFile;
    return mmapFile;
}


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

Handle<Value>
cleanupMMapFileJS(const Arguments& args)
{
    try {
        ExcCheck(args.Length() == 1 && !args[0]->IsUndefined(),
                "Name of the map file is required");

        string mapFile = getArg<string>(args, 0, "", "mapFile");
        cleanupMMapFile(mapFile);

        return Handle<Object>();
    }
    HANDLE_JS_EXCEPTIONS;
}


} // namepsace JS
} // namespace Datacratic
