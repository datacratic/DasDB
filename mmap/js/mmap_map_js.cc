/* mmap_map_js.cc                                                 -*- C++ -*-
   Rémi Attab, 20 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   JS wrapper for the mmap map container
*/

#include "mmap_js.h"
#include "mmap_file_js.h"
#include "mmap/mmap_map.h"

#include "soa/sigslot/slot.h"
#include "soa/js/js_call.h"
#include "soa/js/js_wrapped.h"
#include "jml/utils/smart_ptr_utils.h"
#include "jml/utils/exc_check.h"

#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/stringize.hpp>

using namespace std;
using namespace v8;
using namespace node;
using namespace Datacratic::MMap;

namespace Datacratic {
namespace JS {


/*****************************************************************************/
/* MMAP MAP                                                                  */
/*****************************************************************************/

template<
    typename Key,
    typename Value,
    typename MapT,
    const char * const & ClassNameT>
struct MapJS : public JSWrapped2<
    MapT, MapJS<Key, Value, MapT, ClassNameT>, ClassNameT, mmapModule>
{
    typedef MapJS<Key, Value, MapT, ClassNameT> MapJsT;
    typedef JSWrapped2<MapT, MapJsT, ClassNameT, mmapModule> WrapperT;

    // Required to instantiate the template of this class.
    // Should not be used for anything else.
    MapJS() {}

    MapJS(
            Handle<Object> This,
            const std::shared_ptr<MapT>& map = std::shared_ptr<MapT>())
    {
        HandleScope scope;
        wrap(This, map);
    }

    static Handle<v8::Value>
    New(const Arguments& args)
    {
        try {
            ExcCheck(args.Length() == 2
                    && !args[0]->IsUndefined() && !args[1]->IsUndefined(),
                    "Both the mmapFile and the id are required");

            std::shared_ptr<MMapFile> mmapFile =
                getMMapFileSharedPointer(args[0]);
            ExcCheck(mmapFile, "Argument is not a MMapFile object");

            unsigned id = getArg<unsigned>(args, 1, 0, "id");

            auto obj = ML::make_std_sp(new MapT(mmapFile.get(), id));
            new MapJsT(args.This(), obj);

            return args.This();
        }
        HANDLE_JS_EXCEPTIONS;
    }

    static void
    Initialize()
    {
        Persistent<FunctionTemplate> t = Register(New);

        registerMemberFn(&MapT::size, "size");
        registerMemberFn(&MapT::empty, "empty");

        NODE_SET_PROTOTYPE_METHOD(t, "exists", exists);
        NODE_SET_PROTOTYPE_METHOD(t, "get", get);
        NODE_SET_PROTOTYPE_METHOD(t, "foreach", foreach);

        if (MapT::PolicyT::isMutable) {
            registerMemberFn(&MapT::clear, "clear");

            NODE_SET_PROTOTYPE_METHOD(t, "set", set);
            NODE_SET_PROTOTYPE_METHOD(t, "del", del);
        }

    }

    static Handle<v8::Value>
    exists(const Arguments& args)
    {
        try {
            ExcCheck(args.Length() == 1 && !args[0]->IsUndefined(),
                    "Invalid arguments (expected exists(key))");

            Key key = getArg<Key>(args, 0, Key(), "key");

            bool count = WrapperT::getShared(args)->count(key);

            return Boolean::New(count);
        }
        HANDLE_JS_EXCEPTIONS;
    }

    static Handle<v8::Value>
    get(const Arguments& args)
    {
        try {
            ExcCheck(args.Length() == 1 && !args[0]->IsUndefined(),
                    "Invalid arguments (expected get(key))");

            Key key = getArg<Key>(args, 0, Key(), "key");
            auto res = WrapperT::getShared(args)->get(key);

            JSValue value;
            if (res.first)
                to_js(value, res.second);

            return value;
        }
        HANDLE_JS_EXCEPTIONS;
    }

    static Handle<v8::Value>
    set(const Arguments& args)
    {
        try {
            ExcCheck(args.Length() == 2
                    && !args[0]->IsUndefined() && !args[1]->IsUndefined(),
                    "Invalid arguments (expected set(key, value))");

            Key key = getArg<Key>(args, 0, Key(), "key");
            Value value = getArg<Value>(args, 1, Value(), "value");

            auto ret = WrapperT::getShared(args)->insert(key, value);

            return Boolean::New(ret.second);
        }
        HANDLE_JS_EXCEPTIONS;
    }


    static Handle<v8::Value>
    del(const Arguments& args)
    {
        try {
            ExcCheck(args.Length() == 1 && !args[0]->IsUndefined(),
                    "Invalid arguments (expected del(key))");

            Key key = getArg<Key>(args, 0, Key(), "key");

            bool success = WrapperT::getShared(args)->remove(key);

            return Boolean::New(success);
        }
        HANDLE_JS_EXCEPTIONS;
    }

    static Handle<v8::Value>
    foreach(const Arguments& args)
    {
        try {
            ExcCheck(args.Length() <= 2 && !args[0]->IsUndefined(),
                    "Invalid arguments (expected foreach([prefix,] callback))");

            int cbArg;
            typename MapT::iterator it, end;

            if (args.Length() == 1) {
                cbArg = 0;
                tie(it, end) = WrapperT::getShared(args)->beginEnd();
            }
            else {
                cbArg = 1;
                Key key = getArg<Key>(args, 0, "prefix");
                tie(it, end) = WrapperT::getShared(args)->bounds(key);
            }

            Local<Function> fn = getArg(args, cbArg, "callback");

            for (; it != end; ++it) {
                HandleScope scope;

                JSValue jsKey, jsValue;
                to_js(jsKey, it.key());
                to_js(jsValue, it.value());

                int argc = 2;
                Handle<v8::Value> argv[argc];
                argv[0] = jsKey;
                argv[1] = jsValue;

                fn->Call(args.This(), argc, argv);
            }

            return Handle<v8::Value>();
        }
        HANDLE_JS_EXCEPTIONS;
    }
};

/** This macro contains all the boilerplate needed to get the MapJS of a given
    type exposed to js stuff.

    Note that on the third line (after the char stuff), we instantiate
    an empty object. This is required to instanciate the template and
    make it's existance known to whoever uses it.

    The params correspond to template paramenters for MapJS:
    K = Key
    V = Value
    T = MapType
    N = ClassNameT
*/
#define JS_TEMPLATE(N, K, V, T)                                         \
                                                                        \
    extern const char * const BOOST_PP_CAT(N, Name);                    \
    const char * const BOOST_PP_CAT(N, Name) = BOOST_PP_STRINGIZE(N);   \
                                                                        \
    static MapJS<K,V,T<K,V>,BOOST_PP_CAT(N, Name)> BOOST_PP_CAT(N, obj); \
                                                                        \
    std::shared_ptr< T<K,V> >                                         \
    from_js(const JSValue& value, std::shared_ptr< T<K,V> >*)         \
    {                                                                   \
        return MapJS<K,V,T<K,V>,BOOST_PP_CAT(N, Name)>::fromJS(value);  \
    }                                                                   \
                                                                        \
    std::shared_ptr< T<K,V> >                                         \
    from_js_ref(const JSValue& value, std::shared_ptr< T<K,V> >*)     \
    {                                                                   \
        return MapJS<K,V,T<K,V>,BOOST_PP_CAT(N, Name)>::fromJS(value);  \
    }                                                                   \
                                                                        \
    T<K,V>*                                                             \
    from_js(const JSValue& value, T<K,V>**)                             \
    {                                                                   \
        return MapJS<K,V,T<K,V>,BOOST_PP_CAT(N, Name)>::fromJS(value).get(); \
    }                                                                   \
                                                                        \
    void                                                                \
    to_js(JS::JSValue& value, const std::shared_ptr< T<K,V> >& map)   \
    {                                                                   \
        value = MapJS<K,V,T<K,V>,BOOST_PP_CAT(N, Name)>::toJS(map);     \
    }

// Instantiate the mutable boilerplate
JS_TEMPLATE(MapIntInt, uint64_t, uint64_t, MutableMap)
JS_TEMPLATE(MapIntStr, uint64_t, string,   MutableMap)
JS_TEMPLATE(MapStrInt, string,   uint64_t, MutableMap)
JS_TEMPLATE(MapStrStr, string,   string,   MutableMap)

// Instantiate the snapshot boilerplate
JS_TEMPLATE(MapSnapshotIntInt, uint64_t, uint64_t, ConstSnapshotMap)
JS_TEMPLATE(MapSnapshotIntStr, uint64_t, string,   ConstSnapshotMap)
JS_TEMPLATE(MapSnapshotStrInt, string,   uint64_t, ConstSnapshotMap)
JS_TEMPLATE(MapSnapshotStrStr, string,   string,   ConstSnapshotMap)


// Instantiate the transactional boilerplate
JS_TEMPLATE(MapTransactionIntInt, uint64_t, uint64_t, TransactionalMap)
JS_TEMPLATE(MapTransactionIntStr, uint64_t, string,   TransactionalMap)
JS_TEMPLATE(MapTransactionStrInt, string,   uint64_t, TransactionalMap)
JS_TEMPLATE(MapTransactionStrStr, string,   string,   TransactionalMap)


} // namepsace JS
} // namespace Datacratic
