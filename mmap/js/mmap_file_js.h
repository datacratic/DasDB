/* mmap_file_js.h                                                 -*- C++ -*-
   Rémi Attab, 19 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   JS wrapper for the mmap file handling mechanism.
*/

#ifndef __mmap_js__mmap_file_js_h__
#define __mmap_js__mmap_file_js_h__


#include "mmap_js.h"
#include "mmap/mmap_file.h"
#include "soa/js/js_wrapped.h"
#include "jml/utils/smart_ptr_utils.h"


namespace Datacratic {
namespace JS {

void
to_js(JS::JSValue & value, const std::shared_ptr<MMap::MMapFile> & br);

std::shared_ptr<MMap::MMapFile>
getMMapFileSharedPointer(const JS::JSValue &);

std::shared_ptr<MMap::MMapFile>
from_js(const JSValue& value, std::shared_ptr<MMap::MMapFile>*);

std::shared_ptr<MMap::MMapFile>
from_js_ref(const JSValue & value, std::shared_ptr<MMap::MMapFile> *);

MMap::MMapFile*
from_js(const JSValue& value, MMap::MMapFile**);


v8::Handle<v8::Value>
cleanupMMapFileJS(const v8::Arguments& args);


} // namespace JS
} // namespace Datacratic


#endif /* __mmap_js__mmap_file_js_h__ */

