/* sync_stream.cc                                                    -*- C++ -*-
   Rémi Attab, 30 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Synchronized stream that avoids interleaved messages.
*/

#include "sync_stream.h"

#include <iostream>

namespace Datacratic {
namespace MMap {

namespace SyncDetails {

ML::Thread_Specific<StdSyncStreams> tlsStreams;

}; // namespace SyncDetails;


} // namespace MMap
} // namespace Datacratic
