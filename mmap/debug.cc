/** debug.cc                                 -*- C++ -*-
    Rémi Attab, 06 Jun 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Consolidated debug flags for the various MMap components.

*/

#include "debug.h"
#include "memory_tracker.h"


namespace Datacratic {
namespace MMap {

bool regionExceptionTest = false;

bool trieDebug = false;
bool trieValidate = false;

bool trieMemoryCheck = false;
MemoryTracker trieMemoryTracker(false);

bool kfMemoryCheck = false;
MemoryTracker kfMemoryTracker(false);

size_t setRootSuccesses = 0;
size_t setRootFailures = 0;
size_t setRootFastRetries = 0;
size_t setRootSlowRetries = 0;

int64_t mergeIdleTime = 0;
int64_t mergeActiveTime = 0;
__thread double mergeAcquireTime = 0.0;


} // namespace MMap
} // namepsace Datacratic
