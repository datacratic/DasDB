/** profiler.h                                 -*- C++ -*-
    Rémi Attab, 17 May 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Profiling utilities specialized to DasDB.

*/

#pragma once

#include "jml/arch/tick_counter.h"

#include <string>
#include <memory>
#include <iostream>
#include <unordered_map>

namespace Datacratic {
namespace MMap {


/******************************************************************************/
/* PROFILE ENTRY                                                              */
/******************************************************************************/

struct ProfileEntry
{
    ProfileEntry() : ticks(0), count(0) {}
    double ticks;
    size_t count;

    ProfileEntry& operator+=(const ProfileEntry& other)
    {
        ticks += other.ticks;
        count += other.count;
        return *this;
    }
};


/******************************************************************************/
/* PROFILE DATA                                                               */
/******************************************************************************/

struct ProfileData
{
    ProfileData(ProfileData* parent) : parent(parent) {}
    ~ProfileData()
    {
        for (auto child : children) delete child.second;
    }

    ProfileData* profile(const std::string& name);
    void record(double elapsed = 0.0);

    void dump(std::ostream& stream) const;
    void dump(std::ostream& stream, int indent) const;

    ProfileData* parent; // should probably use a weak_ptr
    ProfileEntry entry;
    std::unordered_map<std::string, ProfileData*> children;
};

ProfileData* getProfile(const char* name);
void dumpProfiler(std::ostream& stream = std::cerr);
void resetProfiler();


/******************************************************************************/
/* PROFILER                                                                   */
/******************************************************************************/

struct Profiler
{
    Profiler(const char* name) :
        profile(getProfile(name)), start(ML::ticks())
    {}
    ~Profiler() { reset(); }

    void reset()
    {
        if (!profile) return;
        profile->record(ML::ticks() - start);
        profile = nullptr;
    }

private:

    ProfileData* profile;
    double start;
};


} // namespace MMap
} // namespace Datacratic
