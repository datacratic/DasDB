/** profiler.cc                                 -*- C++ -*-
    Rémi Attab, 17 May 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Profiling implementation details.

*/

#include "profiler.h"
#include "tools/mmap_perf_utils.h" // I know... terrible...
#include "jml/arch/format.h"

using namespace std;
using namespace ML;

namespace Datacratic {
namespace MMap {


/******************************************************************************/
/* PROFILER                                                                   */
/******************************************************************************/

namespace {

struct ProfileNode
{
    ProfileNode(size_t tid, ProfileData* data) :
        tid(tid), data(data)
    {}

    size_t tid;
    ProfileData* data;
    ProfileNode* next;
};


__thread ProfileData* threadProfile;
__thread ProfileData* currentProfile;

std::atomic<size_t> threadId(0);
std::atomic<ProfileNode*> profileList(nullptr);
}

ProfileData* getProfile(const char* name)
{
    if (!currentProfile) {
        if (!threadProfile) {
            threadProfile = new ProfileData(nullptr);

            ProfileNode* node = new ProfileNode(++threadId, threadProfile);
            do {
                node->next = profileList;
            } while(!profileList.compare_exchange_weak(node->next, node));

        }
        currentProfile = threadProfile;
    }

    return currentProfile->profile(name);
}

void dumpProfiler(std::ostream& stream)
{
    ProfileNode* node = profileList;
    while (node) {
        printf("Profile for thread %lu\n", node->tid);

        // Setup a total to improve the child outputs.
        node->data->entry = ProfileEntry();
        for (const auto& child : node->data->children)
            node->data->entry += child.second->entry;

        node->data->dump(stream);
        node = node->next;
        printf("\n");
    }
}

void resetProfiler()
{
    delete threadProfile;
    currentProfile = threadProfile = nullptr;

    ProfileNode* node = profileList.exchange(nullptr);
    ProfileNode* next;
    while (node) {
        next = node->next;
        delete node;
        node = next;
    }
}

ProfileData*
ProfileData::
profile(const std::string& name)
{
    auto& child = children[name];
    if (!child) child = new ProfileData(this);
    return currentProfile = child;
}


void
ProfileData::
record(double elapsed)
{
    entry.count++;
    entry.ticks += elapsed;
    currentProfile = parent;
}

void dumpLine(
        ostream& stream,
        const string& name,
        const string& indent,
        ProfileEntry data,
        ProfileEntry total)
{
    double countAvg = double(data.count) / total.count;
    double ticksPct = data.ticks / total.ticks;

    stream << ML::format("%-50s", indent + fmtPct(ticksPct) + " - " + name)
        << "hits: " << fmtValue(data.count) << " (" << fmtValue(countAvg) << ")"
        << endl;
}

void
ProfileData::
dump(ostream& stream) const
{
    dump(stream, 0);
}

void
ProfileData::
dump(ostream& stream, int indent) const
{
    string ind(indent * 2, ' ');

    struct Entry : ProfileEntry
    {
        string name;
        const ProfileData* profile;

        Entry(const pair<string, ProfileData*>& pair) :
            ProfileEntry(pair.second->entry),
            name(pair.first),
            profile(pair.second)
        {}
        bool operator< (const Entry& other) const { return ticks > other.ticks; }
    };

    set<Entry> sortedChildren;
    for (const auto& child : children) sortedChildren.insert(Entry(child));
    for (const auto& entry : sortedChildren) {
        dumpLine(stream, entry.name, ind, entry, entry.profile->parent->entry);
        entry.profile->dump(stream, indent+1);
    }
}


} // namespace MMap
} // namepsace Datacratic
