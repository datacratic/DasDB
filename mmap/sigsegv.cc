/* sigsegv.cc
   Jeremy Barnes, 24 February 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   Segmentation fault handlers.
*/

#include "sigsegv.h"
#include <ace/Synch.h>
#include <ace/Guard_T.h>
#include "jml/arch/spinlock.h"
#include <signal.h>
#include "jml/arch/atomic_ops.h"
#include "jml/arch/exception.h"


using namespace ML;
using namespace std;

namespace Datacratic {

struct Segv_Descriptor {
    Segv_Descriptor()
        : active(false), ref(0), start(0), end(0)
    {
    }

    bool matches(const void * addr) const
    {
        if (!active || ref == 0)
            return false;
        const char * addr2 = (const char *)addr;

        return addr2 >= start && addr2 < end;
    }

    volatile bool active;
    volatile int ref;
    const char * start;
    const char * end;
};


enum { NUM_SEGV_DESCRIPTORS = 64 };

Segv_Descriptor SEGV_DESCRIPTORS[NUM_SEGV_DESCRIPTORS];

Spinlock segv_lock(false /* don't yield in spinlock; sched_yield() not valid in signal handlers */);

// Number that increments each time a change is made to the segv regions
static volatile uint64_t segv_epoch = 0;

static size_t num_faults_handled = 0;

size_t get_num_segv_faults_handled()
{
    return num_faults_handled;
}

int register_segv_region(const void * start, const void * end)
{
    ACE_Guard<Spinlock> guard(segv_lock);
    
    // Busy wait until we get one
    int idx = -1;
    while (idx == -1) {
        for (unsigned i = 0;  i < NUM_SEGV_DESCRIPTORS && idx == -1;  ++i)
            if (SEGV_DESCRIPTORS[i].ref == 0) idx = i;
        
        if (idx == -1) sched_yield();
    }

    Segv_Descriptor & descriptor = SEGV_DESCRIPTORS[idx];
    descriptor.start  = (const char *)start;
    descriptor.end    = (const char *)end;
    descriptor.active = true;

    ++segv_epoch;

    memory_barrier();

    descriptor.ref = 1;

    return idx;
}

void unregister_segv_region(int region)
{
    ACE_Guard<Spinlock> guard(segv_lock);

    if (region < 0 || region >= NUM_SEGV_DESCRIPTORS)
        throw Exception("unregister_segv_region(): invalid region");

    Segv_Descriptor & descriptor = SEGV_DESCRIPTORS[region];
    
    if (descriptor.ref == 0 || !descriptor.active)
        throw Exception("segv region is not active");
    
    descriptor.active = false;
    descriptor.start = 0;
    descriptor.end = 0;

    ++segv_epoch;

    memory_barrier();

    atomic_add(descriptor.ref, -1);
}

// Race condition:
// If the region was removed between when the signal happened and when the
// lock is obtained, then region will be -1.
//
// There are two possibilities when that happens:
//
// 1.  It was a real segfault, which should be turned into a real signal, or
// 2.  The descriptor was removed due to this race condition
// In this case, we need to retry *one* time

struct Thread_Signal_Info {
    bool in_retry;
    uint64_t old_epoch;
    void * old_ip;
    void * old_addr;
};

__thread Thread_Signal_Info t_signal_info = { 0, 0, 0, 0 };

void default_handle_signal(int signum)
{
    struct sigaction action;
    action.sa_handler = SIG_DFL;
    action.sa_flags = 0;
    sigaction(signum, &action, 0);
    raise(signum);
}

void segv_handler(int signum, siginfo_t * info, void * context)
{
    if (signum != SIGSEGV
        || info->si_code != SEGV_ACCERR)
        default_handle_signal(signum);

    // We could do various things here to filter out the signal


    const char * addr = (const char *)info->si_addr;

    int region = -1;
    {
        ACE_Guard<Spinlock> guard(segv_lock);

        for (unsigned i = 0;  i < NUM_SEGV_DESCRIPTORS && region == -1;  ++i) {
            if (SEGV_DESCRIPTORS[i].matches(addr)) {
                region = i;
                atomic_add(SEGV_DESCRIPTORS[i].ref, 1);
                t_signal_info.in_retry = false;
            }
        }
    }

    if (region == -1) {
        // Not found.  We need to determine if it's because of a real segfault
        // or due to a race between the memory access and the region
        // disappearing.

        void * sig_addr = info->si_addr;
        ucontext_t * ucontext = (ucontext_t *)context;

        // TODO: not very elegant; find a way to get this programatically
        void * sig_ip   = (void *)ucontext->uc_mcontext.gregs[16];

        // Is it exactly the same instruction reading exactly the same
        // address without any intervening changes in the descriptors?
        if (t_signal_info.in_retry
            && t_signal_info.old_epoch == segv_epoch
            && t_signal_info.old_ip == sig_ip
            && t_signal_info.old_addr == sig_addr) {

            // Must be a real sigsegv; re-raise it with the default handler
            t_signal_info.in_retry = false;

            default_handle_signal(signum);
        }

        t_signal_info.in_retry = true;
        t_signal_info.old_epoch = segv_epoch;
        t_signal_info.old_ip = sig_ip;
        t_signal_info.old_addr = sig_addr;

        // Now return, to retry it
        return;
    }

    Segv_Descriptor & descriptor = SEGV_DESCRIPTORS[region];

    // busy wait for it to become inactive
    //timespec zero_point_one_ms = {0, 100000};

    // TODO: should we call nanosleep in a signal handler?
    // NOTE: doesn't do the right thing in linux 2.4; small nanosleeps are busy
    // waits

    while (descriptor.active) {
        //nanosleep(&zero_point_one_ms, 0);
    }

    atomic_add(descriptor.ref, -1);
    atomic_add(num_faults_handled, 1);
}

static bool is_segv_handler_installed()
{
    struct sigaction action;

    int res = sigaction(SIGSEGV, 0, &action);
    if (res == -1)
        throw Exception(errno, "is_segv_handler_installed()", "sigaction");

    return action.sa_sigaction == segv_handler;
}

void install_segv_handler()
{
    if (is_segv_handler_installed())
        return;

    // Install a segv handler
    struct sigaction action;

    action.sa_sigaction = segv_handler;
    action.sa_flags = SA_SIGINFO;

    int res = sigaction(SIGSEGV, &action, 0);

    if (res == -1)
        throw Exception(errno, "install_segv_handler()", "sigaction");
}


} // namespace Datacratic
