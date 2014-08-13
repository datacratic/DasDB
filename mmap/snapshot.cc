/* snapshot.cc
   Jeremy Barnes, 22 February 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

*/

#include "snapshot.h"
#include "sigsegv.h"
#include "journal.h"
#include "jml/arch/exception.h"
#include "jml/utils/string_functions.h"
#include "jml/arch/vm.h"
#include "jml/utils/guard.h"
#include "jml/utils/exc_check.h"

#include <boost/bind.hpp>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <iostream>


using namespace std;
using namespace ML;


namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* SNAPSHOT                                                                  */
/*****************************************************************************/

struct Snapshot::Itl {
    Itl()
        : pid(-1), control_fd(-1), snapshot_pm_fd(-1)
    {
    }

    pid_t pid;
    int control_fd;
    int snapshot_pm_fd;
};

Snapshot::
Snapshot(const string& journalFile, Worker worker)
    : itl(new Itl()),
      journalFile(journalFile)
{
    install_segv_handler();

    if (!worker)
        worker = boost::bind(&Snapshot::run_child, this, _1);
    
    // Create the socket pair to communicate

    int sockets[2];
    int res = socketpair(AF_UNIX, SOCK_STREAM, 0, sockets);
    if (res == -1) {
    }

    pid_t pid = fork();

    if (pid == -1)
        throw Exception("error in fork");

    else if (pid == 0) {
        /* We are the child.  Do cleaning up here */
        // ...

        close(sockets[1]);

        int return_code = -1;
        try {
            return_code = worker(sockets[0]);
        }
        catch (const std::exception & exc) {
            cerr << "child exiting with exception " << exc.what() << endl;
        }
        catch (...) {
            cerr << "child exiting with unknown exception " << endl;
        }

        // cerr << "child exiting with return code " << return_code << endl;

        // Now die.  We do it this way to avoid any destructors running, etc
        // which might do things that we don't want them to and interfere with
        // the parent process.
        _exit(return_code);
    }

    close(sockets[0]);

    itl->pid = pid;
    itl->control_fd = sockets[1];

    int pm_fd = open(format("/proc/%d/pagemap", pid).c_str(), O_RDONLY);
    if (pm_fd == -1)
        throw Exception("open pagemap; " + string(strerror(errno)));

    itl->snapshot_pm_fd = pm_fd;
}

Snapshot::
~Snapshot()
{
    if (!itl->pid)
        return;

    int return_code = terminate();

    if (return_code != 0)
        cerr << "warning: snapshot termination returned " << return_code
             << endl;
}

int
Snapshot::
pagemap_fd() const
{
    return itl->snapshot_pm_fd;
}

int
Snapshot::
control_fd() const
{
    return itl->control_fd;
}

void
Snapshot::
send_message(const char * data, size_t sz)
{
    int res = write(itl->control_fd, data, sz);
    if (res != sz)
        throw Exception("write: " + string(strerror(errno)));
}

std::string
Snapshot::
recv_message()
{
    char buffer[65536];
    int found = read(itl->control_fd, buffer, 65536);
    if (found == -1)
        throw Exception("read: " + string(strerror(errno)));
    if (found == 65536)
        throw Exception("read: message was too long");

    string result(buffer, buffer + found);
    return result;
}

void
Snapshot::
recv_message(char * data, size_t sz, bool all_at_once)
{
    //cerr << getpid() << " recv_message of " << sz << " bytes" << endl;
    int found = read(itl->control_fd, data, sz);
    if (found == -1)
        throw Exception("read: " + string(strerror(errno)));
    if (found != sz) {
        cerr << "wanted: " << sz << endl;
        cerr << "found:  " << found << endl;
        throw Exception("read: message was not long enough");
    }
}

void
Snapshot::
send(const std::string & str)
{
    if (str.length() > 65536)
        throw Exception("string too long to send");
    send<int>(str.length());
    send_message(str.c_str(), str.length());
}

void
Snapshot::
recv(std::string & str)
{
    int sz = recv<int>();
    if (sz < 0 || sz > 65536)
        throw Exception("string too long to receive");
    str.resize(sz);
    recv_message(&str[0], sz);
}

boost::tuple<size_t, size_t, size_t>
Snapshot::
sync_and_reback(int fd,
                size_t file_offset,
                void * mem_start,
                size_t mem_size,
                Reclaim_Option reclaim_snapshot,
                int current_pagemap_file)
{
    Call_Guard guard;

    if (current_pagemap_file == -1) {
        current_pagemap_file = open("/proc/self/pagemap", O_RDONLY);
        if (current_pagemap_file != -1)
            guard.set(boost::bind(close, current_pagemap_file));
    }
    if (current_pagemap_file == -1)
        throw Exception(errno, "sync_and_reback()",
                        "open(\"/proc/self/pagemap\", O_RDONLY)");

    size_t n_written_in_sync
        = sync_to_disk(fd, file_offset, mem_start, mem_size,
                       Snapshot::SYNC_ONLY);

    size_t n_rebacked
        = reback_range_after_write(fd, file_offset, mem_start, mem_size,
                                   pagemap_fd(), current_pagemap_file);
    
    size_t n_reclaimed = 0;
    if (reclaim_snapshot == RECLAIM)
        n_reclaimed = sync_to_disk(fd, file_offset, mem_start, mem_size,
                                   Snapshot::RECLAIM_ONLY);

    return boost::make_tuple(n_written_in_sync, n_rebacked, n_reclaimed);
}

static bool needs_backing(const Pagemap_Entry & current_pagemap,
                          const Pagemap_Entry & old_pagemap)
{
    return current_pagemap.present
        && old_pagemap.present
        && current_pagemap.swapped == old_pagemap.swapped
        && current_pagemap.pfn == old_pagemap.pfn;
}

size_t
Snapshot::
reback_range_after_write(int backing_file_fd,
                         size_t backing_file_offset,
                         void * mem_start,
                         size_t mem_size,
                         int old_pagemap_file,
                         int current_pagemap_file)
{
    bool debug = false;

    if (debug) {
        cerr << endl;
        cerr << "========= reback_range_after_write pid=" << getpid() << endl;
    }

    if (!is_page_aligned(mem_start))
        throw Exception("reback_range_after_write(): mem_start not page aligned");
    if (mem_size % page_size != 0)
        throw Exception("reback_range_after_write(): not an integral number of"
                        "pages");

    size_t npages = mem_size / page_size;

    size_t CHUNK = 1024;  // do 1024 pages = 4MB at once

    // To store the page map entries in
    Pagemap_Entry current_pagemap[CHUNK];
    Pagemap_Entry old_pagemap[CHUNK];

    char * mem = (char *)mem_start;

    size_t result = 0;

    for (unsigned i = 0;  i < npages;  i += CHUNK, mem += CHUNK * page_size) {
        int todo = std::min(npages - i, CHUNK);

        Pagemap_Reader pm_old(mem, todo * page_size, old_pagemap,
                              old_pagemap_file);
        Pagemap_Reader pm_current(mem, todo * page_size, current_pagemap,
                                  current_pagemap_file);

        if (debug) {
            cerr << "pm_old = " << endl << pm_old << endl;
            cerr << "pm_current = " << endl << pm_current << endl;
        }
        
        // The value of j at which we start backing pages
        int backing_start = -1;
        
        // Now go through and back those pages needed
        for (unsigned j = 0;  j <= todo;  ++j) {  /* <= for last page */
            bool need_backing
                =  j < todo && needs_backing(current_pagemap[j], old_pagemap[j]);
            
            if (debug)
                cerr << "j = " << j << " need_backing = " << need_backing
                     << endl;

            if (backing_start != -1 && !need_backing) {
                // We need to re-map the pages from backing_start to
                // backing_end.  Note that remapping pages that are not
                // present doesn't cause any problems; we could potentially
                // make our chunks larger (and reduce the cost of TLB
                // invalidations from mprotect calls) by including these
                // pages in the ranges (the blank ones were never paged in):
                //
                // +-------+-------+-------+-------+-------+-------+-------+
                // | dirty |       | clean |       | clean |       | clean |
                // +-------+-------+-------+-------+-------+-------+-------+
                //
                // no coalesce:    <-- 1 -->       <-- 2 -->       <-- 3 -->
                // coalesce:       <------------------ 1 ------------------>
                //
                // By coalescing, we could reduce our 3 mprotect calls and
                // 3 mmap calls into one of each.

                int backing_end = j;

                int npages = backing_end - backing_start;

                if (debug)
                    cerr << "need to re-back " << npages << " pages from "
                         << backing_start << " to " << backing_end << endl;
                
                char * start = mem + backing_start * page_size;
                size_t len   = npages * page_size;


                // 1.  Add this read-only region to the SIGSEGV handler's list
                // of active regions

                int region = register_segv_region(start, start + len);

                // Make sure that the segv region will be removed once we exit
                // this scope
                Call_Guard segv_unregister_guard
                    (boost::bind(unregister_segv_region, region));

                // 2.  Call mprotect() to switch to read-only
                int res = mprotect(start, len, PROT_READ);
                if (res == -1)
                    throw Exception(errno, "reback_range_after_write()",
                                    "mprotect() read-only before switch");

                // Make sure that an exception will cause the memory to be
                // un-protected.
                Call_Guard mprotect_guard(boost::bind(mprotect, start, len,
                                                      PROT_READ | PROT_WRITE));
                
                // 3.  Re-scan the page map as entries may have changed
                pm_current.update(start, start + len);


                // TODO: what about exceptions from here onwards?
                // 4.  Scan again and break into chunks
                
                int backing_start2 = -1;
                char* protect_start = start;

                for (unsigned k = backing_start;  k <= backing_end;  ++k) {
                    bool need_backing2
                        =  k < backing_end
                        && needs_backing(current_pagemap[k], old_pagemap[k]);
                    
                    result += need_backing2;

                    if (backing_start2 != -1 && !need_backing2) {
                        // We need to re-map the file behind backing_start2
                        // to k
                        int backing_end2 = k;
                        
                        int npages = backing_end2 - backing_start2;
                        char * start = mem + backing_start2 * page_size;
                        size_t len   = npages * page_size;

                        off_t foffset
                            = (backing_file_offset + i * page_size)
                            + backing_start2 * page_size;

                        // Before we reback, make sure the previous pages have
                        // the right protection. Avoids a kernel bug(?) where
                        // the VMAs involved wouldn't be merged properly.
                        size_t protect_len = start - protect_start;
                        if (protect_len > 0) {
                            int res = mprotect(protect_start, protect_len,
                                    PROT_READ | PROT_WRITE);
                            ExcCheckErrno(res != -1, "failed to unprotect");
                        }
                        protect_start = start + len;

                        // Reback!
                        void * addr = mmap(start, len,
                                           PROT_READ | PROT_WRITE,
                                           MAP_PRIVATE | MAP_FIXED,
                                           backing_file_fd, foffset);

                        if (addr != start)
                            throw Exception(errno, "reback_range_after_write()",
                                            "mmap backing file");

                        backing_start2 = -1;
                    }

                    if (need_backing2 && backing_start2 == -1)
                        backing_start2 = k;

                } // end for

                // unprotect any leftover pages.
                size_t protect_len = (start + len) - protect_start;
                if (protect_len > 0) {
                    int res = mprotect(protect_start, protect_len,
                            PROT_READ | PROT_WRITE);
                    ExcCheckErrno(res != -1, "failed to unprotect");
                }

                // Since everything was just unprotected, we don't need the
                // guard anymore.
                mprotect_guard.clear();

                backing_start = -1;
            }

            if (need_backing && backing_start == -1)
                backing_start = j;
        }

        if (debug) {
            pm_current.update();
            cerr << "pm_current after = " << endl << pm_current << endl;
        }

    }

    if (debug) {
        cerr << "========= end reback_range_after_write pid="
             << getpid() << endl;
        cerr << endl;
    }

    return result * page_size;
}

size_t
Snapshot::
sync_to_disk(int fd,
             size_t file_offset,
             void * mem_start,
             size_t mem_size,
             Sync_Op op)
{
    if (op == RECLAIM_ONLY || op == SYNC_AND_RECLAIM)
        throw ML::Exception("reclaim op is not suported");

    bool debug = false;

    if (debug) cerr << "sync_to_disk(): pid = " << getpid()
                    << " op = " << op << endl;

    if (op != RECLAIM_ONLY
        && op != SYNC_ONLY
        && op != SYNC_AND_RECLAIM
        && op != DUMP)
        throw Exception("sync_to_disk(): invalid op");

    if (file_offset % page_size != 0)
        throw Exception("file offset not on a page boundary");

    size_t mem = (size_t)mem_start;
    if (mem % page_size != 0)
        throw Exception("mem_start not on a page boundary");

    if (mem_size % page_size != 0)
        throw Exception("mem_size not a multiple of page_size");

    send('s');

    send(fd);
    send(file_offset);
    send(mem_start);
    send(mem_size);
    send(op);

    ssize_t result = recv<size_t>();
    
    if (result == -1) {
        string error = recv_message();
        
        throw Exception("sync_to_disk(): snapshot process returned error: "
                        + error);
    }
        
    return result;
}

void
Snapshot::
client_sync_to_disk()
{
    try {
        int          fd          = recv<int>();
        size_t       file_offset = recv<size_t>();
        const char * mem_start   = recv<char *>();
        size_t       mem_size    = recv<size_t>();
        Sync_Op      op          = recv<Sync_Op>();

        // cerr << "memory:" << endl;
        // dump_page_info(mem_start, mem_size);

        // Now go and do it
        size_t result = 0;
        
        size_t npages = mem_size / page_size;
        
        vector<unsigned char> flags;
        if (op != DUMP)
            flags = page_flags(mem_start, npages);

        unique_ptr<Journal> journal;
        if (op == SYNC_ONLY || op == SYNC_AND_RECLAIM || op == DUMP)
            journal.reset(new Journal(fd, journalFile));

        // TODO: chunk at a time to avoid too many TLB invalidations

        off_t wanted_ofs = file_offset;
        for (unsigned i = 0;  i < npages;
             ++i, mem_start += page_size, wanted_ofs += page_size) {
            // Look at what the VM subsystem says:
            // - If the page isn't present or in swapped, then it has never
            //   been touched and what's already on the disk is the only
            //   possibility;
            
            // Can we skip this page?
            if (op != DUMP && !flags[i]) continue;

            // Do we need to write it to disk?
            if (op == SYNC_ONLY || op == SYNC_AND_RECLAIM || op == DUMP) {
                journal->addEntry(wanted_ofs, page_size, mem_start);
            }

            // Do we need to re-map the disk page on our process?
            int res = 0;
            if (op == RECLAIM_ONLY || op == SYNC_AND_RECLAIM) {
                void * addr = mmap((void *)mem_start, page_size,
                                   PROT_READ | PROT_WRITE,
                                   MAP_PRIVATE | MAP_FIXED, fd,
                                   wanted_ofs);

                if (addr != mem_start)
                    throw Exception("mmap failed: "  + string(strerror(errno)));

                // Map the page in by reading it
                char c = *(const char *)addr;
                // no-op that prevents the read from being optimized away.
                __asm__
                    (" # [in]"
                     : 
                     : [in] "r" (c)
                     :
                     );

                if (op == RECLAIM_ONLY) res = page_size;
            }
            
            result += res;
        }

        // cerr << "memory after:" << endl;
        // dump_page_info(mem_start - npages * page_size, mem_size);

        // Commit the writes to disk.
        if (op == SYNC_ONLY || op == SYNC_AND_RECLAIM || op == DUMP) {
            result += journal->applyToTarget();
        }

        send(result);

    }
    catch (const std::exception & exc) {
        ssize_t result = -1;
        send(result);
        std::string message = exc.what();
        send(message);
    }
    catch (...) {
        ssize_t result = -1;
        send(result);
        std::string message = "unknown exception caught";
        send(message);
    }
}

int
Snapshot::
terminate()
{
    if (!itl->pid)
        throw Exception("Snapshot::terminate(): already terminated");

    int res = close(itl->control_fd);
    if (res == -1)
        cerr << "warning: Snapshot::terminate(): close returned "
             << strerror(errno) << endl;

    res = close(itl->snapshot_pm_fd);
    if (res == -1)
        cerr << "warning: Snapshot::terminate(): close returned "
             << strerror(errno) << endl;

    int status = -1;
    res = waitpid(itl->pid, &status, 0);

    //cerr << "Snapshot::terminate(): res = " << res
    //     << " status = " << status << endl;

    if (res != itl->pid) {
        cerr << "warning: Snapshot::terminate(): waitpid returned pid "
             << res << " status " << status << endl;
    }
    
    itl->pid = 0;

    return status;
}

void
Snapshot::
disassociate()
{
    throw Exception("not done");
}

int
Snapshot::
run_child(int control_fd)
{
    itl->control_fd = control_fd;

    while (true) {
        char c;
        int res = read(control_fd, &c, 1);
        if (res == 0)
            return 0;

        if (res != 1) {
            cerr << "Snapshot: child read returned " << strerror(errno)
                 << endl;
            return -1;
        }

        if (c == 's')
            client_sync_to_disk();
        else {
            cerr << "Snapshot: child got unknown command "
                 << c << endl;
            return -1;
        }
    }
}

} // namespace MMap
} // namespace Datacratic
