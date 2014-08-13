/* snapshot.h                                                      -*- C++ -*-
   Jeremy Barnes, 22 February 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   Class to create and manipulate snapshots of a process's virtual memory
   space.

   Used to allow the following features with memory-mapped (file backed)
   data structures:
   - Hot snapshotting
   - Hot replication
   - Journaling and fault tolerance
*/

#ifndef __storage__snapshot_h__
#define __storage__snapshot_h__


#include <boost/scoped_ptr.hpp>
#include <boost/function.hpp>
#include <boost/tuple/tuple.hpp>
#include <string>


namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* SNAPSHOT                                                                  */
/*****************************************************************************/

/** This is a structure that controls a snapshot of the memory at a given
    time.  Underneath, it's a separate process that was forked with copy-on-
    write mode on to capture that status of the virtual memory at that point
    in time.  A pipe is maintained with the other process in order to allow
    it to be controlled
*/

struct Snapshot {

    // The function that will be run in the snapshot.  Takes as an argument
    // a single int which is the control_fd for it to work on.  Returns a
    // single int which will be returned to the parent once it exits.
    typedef boost::function<int (int)> Worker;

    // Create a snapshot of the current process
    Snapshot(const std::string& journalFile, Worker worker = Worker());
    
    // Terminate the snapshot of the current process
    ~Snapshot();

    int control_fd() const;

    void send_message(const char * data, size_t sz);

    std::string recv_message();

    void recv_message(char * data, size_t sz, bool all_at_once = true);

    template<typename X>
    void send(const X & x)
    {
        send_message((const char *)&x, sizeof(X));
    }

    void send(const std::string & str);

    template<typename X>
    void recv(X & x)
    {
        recv_message((char *)&x, sizeof(X));
    }

    void recv(std::string & str);

    template<typename X>
    X recv()
    {
        X result;
        recv(result);
        return result;
    }

    // Return the FD of the /proc/pid/pagemap for the snapshot process.
    int pagemap_fd() const;

    // Sync a snapshot to disk, and make unmodified local pages be backed
    // by the disk again.
    //
    // Perform a complete sync of the given file-backed memory range across
    // the disk, the snapshot process and the current process.  Can be called
    // while arbitrary reads and writes are occuring to the mapped range, with
    // one caveat (a system call that returns EFAULT must be re-tried).
    //
    // Preconditions:
    // - The file descriptor fd at offset file_offset was memory mapped at
    //   mem_start with a MAP_PRIVATE mapping and extends at least mem_size
    //   bytes.  This mapping must have occurred *before* the snapshot was
    //   created (so it is valid both for the current process and the snapshot).
    // - If current_pagemap_file is not -1, it is the file /proc/self/pagemap
    //   from the current process.
    // - No memory writes are occurring in the snapshot at the same time
    // - No process is writing to the given section of the backing file at the
    //   same time.
    // - Only one thread can call this function at a time.
    //
    // Postconditions:
    // - The given range of the file matches *exactly* the contents of the
    //   memory at the time the snapshot was taken.
    // - The snapshot process has no private page table entries in the range;
    //   all of its pages are backed by the file.
    // - Any pages of the current process that have not been modified since
    //   the snapshot was taken will also be backed by the file (not private)
    // - The memory of the current process is unaffected, and any reads and
    //   writes that happened while the process was being performed continued
    //   transparently, EXCEPT for system calls that wrote to the memory, which
    //   may have returned EFAULT and should have been restarted.
    //
    // The algorithm is:
    // 1.  We ask the snapshot to sync itself to disk, using sync_to_disk() in
    //     SYNC_ONLY mode.
    // 2.  We make the current process re-sync its unmodified pages from the
    //     disk file, using reback_range_after_write()
    // 3.  Optionally (not necessary if the snapshot process will die), we
    //     update the snapshot's private pages to the mapped ones that were
    //     written.
    
    enum Reclaim_Option {
        NO_RECLAIM,
        RECLAIM
    };

    boost::tuple<size_t, size_t, size_t>
    sync_and_reback(int fd,
                    size_t file_offset,
                    void * mem_start,
                    size_t mem_size,
                    Reclaim_Option reclaim_snapshot,
                    int current_pagemap_file = -1);

    // What operation to perform on the file?
    enum Sync_Op {
        RECLAIM_ONLY,     ///< Replace private pages with disk-backed ones
        SYNC_ONLY,        ///< Dump modified (private) pages to disk
        SYNC_AND_RECLAIM, ///< Sync then reclaim
        DUMP              ///< Dump all pages to disk
    };

    // Go through the given memory range in the snapshot's address space
    // and perform an operation on each page that is found to be out-of-sync
    // (ie, modified) from the copy on disk.
    //
    // If op is SYNC_ONLY, then the pages that are out-of-sync will be written
    // to disk.  Note that nothing is modified that will make the pages look
    // in-sync, so calling twice with SYNC_ONLY will result in the pages being
    // written twice.
    //
    // If op is RECLAIM_ONLY, then it is assumed that a SYNC_ONLY operation
    // has already run and no page has been modified since.  Those pages that
    // are out-of-sync will have the corresponding disk page re-mapped onto
    // that address.  At the end, no pages will be out of sync.  The disk will
    // not be touched.  Any pages that were modified since the SYNC_ONLY
    // completed will be reverted to their on-disk versions.  The primary
    // advantage of this call is that it turns private pages (which must be
    // evicted to swap) into backed pages (that can be cheaply evicted), and
    // reduces the memory pressure on the machine.
    //
    // If op is RECLAIM_AND_SYNC, then the sync will happen, followed by the
    // reclaim.
    //
    // If op is DUMP, then all pages will be written to the disk file.  Note
    // that in this case, no reclamation takes place.  The file must have been
    // expanded (via truncate() or similar) to be large enough to accomodate
    // the data.  This is useful when creating a file for the first time.
    //
    // This function is not tolerant to simultaneous modification of the pages
    // on the snapshot, nor to simultaneous modification of the disk file from
    // any process.
    //
    // Note that the reclaim operations don't actually read the content of the
    // pages, so it is not important that they be in memory.
    size_t sync_to_disk(int fd,
                        size_t file_offset,
                        void * mem_start,
                        size_t mem_size,
                        Sync_Op op);


    /** Make the VM subsystem know that modified pages in a MAP_PRIVATE mmap
        segment have now been written to disk and so the private copy can be
        returned to the system.  The function operates transparently to all
        other threads, which can read and write memory as they wish (using
        appropriate memory barriers) transparently.
        
        In order to detect which pages are now written to disk, it is necessary
        to have forked another process that has exactly those pages that were
        written to disk in memory.  It can't have remapped the written pages
        onto its address space yet.  A file descriptor to that process's open
        /proc/pid/pagemaps file needs to be passed in, as well as a file
        descriptor to the *current* process's /proc/self/pagemaps file.
        
        In order to determine if a page can be updated, we perform the following
        process:
          - If the page is not present in the current process, then it must
            still be backed by the file and so it is skipped;
          - If the page is present in the current process but not present in the
            previous process, then the current process must have written to it
            and so it is skipped;
          - If the page is present in both processes but the physical page is
            different, then it must have been modified in the current process
            since the snapshot and so it is skipped;
          - Otherwise, we can memory map the portion of the file in the current
            process as it must be identical to the portion of the file.

        In order to do this atomically, the following procedure is used:

        1.  We remap the memory range read-only so that writes are no longer
            possible.
        2.  We re-perform the check to make sure that the page wasn't written
            to between when we checked and when we made it read-only.
        3.  We memory map the underlying page as a copy on write mapping.

        If between 1 and 3 there was an attempted write to the page being
        remapped, the thread that was doing it will get a segmentation fault.
        This fault is handled by:

        1.  Checking that the fault address was within the memory range.  If
            not, a normal segfault is generated.
        2.  Busy-waiting in the signal handler until the page is re-mapped
            and can therefore be written to.
        3.  Returning from the signal handler to allow the write to happen to
            the newly mapped page.

        Of course, to do this we need to make sure that all threads that start
        up handle sigsegv.
    */

    static size_t reback_range_after_write(int fd,
                                           size_t file_offset,
                                           void * mem_start,
                                           size_t mem_size,
                                           int old_pagemap_file,
                                           int current_pagemap_file);
    
    // Terminate the snapshot; the process will die and the connection will
    // be lost.  Asynchronous.  Returns the return code of the child function.
    int terminate();

    // Disassociate ourselves with the snapshot.  Used after a fork by the
    // child process so that the child process exiting doesn't cause problems
    // for the parent process.
    void disassociate();

private:
    // Run the default function for the child process
    int run_child(int control_fd);

    // Client process for the sync_to_disk command
    void client_sync_to_disk();

    struct Itl;
    boost::scoped_ptr<Itl> itl;
    std::string journalFile;
};

} // namespace MMap
} // namespace Datacratic

#endif /* __storage__snapshot_h__ */
