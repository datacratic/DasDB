/* mmap_test.h                                              -*- C++ -*-
   Rémi Attab, 6 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Collection of teting utilities for the mmap.
*/

#ifndef __mmap__test_h__
#define __mmap__test_h__

#include "mmap/debug.h"
#include "mmap/mmap_file.h"
#include "mmap/memory_tracker.h"
#include "soa/utils/print_utils.h"
#include "soa/utils/threaded_test.h"
#include "jml/utils/exc_check.h"
#include "jml/utils/exc_assert.h"
#include "jml/compiler/compiler.h"

#include <string>


namespace Datacratic {
namespace MMap {



/******************************************************************************/
/* TEST FOLDER FIXTURE                                                        */
/******************************************************************************/

/** Changes the current working dir to a test folder where files can be created
 * and deleted without impacting the source tree.
 */
struct TestFolderFixture
{
    TestFolderFixture(const std::string& name);
    virtual ~TestFolderFixture();

    /** Returns a name that is unique per test and per user. Suitable for a mmap
        file name.
    */
    std::string uniqueName() const;

private:
    std::string name;
    std::string oldWd;
    static int testCount;
};


/*****************************************************************************/
/* MMAP FILE UTILS                                                           */
/*****************************************************************************/

/** Deletes any leftover files related to the mmap of the given name

    \todo Might want to expose to the user.
*/
void cleanupMMapTest(const std::string& name, bool cleanMainFile);

/** Prints if one of the files associated with a given mmap still exists  */
void checkMMapCleanup(const std::string& name);

/** Automates all the file handling checking for a test. */
struct MMapFileFixture : public TestFolderFixture {

    MMapFileFixture(const std::string& name, bool cleanup = true);

    ~MMapFileFixture()
    {
        checkMMapCleanup(filename);
        cleanupMMapTest(filename, true);
    }

    uint64_t recoverMMap();

    std::string filename;
    std::string journalFile;
};

/** Automates all the file handling associated to Anonymous mmaps. */
struct MMapAnonFixture {
    MMapAnonFixture() : filename(initFile()), area(RES_CREATE) {}
    ~MMapAnonFixture()
    {
        checkMMapCleanup(filename);
        cleanupMMapTest(filename, true);
    }

private:

    std::string filename; // Only used for shm file cleanup.
    std::string initFile();

public:

    MMapFile area;

};


/*****************************************************************************/
/* PROGRESS                                                                  */
/*****************************************************************************/

/** Progress tracker that can be snapshotted atomically.*/
struct Progress {
    volatile union {
        struct {
            uint16_t threads[8];
        };
        struct {
            uint64_t q __attribute__((__vector_size__(16)));
        };
    };

    Progress() {
        memset(threads, 0, 16);
    }

    Progress snapshot() {
        Progress s;
        s.q = q;
        return s;
    }

    void restore(Progress& p) {
        q = p.q;
    }

    std::string print();

} JML_ALIGNED(16);


/*****************************************************************************/
/* FORK                                                                      */
/*****************************************************************************/

/** Helper to fork a process and then kill it as brutally as possible. */
// \todo Need to close up the pipes properly.
struct Fork {

    enum fds { none = 0, out = 1, err = 2 };

    /** Forks a process and executes fn.
        stdout and stderr can be suppressed by ORing one of the fds enum.
    */
    Fork(const std::function<void(void)>& fn, fds redirect = none);

    ~Fork();

    /** Sends a signal to the process and waits for it to die.

        Note that any child of the child process might not take the killing of
        its parent too well. If its endless whinning is bothering your
        conscience you can shut it up in the constructor by passing the one of
        the fds enum.
    */
    void kill();

    pid_t pid;

private:

    static void repeaterThread();
    static void snapshotThread();

    /** Redirects an fd to /dev/null */
    void supressFd(int fd);

    int parentPipe[2];
    int childPipe[2];

    static Fork* this_;
};


/******************************************************************************/
/* TEST SWITCHES                                                              */
/******************************************************************************/

struct TestSwitchGuard
{
    TestSwitchGuard(bool& s, bool value) :
        testSwitch(s)
    {
        oldValue = testSwitch;
        testSwitch = value;
    }

    TestSwitchGuard(
            bool& s, const std::function<void(void)>& fn, bool value)
        : testSwitch(s), doneFn(fn)
    {
        oldValue = testSwitch;
        testSwitch = value;
    }

    ~TestSwitchGuard()
    {
        if(doneFn) doneFn();
        testSwitch = oldValue;
    }

private:
    bool& testSwitch;
    bool oldValue;
    std::function<void(void)> doneFn;
};


inline TestSwitchGuard
enableTrieMemoryTracker(bool leakCheck = true)
{
    auto doLeakCheck = [=] {
        if (!leakCheck) return;
        trieMemoryTracker.dumpLeaks();
    };
    return TestSwitchGuard(trieMemoryCheck, doLeakCheck, true);
}


inline TestSwitchGuard
enableKeyFragmentTracker(bool leakCheck = true)
{
    auto doLeakCheck = [=] {
        if (!leakCheck) return;
        kfMemoryTracker.dumpLeaks();
    };
    return TestSwitchGuard(kfMemoryCheck, doLeakCheck, true);
}



inline TestSwitchGuard
enableTrieDebug()
{
    return TestSwitchGuard(trieDebug, true);
}


inline TestSwitchGuard
enableRegionResizeExceptionTest()
{
    return TestSwitchGuard(
            regionExceptionTest, [] { resetRegionExceptionTest(); }, true);
}

} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__test_h__ */
