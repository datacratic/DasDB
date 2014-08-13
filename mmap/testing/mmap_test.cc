/* mmap_test.cc                                              -*- C++ -*-
   Rémi Attab, 16 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Collection of teting utilities for the mmap.
*/


#include "mmap_test.h"
#include "mmap/sync_stream.h"
#include "mmap/journal.h"
#include "jml/utils/guard.h"

#include <memory>
#include <iostream>
#include <sstream>
#include <array>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>

using namespace std;
using namespace ML;

namespace Datacratic {
namespace MMap {


/******************************************************************************/
/* TEST FOLDER FIXTURE                                                        */
/******************************************************************************/

namespace {
const string tmp_dir = "./build/x86_64/tmp/";
};

int TestFolderFixture::testCount = 0;

TestFolderFixture::
TestFolderFixture(const string& name) :
    name(name)
{
    {
        std::array<char, 1024> buf;
        char* ret = getcwd(buf.data(), buf.size());
        ExcAssertErrno(ret);
        oldWd = std::string(buf.data());
    }

    string testDir = tmp_dir + name + "_" + to_string(testCount++);

    auto checkDir = [&](const string& dir) {
        struct stat st;
        int ret = stat(dir.c_str(), &st);
        if (ret) {
            ret = mkdir(dir.c_str(), 0774);
            ExcAssertErrno(!ret);
        }
    };
    checkDir(tmp_dir);
    checkDir(testDir);

    int ret = chdir(testDir.c_str());
    ExcAssertErrno(!ret);
}

TestFolderFixture::
~TestFolderFixture()
{
    int ret = chdir(oldWd.c_str());
    ExcAssertErrno(!ret);
}

string
TestFolderFixture::
uniqueName() const
{
    stringstream ss;
    ss << name << "_" << testCount << "_" << getuid();
    return ss.str();
}


/*****************************************************************************/
/* MMAP FILE UTILS                                                           */
/*****************************************************************************/

void
cleanupMMapTest(const std::string& name, bool cleanMainFile)
{
    // gc lock file.
    auto gcName = [] (const std::string& name, unsigned id) -> std::string {
        std::stringstream ss;
        ss << "gc." << name << "_" << id;
        return ss.str();
    };

    // boost ipc mutex file.
    auto semName = [] (const std::string& name) -> std::string {
        return "sem." + name;
    };

    auto trieName = [] (const std::string& name, unsigned id) -> std::string {
        stringstream ss;
        ss << "trie." << name << "_" << id;
        return ss.str();
    };

    auto rm = [] (
            const std::function<int(const char*)>& fn,
            const std::string& name)
    {
        int res = fn(name.c_str());
        (void) res;
        // if (!res) std::cerr << "Deleted: " << name << std::endl;
    };

    if (cleanMainFile) {
        rm(unlink, name);
        rm(unlink, name + ".log");
    }

    rm(shm_unlink, semName(name));
    rm(shm_unlink, semName("resize." + name));
    rm(shm_unlink, semName("snapshot." + name));
    rm(shm_unlink, gcName(name, 0));
    rm(shm_unlink, semName(gcName(name, 0)));

    for (int id = 0; id < 64; ++id) {
        rm(shm_unlink, gcName(name, id));
        rm(shm_unlink, semName(gcName(name, id)));
        rm(shm_unlink, semName(trieName(name, id)));
    }
}

void
checkMMapCleanup(const std::string& name)
{
    // gc lock file.
    auto gcName = [] (const std::string& name, unsigned id) -> std::string {
        std::stringstream ss;
        ss << "/dev/shm/gc." << name << "_" << id;
        return ss.str();
    };

    // boost ipc mutex file.
    auto semName = [] (const std::string& name) -> std::string {
        return "/dev/shm/sem." + name;
    };


    auto trieName = [] (const std::string& name, unsigned id) -> std::string {
        stringstream ss;
        ss << "trie." << name << "_" << id;
        return ss.str();
    };

    auto check = [] (const std::string& name) {
        struct stat buf;

        if (!stat(name.c_str(), &buf))
            std::cerr << "Leftover: " << name << std::endl;
    };

    check(name);
    check(name + ".log");
    check(semName(name));
    check(semName("resize." + name));
    check(semName("snapshot." + name));
    check(gcName(name, 0));
    check(semName(gcName(name, 0)));

    for (int id = 0; id < 64; ++id) {
        check(gcName(name, id));
        check(semName(gcName(name, id)));
        check(semName(trieName(name, id)));
    }

}


MMapFileFixture::
MMapFileFixture(const string& name, bool cleanup) :
    TestFolderFixture(name)
{
    filename = uniqueName();
    journalFile = filename + ".log";

    // In case the previous run wasn't able to clean itself up.
    cleanupMMapTest(filename, cleanup);
}

uint64_t
MMapFileFixture::
recoverMMap()
{
    int fd = open(filename.c_str(), O_RDWR);
    ExcCheckErrno(fd != -1, "Failed to open the mmap file:" + filename);

    ML::Call_Guard close_guard([&] { close(fd); });

    return Journal::undo(fd, journalFile);
}


std::string
MMapAnonFixture::
initFile()
{
    // Make sure 2 users running the tests don't interfere with each other.
    std::stringstream ss;
    ss << "mmap_anon_" << getuid();
    std::string filename = ss.str();

    cleanupMMapTest(filename, true);

    return filename;
}


/*****************************************************************************/
/* PROGRESS                                                                  */
/*****************************************************************************/

std::string
Progress::
print()
{
    std::stringstream ss;

    ss << "{ ";
    for (int id = 0; id < 8; ++id) {
        if (!threads[id]) continue;
        ss << id << ":" << threads[id] << ", ";
    }
    ss << "}";

    return ss.str();
}



/*****************************************************************************/
/* FORK                                                                      */
/*****************************************************************************/

Fork* Fork::this_ = NULL;


Fork::
Fork(const std::function<void(void)>& fn, fds redirect)
{
    // Make sure this object is available statically.
    ExcAssert(!this_);
    this_ = this;


    // Make sure boost.test doesn't complain when we call kill().
    signal(SIGCHLD, SIG_DFL);

    int res = pipe(parentPipe);
    ExcCheckErrno(res != -1, "Failed to create the parent pipe");

    pid = fork();
    ExcCheckErrno(pid != -1, "failed to fork");

    if (!pid) {

        // non-blocking because we don't know if there's a child
        // listening or not.
        int res = pipe(childPipe);
        ExcCheckErrno(res != -1, "Failed to create the child pipe");

        std::thread th(repeaterThread);
        th.detach();

        res = pthread_atfork(NULL, NULL, snapshotThread);
        ExcCheckErrno(!res, "Failed to install the fork hook");

        // \todo This became broken somehow.
        if (redirect & out) supressFd(1);
        if (redirect & err) supressFd(2);

        fn();

        return;
    }
}

Fork::
~Fork()
{
    (void) close(parentPipe[0]);
    (void) close(parentPipe[1]);
    this_ = NULL;
}


void
Fork::
kill()
{
    char c = 'k';

    // sync_cerr() << "kill(" << pid << ")" << endl << sync_dump;

    ssize_t sizeWritten = write(parentPipe[1], &c, 1);
    ExcCheckErrno(sizeWritten != -1, "Failed to write to pipe");
    ExcCheckEqual(sizeWritten, 1, "Failed to write all bytes to pipe");

    int status = -1;
    int res = waitpid(pid, &status, 0);
    ExcCheckErrno(res != -1, "waitpid failed");

    return;
}


void
Fork::
repeaterThread()
{
    (void) close(this_->parentPipe[1]);

    // sync_cerr() << "repeaterThread(" << getpid() << ")\n" << sync_dump;

    char c;

    int sizeRead = read(this_->parentPipe[0], &c, 1);
    ExcCheckEqual(sizeRead, 1, "Failed to read from the pipe");

    int sizeWritten = write(this_->childPipe[1], &c, 1);
    ExcCheckEqual(sizeWritten, 1, "Failed to write to the pipe");

    // sync_cerr() << "killing repeater: " << getpid() << endl << sync_dump;

    ExcCheckEqual(c, 'k', "Unknown message");
    ::kill(getpid(), SIGKILL);
}

void
Fork::
snapshotThread()
{
    (void) close(this_->childPipe[1]);

    auto runThread = [&] {
        // sync_cerr() << "snapshotThread(" << getpid() << ")\n" << sync_dump;

        char c;

        ssize_t sizeRead = read(this_->childPipe[0], &c, 1);
        ExcCheckErrno(sizeRead != -1, "Failed to read from the pipe");
        ExcCheckEqual(sizeRead, 1, "Failed to read from the pipe");

        // sync_cerr() << "killing snapshot: " << getpid() << endl << sync_dump;

        ExcCheckEqual(c, 'k', "Unknown message");
        ::kill(getpid(), SIGKILL);
    };

    std::thread th(runThread);
    th.detach();
}

void
Fork::
supressFd(int fd)
{
    int fdNull = open("/dev/null", O_APPEND);
    ExcCheckErrno(fd != -1, "failed to open /dev/null");

    int res = dup2(fdNull, fd);
    ExcCheckErrno(res != -1, "failed to replace the fd");
}


} // namespace MMap
} // namespace Datacratic
