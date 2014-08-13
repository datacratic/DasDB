/** simple_snapshot.cc                                 -*- C++ -*-
    Rémi Attab, 31 Aug 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Experimental bla bla bla... Just look at the header file...

*/

#include "simple_snapshot.h"
#include "journal.h"
#include "jml/utils/exc_check.h"
#include "jml/utils/exc_assert.h"

#include <iostream>
#include <array>
#include <algorithm>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>


using namespace std;
using namespace ML;

namespace Datacratic {
namespace MMap {


/******************************************************************************/
/* UTILITIES                                                                  */
/******************************************************************************/

namespace {
enum { MSG_SYNC, MSG_KILL, MSG_DONE, MSG_ERR };

void send(int fd, uint64_t msg)
{
    int bytes = write(fd, &msg, sizeof(msg));
    ExcCheckErrno(bytes == sizeof(msg), "Unable to write to pipe");
}

void send(int fd, const string& msg)
{
    int bytes = write(fd, msg.c_str(), msg.size());
    ExcCheckErrno(bytes == msg.size(), "Unable to write to pipe");
}

int64_t recvInt(int fd)
{
    int64_t buf = 0;

    int bytes = read(fd, &buf, sizeof(buf));
    ExcCheckErrno(bytes == sizeof(buf), "Unable to read from pipe");

    return buf;
}

string recvStr(int fd)
{
    array<char, 0x1000> buf;

    int bytes = read(fd, buf.data(), buf.size());
    ExcCheckErrno(bytes > 0, "Unable to read from pipe");

    return string(buf.data(), bytes);
}

}


/******************************************************************************/
/* SIMPLE SNAPSHOT                                                            */
/******************************************************************************/

SimpleSnapshot::
SimpleSnapshot(
        DirtyPageTable& dirtyPages,
        const std::string& journalFile) :
    dirtyPages(dirtyPages),
    journalFile(journalFile)
{
    int sockets[2];
    int res = socketpair(AF_UNIX, SOCK_STREAM, 0, sockets);
    ExcCheckErrno(!res, "Unable to create the snapshot pipe");

    pid_t pid = fork();
    ExcCheckErrno(pid >= 0, "Unable to create the snapshot");

    // Parent process.
    if (pid) {
        close(sockets[0]);
        snapshotPid = pid;
        snapshotFd = sockets[1];
    }

    // Snapshot process
    else {
        close(sockets[1]);
        runChild(sockets[0]);
    }
}


SimpleSnapshot::
~SimpleSnapshot()
{
    if (!snapshotPid) return;

    send(snapshotFd, MSG_KILL);

    int ret = close(snapshotFd);
    if (ret) cerr << "WARNING: Unable to close the snapshot file pipe." << endl;

    pid_t pid = waitpid(snapshotPid, nullptr, 0);
    ExcCheckErrno(pid == snapshotPid, "Unable to wait for the snapshot");

    snapshotPid = 0;
}

size_t
SimpleSnapshot::
sync(int fd, size_t foffset, void* start, size_t len)
{
    ExcCheckEqual(foffset % page_size, 0, "offset not on a page boundary");
    ExcCheckEqual(len % page_size, 0, "len not on a multiple of page_size");
    ExcCheckEqual(reinterpret_cast<uint64_t>(start) % page_size, 0,
            "start not on a page boundary");

    send(snapshotFd, MSG_SYNC);
    send(snapshotFd, fd);
    send(snapshotFd, foffset);
    send(snapshotFd, reinterpret_cast<uint64_t>(start));
    send(snapshotFd, len);

    int ret = recvInt(snapshotFd);
    ExcAssert(ret == MSG_DONE || ret == MSG_ERR);

    if (ret == MSG_ERR) {
        string err = recvStr(snapshotFd);
        throw Exception("sync error: " + err);
    }

    size_t bytesWritten = recvInt(snapshotFd);
    return bytesWritten;
}

int
SimpleSnapshot::
runChild(int controlFd)
{
    int ret = -1;

    try {
        while(true) {
            int msg = recvInt(controlFd);

            if (msg == MSG_KILL) {
                ret = 0;
                break;
            }

            if (msg == MSG_SYNC) {
                int fd         = recvInt(controlFd);
                size_t foffset = recvInt(controlFd);
                char* start    = reinterpret_cast<char*>(recvInt(controlFd));
                size_t len     = recvInt(controlFd);

                size_t bytesWritten = doSync(fd, foffset, start, len);

                send(controlFd, MSG_DONE);
                send(controlFd, bytesWritten);
            }

            else ExcCheck(false, "Unknown message");
        }
    }
    catch (const std::exception & exc) {
        cerr << "child exiting with exception " << exc.what() << endl;
        send(controlFd, MSG_ERR);
        send(controlFd, exc.what());
    }
    catch (...) {
        cerr << "child exiting with unknown exception " << endl;
        send(controlFd, MSG_ERR);
        send(controlFd, "unknown exception");
    }

    // Skips all the destructors so we don't screw up any existing
    // structures in the parent process.
    _exit(ret);
}

size_t
SimpleSnapshot::
doSync(int fd, size_t foffset, char* start, size_t len)
{
    Journal journal(fd, journalFile);

    uint64_t page = dirtyPages.nextPage(foffset);
    for (; page < foffset + len; page = dirtyPages.nextPage(page + page_size)) {
        journal.addEntry(page, page_size, start + page);        
    }

    return journal.applyToTarget();
}



} // namespace MMap
} // namepsace Datacratic
