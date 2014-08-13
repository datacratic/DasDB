/** simple_snapshot_test.cc                                 -*- C++ -*-
    Rémi Attab, 31 Aug 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Simple Snapshot tests

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "mmap/mmap_const.h"
#include "mmap/simple_snapshot.h"

#include <iostream>
#include <array>
#include <boost/test/unit_test.hpp>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;

struct SimpleJournalFixture : public TestFolderFixture
{
    SimpleJournalFixture() : TestFolderFixture("mmap_simple_journal") {}
    virtual ~SimpleJournalFixture() {}
};


// Handles the cleanup of the snapshot journal.
struct JournalFixture : public SimpleJournalFixture
{
    JournalFixture() :
        journalFile("snapshot_test.log")
    {
        unlink(journalFile.c_str());
    }

    virtual ~JournalFixture()
    {
        unlink(journalFile.c_str());
    }

    string journalFile;
};

struct TestRegion
{
    TestRegion(const string& file, size_t pageCount, bool wipe = true) :
        file(file),
        size(pageCount * page_size)
    {
        int flags = (wipe ? O_CREAT | O_TRUNC : 0) | O_RDWR;
        fd = open(file.c_str(), flags, 0666);
        ExcAssertErrno(fd >= 0);

        int ret = ftruncate(fd, size);
        ExcAssertErrno(!ret);

        void* addr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
        ExcAssertErrno(addr != MAP_FAILED);

        mem = (char*) addr;
    };

    void close()
    {
        if (mem) munmap(mem, size);
        mem = nullptr;

        if (fd != -1) ::close(fd);
        size = 0;
    }

    ~TestRegion()
    {
        close();
    }

    void setPage(char c, uint64_t page)
    {
        uint64_t first = page << page_shift;
        uint64_t last = first + page_size;
        std::fill(mem + first, mem + last, c);
    }

    void checkFile(char c, uint64_t page)
    {
        uint64_t first = page << page_shift;

        std::array<char, page_size> buffer;
        ssize_t bytesRead = pread(fd, buffer.data(), page_size, first);
        ExcAssertErrno(bytesRead != -1);
        ExcAssertEqual(bytesRead, page_size);

        bool b = std::all_of(
                buffer.begin(), buffer.end(),
                [=](char rhs) -> bool { return c == rhs; });
        BOOST_CHECK(b);
    }

    void checkPage(char c, uint64_t page)
    {
        uint64_t first = page << page_shift;
        uint64_t last = first + page_size;

        bool b = all_of(
                mem + first, mem + last,
                [=](char rhs) -> bool { return c == rhs; });
        BOOST_CHECK(b);

        checkFile(c, page);
    }

    string file;
    int fd;
    char* mem;
    size_t size;
};


BOOST_FIXTURE_TEST_CASE( test_basics, JournalFixture )
{
    enum { PageCount = 10 };

    {
        TestRegion r0("test.mmap", PageCount);
        DirtyPageTable dirtyPages;

        for (uint64_t page = 0; page < PageCount; page += 2) {
            r0.setPage('a', page);
            dirtyPages.markPage(page * page_size);
        }

        SimpleSnapshot s0(dirtyPages, journalFile);
        size_t bytesWritten = s0.sync(r0.fd, 0ULL, r0.mem, r0.size);

        BOOST_CHECK_EQUAL(bytesWritten, (PageCount / 2) * page_size);

        for (uint64_t page = 0; page < PageCount; page++)
            r0.checkPage(page % 2 ? '\0' : 'a', page);
    }

    {
        TestRegion r0("test.mmap", PageCount);
        DirtyPageTable dirtyPages;

        for (uint64_t page = 1; page < PageCount; page += 2) {
            r0.setPage('b', page);
            dirtyPages.markPage(page * page_size);
        }

        SimpleSnapshot s0(dirtyPages, journalFile);
        size_t bytesWritten = s0.sync(r0.fd, 0ULL, r0.mem, r0.size);

        BOOST_CHECK_EQUAL(bytesWritten, (PageCount / 2) * page_size);

        for (uint64_t page = 0; page < PageCount; page++)
            r0.checkPage(page % 2 ? 'b' : '\0', page);
    }

    {
        TestRegion r0("test.mmap", PageCount);
        DirtyPageTable dirtyPages;

        {
            SimpleSnapshot s0(dirtyPages, journalFile);
            size_t bytesWritten = s0.sync(r0.fd, 0ULL, r0.mem, r0.size);
            BOOST_CHECK_EQUAL(bytesWritten, 0);
        }

        for (uint64_t page = PageCount /2; page < PageCount; page++) {
            r0.setPage('c', page);
            dirtyPages.markPage(page * page_size);
        }

        {
            SimpleSnapshot s0(dirtyPages, journalFile);
            size_t bytesWritten = s0.sync(r0.fd, 0ULL, r0.mem, r0.size);
            BOOST_CHECK_EQUAL(bytesWritten, (PageCount / 2) * page_size);
        }

        for (uint64_t page = 0; page < PageCount; page++)
            r0.checkPage(page < PageCount /2 ? '\0' : 'c', page);
    }


}

