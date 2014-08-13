/* journal_test.cc                                                   -*- C++ -*-
   Rémi Attab, 12 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Test for the journalling facility
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "mmap/journal.h"
#include "mmap/mmap_const.h"
#include "jml/utils/exc_check.h"

#include <boost/test/unit_test.hpp>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <array>
#include <algorithm>
#include <iostream>


using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace std;

enum { target_size = page_size * 10 };
typedef array<char, target_size> file_array;

struct JournalFixture {

    JournalFixture() :
        journalFile("journal_test_file.log"),
        targetFile("journal_test_file")
    {
        targetFd = open(targetFile.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        ExcCheckErrno(targetFd != -1, "Failed to open test file");

        int res = ftruncate(targetFd, target_size);
        ExcCheckErrno(res != -1, "Failed to truncate the test file");
    }

    ~JournalFixture() {
        close(targetFd);
        unlink(targetFile.c_str());
    }

    string journalFile;
    string targetFile;
    int targetFd;
};

file_array readFile(int fd) {
    file_array buffer;

    ssize_t res = pread(fd, buffer.data(), target_size, 0);
    ExcCheckErrno(res != -1, "Failed to read the file");
    ExcCheckEqual(res, target_size, "Failed to read enough bytes");

    return buffer;
}

void dump(file_array& mem) {
    stringstream ss;
    ss << hex;

    for (uint64_t i = 0; i < target_size; ++i) {
        ss << mem[i] << " ";
        if (i % 64 == 63) ss << endl;
    }

    cerr << ss.str();
}

BOOST_FIXTURE_TEST_CASE( test_journal, JournalFixture )
{
    enum { CHUNK = cache_line };

    file_array mem;

    auto journal = [&] (const string& name, uint64_t expectedWritten) {
        cerr << "--- " << name << " ---" << endl;

        Journal journal(targetFd, journalFile);
        journal.addEntry(0, target_size, mem.data());

        uint64_t written = journal.applyToTarget();
        BOOST_CHECK_EQUAL(written, expectedWritten);

        file_array buffer = readFile(targetFd);
        BOOST_CHECK(equal(buffer.begin(), buffer.end(), mem.begin()));

        cerr << endl;
    };

    // No modifications, shouldn't journal or write anything.
    fill(mem.begin(), mem.end(), 0);
    journal("null", 0);

    // 1 byte = 1 chunk written.
    mem[CHUNK] = 0x55;
    journal("1 chunk", CHUNK);

    // Fill the entire area. Everything should get written back to the file.
    fill(mem.begin(), mem.end(), 0xFF);
    journal("all", target_size);

    // write one byte per chunks. Everything should written and journalled.
    for (uint64_t i = 0; i < target_size; i += CHUNK)
        mem[i] = 0;
    journal("first byte of all chunks", target_size);

    // One byte every 2 chunk. Half the page should be written.
    for (uint64_t i = 0; i < target_size; i += 2*CHUNK)
        mem[i] = 0xFF;
    journal("first byte of every other chunks", target_size / 2);

    // First byte of every chunk in 2 large chunks.
    enum {LARGE_CHUNK = target_size / 4 };
    for (uint64_t i = 0; i < LARGE_CHUNK; i += CHUNK)
        mem[i] = 0x11;
    for (uint64_t i = LARGE_CHUNK*2; i < LARGE_CHUNK*3; i += CHUNK)
        mem[i] = 0x11;
    journal("2 large chunk", LARGE_CHUNK * 2);

    // Multiple add entry calls should still write everything.
    fill(mem.begin(), mem.end(), 0x33);
    {
        cerr << "--- 2 entries ---" << endl;

        Journal journal(targetFd, journalFile);
        journal.addEntry(0, target_size/2, mem.data());
        journal.addEntry(
                target_size/2, target_size/2, mem.data() + target_size/2);

        uint64_t written = journal.applyToTarget();
        BOOST_CHECK_EQUAL(written, target_size);

        file_array buffer = readFile(targetFd);
        BOOST_CHECK(equal(buffer.begin(), buffer.end(), mem.begin()));

        cerr << endl;
    }
}

BOOST_FIXTURE_TEST_CASE( test_undo, JournalFixture )
{
    enum { iterations = 20 };

    uint64_t undoCount = 0;
    uint64_t successCount = 0;
    uint64_t noopCount = 0;

    for (int i = 0; i < iterations; ++i) {
        file_array oldMem = readFile(targetFd);
        file_array mem = oldMem;

        fill(mem.begin(), mem.end(), (char)i);

        Journal journal(targetFd, journalFile);

        for (uint64_t j = 0; j < target_size; j += page_size) {
            journal.addEntry(j, page_size, mem.data() + j);
        }

        Fork child([&] { journal.applyToTarget(); }, Fork::err);

        uint64_t waitMs = random() % 500;
        this_thread::sleep_for(chrono::milliseconds(waitMs));
        child.kill();

        // If the journal was deleted then the write was completed.
        struct stat s;
        bool writeCompleted = stat(journalFile.c_str(), &s);

        uint64_t undone = Journal::undo(targetFd, journalFile);

        file_array file = readFile(targetFd);

        // If undo wrote something then, then we're back to the original state.
        if (undone > 0) {
            cerr << "UNDO   : "
                << "attempt=" << i
                << ", expected=" << ((uint64_t)oldMem.front())
                << ", actual=" << ((uint64_t)file.front())
                << endl;


            BOOST_CHECK(equal(file.begin(), file.end(), oldMem.begin()));
            undoCount++;
        }

        // If the journal file doesn't exist then the write completed correctly.
        else if (writeCompleted) {
            cerr << "WRITTEN: "
                << "attempt=" << i
                << ", expected=" << ((uint64_t)mem.front())
                << ", actual=" << ((uint64_t)file.front())
                << endl;
            BOOST_CHECK(equal(file.begin(), file.end(), mem.begin()));
            successCount++;
        }

        // If the journal file still exists but nothing was undone
        // (incomplete journal) then no writes took place.
        else {
            cerr << "NOWRITE: "
                << "attempt=" << i
                << ", expected=" << ((uint64_t)oldMem.front())
                << ", actual=" << ((uint64_t)file.front())
                << endl;
            BOOST_CHECK(equal(file.begin(), file.end(), oldMem.begin()));
            noopCount++;
        }
    }

    auto ratio = [&](double count) -> double {
        return (count / iterations) * 100;
    };

    cerr << endl
        << iterations << " iterations "
        << ratio(undoCount) << "% undo "
        << ratio(successCount) << "% success "
        << ratio(noopCount) << "% noop"
        << endl;
}
