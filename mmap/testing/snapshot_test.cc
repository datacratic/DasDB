/* storage_test.cc
   Jeremy Barnes, 18 February 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   Test of the storage functionality.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "mmap/snapshot.h"
#include "mmap/sigsegv.h"
#include "jml/utils/string_functions.h"
#include "jml/utils/file_functions.h"
#include "jml/utils/info.h"
#include "jml/utils/guard.h"
#include "jml/arch/exception.h"
#include "jml/arch/vm.h"
#include "jml/arch/atomic_ops.h"
#include "jml/utils/exc_assert.h"

#include <boost/test/unit_test.hpp>
#include <boost/bind.hpp>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <signal.h>
#include <fcntl.h>
#include <errno.h>

#include <set>
#include <array>
#include <thread>
#include <iostream>
#include <algorithm>


using namespace ML;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace std;

using boost::unit_test::test_suite;

// TODO: test that this only runs from the parent.  For the moment, we need
// to look at the logfile and make sure that the line occurs only once with
// a single PID.
struct With_Global_Destructor {
    ~With_Global_Destructor()
    {
        cerr << "global destructor called from pid " << getpid() << endl;
    }

} global;


// Handles the cleanup of the snapshot journal.
struct JournalFixture {
    JournalFixture() :
        journalFile ("snapshot_test.log")
    {
        unlink(journalFile.c_str());
    }
    ~JournalFixture()
    {
        unlink(journalFile.c_str());
    }

    string journalFile;
};


int test_snapshot_child(int & var, int control_fd)
{
    try {

        BOOST_CHECK_EQUAL(var, 0);

        // wait for the 'x' to say 'start'
        char buf = '0';
        int res = read(control_fd, &buf, 1);
        BOOST_REQUIRE_EQUAL(res, 1);
        BOOST_REQUIRE_EQUAL(buf, 'x');
        
        BOOST_CHECK_EQUAL(var, 0);

        var = 1;

        buf = 'a';
        res = write(control_fd, &buf, 1);

        BOOST_REQUIRE_EQUAL(res, 1);

        res = read(control_fd, &buf, 1);
        BOOST_REQUIRE_EQUAL(res, 1);
        BOOST_REQUIRE_EQUAL(buf, 'y');

        BOOST_CHECK_EQUAL(var, 1);

        buf = 'b';
        res = write(control_fd, &buf, 1);
        BOOST_REQUIRE_EQUAL(res, 1);

        return 0;
    } catch (const std::exception & exc) {
        cerr << "child: error " << exc.what() << endl;
        return 1;
    } catch (...) {
        cerr << "child: error" << endl;
        return 1;
    }
}

BOOST_FIXTURE_TEST_CASE( test_snapshot, JournalFixture )
{
    // Stop boost from thinking that a child exiting is an error
    signal(SIGCHLD, SIG_DFL);

    int var = 0;

    Snapshot::Worker w
        = boost::bind(test_snapshot_child, boost::ref(var), _1);
    Snapshot s(journalFile, w);

    BOOST_CHECK_EQUAL(var, 0);

    // write an "x" to say "start"
    char buf = 'x';
    int res = write(s.control_fd(), &buf, 1);
    
    BOOST_REQUIRE_EQUAL(res, 1);

    res = read(s.control_fd(), &buf, 1);
    BOOST_REQUIRE_EQUAL(res, 1);
    BOOST_REQUIRE_EQUAL(buf, 'a');

    var = 2;

    buf = 'y';
    res = write(s.control_fd(), &buf, 1);
    
    BOOST_REQUIRE_EQUAL(res, 1);
    
    res = read(s.control_fd(), &buf, 1);
    BOOST_REQUIRE_EQUAL(res, 1);
    BOOST_REQUIRE_EQUAL(buf, 'b');

    BOOST_CHECK_EQUAL(s.terminate(), 0);
}

struct Backed_Region {
    Backed_Region(const std::string & filename, size_t size, bool wipe = true)
    {
        fd = open(filename.c_str(), (wipe ? (O_CREAT | O_TRUNC) : 0) | O_RDWR,
                  0666);
        if (fd == -1)
            throw Exception("Backed_Region(): open + " + filename + ": "
                            + string(strerror(errno)));

        size_t sz = get_file_size(fd);

        if (!wipe && sz != size)
            throw Exception("backing file was wrong size");

        if (sz != size) {
            int res = ftruncate(fd, size);
            if (res == -1)
                throw Exception("truncate didn't work: " + string(strerror(errno)));
        }            

        void * addr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);

        if (addr == 0)
            throw Exception("mmap failed: " + string(strerror(errno)));

        data = (char *)addr;
        this->size = size;
    }

    void close()
    {
        if (data) munmap(data, size);
        data = 0;
        if (fd != -1) ::close(fd);
        fd = -1;
        size = 0;
    }
    
    ~Backed_Region()
    {
        close();
    }

    int fd;
    char * data;
    size_t size;
};

void set_page(char * data, int page_offset, const std::string & str)
{
    data += ML::page_size * page_offset;

    for (unsigned i = 0;  i < ML::page_size;  ++i) {
        int j = i % str.length();
        data[i] = str[j];
    }

    data[ML::page_size - 1] = 0;
}

// This test case tests our ability to create a snapshot and to write an
// area of memory from that snapshot to disk.
BOOST_FIXTURE_TEST_CASE( test_backing_file, JournalFixture )
{
    signal(SIGCHLD, SIG_DFL);

    int npages = 5;

    Call_Guard guard(boost::bind(unlink, "region1"));

    int files_open_before = num_open_files();

    // 1.  Create a backed regions
    Backed_Region region1("region1", npages * ML::page_size);

    // 2.  Write to the first one
    const char * cs1 = "1abcdef\0";
    string s1(cs1, cs1 + 8);
    set_page(region1.data, 0, s1);
    set_page(region1.data, 1, s1);
    set_page(region1.data, 2, s1);
    set_page(region1.data, 3, s1);
    set_page(region1.data, 4, s1);
    
    // 3.  Create a snapshot
    Snapshot snapshot1(journalFile);

    // 4.  Write the snapshot to region1
    size_t written
        = snapshot1.sync_to_disk(region1.fd, 0, region1.data, region1.size,
                                 Snapshot::DUMP);
    
    BOOST_CHECK_EQUAL(written, npages * ML::page_size);

    // 5.  Re-map it and check that it gave the correct data
    Backed_Region region1a("region1", npages * ML::page_size, false);

    BOOST_CHECK_EQUAL_COLLECTIONS(region1.data, region1.data + region1.size,
                                  region1a.data, region1a.data + region1a.size);

    BOOST_CHECK_EQUAL(snapshot1.terminate(), 0);

    region1.close();
    region1a.close();

    // Make sure that everything was properly closed
    BOOST_CHECK_EQUAL(files_open_before, num_open_files());
}

// This test case makes sure that pages that weren't modified in the snapshot
// from the originally mapped file are not written to disk needlessly
// TODO: make sure that the number of mappings doesn't explode...

BOOST_FIXTURE_TEST_CASE( test_backing_file_efficiency, JournalFixture )
{
    // Don't make boost::test think that processes exiting is a problem
    signal(SIGCHLD, SIG_DFL);

    int npages = 5;

    // So we can make sure that all file descriptors were returned
    int files_open_before = num_open_files();

    Call_Guard unlink_guard(boost::bind(unlink, "region1"));

    // 1.  Create a backed regions
    Backed_Region region1("region1", npages * ML::page_size);

    // 2.  Write to 3 of the 5 pages
    const char * cs1 = "1abcdef\0";
    string s1(cs1, cs1 + 8);
    set_page(region1.data, 0, s1);
    set_page(region1.data, 2, s1);
    set_page(region1.data, 4, s1);

    dump_page_info(region1.data, region1.data + region1.size);

    cerr << "<=========== shapshot1" << endl;
    
    // 3.  Create a snapshot
    Snapshot snapshot1(journalFile);

    // 4.  Sync changed pages to region1
    size_t written
        = snapshot1.sync_to_disk(region1.fd, 0, region1.data, region1.size,
                                 Snapshot::SYNC_ONLY);

    BOOST_CHECK_EQUAL(written, 3 * ML::page_size);

    int pagemap_fd = open("/proc/self/pagemap", O_RDONLY);
    if (pagemap_fd == -1)
        throw Exception("opening /proc/self/pagemap: %s", strerror(errno));
    Call_Guard guard(boost::bind(close, pagemap_fd));

    cerr << "before reback:" << endl;
    dump_page_info(region1.data, region1.data + region1.size);

    size_t n_rebacked
        = Snapshot::
        reback_range_after_write(region1.fd, 0, region1.data, region1.size,
                                 snapshot1.pagemap_fd(), pagemap_fd);
    
    cerr << "after reback:" << endl;
    dump_page_info(region1.data, region1.data + region1.size);

    close(pagemap_fd);
    guard.clear();

    BOOST_CHECK_EQUAL(n_rebacked, 3 * ML::page_size);

    BOOST_CHECK_EQUAL(snapshot1.terminate(), 0);

    set_page(region1.data, 1, s1);

    Snapshot snapshot2(journalFile);

    written = snapshot2.sync_to_disk(region1.fd, 0, region1.data, region1.size,
                                     Snapshot::SYNC_ONLY);
    
    BOOST_CHECK_EQUAL(written, (int)ML::page_size);

    // 6.  Re-map it and check that it gave the correct data
    Backed_Region region1a("region1", npages * ML::page_size, false);

    BOOST_CHECK_EQUAL_COLLECTIONS(region1.data, region1.data + region1.size,
                                  region1a.data, region1a.data + region1a.size);

    BOOST_CHECK_EQUAL(snapshot2.terminate(), 0);

    region1.close();
    region1a.close();

    // Make sure that everything was properly closed
    BOOST_CHECK_EQUAL(files_open_before, num_open_files());
}


#if 0
// This test case makes sure that pages that weren't modified in the snapshot
// from the originally mapped file are not written to disk needlessly
// TODO: make sure that the number of mappings doesn't explode...

BOOST_AUTO_TEST_CASE( test_backing_file_efficiency2 )
{
    // Don't make boost::test think that processes exiting is a problem
    signal(SIGCHLD, SIG_DFL);

    int npages = 5;

    Call_Guard guard(boost::bind(unlink, "region1"));

    // So we can make sure that all file descriptors were returned
    int files_open_before = num_open_files();

    // 1.  Create a backed region
    Backed_Region region1("region1", npages * ML::page_size);

    // 2.  Write to some of the pages
    const char * cs1 = "1abcdef\0";
    string s1(cs1, cs1 + 8);

    set<int> pages_changed;

    for (unsigned i = 0;  i < npages;  ++i) {
        int page = random() % npages;
        set_page(region1.data, page, s1);
        pages_changed.insert(page);
    }

    cerr << "wrote to " << pages_changed.size() << " of " << npages
         << " pages" << endl;

    // 3.  Create a snapshot
    Snapshot snapshot1;

    cerr << endl << "before first sync" << endl;
    dump_page_info(region1.data, region1.data + region1.size);
    cerr << endl;

    // 4.  Sync changed pages to snapshot
    size_t written, rebacked, reclaimed;
    boost::tie(written, rebacked, reclaimed)
        = snapshot1.sync_and_reback(region1.fd, 0, region1.data, region1.size,
                                    Snapshot::RECLAIM);

    BOOST_CHECK_EQUAL(written,   pages_changed.size() * ML::page_size);
    BOOST_CHECK_EQUAL(rebacked,  pages_changed.size() * ML::page_size);
    BOOST_CHECK_EQUAL(reclaimed, pages_changed.size() * ML::page_size);
    
    cerr << endl << "after first sync" << endl;
    dump_page_info(region1.data, region1.data + region1.size);
    cerr << endl;


    // Re-map it and check that it gave the correct data
    {
        Backed_Region region1a("region1", npages * ML::page_size, false);
        
        BOOST_CHECK_EQUAL_COLLECTIONS(region1.data, region1.data + region1.size,
                                      region1a.data,
                                      region1a.data + region1a.size);
    }

    cerr << endl << "first sync and mapped" << endl;
    dump_page_info(region1.data, region1.data + region1.size);
    cerr << endl;

    // 5.  Check that nothing is synced a second time
    boost::tie(written, rebacked, reclaimed)
        = snapshot1.sync_and_reback(region1.fd, 0, region1.data, region1.size,
                                    Snapshot::RECLAIM);

    BOOST_CHECK_EQUAL(written,   0);
    BOOST_CHECK_EQUAL(rebacked,  0);
    BOOST_CHECK_EQUAL(reclaimed, 0);

    // Re-map it and check that it gave the correct data
    {
        Backed_Region region1a("region1", npages * ML::page_size, false);
        
        BOOST_CHECK_EQUAL_COLLECTIONS(region1.data, region1.data + region1.size,
                                      region1a.data,
                                      region1a.data + region1a.size);
    }

    cerr << endl << "before changing pages again" << endl;
    dump_page_info(region1.data, region1.data + region1.size);
    cerr << endl;

    set<int> pages_changed2;
    
    string s2 = "wxywxywx";
    for (unsigned i = 0;  i < npages / 5;  ++i) {
        int page = random() % npages;
        set_page(region1.data, page, s2);
        pages_changed2.insert(page);
    }

    cerr << "wrote to " << pages_changed2.size() << " of " << npages
         << " pages" << endl;

    cerr << endl << "after changing pages again" << endl;
    dump_page_info(region1.data, region1.data + region1.size);
    cerr << endl;

    // Check that only the changed pages were written
    boost::tie(written, rebacked, reclaimed)
        = snapshot1.sync_and_reback(region1.fd, 0, region1.data, region1.size,
                                    Snapshot::RECLAIM);
    
    BOOST_CHECK_EQUAL(written,   pages_changed2.size() * ML::page_size);
    BOOST_CHECK_EQUAL(rebacked,  pages_changed2.size() * ML::page_size);
    BOOST_CHECK_EQUAL(reclaimed, pages_changed2.size() * ML::page_size);


    BOOST_CHECK_EQUAL(snapshot1.terminate(), 0);


    // Re-map it and check that it gave the correct data
    {
        Backed_Region region1a("region1", npages * ML::page_size, false);
        
        BOOST_CHECK(std::equal(region1.data, region1.data + region1.size,
                               region1a.data));
    }

    region1.close();

    // Make sure that everything was properly closed
    BOOST_CHECK_EQUAL(files_open_before, num_open_files());
}

#endif

// TODO: add a test where we simultaneously write all over the memory and
// make sure that the writing doesn't affect the snapshotting and vice-versa


// \todo The fsync in writeback phase is killing performances and should be
//       done in it's own thread.
// Note that the test still works but it takes forever to finish.
#if 0

// Prototype test for our first implementation into mmap.
// Support for N readers and writers within the same process.
BOOST_FIXTURE_TEST_CASE( test_concurrency, JournalFixture )
{
    enum {
        writerThreads = 4,
        readerThreads = 4,

        iterations = 100,
        npages = iterations * writerThreads,

        total_size = npages * ML::page_size
    };


    signal(SIGCHLD, SIG_DFL);

    // This call wards off the gremlin. Do try to not forget it...
    install_segv_handler();

    int files_open_before = num_open_files();

    volatile int writesDone = 0;

    Call_Guard unlink_guard(boost::bind(unlink, "region"));
    Backed_Region writeRegion("region", total_size, true);

    mutex snapshotLock;
    array<int, writerThreads> progress;
    for (int i = 0; i < progress.size(); ++i) progress[i] = 0;

    // Each thread is given a page that it will fill with it's id. After every
    // page is filled a snapshot is made and written back to the disk.
    auto doWriterThread = [&] (int id) -> int {
        int errCount = 0;

        for (int page = 0; page < iterations; ++page) {
            size_t pos = (iterations * id + page) * ML::page_size;

            // write the page.
            for (size_t i = pos; i < pos + ML::page_size; ++i)
                writeRegion.data[i] = id;

            size_t written, rebacked, reclaimed;
            {
                // Removing this lock causes a livelock
                // \todo It would be nice if we could get rid of it.
                lock_guard<mutex> guard(snapshotLock);

                Snapshot snapshot(journalFile);

                boost::tie(written, rebacked, reclaimed) =
                    snapshot.sync_and_reback(
                            writeRegion.fd, 0,
                            writeRegion.data, total_size,
                            Snapshot::NO_RECLAIM);

                // stringstream ss;
                // ss << "WRITE: "
                //     << "writer=" << id
                //     << ", page=" << (pos / ML::page_size)
                //     << ", dump: ";
                // dump_page_info(
                //         writeRegion.data + pos, 
                //         writeRegion.data + pos + ML::page_size,
                //         ss);
                // cerr << ss.str();

                // signal to the reader that we're done writting the page.
                ML::atomic_add(progress[id], 1);

                snapshot.terminate();
            }

            if (reclaimed != 0) {
                cerr << "snapshot reclaimed\n";
                errCount++;
            }

            // \todo Happens occasionally and I'm not sure if it's a problem.
            // if (written != rebacked) {
            //     cerr << "written != rebacked\n";
            //     errCount++;
            // }
        }

        // signal to the readers that we're done.
        ML::atomic_add(writesDone, 1);
        return errCount;
    };

    // Reads pages in teh region based on the progress array and checks that
    // each pages contains what it should.
    auto checkRegion = [&](Backed_Region& region) -> int {
        int errCount = 0;

        for (int writerId = 0; writerId < writerThreads; ++writerId) {

            for (int page = 0; page < progress[writerId]; ++page) {
                size_t pos = (iterations * writerId + page) * ML::page_size;

                // read the page.
                for (size_t i = pos; i < pos + ML::page_size; ++i) {
                    if (region.data[i] != writerId) {

                        stringstream ss;
                        ss << "ERROR: bad read data -> "
                            << "writerId=" << writerId
                            << ", page=" << (pos / ML::page_size)
                            << ", data[" << i << "]=" 
                            << ((uint64_t)region.data[i])
                            << ", dump: ";
                        dump_page_info(
                                region.data + pos,
                                region.data + pos + ML::page_size,
                                ss);
                        cerr << ss.str();

                        errCount++;
                        break;
                    }
                }
            }
        } // nesting is FUN.

        return errCount;
    };

    // Note that we have no guarantees that what the readers see in their own
    // mapping is consistent with what the writers are writting.
    auto doReaderThread = [&] (int id) -> int {
        int errCount = 0;
        Backed_Region readRegion("region", total_size, false);

        while(writesDone != writerThreads)
            errCount += checkRegion(readRegion);

        readRegion.close();
        return errCount;
    };

    ThreadedTest test;
    test.start(doWriterThread, writerThreads, 0);
    test.start(doReaderThread, readerThreads, 1);
    int errCount = test.joinAll(100000);

    // Close the write region and make sure all files have been closed.
    writeRegion.close();
    BOOST_CHECK_EQUAL(files_open_before, num_open_files());

    bool allDone = all_of(progress.begin(), progress.end(), [&](int p) -> bool {
                return p == iterations; 
            });
    BOOST_CHECK(allDone);

    // Remap the file to make sure that it was properly written.
    {
        Backed_Region region("region", total_size, false);
        BOOST_CHECK_EQUAL(checkRegion(region), 0);
        region.close();
    }

    BOOST_CHECK_EQUAL(files_open_before, num_open_files());
    BOOST_CHECK_EQUAL(errCount, 0);
}

#endif


// This test is used as a reference for the paging behaviour.
// \todo Turn this into an actual sanity check for the kernel behaviour.
#if 0

int dump_snapshot_worker (char* start, char* end, int control_fd){
    int res;
    char buf;

    try {
        while (true) {
            res = read(control_fd, &buf, 1);
            BOOST_REQUIRE_EQUAL(res, 1);

            if (buf == 'x') break;

            if (buf == 'p') {
                cerr << "{" << hex
                    << (page_flags(start,1)[0] ? ((uint64_t)start[0]) : 0xFF)
                    << dec << "}\t";
                dump_page_info(start, end);
            }
            else if (buf == 'r') {
                for (int i = 0; i < 3; ++i)
                    cerr << start[i] << " ";
                cerr << endl << endl;
            }

            buf = 'd';
            res = write(control_fd, &buf, 1);
            BOOST_REQUIRE_EQUAL(res, 1);
        };

        return 0;
    }
    catch (const exception& ex) {
        cerr << "ERROR(snapshot_dump): " << ex.what() << endl;
        buf = 'e';
        res = write(control_fd, &buf, 1);
        return 1;
    }
    catch (...) {
        cerr << "ERROR(snapshot_dump): " << string(strerror(errno)) << endl;
        buf = 'e';
        res = write(control_fd, &buf, 1);
        return 1;
    }
}

void msg_snapshot(Snapshot& snapshot, char c) {
    snapshot.send(c);

    char b;
    snapshot.recv(b);
    BOOST_REQUIRE_EQUAL(b, 'd');
}

void kill_snapshot(Snapshot& snapshot) {
    snapshot.send('x');
    snapshot.terminate();
}

void print_state(
        char region_name, Backed_Region& region, 
        const string& snapshot_name, Snapshot& snapshot) 
{
    cerr << "region" << region_name << ":\t";

    cerr << "{" << hex
        << (page_flags(region.data,1)[0] ? ((uint64_t)region.data[0]) : 0xFF)
        << dec << "}\t";
    dump_page_info(region.data, region.data + region.size);

    cerr << "snap" << region_name << "_" << snapshot_name << ":\t";
    msg_snapshot(snapshot, 'p');

    cerr << endl;
}


BOOST_FIXTURE_TEST_CASE( test_reference, JournalFixture )
{ 
    signal(SIGCHLD, SIG_DFL);


    Backed_Region region1("region", ML::page_size);
    Snapshot::Worker dump1 = boost::bind(
            dump_snapshot_worker, region1.data, region1.data + region1.size, _1);

    Backed_Region region2("region", ML::page_size);
    Snapshot::Worker dump2 = boost::bind(
            dump_snapshot_worker, region2.data, region2.data + region2.size, _1);

    Backed_Region region3("region", ML::page_size);
    Snapshot::Worker dump3 = boost::bind(
            dump_snapshot_worker, region3.data, region3.data + region3.size, _1);

    cerr << endl << endl
        << "==================================================================" 
        << endl << endl;

    // Default state should have no pages present.
    Snapshot snap1_init(journalFile, dump1);
    Snapshot snap2_init(journalFile, dump2);
    Snapshot snap3_init(journalFile, dump3);
    print_state('1', region1, "init", snap1_init);
    print_state('2', region2, "init", snap2_init);
    print_state('3', region3, "init", snap3_init);


    cerr << "*** READ *** " << endl << endl;

    // the read data is outputed to avoid compiler optimizations.
    for (int i = 0; i < 3; ++i)
        cerr << region1.data[i] << " ";
    for (int i = 0; i < 3; ++i)
        cerr << region2.data[i] << " ";
    cerr << endl << endl;

    // The page should be present in the region but not the snapshot.
    Snapshot snap1_read(journalFile, dump1);
    Snapshot snap2_read(journalFile, dump2);
    print_state('1', region1, "read", snap1_read);
    print_state('2', region2, "read", snap2_read);


    cerr << "*** READ SNAPSHOT 1 *** " << endl << endl;
    msg_snapshot(snap1_read, 'r');

    // Page state should be the same in the snapshot and the region.
    // region2 and it's snapshot should be unaffected.
    print_state('1', region1, "read", snap1_read);
    print_state('2', region2, "read", snap2_read);


    cerr << "*** WRITE 1 *** " << endl << endl;
    for (int i = 0; i < 3; ++i)
        region1.data[i] = 1;

    // Writting to the region should trigger a CoW (new pfn).
    // Region 2 and it's snapshot should be unaffected.
    Snapshot snap1_write1(journalFile, dump1);
    Snapshot snap2_write1(journalFile, dump2);
    print_state('1', region1, "write1", snap1_write1);
    print_state('2', region2, "write1", snap2_write1);


    cerr << "*** WRITE 2 *** " << endl << endl;
    for (int i = 0; i < 3; ++i)
        region2.data[i] = 1;

    // Writting to the region should trigger a CoW (new pfn).
    Snapshot snap1_write2(journalFile, dump1);
    Snapshot snap2_write2(journalFile, dump2);
    print_state('1', region1, "write2", snap1_write2);
    print_state('2', region2, "write2", snap2_write2);


    cerr << "*** WRITE 3 *** " << endl << endl;
    for (int i = 0; i < 3; ++i)
        region2.data[i] = 1;

    // Writting again to region2 should trigger a CoW because of snap2_write2.
    // The pfn for region2 should be the same of snap2_write3 but different to
    // snap2_write2.
    Snapshot snap2_write3(journalFile, dump2);
    print_state('2', region2, "write2", snap2_write2);
    print_state('2', region2, "write3", snap2_write3);


    cerr << "*** READ SNAPSHOT 2 *** " << endl << endl;
    msg_snapshot(snap2_read, 'r');

    // Reading from snap2_read should load the correct pfn which is the
    // same as snap1_read which was taken at the same time as snap2_read.
    print_state('1', region1, "read", snap1_read);
    print_state('2', region2, "read", snap2_read);


    cerr << "*** WRITE DISK 1 ***" << endl << endl;
    {
        char c = 0x55;
        int res = pwrite(region2.fd, &c, 1, 0);
        ExcAssertErrno(res == 1);
    }

    // Write directly to the file and see what happens.
    // If there are pages present, then a CoW was triggered.
    Snapshot snap3_disk1(journalFile, dump3);
    print_state('3', region3, "init", snap3_init);
    print_state('3', region3, "disk1", snap3_disk1);


    cerr << "*** READ Region3 ***" << endl << endl;

    // If this reads 0x55 then no CoW was triggered.
    cerr << "region3.content={" << hex
        << ((uint64_t)region3.data[0])
        << dec << "}" << endl << endl;

    Snapshot snap3_read(journalFile, dump3);
    print_state('3', region3, "init", snap3_init);
    print_state('3', region3, "read", snap3_read);


    cerr << "*** WRITE DISK 2 ***" << endl << endl;
    {
        char c = 0x11;
        int res = pwrite(region2.fd, &c, 1, 0);
        ExcAssertErrno(res == 1);
    }

    // See if the CoW behaviour is the same if the page is present.
    Snapshot snap3_disk2(journalFile, dump3);
    print_state('3', region3, "init", snap3_init);
    print_state('3', region3, "disk2", snap3_disk1);


    cerr << "*** DONE *** " << endl << endl;

    // That's a lot of snapshots...
    kill_snapshot(snap1_init);
    kill_snapshot(snap2_init);
    kill_snapshot(snap3_init);
    kill_snapshot(snap1_read);
    kill_snapshot(snap2_read);
    kill_snapshot(snap3_read);
    kill_snapshot(snap1_write1);
    kill_snapshot(snap2_write1);
    kill_snapshot(snap1_write2);
    kill_snapshot(snap2_write2);
    kill_snapshot(snap2_write3);
    kill_snapshot(snap3_disk1);
    kill_snapshot(snap3_disk2);
}

#endif
