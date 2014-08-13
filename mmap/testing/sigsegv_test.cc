/* sigsegv_test.cc
   Jeremy Barnes, 23 February 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   Test of the segmentation fault handler functionality.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "jml/utils/string_functions.h"
#include "jml/utils/file_functions.h"
#include "jml/utils/info.h"
#include "jml/arch/atomic_ops.h"
#include "jml/arch/vm.h"
#include <boost/test/unit_test.hpp>
#include <boost/bind.hpp>
#include <iostream>
#include "mmap/sigsegv.h"
#include <signal.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include "jml/utils/guard.h"
#include <boost/bind.hpp>
#include <fstream>
#include <vector>

#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>


using namespace ML;
using namespace Datacratic;
using namespace std;


void * mmap_addr = 0;

volatile int num_handled = 0;

volatile siginfo_t handled_info;
volatile ucontext_t handled_context;

void test1_segv_handler(int signum, siginfo_t * info, void * context)
{
    cerr << "handled" << endl;

    ++num_handled;
    ucontext_t * ucontext = (ucontext_t *)context;

    (ucontext_t &)handled_context = *ucontext;
    (siginfo_t &)handled_info = *info;

    // Make the memory writeable
    int res = mprotect(mmap_addr, page_size, PROT_READ | PROT_WRITE);
    if (res == -1)
        cerr << "error in mprotect: " << strerror(errno) << endl;

    // Return to the trapping statement, which will perform the call
}

BOOST_AUTO_TEST_CASE ( test1_segv_restart )
{
    // Create a memory mapped page, read only
    mmap_addr = mmap(0, page_size, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS,
                       -1, 0);

    BOOST_REQUIRE(mmap_addr != MAP_FAILED);

    // Install a segv handler
    struct sigaction action;
    action.sa_sigaction = test1_segv_handler;
    action.sa_flags = SA_SIGINFO | SA_RESETHAND;

    int res = sigaction(SIGSEGV, &action, 0);
    
    BOOST_REQUIRE_EQUAL(res, 0);

    char * mem = (char *)mmap_addr;
    BOOST_CHECK_EQUAL(*mem, 0);

    cerr << "before handler" << endl;
    cerr << "addr = " << mmap_addr << endl;

    // write to the memory address; this will cause a SEGV
 dowrite:
    *mem = 'x';

    cerr << "after handler" << endl;

    void * x = &&dowrite;
    cerr << "x = " << x << endl;

    cerr << "signal info:" << endl;
    cerr << "  errno:   " << strerror(handled_info.si_errno) << endl;
    cerr << "  code:    " << handled_info.si_code << endl;
    cerr << "  si_addr: " << handled_info.si_addr << endl;
    cerr << "  status:  " << strerror(handled_info.si_status) << endl;
    cerr << "  RIP:     " << format("%12p", (void *)handled_context.uc_mcontext.gregs[16]) << endl;

    // Check that it was handled properly
    BOOST_CHECK_EQUAL(*mem, 'x');
    BOOST_CHECK_EQUAL(num_handled, 1);
}

void test2_segv_handler_thread(char * addr)
{
    *addr = 'x';
}

BOOST_AUTO_TEST_CASE ( test2_segv_handler )
{
    // Create a memory mapped page, read only
    void * vaddr = mmap(0, page_size, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS,
                       -1, 0);
    
    char * addr = (char *)vaddr;

    BOOST_REQUIRE(addr != MAP_FAILED);

    install_segv_handler();

    int region = register_segv_region(addr, addr + page_size);

    int nthreads = 1;

    boost::thread_group tg;
    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.create_thread(boost::bind(&test2_segv_handler_thread, addr));

    sleep(1);

    int res = mprotect(vaddr, page_size, PROT_READ | PROT_WRITE);

    BOOST_CHECK_EQUAL(res, 0);

    unregister_segv_region(region);

    tg.join_all();

    BOOST_CHECK_EQUAL(*addr, 'x');

    BOOST_CHECK_EQUAL(get_num_segv_faults_handled(), 1);
}

// Thread to continually modify the memory
void test2_segv_handler_stress_thread1(int * addr,
                                       int npages,
                                       boost::barrier & barrier,
                                       volatile bool & finished)
{
    barrier.wait();

    cerr << "m";

    int * end = addr + npages * page_size / sizeof(int);

    while (!finished) {
        for (int * p = addr;  p != end;  ++p)
            atomic_add(*p, 1);
        memory_barrier();
    }

    cerr << "M";
}

// Thread to continually unmap and remap the memory
void test2_segv_handler_stress_thread2(int * addr,
                                       boost::barrier & barrier,
                                       volatile bool & finished)
{
    barrier.wait();

    cerr << "p";

    while (!finished) {
        int region = register_segv_region(addr, addr + page_size);
        int res = mprotect(addr, page_size, PROT_READ);
        if (res == -1) {
            cerr << "mprotect(PROT_READ) returned " << strerror(errno)
                 << endl;
            abort();
        }
        res = mprotect(addr, page_size, PROT_READ | PROT_WRITE);
        if (res == -1) {
            cerr << "mprotect(PROT_WRITE) returned " << strerror(errno)
                 << endl;
            abort();
        }

        unregister_segv_region(region);
    }

    cerr << "P";
}

BOOST_AUTO_TEST_CASE ( test2_segv_handler_stress )
{
    int npages = 8;

    // Create a memory mapped page, read only
    void * vaddr = mmap(0, npages * page_size, PROT_WRITE | PROT_READ,
                        MAP_PRIVATE | MAP_ANONYMOUS,
                       -1, 0);
    
    int * addr = (int *)vaddr;

    BOOST_REQUIRE(addr != MAP_FAILED);

    install_segv_handler();

    // 8 threads simultaneously causing faults, with 8 threads writing to
    // pages

    int nthreads = 8;

    volatile bool finished = false;

    boost::barrier barrier(nthreads + npages);

    boost::thread_group tg;
    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.create_thread(boost::bind(&test2_segv_handler_stress_thread1,
                                     addr, npages, boost::ref(barrier),
                                     boost::ref(finished)));

    for (unsigned i = 0;  i < npages;  ++i)
        tg.create_thread(boost::bind(&test2_segv_handler_stress_thread2,
                                     addr + i * page_size,
                                     boost::ref(barrier),
                                     boost::ref(finished)));

    sleep(2);

    finished = true;

    tg.join_all();

    cerr << endl;

    // All values in all of the pages should be the same value
    int val = *addr;

    cerr << "val = " << val << endl;
    
    for (unsigned i = 0;  i < npages;  ++i)
        for (unsigned j = 0;  j < page_size / sizeof(int);  ++j)
            BOOST_CHECK_EQUAL(addr[i * page_size / sizeof(int) + j], val);

    cerr << get_num_segv_faults_handled() << " segv faults handled" << endl;

    BOOST_CHECK(get_num_segv_faults_handled() > 1);
}

BOOST_AUTO_TEST_CASE ( test3_normal_segv_still_works )
{
    // Don't make boost::test think that processes exiting is a problem
    signal(SIGCHLD, SIG_DFL);

    pid_t pid = fork();

    if (pid == 0) {
        install_segv_handler();

        *(char *)0 = 12;

        raise(SIGKILL);
    }

    // Give it one second to exit with a SIGSEGV
    sleep(1);

    // Force it to exit anyway with a SIGKILL
    kill(pid, SIGKILL);

    int status = -1;
    pid_t res = waitpid(pid, &status, 0);

    // If it exited properly with a SIGSEGV, then we'll get that in the status.
    // If it didn't exit, then we'll get a SIGKILL in the status instead.

    BOOST_CHECK_EQUAL(res, pid);
    BOOST_CHECK(WIFSIGNALED(status));
    BOOST_CHECK_EQUAL(WTERMSIG(status), SIGSEGV);
}
