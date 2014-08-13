/** sys_mmap_test.cc                                 -*- C++ -*-
    Rémi Attab, 05 Nov 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Tests for the mmap syscall to test various things.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "mmap/mmap_const.h"
#include "jml/utils/exc_assert.h"
#include "jml/utils/guard.h"

#include <boost/test/unit_test.hpp>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <algorithm>
#include <string>

using namespace std;
using namespace ML;
using namespace Datacratic::MMap;


struct SysMMapFixture : public TestFolderFixture {
    SysMMapFixture() : TestFolderFixture("sys_mmap_test") {}
    virtual ~SysMMapFixture () {}
};

/** Tests whether we can remap an VMA by specifying only the last page in a
    range. Turns out that we can! This makes it much easier to resize the region
    while there's an ongoing snapshot because it doesn't matter if the VMA is
    fragmented.

 */
BOOST_FIXTURE_TEST_CASE( test_remap, SysMMapFixture )
{
    int fd = open("test_file.bin", O_RDWR | O_CREAT | O_TRUNC, 0660);
    ExcCheckErrno(fd != -1, "open failed");

    {
        int ret = ftruncate(fd, page_size * 2);
        ExcCheckErrno(ret != -1, "truncate failed");
    }

    uint8_t* addr = (uint8_t*) mmap(
            nullptr, page_size * 2, PROT_READ | PROT_WRITE,
            MAP_SHARED, fd, 0);
    ExcCheckErrno(addr != MAP_FAILED, "mmap failed");

    fill(addr, addr + page_size * 2, 0xA5);

    {
        int ret = ftruncate(fd, page_size * 4);
        ExcCheckErrno(ret != -1, "expand truncate failed");
    }

    uint8_t* new_addr = (uint8_t*) mremap(
            addr + page_size, page_size, page_size * 3, 0, 0);
    ExcCheckErrno(new_addr != MAP_FAILED, "mremap failed");
    ExcCheckEqual(new_addr, addr + page_size, "mmap moved");

    fill(addr + page_size * 2, addr + page_size * 4, 0xA5);

    bool eq = all_of(addr, addr + page_size * 4, [](uint8_t v) {
                return v == 0xA5; });
    ExcCheck(eq, "check failed");
}
