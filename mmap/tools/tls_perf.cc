/** tls_test.cc                                 -*- C++ -*-
    Rémi Attab, 19 Jul 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_perf_utils.h"
#include "soa/utils/threaded_test.h"
#include "jml/arch/thread_specific.h"

#include <boost/thread.hpp>
#include <array>
#include <atomic>

using namespace std;
using namespace ML;
using namespace Datacratic;
using namespace Datacratic::MMap;

struct Data
{
    void blah() { data++; }
    uint64_t data;
};

typedef boost::thread_specific_ptr<Data> BoostTls;
typedef ML::ThreadSpecificInstanceInfo<Data, Data> JmlTls;

void init(BoostTls& tls) { tls.reset(new Data()); }
void init(JmlTls&) {}

template<typename Tls, size_t N>
size_t __attribute__ ((noinline))
perfThread(array<Tls*, N>& instances, unsigned iteration)
{
    if (!iteration) {
        for (size_t i = 0; i < instances.size(); ++i)
            init(*instances[i]);
    }

    for (size_t i = 0; i < instances.size(); ++i)
        instances[i]->get()->blah();

    return instances.size();
}

template<typename Tls>
void perf(string title)
{
    enum {
        Threads = 8,
        Duration = 1000,
        Instances = 100
    };

    array<Tls*, Instances> instances;
    for (size_t i = 0; i < instances.size(); ++i)
        instances[i] = new Tls();

    auto runThread = [&] (unsigned, unsigned iteration) -> size_t
    {
        return perfThread(instances, iteration);
    };

    ThreadedTimedTest test;
    unsigned gr = test.add(runThread, Threads);
    test.run(Duration);

    ML::distribution<double> latency, throughput;
    tie(latency, throughput) = test.distributions(gr);

    printf("%10s: latency=[ %s, %s, %s ] throughput=[ %s, %s, %s ]\n",
            title.c_str(),
            fmtElapsed(latency.max()).c_str(),
            fmtElapsed(latency.mean()).c_str(),
            fmtElapsed(latency.min()).c_str(),
            fmtValue(throughput.max()).c_str(),
            fmtValue(throughput.mean()).c_str(),
            fmtValue(throughput.min()).c_str());


    for (Tls* instance: instances) delete instance;
}

int main(int argc, char** argv)
{
    perf<JmlTls>("jml");
    perf<BoostTls>("boost");
}
