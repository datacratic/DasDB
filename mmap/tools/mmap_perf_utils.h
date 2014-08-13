/** mmap_perf_utils.h                                 -*- C++ -*-
    Rémi Attab, 25 Jan 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Utitlities for the DasDB performance tests.
n
*/

#ifndef __mmap__mmap_perf_utils_h__
#define __mmap__mmap_perf_utils_h__

#include <set>
#include <vector>
#include <string>
#include <atomic>
#include <istream>

#include "mmap/testing/mmap_test.h"
#include "soa/utils/print_utils.h"

namespace Datacratic {
namespace MMap {


/******************************************************************************/
/* RANDOM GENERATOR                                                           */
/******************************************************************************/

template<typename T> struct RandomGenerator {};

template<>
struct RandomGenerator<std::string>
{
    enum { Size = 64 };

    static std::string next()
    {
        return randomString(random() % Size + 1);
    }
};

template<>
struct RandomGenerator<uint64_t>
{
    static uint64_t next()
    {
        return random() << 32 | random();
    }
};


/******************************************************************************/
/* VALUE CONVERTER                                                            */
/******************************************************************************/

template<typename T> struct ValueConverter {};

template<>
struct ValueConverter<std::string>
{
    static std::string convert(const std::string& s) { return s; }
};

template<>
struct ValueConverter<uint64_t>
{
    static uint64_t convert(const std::string& s) { return std::stoull(s); }
};


/******************************************************************************/
/* GENERATOR                                                                  */
/******************************************************************************/


/** Preserves the key ordering. */
template<typename T>
struct Generator
{
    Generator(size_t size) : size(size), index(0) {}

    /** random generator */
    void load()
    {
        std::set<T> dedup;
        data.reserve(size);

        while (data.size() < size) {
            T val = RandomGenerator<T>::next();
            if (dedup.insert(val).second) data.push_back(val);
        }
    }

    void load(std::istream& stream)
    {
        std::set<T> dedup;
        data.reserve(size);

        std::string line;

        while (data.size() < size) {
            ExcCheck(stream, "Not enough values in the stream.");
            std::getline(stream, line);
            T val = ValueConverter<T>::convert(line);
            if (dedup.insert(val).second) data.push_back(val);
        }
    }

    void sort()
    {
        std::sort(data.begin(), data.end());
    }

    T next()
    {
        size_t i = index.fetch_add(1);
        ExcAssertLessEqual(i, data.size());
        return data[i];
    }

    T operator[] (size_t i) { return data[i]; }

private:

    size_t size;
    std::vector<T> data;
    std::atomic<size_t> index;

};


/******************************************************************************/
/* PARTITIONS                                                                 */
/******************************************************************************/

template<typename T>
struct Partition
{
    Partition(Generator<T>& gen, size_t start, size_t len) :
        gen(gen), start(start), len(len)
    {}

    T operator[] (size_t i)
    {
        return gen[start + (i % len)];
    }

private:
    Generator<T>& gen;
    size_t start;
    size_t len;
};


/******************************************************************************/
/* FORMATTING                                                                 */
/******************************************************************************/

std::string fmtElapsed(double elapsed)
{
    char scale;

    if (elapsed >= 1.0) scale = 's';

    if (elapsed < 1.0) {
        elapsed *= 1000.0;
        scale = 'm';
    }

    if (elapsed < 1.0) {
        elapsed *= 1000.0;
        scale = 'u';
    }

    if (elapsed < 1.0) {
        elapsed *= 1000.0;
        scale = 'n';
    }

    std::array<char, 32> buffer;
    snprintf(buffer.data(), buffer.size(), "%6.2f%c", elapsed, scale);
    return std::string(buffer.data());
}


std::string fmtValue(double value)
{
    char scale;

    if (value >= 1.0) scale = ' ';

    if (value >= 1000.0) {
        value /= 1000.0;
        scale = 'k';
    }

    if (value >= 1000.0) {
        value /= 1000.0;
        scale = 'm';
    }

    if (value >= 1000.0) {
        value /= 1000.0;
        scale = 'g';
    }

    std::array<char, 32> buffer;
    snprintf(buffer.data(), buffer.size(), "%6.2f%c", value, scale);
    return std::string(buffer.data());
}

std::string fmtPct(double value)
{
    std::array<char, 32> buffer;
    snprintf(buffer.data(), buffer.size(), "%6.2f%%", value * 100.0);
    return std::string(buffer.data());
}



/******************************************************************************/
/* TRANSACTION STATS                                                          */
/******************************************************************************/

struct TransactionStats
{

    enum { ElapsedMul = 1000000 };

    void reset()
    {
        failCount = 0;
        failElapsed = 0;

        successCount = 0;
        successElapsed = 0;
    }

    void fail(double elapsed)
    {
        failCount++;

        size_t elapsedMicros = elapsed * ElapsedMul;
        size_t old = failElapsed;
        while (!failElapsed.compare_exchange_weak(old, old + elapsedMicros));
    }

    void success(double elapsed)
    {
        successCount++;

        size_t elapsedMicros = elapsed * ElapsedMul;
        size_t old = successElapsed;
        while (!successElapsed.compare_exchange_weak(old, old + elapsedMicros));
    }

    double commitRate()
    {
        return static_cast<double>(successCount) / (successCount + failCount);
    }

    double commitLatency()
    {
        return (static_cast<double>(successElapsed) / ElapsedMul) / successCount;
    }

    std::atomic<size_t> failCount;
    std::atomic<size_t> failElapsed;

    std::atomic<size_t> successCount;
    std::atomic<size_t> successElapsed;
};


} // namespace MMap
} // Datacratic

#endif // __mmap__mmap_perf_utils_h__
