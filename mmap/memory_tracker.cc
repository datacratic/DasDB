/* memory_tracker.h
   Rémi Attab, 8 Febuary 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.
*/


#include "memory_tracker.h"
#include "mmap_trie_node.h"
#include "sync_stream.h"

#include "jml/arch/backtrace.h"
#include "jml/utils/abort.h"

#include <mutex>
#include <sstream>

using namespace Datacratic;
using namespace MMap;
using namespace ML;
using namespace std;


MemoryTracker::MemoryTracker(bool backtrace) :
    backtrace(backtrace)
{}


void MemoryTracker::trackAlloc(uint64_t offset, TriePtr node) {
    string bt = getBacktrace();

    {
        lock_guard<mutex> guard(blocksLock);

        Block& entry = blocks[offset];

        if (entry.allocCounter != entry.deallocCounter) {
            entry.dump();
            do_abort();
            throw ML::Exception(
                    "Attempting to double allocate a block: %lld", offset);
        }

        if (entry.readCounter != 0) {
            entry.dump();
            do_abort();
            throw ML::Exception(
                    "Attempting to alloc a block in use: %lld", offset);
        }

        entry.allocCounter++;
        entry.offset = offset;
        entry.node = node;
        entry.lastBacktrace = bt;
    }
}

void MemoryTracker::trackAlloc(uint64_t offset) {
    string bt = getBacktrace();

    lock_guard<mutex> guard(blocksLock);

    Block& entry = blocks[offset];

    if (entry.allocCounter != entry.deallocCounter) {
        entry.dump();
        do_abort();
        throw ML::Exception(
                "Attempting to double allocate a block: %lld", offset);
    }

    if (entry.readCounter != 0) {
        entry.dump();
        do_abort();
        throw ML::Exception(
                "Attempting to alloc a block in use: %lld", offset);
    }

    entry.allocCounter++;
    entry.offset = offset;
    entry.node = TriePtr();
    entry.lastBacktrace = bt;

}

void MemoryTracker::trackDealloc(uint64_t offset) {
    string bt = getBacktrace();

    lock_guard<mutex> guard(blocksLock);

    Block& entry = blocks[offset];
    entry.deallocCounter++;

    if (entry.deallocCounter > entry.allocCounter) {
        entry.dump();
        do_abort();
        throw ML::Exception(
                "Attempting to double free a block: %lld", offset);
    }

    if (entry.readCounter != 0) {
        entry.dump();
        do_abort();
        throw ML::Exception(
                "Attempting to dealloc a block in use: %lld", offset);
    }

    entry.lastBacktrace = bt;
}


string MemoryTracker::getBacktrace() const {
    if (!backtrace)
        return "";

    stringstream ss;
    ::backtrace(ss, 4);
    return ss.str();
}


MemoryTracker::Block MemoryTracker::getBlock(uint64_t offset) const {
    Block block;
    {
        lock_guard<mutex> guard(blocksLock);

        auto it = blocks.find(offset);
        if (it == blocks.cend()) {
            do_abort();
            throw ML::Exception("Accessing uninitialized block: %lld", offset);
        }
        block = it->second;
    }
    return block;
}


void MemoryTracker::checkRead(uint64_t offset, TriePtr node) const {
    Block block = getBlock(offset);

    if (block.allocCounter == block.deallocCounter) {
        block.dump();
        do_abort();
        throw ML::Exception("Reading deallocated block: %lld", offset);
    }

    if (block.node.type != node.type) {
        block.dump();
        stringstream ss;
        ss << "{expected=" << block.node
            << ", actual=" << node
            << "}";
        do_abort();
        throw ML::Exception(
                "Wrong node type: %lld, %s", offset, ss.str().c_str());
    }
}

void MemoryTracker::checkRead(uint64_t offset) const {
    Block block = getBlock(offset);

    if (block.allocCounter == block.deallocCounter) {
        block.dump();
        do_abort();
        throw ML::Exception("Reading deallocated block: %lld", offset);
    }
}

void MemoryTracker::startRead(uint64_t offset) {
    lock_guard<mutex> guard(blocksLock);

    Block& entry = blocks[offset];

    if (entry.deallocCounter == entry.allocCounter) {
        entry.dump();
        do_abort();
        throw ML::Exception(
                "Attempting to read a freed block: %lld", offset);
    }

    entry.readCounter++;
}

void MemoryTracker::endRead(uint64_t offset) {
    lock_guard<mutex> guard(blocksLock);

    Block& entry = blocks[offset];

    if (entry.deallocCounter == entry.allocCounter) {
        entry.dump();
        do_abort();
        throw ML::Exception(
                "Attempting to read a freed block: %lld", offset);
    }

    ExcAssertGreater(entry.readCounter, 0);
    entry.readCounter--;
}

void MemoryTracker::Block::dump(ostream& stream) const {
    if (!lastBacktrace.empty())
        stream << "================== MEMORY BLOCK ==================" << endl;
    else stream << "=== BLOCK: ";

    stream << "offset=" << offset
        << ", allocCounter=" << allocCounter << "/" << deallocCounter
        << ", readCounter=" << readCounter
        << ", node={ " << node << " }" 
        << endl;

    if (!lastBacktrace.empty())
        stream << lastBacktrace << endl;
}

void MemoryTracker::checkForLeaks(
        function<void(const Block& block)> cb) const
{
    lock_guard<mutex> guard(blocksLock);

    for (auto it = blocks.begin(), end = blocks.end(); it != end; ++it) {
        Block entry = it->second;

        if (entry.allocCounter == entry.deallocCounter)
            continue;

        cb(entry);
    }
}

void MemoryTracker::dumpLeaks(ostream& stream) const {
    uint32_t counter = 0;
    checkForLeaks([&](const Block& block) {
                block.dump(stream);
                counter++;
            });
    
    if (counter > 0)
        cerr << counter << " Leaks" << endl;
}
