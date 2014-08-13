/* sync_stream.h                                                     -*- C++ -*-
   Rémi Attab, 30 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Synchronized stream that avoids interleaved messages.

   \todo This lib should probably be added to jml/utils.
*/

#ifndef __mmap__sync_stream_h__
#define __mmap__sync_stream_h__

#include "jml/compiler/compiler.h"
#include "jml/arch/thread_specific.h"

#include <sstream>
#include <iostream>

namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* SYNC STREAM                                                               */
/*****************************************************************************/

/** Synchronized stream that buffers a thread's output and dumps it all at once.
    The advantage here is that you won't get interleaved messages in a
    multithreaded environment. Useful for debugging.

    In order to dump the content of the stream into its associated output, pipe
    the sync_dump symbol into the stream.

    sync_cerr() << "The sky is " << aColor << "!" << endl << sync_dump;

    is equivalent-ish to

    stringstream ss;
    ss << "The sky is " << aColor << "!" << endl;
    cerr << ss.str();
*/

struct SyncStream : virtual std::stringstream
{
    SyncStream(std::ostream& outputStream) : os(outputStream) {}
    std::ostream& os;
};

/** Symbol that signals to the stream that it should be dumped to its output. */
extern struct SyncStreamDump {} sync_dump;

inline std::ostream&
operator<< (std::ostream& stream, SyncStreamDump)
{
    SyncStream* ss = dynamic_cast<SyncStream*>(&stream);
    if (ss) {
        ss->os << ss->str();

        // Reset the stream so that it can be re-used.
        ss->clear();
        ss->str("");
    }

    return stream;
}


/*****************************************************************************/
/* STANDARD SYNC STREAMS                                                     */
/*****************************************************************************/

/** Implementation details. */
namespace SyncDetails {

/** \todo Could be made generic and exposed to the user. */
struct StdSyncStreams {

    StdSyncStreams() : cerr(std::cerr), cout(std::cout) {}

    SyncStream cerr;
    SyncStream cout;
};

extern ML::Thread_Specific<StdSyncStreams> tlsStreams;

}; // namespace SyncDetails;



/** Sync streams for the standard outputs.
    Note that these are thread local which means that writes by one thread won't
    be visible to any other threads until sync_dump is piped into the stream.
*/
inline SyncStream& sync_cerr() { return SyncDetails::tlsStreams.get()->cerr; }
inline SyncStream& sync_cout() { return SyncDetails::tlsStreams.get()->cout; }


} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__sync_stream_h__ */
