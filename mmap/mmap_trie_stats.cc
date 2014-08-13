/** mmap_trie_stats.cc                                 -*- C++ -*-
    Rémi Attab, 20 Dec 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Blah

*/

#include "mmap_trie_stats.h"

using namespace std;
using namespace ML;

namespace Datacratic {
namespace MMap {


/******************************************************************************/
/* PRINT UTILS                                                                */
/******************************************************************************/

namespace {

string pct(double d) { return ML::format("%6.2f%%", d * 100); }
string pct(double num, double denum) { return pct(num / denum); }

string by(double d) { return ML::format("%6.2f ", d); }
string kb(double d) { return ML::format("%6.2fk", d / 1000); }
string mb(double d) { return ML::format("%6.2fm", d / (1000  * 1000)); }
string dumpSize(double d)
{
    if (d < 1000) return by(d);
    if (d < 1000 * 1000) return kb(d);
    return mb(d);
}

} // namespace anonymous

/******************************************************************************/
/* NODE STATS                                                                 */
/******************************************************************************/

NodeStats::
NodeStats(size_t count) :
    nodeCount(count),
    totalBytes(0), bookeepingBytes(0), unusedBytes(0), externalKeyBytes(0),
    branches(0), values(0),
    avgBits(0), maxBits(0),
    avgBranchingBits(0), maxBranchingBits(0)
{}


NodeStats&
NodeStats::
operator+= (const NodeStats& other)
{
    nodeCount += other.nodeCount;

    totalBytes += other.totalBytes;
    bookeepingBytes += other.bookeepingBytes;
    unusedBytes += other.unusedBytes;
    externalKeyBytes += other.externalKeyBytes;

    branches += other.branches;
    values += other.values;

    auto calcAvg = [](
            double lhs, uint64_t lhsTotal,
            double rhs, uint64_t rhsTotal) -> double
        {
            if (!lhsTotal) return rhs;
            if (!rhsTotal) return lhs;
            if (!lhsTotal && !rhsTotal) return 0;

            double total = ((lhs * lhsTotal) + (rhs * rhsTotal));
            return total / (lhsTotal + rhsTotal);
        };

    maxBits = std::max(maxBits, other.maxBits);
    avgBits = calcAvg(
            avgBits, values + branches,
            other.avgBits, other.values + other.branches);

    maxBranchingBits = std::max(maxBranchingBits, other.maxBranchingBits);
    avgBranchingBits = calcAvg(
            avgBranchingBits, branches,
            other.avgBranchingBits, other.branches);

    return *this;
}


NodeStats
NodeStats::
toScale(double scale) const
{
    NodeStats stats = *this;

    stats.nodeCount *= scale;

    stats.totalBytes *= scale;
    stats.bookeepingBytes *= scale;
    stats.unusedBytes *= scale;
    stats.externalKeyBytes *= scale;

    stats.branches *= scale;
    stats.values *= scale;

    return stats;
}


void
NodeStats::
dump(uint64_t totalNodes, uint64_t totalTrieBytes, std::ostream& stream) const
{
    stream
        << "\tnodeCount:        " << dumpSize(nodeCount)
        << " (" << pct(nodeCount, totalNodes) << ")" << endl

        << "\ttotalBytes:       " << dumpSize(totalBytes)
        << " (" << pct(totalBytes, totalTrieBytes) << ")" << endl

        << "\tbookeepingBytes:  " << dumpSize(bookeepingBytes)
        << " (" << pct(bookeepingBytes, totalBytes) << ")" << endl

        << "\tunusedBytes:      " << dumpSize(unusedBytes)
        << " (" << pct(unusedBytes, totalBytes) << ")" << endl

        << "\texternalKeyBytes: " << dumpSize(externalKeyBytes)
        << " (" << pct(externalKeyBytes, totalBytes) << ")" << endl

        << "\tbranches:         " << dumpSize(branches) << endl
        << "\tvalues:           " << dumpSize(values) << endl

        << "\tavgBits:          " << dumpSize(avgBits) << endl
        << "\tmaxBits:          " << dumpSize(maxBits) << endl

        << "\tavgBranchingBits: " << dumpSize(avgBranchingBits) << endl
        << "\tmaxBranchingBits: " << dumpSize(maxBranchingBits) << endl;
}



/******************************************************************************/
/* TRIE STATS                                                                 */
/******************************************************************************/

TrieStats::
TrieStats() :
    scale(1.0), probedKeys(0), totalKeys(0),
    maxDepth(0), avgDepth(0),
    maxKeyLen(0), avgKeyLen(0)
{}


NodeStats
TrieStats::
totalNodeStats() const
{
    NodeStats total;
    for (const auto& entry : nodeStats)
        total += entry.second;
    return total;
}

TrieStats
TrieStats::
toScale() const
{
    TrieStats stats = *this;
    stats.scale = 1.0;

    // not the most efficient way to do it but meh...
    stats.nodeStats.clear();

    for (const auto& entry : nodeStats)
        stats.nodeStats[entry.first] = entry.second.toScale(scale);

    return stats;
}

void
TrieStats::
dump(std::ostream& stream) const
{
    stream << "Global Stats:" << endl
        << "\tscale:            " << dumpSize(scale) << endl
        << "\ttotalKeys:        " << dumpSize(totalKeys) << endl
        << "\tprobbedKeys:      " << dumpSize(probedKeys)
        << " (" << pct(probedKeys, totalKeys) << ")" << endl
        << "\tavgDepth:         " << dumpSize(avgDepth) << endl
        << "\tmaxDepth:         " << dumpSize(maxDepth) << endl
        << "\tavgKeyLen:        " << dumpSize(avgKeyLen) << endl
        << "\tmaxKeyLen:        " << dumpSize(maxKeyLen) << endl;

    stream << "Total Node Stats:" << endl;
    NodeStats totalStats = totalNodeStats();
    totalStats.dump(totalStats.nodeCount, totalStats.totalBytes, stream);

    for (const auto& entry : nodeStats) {
        NodeType type = static_cast<NodeType>(entry.first);
        stream << nodeTypeName(type) << " Node Stats: " << endl;
        entry.second.dump(totalStats.nodeCount, totalStats.totalBytes);
    }

}



} // namespace MMap
} // namepsace Datacratic
