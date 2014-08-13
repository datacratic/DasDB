/** mmap_check.cc                                 -*- C++ -*-
    Mathieu Stefani, 07 May 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Little utility to check and recover a potentially
    broken Trie

*/

#include <iostream>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/filesystem.hpp>
#include <boost/utility.hpp>
#include "trie_check.h"
#include "mmap/mmap_trie.h"
#include "mmap/trie_key.h"
#include "mmap/mmap_file.h"
#include "mmap/trie_allocator.h"
#include "mmap/mmap_trie_node.h"
#include "mmap/tools/mmap_perf_utils.h"
#include "mmap/mmap_trie_node_impl.h"
#include "mmap/mmap_trie_dense_branching_node.h"
#include "jml/utils/exc_check.h"
#include "jml/utils/guard.h"
#include <string>
#include <vector>
#include <iostream>
#include <limits>
#include <functional>

class Application {
public:
    Application() {
    }

    /* Non Copyable */
    Application(const Application &other) = delete;
    Application &operator=(const Application &other) = delete;

    bool processCommandLine(int argc, const char *argv[]) {
        using namespace std;
        using namespace Datacratic::MMap;
        namespace po = boost::program_options;
        po::options_description desc;    
        desc.add_options()
            ("file", po::value<string>()->required(), "file name")
            ("min-id", po::value<int>()->default_value(1), 
                 "Minimum Trie ID to check (default 1)")
            ("max-id", po::value<int>(), 
                 "Maximum Trie ID to check")
            ("verbose,v", "Toggle verbose mode")
            ("recover,r", po::value<bool>()->default_value(false),
                  "Try to recover when a trie is corrupted")
            ("help,h", "Display this message")
        ;

        po::positional_options_description positional_options;
        positional_options.add("file", 1);

        po::variables_map vm;
        try {
            po::store(po::command_line_parser(argc, argv).
                           options(desc).
                           positional(positional_options).
                           run(), 
                           vm);
            po::notify(vm);
        } catch (const std::exception &e) {
            cerr << e.what() << endl;
            cerr << desc << endl;
            return false;
        }

        if (vm.count("help")) {
            cout << desc << endl;
            return true;
        }

        string map_file;
        int min_id;
        int max_id;

        map_file = vm["file"].as<string>();
        const bool verbose = vm.count("verbose");
        const bool recover = vm.count("recover");

        struct Stats {
            int totalTries;
            int totalCorrupted;
            int successRecoveries;
            int failedRecoveries;
        };

        Stats stats;
        memset(&stats, 0, sizeof stats);

        if (boost::filesystem::exists(map_file)) {
            MMapFile dasdb(RES_OPEN, map_file);
            min_id = vm["min-id"].as<int>();
            if (vm.count("max-id")) {
                max_id = vm["max_id"].as<int>();
            }
            else {
                max_id = TrieAllocator::MaxTrieId;
            }


            cout << "Checking..." << endl;
            for (int id = min_id; id < max_id; ++id) {
                const bool allocated = dasdb.trieAlloc.isAllocated(id);
                if (allocated) {
                    cout << "Checking Trie #" << id << endl;
                    ++stats.totalTries;
                    TrieChecker checker(false);
                    auto current = dasdb.trie(id).current();
                    const bool checked = checker(current);
                    if (!checked) {
                        ++stats.totalCorrupted;
                        if (recover) {
                            cout << "Trie #" << id << "corrupted, trying to "
                                    "recover..." << endl;
                            TrieRepair repair;
                            auto corruption = checker.corruption();
                            const bool recovered = repair(current, corruption);
                            if (recovered) {
                                cout << "Successfully recovered" << endl;
                                ++stats.successRecoveries;
                            }
                            else {
                                cout << "Failed to recover" << endl;
                                ++stats.failedRecoveries;
                            }
                        }
                        else {
                            cout << "Trie #" << id << " is corrupted!" << endl;
                        }
                                    
                    } else {
                        cout << "Trie #" << id << " is not corrupted" << endl;
                    }
                }
            }

            if (stats.totalCorrupted > 0 && 
                stats.successRecoveries == stats.totalCorrupted) {
                dasdb.snapshot();
            }

            if (verbose) {
                cout << "------------- Statistics -------------" << endl;
                cout << left << "Total allocated tries: " 
                     << right << stats.totalTries << endl;
                cout << left << "Total corrupted tries: " 
                     << right << stats.totalCorrupted << endl;
                cout << left << "Successfully recovered tries: " 
                     << right << stats.successRecoveries << endl;
                cout << left << "Tries not recovered: "
                     << right << stats.failedRecoveries << endl;
            }


        } else {
            cerr << map_file << " unknown file or directory" << endl;
            return false;
        }

        return true;
     }

};


int main(int argc, const char *argv[]) {
    Application app;
    app.processCommandLine(argc, argv); 
    return 0;
}

