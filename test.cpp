#include "gqf.h"
#include <cassert>
#include <random>
#include <sstream>

int main(int argc, char *argv[]) {
    uint64_t qbits = argc > 1 ? atoi(argv[1]): 10;
    uint64_t nhashbits = qbits + 8;
    uint64_t nslots = (1ULL << qbits);
    uint64_t nvals = 250*nslots/1000;
    qf::filter filt(nslots, nhashbits, 0);
    std::mt19937_64 mt(137);
    std::vector<uint64_t> inputs;
    while(inputs.size() < nvals) inputs.push_back(mt());
    std::fprintf(stderr, "made filter\n");
    for(size_t i(0); i < nvals; ++i) {
        filt.insert(inputs[i], 0, 16);
    }
    for(const auto kmer: inputs) {
        const auto count = filt.count(kmer);
        assert(count || !std::fprintf(stderr, "Kmer %llu is missing.\n", (long long unsigned)kmer));
    }
    for(const auto tup: filt) {
        std::fprintf(stderr, "filt1: %zu|%zu|%zu\n", size_t(tup[0]), size_t(tup[1]), size_t(tup[2]));
        std::fprintf(stderr, "Official to_string of this: %s\n", qf::filter::tup2str(tup).data());
    }
}
