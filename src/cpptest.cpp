#include "gqf.h"
#include <sstream>

int main(int argc, char *argv[]) {
    uint64_t qbits = argc > 1 ? atoi(argv[1]): 8;
    uint64_t nhashbits = qbits + 8;
    uint64_t nslots = (1ULL << qbits);
    uint64_t nvals = 250*nslots/1000;
    qf::filter filt(nslots, nhashbits, 0);
    std::fprintf(stderr, "made filter\n");
    for(size_t i(0); i < nvals; ++i) {
        filt.insert(i, 0, i * i);
    }
    for(const auto el: filt) {
        std::fprintf(stderr, "filt1: %zu|%zu|%zu\n", size_t(el[0]), size_t(el[1]), size_t(el[2]));
    }
}
