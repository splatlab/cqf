#include "gqf.h"
#include <sstream>

int main(int argc, char *argv[]) {
    size_t nels(8);
    if(argc > 1) std::stringstream(argv[1]) >> nels;
    size_t qbits(argc > 2 ? std::atoi(argv[2]): 4);
	uint64_t nhashbits = qbits + 8;
	uint64_t nslots = (1ULL << qbits);
    qf::filter filt(nslots, nhashbits, 0);
    std::fprintf(stderr, "made filter\n");
    for(size_t i(0); i < nels; ++i) {
        filt.insert(i, 0, 50);
    }
    for(const auto el: filt) {
        std::fprintf(stderr, "filt1: %zu|%zu|%zu\n", size_t(el[0]), size_t(el[1]), size_t(el[2]));
    }
}
