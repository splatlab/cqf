TARGETS=main bm

ifdef D
	DEBUG=-g
	OPT=
else
	DEBUG=
	OPT=-Ofast
endif

ifdef NH
	ARCH=
else
	ARCH=-msse4.2 -D__SSE4_2_
endif

ifdef P
	PROFILE=-pg -no-pie # for bug in gprof.
endif

CC = gcc -std=gnu11
CXX = g++ -std=c++11
LD= g++ -std=c++11

CXXFLAGS = -Wall $(DEBUG) $(PROFILE) $(OPT) $(ARCH) -m64 -I. -Wno-unused-result -Wno-unused-function -Wno-strict-aliasing

LDFLAGS = $(DEBUG) $(PROFILE) $(OPT) -lpthread -lssl -lcrypto -lm

#
# declaration of dependencies
#

all: $(TARGETS)

# dependencies between programs and .o files

main:                  main.o gqf.o	hashutil.o
bm:                    bm.o gqf.o	zipf.o hashutil.o

# dependencies between .o files and .h files

main.o: 								gqf.h
bm.o:										gqf_wrapper.h

# dependencies between .o files and .cc (or .c) files

%.o: %.cc
%.o: %.c

#
# generic build rules
#

$(TARGETS):
	$(LD) $^ -o $@ $(LDFLAGS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE) $< -c -o $@

%.o: %.c
	$(CC) $(CXXFLAGS) $(INCLUDE) $< -c -o $@

clean:
	rm -f *.o $(TARGETS) core

