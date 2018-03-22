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

LOC_INCLUDE=include
LOC_SRC=src
OBJDIR=obj

CC = gcc -std=gnu11
CXX = g++ -std=c++11
LD= gcc -std=gnu11

CXXFLAGS = -Wall $(DEBUG) $(PROFILE) $(OPT) $(ARCH) -m64 -I. -Wno-unused-result -Wno-unused-function -Wno-strict-aliasing

LDFLAGS = $(DEBUG) $(PROFILE) $(OPT) -lpthread -lssl -lcrypto -lm

#
# declaration of dependencies
#

all: $(TARGETS)

# dependencies between programs and .o files

main:		$(OBJDIR)/main.o $(OBJDIR)/gqf.o $(OBJDIR)/gqf_file.o $(OBJDIR)/hashutil.o
bm:		$(OBJDIR)/bm.o $(OBJDIR)/gqf.o $(OBJDIR)/gqf_file.o $(OBJDIR)/zipf.o $(OBJDIR)/hashutil.o

# dependencies between .o files and .h files

$(OBJDIR)/main.o: 	$(LOC_INCLUDE)/gqf.h $(LOC_INCLUDE)/gqf_file.h $(LOC_INCLUDE)/hashutil.h
$(OBJDIR)/bm.o:			$(LOC_INCLUDE)/gqf_wrapper.h

# dependencies between .o files and .cc (or .c) files

$(OBJDIR)/%.o: $(LOC_SRC)/%.cc
$(OBJDIR)/%.o: $(LOC_SRC)/%.c

#
# generic build rules
#

$(TARGETS):
	$(LD) $^ -o $@ $(LDFLAGS)

$(OBJDIR)/%.o: $(LOC_SRC)/%.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE) $< -c -o $@

$(OBJDIR)/%.o: $(LOC_SRC)/%.c
	$(CC) $(CXXFLAGS) $(INCLUDE) $< -c -o $@

clean:
	rm -f $(OBJDIR)/*.o $(TARGETS) core

