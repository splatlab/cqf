TARGETS=test test_threadsafe test_pc bm hm_churn test_runner

ifdef D
	DEBUG=-g -DDEBUG=1
	OPT=
else
	DEBUG=
	OPT=-Ofast
endif

ifdef S
	STRICT= -DSTRICT=1
else
	STRICT=
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
LOC_TEST=tests
OBJDIR=obj

CC = g++ -std=c++11
CXX = g++ -std=c++11
LD= g++ -std=c++11

CXXFLAGS = -Wall $(DEBUG) $(PROFILE) $(OPT) $(ARCH) $(STRICT) -m64 -I. -Iinclude

LDFLAGS = $(DEBUG) $(PROFILE) $(OPT) -lpthread -lssl -lcrypto -lm

#
# declaration of dependencies
#

all: $(TARGETS)

# dependencies between programs and .o files

test:								$(OBJDIR)/test.o $(OBJDIR)/gqf.o \
										$(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

test_threadsafe:		$(OBJDIR)/test_threadsafe.o $(OBJDIR)/gqf.o \
										$(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

test_pc:						$(OBJDIR)/test_partitioned_counter.o $(OBJDIR)/gqf.o \
										$(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

bm:									$(OBJDIR)/bm.o $(OBJDIR)/gqf.o \
										$(OBJDIR)/zipf.o $(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

hm_churn:						$(OBJDIR)/hm_churn.o  $(OBJDIR)/gqf.o \
										$(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

test_runner:				$(OBJDIR)/test_runner.o $(OBJDIR)/gqf.o \
										$(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

# dependencies between .o files and .h files

$(OBJDIR)/test.o: 						$(LOC_INCLUDE)/gqf.h \
															$(LOC_INCLUDE)/hashutil.h \
															$(LOC_INCLUDE)/partitioned_counter.h

$(OBJDIR)/test_threadsafe.o: 	$(LOC_INCLUDE)/gqf.h \
															$(LOC_INCLUDE)/hashutil.h \
															$(LOC_INCLUDE)/partitioned_counter.h

$(OBJDIR)/bm.o:								$(LOC_INCLUDE)/gqf_wrapper.h \
															$(LOC_INCLUDE)/partitioned_counter.h

$(OBJDIR)/hm_churn.o:					$(LOC_INCLUDE)/rhm_wrapper.h $(LOC_INCLUDE)/trhm_wrapper.h

$(OBJDIR)/test_runner.o:			$(LOC_INCLUDE)/rhm_wrapper.h $(LOC_INCLUDE)/trhm_wrapper.h


# dependencies between .o files and .cc (or .c) files

$(OBJDIR)/gqf.o:							$(LOC_SRC)/gqf.c $(LOC_INCLUDE)/gqf.h $(LOC_INCLUDE)/rhm.h $(LOC_INCLUDE)/trhm.h
$(OBJDIR)/hashutil.o:					$(LOC_SRC)/hashutil.c $(LOC_INCLUDE)/hashutil.h
$(OBJDIR)/partitioned_counter.o:	$(LOC_INCLUDE)/partitioned_counter.h

#
# generic build rules
#

$(TARGETS):
	$(LD) $^ -o $@ $(LDFLAGS)

$(OBJDIR)/%.o: $(LOC_SRC)/%.cc | $(OBJDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDE) $< -c -o $@

$(OBJDIR)/%.o: $(LOC_SRC)/%.c | $(OBJDIR)
	$(CC) $(CXXFLAGS) $(INCLUDE) $< -c -o $@

$(OBJDIR)/%.o: $(LOC_TEST)/%.cc | $(OBJDIR)
	$(CC) $(CXXFLAGS) $(INCLUDE) $< -c -o $@

$(OBJDIR):
	@mkdir -p $(OBJDIR)

clean:
	rm -rf $(OBJDIR) $(TARGETS) core

