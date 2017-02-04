CC=gcc -std=gnu11
CXX=g++ -std=c++11
#CFLAGS=-Wall -I. -O3 -Wno-unused-result
CFLAGS=-Wall -I. -g -Wno-unused-result
LDFLAGS= -lcrypto -lm

ALL: main

gqf.o: gqf.c gqf.h

main.o: main.c gqf.h

main: main.o gqf.o
	$(CC) $(CFLAGS) gqf.o main.o $(LDFLAGS) -o main

clean:
	rm -f *.o main core
