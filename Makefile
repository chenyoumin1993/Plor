CC=g++
CFLAGS=-Wall -g -std=c++14

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I../eRPC/src -I../silo

CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -O3 -DERPC_INFINIBAND=true
LDFLAGS = -Wall -L. -L./libs -L../eRPC/build -pthread -g -lerpc -lrt -lnuma -ljemalloc -lprofiler -lboost_system -lboost_coroutine -lcityhash -ldl  -libverbs -lm
LDFLAGS += $(CFLAGS)

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)

all:rundb

rundb : $(OBJS) ../silo/allocator.o \
	../silo/masstree/compiler.o \
	../silo/core.o \
	../silo/counter.o \
	../silo/masstree/json.o \
	../silo/masstree/straccum.o \
	../silo/masstree/string.o \
	../silo/ticker.o \
	../silo/rcu.o
	$(CC) -o $@ $^ $(LDFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp
	$(CC) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb $(OBJS) $(DEPS)
