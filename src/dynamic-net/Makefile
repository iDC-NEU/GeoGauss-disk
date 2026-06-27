# Makefile for src/dynamic-net (heterogeneous network replication)
subdir = src/dynamic-net
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

DN_CPPFLAGS = -I$(srcdir)

OBJS = $(patsubst %.cpp,%.o,$(wildcard *.cpp))

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(DN_CPPFLAGS) -c -o $@ $<

all: $(OBJS) SUBSYS.o objfiles.txt

SUBSYS.o: $(OBJS)
	$(LD) $(LDREL) $(LDOUT) $@ $^

objfiles.txt: $(OBJS)
	echo $(addprefix $(subdir)/,$(OBJS)) > $@

clean:
	rm -f *.o SUBSYS.o objfiles.txt

.PHONY: all clean
