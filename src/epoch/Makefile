# Makefile for src/epoch
subdir = src/epoch
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

EPOCH_DN_CPPFLAGS = -I$(top_srcdir)/src/dynamic-net
EPOCH_DN_SRCS = e_dyn_net.cpp e_dyn_net_bridge.cpp e_wire_envelope.cpp

OBJS = $(patsubst %.cpp,%.o,$(wildcard *.cpp))

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(EPOCH_DN_CPPFLAGS) -c -o $@ $<

all: $(OBJS) SUBSYS.o objfiles.txt

SUBSYS.o: $(OBJS)
	$(LD) $(LDREL) $(LDOUT) $@ $^

objfiles.txt: $(OBJS)
	echo $(addprefix $(subdir)/,$(OBJS)) > $@

clean:
	rm -f *.o SUBSYS.o objfiles.txt

.PHONY: all clean
