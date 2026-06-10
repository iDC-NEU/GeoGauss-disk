# Makefile for src/occ
subdir = src/occ
top_builddir = ../..
include $(top_builddir)/src/Makefile.global


# 所有 cpp 文件对象（含 occ-opt 优化路径）
OBJS = $(patsubst %.cpp,%.o,$(wildcard *.cpp)) \
       $(patsubst ../occ-opt/%.cpp,../occ-opt/%.o,$(wildcard ../occ-opt/*.cpp))

../occ-opt/%.o: ../occ-opt/%.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<

# 生成所有对象文件
all: $(OBJS) SUBSYS.o objfiles.txt

# 生成 SUBSYS.o，供主程序链接
SUBSYS.o: $(OBJS)
	$(LD) $(LDREL) $(LDOUT) $@ $^

# 生成 objfiles.txt，供主程序链接
objfiles.txt: $(OBJS)
	echo $(addprefix $(subdir)/,$(OBJS)) > $@

clean:
	rm -f *.o ../occ-opt/*.o SUBSYS.o objfiles.txt
