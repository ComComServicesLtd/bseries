# bseries
#
# No dependencies beyond libc, pthreads and the POSIX socket API, so the same
# makefile builds for arm, arm64 and x86. Cross compile with:
#
#     make CXX=aarch64-linux-gnu-g++

CXX      ?= g++
CXXFLAGS ?= -std=c++11 -O2 -Wall -Wextra

# -MMD -MP makes the compiler emit a .d file listing the headers each object
# depends on, which is included below. Without it a header change does not
# rebuild anything that included it, and you get a link error or, worse, a
# binary built from two different versions of a struct.
DEPFLAGS  = -MMD -MP
LDFLAGS  ?=
LDLIBS   ?= -pthread

PREFIX   ?= /usr/local

LIB_SOURCES    = bseries.cpp
SERVER_SOURCES = bseries.cpp http_server.cpp bseries_api.cpp bseriesd.cpp

LIB_OBJECTS    = $(LIB_SOURCES:.cpp=.o)
SERVER_OBJECTS = $(SERVER_SOURCES:.cpp=.o)

TESTS = tests/test_bseries tests/test_definitions tests/test_api tests/test_concurrency

.PHONY: all clean test install

all: bseriesd

bseriesd: $(SERVER_OBJECTS)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -c -o $@ $<

-include $(SERVER_OBJECTS:.o=.d)

tests/%: tests/%.cpp $(SERVER_SOURCES)
	$(CXX) $(CXXFLAGS) -I. -o $@ $< bseries.cpp http_server.cpp bseries_api.cpp $(LDLIBS)

test: $(TESTS)
	@failed=0; \
	for t in $(TESTS); do \
	    echo "== $$t"; \
	    ./$$t || failed=1; \
	done; \
	exit $$failed

# The same suite under the sanitizers. Slower, and how the memory safety and
# threading claims in the commit history were checked.
.PHONY: test-asan test-tsan
test-asan:
	$(MAKE) clean
	$(MAKE) test CXXFLAGS="-std=c++11 -g -O1 -Wall -Wextra -fsanitize=address,undefined"
test-tsan:
	$(MAKE) clean
	$(MAKE) test CXXFLAGS="-std=c++11 -g -O1 -Wall -Wextra -fsanitize=thread"

install: bseriesd
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 755 bseriesd $(DESTDIR)$(PREFIX)/bin/bseriesd

clean:
	rm -f *.o *.d bseriesd $(TESTS)
