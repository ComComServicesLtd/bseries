# bseries
#
# No dependencies beyond libc, pthreads and the POSIX socket API, so the same
# makefile builds for arm, arm64 and x86. Cross compile with:
#
#     make CXX=aarch64-linux-gnu-g++

CXX      ?= g++
CXXFLAGS ?= -std=c++11 -O2 -Wall -Wextra

# The asset generator runs on the machine doing the building, so it cannot be
# built with CXX when CXX is a cross compiler.
HOSTCXX  ?= g++

# -MMD -MP makes the compiler emit a .d file listing the headers each object
# depends on, which is included below. Without it a header change does not
# rebuild anything that included it, and you get a link error or, worse, a
# binary built from two different versions of a struct.
DEPFLAGS  = -MMD -MP
LDFLAGS  ?=
LDLIBS   ?= -pthread

PREFIX   ?= /usr/local

LIB_SOURCES    = bseries.cpp
SERVER_SOURCES = bseries.cpp table_set.cpp auth_store.cpp runtime_settings.cpp profile_store.cpp http_server.cpp bseries_api.cpp web_assets.cpp bseriesd.cpp

LIB_OBJECTS    = $(LIB_SOURCES:.cpp=.o)
SERVER_OBJECTS = $(SERVER_SOURCES:.cpp=.o)

TESTS = tests/test_bseries tests/test_definitions tests/test_auth tests/test_profiles tests/test_api tests/test_concurrency

.PHONY: all clean test install static lib install-lib web

all: bseriesd

bseriesd: $(SERVER_OBJECTS)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)

# The admin page, compiled in. Regenerated whenever anything under web/ changes,
# so an edit to the page needs nothing but a rebuild.
WEB_FILES = web/admin.html web/vue.global.prod.js

web: web_assets.cpp

tools/embed: tools/embed.cpp
	$(HOSTCXX) -O2 -Wall -o $@ $<

web_assets.cpp: tools/embed $(WEB_FILES)
	./tools/embed $@ /admin=web/admin.html /admin/vue.global.prod.js=web/vue.global.prod.js

# A self contained binary, for a scratch or distroless container image. Clean
# under musl; under glibc the linker warns that getaddrinfo wants its shared
# libraries back at run time, which is why the image builds on alpine.
static: web_assets.cpp
	$(CXX) $(CXXFLAGS) -static -o bseriesd $(SERVER_SOURCES) $(LDLIBS)

# The library on its own, for linking bseries into another program rather than
# talking to it over HTTP.
LIB_HEADERS = bseries.h bseries_types.h debug.h

lib: libbseries.a

libbseries.a: bseries.o
	$(AR) rcs $@ $^

install-lib: libbseries.a
	install -d $(DESTDIR)$(PREFIX)/lib $(DESTDIR)$(PREFIX)/include/bseries
	install -m 644 libbseries.a $(DESTDIR)$(PREFIX)/lib/libbseries.a
	install -m 644 $(LIB_HEADERS) $(DESTDIR)$(PREFIX)/include/bseries/

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -c -o $@ $<

-include $(SERVER_OBJECTS:.o=.d)

tests/%: tests/%.cpp $(SERVER_SOURCES)
	$(CXX) $(CXXFLAGS) -I. -o $@ $< bseries.cpp table_set.cpp auth_store.cpp runtime_settings.cpp profile_store.cpp http_server.cpp bseries_api.cpp web_assets.cpp $(LDLIBS)

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
	rm -f *.o *.d *.a bseriesd $(TESTS) tools/embed web_assets.cpp
