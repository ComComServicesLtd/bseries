# bseries
C++ 11 High Performance Cached Binary Time Series


Useful for storing large fixed time interval data series for example

pinging of thousands of devices every 1 second.

Each data series has a header which contains the creation date, checksum and misc other information

each time a write is requsted a single data point is appended to the end of this file
Since each point represents a fixed time interval, querying by date is instantaneous.. I.e. last day = last 86400 bytes if datasize is 1 byte

Additionally the database utilizes caching to reduce harddisk throughput, a write ahead log is also used to prevent increasing filesize by small intervals


## Series definitions

A database can hold more than one kind of measurement. A definitions file declares
the interval and the datatype for a range of keys, and any series created in that
range takes that shape:

```
# name      keys            interval   type       null_fill
ping        1-9999          1s         uint8
temperature 10000-19999     60s        float32
power_draw  20000-29999     10s        uint32
door_state  30000-30999     5s         uint8      0x02
```

```cpp
BSeries db;
db.data_directory = "/var/lib/bseries";

if(db.loadDefinitions("/etc/bseries.conf") < 0)
    return 1;  // malformed configuration, do not start

float celsius = 20.5f;
db.write(10500, &celsius, sizeof(celsius));
```

Ranges are matched in file order and the first match wins, so declare narrow ranges
before wide ones. Definitions can also be installed from code with `defineSeries()`.
See `bseries.conf.example` for the full format, the supported types, and how the
null fill byte is chosen.

Supported datatypes are `uint8` `uint16` `uint32` `uint64`, `int8` `int16` `int32`
`int64`, `float32` and `float64`.

A definition applies when a series file is created. Once the file exists its own
header is the authority, because the points already on disk were laid out to it, so
changing a definition does not retype existing series. The database warns when a
definition and a file disagree and keeps using the file.

Keys with no definition fall back to `default_seconds_per_point` and whatever point
width is passed to `write()`, which is what the database did before definitions
existed.


## HTTP API

`bseriesd` serves CRUD over HTTP. It depends on nothing but libc, pthreads and the
POSIX socket API, so it cross compiles for arm, arm64 and x86 with no libraries to
chase:

```
make            # builds bseriesd
make test       # builds and runs the test suite
bseriesd -c /etc/bseriesd.conf
```

See `bseriesd.conf.example` for the settings. Point data crosses the wire as hex,
because a series holds raw binary of whatever width it was defined with.

### In a container

```
docker build -t bseries .
docker run -d -p 127.0.0.1:8086:8086 -v bseries-data:/data bseries
docker logs <id>          # prints the one time bootstrap token
```

The project has no dependencies, so the image is `scratch` plus one static binary
and an empty data directory — nothing else to patch or audit. It builds on alpine
rather than a glibc base on purpose: glibc's `getaddrinfo` wants its shared
libraries back at run time even inside a static binary, and musl's does not.

The container runs as uid 10001, writes only to `/data`, and works with a
read-only root filesystem. `docker-compose.example.yml` is a starting point.

**Configuration comes from the environment.** Every setting the file understands
has a `BSERIES_` twin — `listen` is `BSERIES_LISTEN`, `max_points_per_read` is
`BSERIES_MAX_POINTS_PER_READ` — so no configuration file needs baking into an
image. A file can still be given with `-c` or `BSERIES_CONFIG`, and the
environment wins over it. `BSERIES_CORS_ORIGIN` takes a comma separated list,
since an environment variable cannot be repeated the way a configuration line can.

The image sets `BSERIES_LISTEN=0.0.0.0`, unlike the shipped default of the
loopback. Inside a container the loopback is reachable only from the container
itself, so the normal default would look like the server was ignoring you.

**Health checks need no HTTP client in the image.** `bseriesd --health` connects to
the configured address, asks `/v1/health`, and exits 0 or 1, so `HEALTHCHECK` runs
the same binary.

**Logs go to stderr, unbuffered.** That matters more than it sounds: diagnostics
used to go to stdout through `printf`, which is block buffered when stdout is a
pipe — which is exactly what `docker logs` gives it. A warning would sit in the
buffer until the process exited, and a `kill -9` lost it entirely.

Shutdown flushes every buffer, so give the container time to stop:
`stop_grace_period` in compose, or `--stop-timeout` on `docker run`. The default
10 seconds is enough for a normal database.

### On MikroTik RouterOS

A router is a good place for this: it already knows the numbers worth recording,
and a 1.2MB static binary fits on hardware that has no room for a real database.

```
./tools/build-mikrotik.sh          # arm64; pass linux/arm or linux/amd64 otherwise
scp -O bseries-arm64.tar <router>:
```

then edit and apply `mikrotik.example.rsc`. Check the target first with
`/system resource print`: `architecture-name` is `arm64` for most recent models,
`arm` for older 32 bit ones. Verified on a hAP ax² running RouterOS 7.24.2, where
the container unpacks to 1.2MB and idles around 580KB of memory.

**The image has to be repacked before RouterOS will read it.** Docker has not
written the old `layer.tar`-per-directory format since the containerd image store
became the default — buildx emits an OCI layout with gzipped layers, and
`--output type=docker` does not change that. RouterOS reads only the old layout,
and the failure is unhelpfully late: the container is created, then marked `F`
with `could not load next layer`. `tools/oci-to-docker-archive.py` converts one to
the other, and `build-mikrotik.sh` runs it for you. Build attestations have to go
too — `--provenance=false --sbom=false` — since RouterOS cannot match them to a
platform.

**`Dockerfile.mikrotik` runs as root, unlike the main image.** Not an oversight:
RouterOS mounts a directory out of its own flash, owned by root, and offers
neither a shell nor `chown`, so a uid 10001 process cannot write its own
database. Root is confined to the container's namespace, and the process still
writes nothing outside `/data`.

**Keep the database on a mount.** `/container/mounts` puts it on the router's
flash, where it survives `/container/remove` and an image upgrade; the container's
own layer does not. That flash has a limited erase budget, so the shipped hour
between flushes matters more here than it does on a server — `BSERIES_FLUSH_INTERVAL`
is the dial, and lowering it costs write cycles.

Two RouterOS parameter names are easy to get wrong: mounts are declared with
`list=`, not `name=`, and attached to a container with `mountlists=`, not
`mounts=`. Both fail with a bare `bad parameter`.

### The admin page

`bseriesd` serves a small Vue page at `/` for managing tables, series, keys and
the runtime settings. Point a browser at the database and sign in with a write
key; there is nothing to install and no second server to run.

**It is served by the database itself, and that is the whole point.** A page
hosted anywhere else is a different origin, so every call it made would need
`cors_origin` set and would carry the browser's preflight along with it. Served
from `bseriesd`, the requests are same origin and none of that arises.

**It is compiled into the binary**, so the scratch image still holds one file and
there is still nothing to mount. `web/` holds the source — the page and a
vendored copy of Vue — and `tools/embed.cpp` turns them into `web_assets.cpp`.
The generator is C++ rather than a script so that building still needs nothing
but a compiler and make. After editing anything under `web/`:

```
make web && make
```

Vue is vendored rather than loaded from a CDN deliberately: this is the page you
reach for when a network is misbehaving, and it should not need that network to
render. It costs about 200KB of binary.

**The page itself is unauthenticated**, which it has to be — it is the thing a key
is typed into. It ships no data of its own; everything it displays comes from a
later call carrying the key. The key lives in `sessionStorage`, so it goes away
when the tab closes and never reaches the URL or the server's log.

There is still no TLS, so a key typed into this page crosses the network in
clear. Reach it over the loopback, a trusted interface, or a reverse proxy.

### As a library

`make lib` builds `libbseries.a`, and `make install-lib` installs it with the
headers under `$(PREFIX)/include/bseries`, for linking bseries into a program
directly rather than talking to it over HTTP.

Every data path is rooted at a table (see **Tables** below); `{t}` stands for the
table name.

| Method | Path | Key | |
|---|---|---|---|
| GET | `/v1/health` | none | liveness |
| POST | `/v1/auth/keys?role=&name=` | write | mint an API key |
| GET | `/v1/auth/keys` | write | list keys (never the secrets) |
| DELETE | `/v1/auth/keys/{name}` | write | revoke a key |
| GET | `/v1/config` | read | read the runtime settings |
| POST | `/v1/config?<setting>=<value>` | write | change them, no restart |
| GET | `/v1/tables` | read | list tables |
| GET | `/v1/tables/{t}` | read | table info |
| POST | `/v1/tables/{t}` | write | create a table |
| DELETE | `/v1/tables/{t}?force=` | write | drop a table |
| GET | `/v1/{t}/series?keys=` or `?limit=&after=` | read | list series |
| GET | `/v1/{t}/series/{key}` | read | header and size |
| POST | `/v1/{t}/series/{key}?type=&interval=&start=` | write | create |
| DELETE | `/v1/{t}/series/{key}` | write | delete |
| POST | `/v1/{t}/series/{key}/migrate` | write | rewrite a version 1 series as version 3 |
| GET | `/v1/{t}/series/{key}/data?start=&end=` | read | read a range |
| GET | `/v1/{t}/data?keys=&start=&end=` | read | read a range across several series |
| POST | `/v1/{t}/data` | write | write points to several series |
| POST | `/v1/{t}/now` | write | push one reading into every series, at the nearest slot |
| POST | `/v1/series/{key}/data?timestamp=` | write | write points |

`start` and `end` are unix timestamps; `end` defaults to now. A write body is the
hex of one or more consecutive points, placed at `timestamp`, `timestamp+interval`,
and so on. `start` on create sets where the series' first point sits, which is how
a series is prepared for a backfill — a write before the series start is refused
rather than silently relocated.

```
$ curl -XPOST -H 'X-API-Key: $WRITE_KEY' \
    'localhost:8086/v1/series/10500?type=float32&interval=60&start=1700000000'
{"key":10500,"version":2,"type":"float32","datasize":4,"interval":60,...}

$ curl -XPOST -H 'X-API-Key: $WRITE_KEY' --data-binary '0000a4410000aa4100000c42' \
    'localhost:8086/v1/series/10500/data?timestamp=1700000000'
{"key":10500,"points_written":3,"first_timestamp":1700000000,...}

$ curl -H 'X-API-Key: $READ_KEY' \
    'localhost:8086/v1/series/10500/data?start=1700000000&end=1700000300'
{"key":10500,"type":"float32","interval":60,"n_points":5,"real_points":3,
 "null_fill":"ffffffff","data":"0000a4410000aa4100000c42ffffffffffffffff"}
```

### Tables

A table name may contain **letters, underscore and hyphen**, must start with a
letter, and is at most 64 characters.

A table is an independent namespace of series, so one database can hold several
unrelated collections without their keys colliding: device 1 in `network` and
device 1 in `power` are different series with different intervals and types.

On disk a table is exactly a directory of series files. Nothing about the storage
format changes — a table is a place, not a new kind of thing.

```
$ curl -XPOST -H 'X-API-Key: $WRITE_KEY' localhost:8086/v1/tables/network
{"table":"network","created":true}

$ curl -H 'X-API-Key: $READ_KEY' localhost:8086/v1/tables
{"tables":["default","network","power"],"count":3}
```

**The table named `default` is the data directory itself**, not a subdirectory of
it. That is what lets a database written before tables existed keep working
untouched: its files stay where they are, and the unqualified paths
(`/v1/series/...`, `/v1/data`, `/v1/now`) still work and address that table.
`/v1/default/series/5` and `/v1/series/5` are the same series.

```
data/5                  the default table, which is the root
data/network/1          table "network"
data/power/1            table "power"
```

Tables must be created before they can be written to; a write to an unknown table
is a 404 rather than a new table, so a typo in a table name cannot quietly fork a
database's data into a second copy nobody is reading. Set `auto_create_tables 1`
in the config if you would rather have the convenience.

Dropping a table refuses while it still holds series. `force=1` deletes them with
it, and is the only way to destroy data through that endpoint. The default table
cannot be dropped, since it is the data directory.

Digits are excluded deliberately: the default table is the data directory itself
and series files are named for their key, so a name that could look like a number
would be ambiguous. Requiring a letter first also keeps a name from starting with a
hyphen, which becomes an option the first time anyone types it into a shell. The
character set makes a name safe as a path component too, which matters because it
arrives from a URL — `..`, `/` and their percent encoded forms are all refused.
`health`, `tables`, `auth`, `series`, `data` and `now` are reserved.

### Definitions per table

A `table` line in the definitions file switches which table the definitions after
it belong to. Those before any such line belong to `default`:

```
legacy      1-99        10s  uint8      # the default table

table network
ping        1-9999      1s   uint8
latency     10000-19999 60s  float32

table power
draw        1-999       10s  uint32
```

Key ranges may overlap between tables, since the tables are independent: key 1 is a
1 second `uint8` in `network` and a 10 second `uint32` in `power`.

A malformed line fails the whole load whichever table is being read, so a typo
under one table is reported rather than waiting until that table is first used.

### Selecting series

Every read takes the same `keys` selector: a comma separated list which may contain
`N-M` ranges.

```
keys=1,4,5          three specific series
keys=1-500          a range
keys=1-500,10500    both
```

It works on `/v1/data` and on `/v1/series`, so the same selection can fetch metadata
or data. Without `keys`, `/v1/series` walks the data directory in key order and
takes `limit` and `after` for paging. `skip_missing=1` omits absent series instead
of returning an entry naming the error.

### Reading several series at once

`/v1/data` takes `keys` as a comma separated list, which may contain `N-M` ranges,
and returns one dataset object per series:

```
$ curl -H 'X-API-Key: $READ_KEY' \
    'localhost:8086/v1/data?keys=10500-10502&start=1700000000&end=1700000240'
{"start":1700000000,"end":1700000240,"series":[
  {"key":10500,"type":"float32","interval":60,"n_points":4,"real_points":3,
   "null_fill":"ffffffff","data":"0000a4410000aa410000b041ffffffff"},
  {"key":10501, ... },
  {"key":10502, ... }],
 "count":3}
```

The JSON array holds one entry per *series*, never one per point: a day of one
second points is 86400 values, and wrapping each of those in JSON punctuation costs
several times what the data itself does. Each series' whole range is one hex blob,
the same as the single series endpoint returns.

A series that cannot be read becomes an entry carrying an `error` rather than
failing the request, so one missing key out of two hundred does not cost the caller
the other hundred and ninety nine. Pass `skip_missing=1` to leave absent series out
of the array entirely — useful when `keys` is a wide range over a sparse key space.

`max_points_per_read` is a budget for the whole request rather than per series,
otherwise asking for more series would multiply the cap. `max_series_per_read`
bounds how many keys one request may name, and a range is checked against it before
being expanded, so `keys=0-4000000000` is refused rather than materialised.

### Writing several series at once

`POST /v1/data` takes one record per line, which streams straight from a file or a
pipe and needs no JSON on the way in:

```
<key>  <timestamp|now>  <hex>
```

The hex is one or more consecutive points, placed the same way the single series
endpoint places them. Blank lines and `#` comments are ignored.

```
$ printf '# one line per device\n10 1700000000 0a0b0c\n11 1700000000 141516\n' \
    | curl -XPOST -H 'X-API-Key: $WRITE_KEY' --data-binary @- localhost:8086/v1/data
{"records":2,"records_written":2,"records_failed":0,"points_written":6,"points_expected":6}
```

The whole body is parsed and validated before anything is written, so a typo on
line four hundred cannot leave the first three hundred and ninety nine applied.
Records are then applied independently: one device with a bad clock does not cost
the other thousand their points.

The database has no transactions, so a failure during that second pass leaves a
partial batch. That is reported rather than glossed over — `records_written` and
`records_failed`, plus a `results` array naming each record's line, state and
error. A batch where nothing landed reports `write_failed`, not `partial_write`.
Pass `verbose=1` to get `results` on success too.

```json
{"records":3,"records_written":2,"records_failed":1,"points_written":2,
 "results":[{"line":1,"key":1,"state":"written","error":"ok",...},
            {"line":2,"key":2,"state":"failed","error":"timestamp_before_series_start",...},
            {"line":3,"key":3,"state":"written","error":"ok",...}],
 "error":"partial_write","message":"the batch was applied in part, see results"}
```

`max_points_per_write` caps the total points one request may carry.

### Streaming

Read responses are written out as they are produced, using chunked transfer
encoding, rather than assembled in memory first. A response therefore costs one
series' points plus a 64KB buffer, not the size of the whole answer, and that no
longer multiplies by `max_connections`. Hex encoding happens a slice at a time for
the same reason.

The consequence is that a read carries no `Content-Length`, and that the status is
committed before the body is produced: anything that could make a read fail is
checked up front, and a series that fails after that point is reported as an entry
inside the array. An HTTP/1.0 client gets a close delimited body instead of chunked
encoding, and that connection is not reused.

### Pushing one reading into every series

`POST /v1/now` is the shape a poller wants: it has just sampled a thousand devices
and does not care which slot each reading lands in, only that they all land in the
one nearest now. One record per line, and **no timestamps in the body at all**, so
a client with a wrong clock cannot scatter readings across the grid:

```
<key>  <hex>
```

```
$ printf '1 0a\n2 14\n3 1e\n' \
    | curl -XPOST -H 'X-API-Key: $WRITE_KEY' --data-binary @- localhost:8086/v1/now
{"now":1700000005,"records":3,"records_written":3,"records_failed":0,"overwritten":0}
```

Unlike `/v1/data`, this rounds to the **nearest** slot rather than flooring, which
halves the worst case placement error; exactly halfway takes the later slot. Each
series is snapped to its own grid, since each has its own start and interval. One
value per series is the contract — a line carrying a run of points is refused with
`expected_single_point`, because that is what `/v1/data` is for. `at=` overrides
the server clock, for replay and testing.

### When a reading replaces another

A slot holds one value, so a second reading landing in a slot that already holds a
real one replaces it. Every write endpoint now reports when that happened:

```
$ printf '1 0c\n2 16\n3 20\n' | curl -XPOST ... localhost:8086/v1/now
{"now":1700000055,"records":3,"records_written":3,"records_failed":0,"overwritten":3,
 "results":[{"line":1,"key":1,"state":"written","slot":1700000060,"overwritten":true},
            {"line":2,"key":2,"state":"written","slot":1700000060,"overwritten":true},
            {"line":3,"key":3,"state":"written","slot":1700000060,"overwritten":true}]}
```

A steady stream of these means the poller is sampling faster than the series
interval, or its period is drifting against the grid — either way readings are
being discarded, and now you can see it rather than infer it later from a gap.

`results` lists only the records worth looking at: the ones that failed and the
ones that replaced a reading. For a two thousand device batch that is the
difference between a line of JSON and a megabyte of it. `verbose=1` lists every
record instead. This applies to `/v1/data` as well.

Detection costs nothing on the cached write path, where the slot is already in
memory, and one point read on the direct path. `write()` only does it when a
caller passes an `overwrote` flag, so the library's own hot path is unchanged.

### What a write costs

Points are appended into an in memory buffer and only reach the disk when that
buffer fills, so a steady ingest does no file I/O at all for most points.
Measured on a 500 series, one point per series ingest over HTTP:

```
100000 points:  read syscalls 0   write syscalls 0   disk bytes 0
```

A block flushes once it fills, and on a clean shutdown. Straight against the
library with the flush timer off, 100000 points into one series is 121 write
syscalls — one per 1024 point block.

Getting there needs `seriesShape()` rather than `seriesInfo()` on any write path.
`seriesInfo()` reads the header off disk deliberately, so probing unknown keys
cannot grow the in memory index; calling it per point cost an open, two reads, a
seek and a close for something the open series already knew.

**Buffered points are readable.** A read fills its output from the file and then
copies the write ahead buffer over the top, so a point is queryable the moment it
is written and no flush is needed to see it. The flush timer is about what
survives losing power, not about what a read can see.

What the buffer *is* missing from is the file, and so from `points_in_file`, which
is derived from the file's size. A series written to in the last hour can report
`"points_in_file":0` and still read back everything in it. `buffered_points` is
what is held in memory, and `points` is the two together — the number that answers
"how much is in this series".

```
{"key":1234,…,"points_in_file":0,"buffered_points":6,"points":6,"file_size":20}
```

### Durability: the flush timer

A buffer holds `write_ahead_size` **points**, not a span of time, so on a slow
series it can hold days of data. `flush_interval` bounds that in seconds: any
series holding buffered points for longer is written out, whether or not it is
still being written to. This matters because eviction only touches series that have
gone *idle* — without the timer it is the busiest series that hold the most
unflushed data, which is backwards.

Default 3600 seconds; `0` disables it. `SIGTERM` and `SIGINT` still flush
everything, so a normal restart loses nothing either way, and `series_max_idle`
already flushes anything that stops being written — the timer is specifically about
series that are *continuously* written.

What a series actually gets is whichever bound comes first:

```
exposure = min(flush_interval, write_ahead_size x series interval)
```

That second term matters more than it looks. At 1024 point blocks:

| interval | block fills in | what bounds exposure at a 1 hour timer |
|---|---|---|
| 1s | 17 min | the block — the timer never improves on it |
| 5s | 85 min | the timer |
| 10s | 2.8 h | the timer |
| 60s | 17.1 h | the timer |
| 5m | 85.3 h | the timer |

So on a 1 second series an hourly timer buys no durability the block was not
already giving, and costs the padding. Measured per point on a 1s series:

| flush interval | write syscalls per point |
|---|---|
| off | 0.001 |
| 1 hour | 0.139 |
| 10 min | 0.661 |
| 1 min | 0.961 |

The slower series are where the timer earns its cost, and they are cheap to flush
precisely because they are slow: a 60 second series doing write through is about 56
file writes an hour.

```
wrote 3000 points          file: 20 bytes      (nothing due yet)
after the timer            file: padded out to whole blocks
kill -9, restart           3000 of 3000 points survived
```

### Blocks, and what a flush costs

Points are buffered a block at a time, `write_ahead_size` points to a block,
default 1024. **A block is always written whole**: where fewer points than that
have been recorded, the rest go down as null fill, so the file only ever grows by
whole blocks and a partly filled one is padded ahead rather than left short.

That has a consequence worth understanding before choosing a flush interval. Once a
block has been padded the file covers all of it, so later points inside that block
are written **straight to the file** rather than buffered. They are durable as they
arrive — and they cost a write each. Measured on one series over 4096 points with
1024 point blocks:

| flush timer fires | write syscalls per point |
|---|---|
| never, the block fills first | 0.001 |
| every 300 points | 0.817 |
| every 60 points | 0.948 |

So the interval to choose is really a question about its ratio to how long a block
takes to fill:

* **Longer than a block takes to fill** — the block fills first, the timer rarely
  fires, and writes cost almost nothing. A 1s series with 1024 point blocks fills
  in about 17 minutes.
* **Shorter than that** — most points are written through to the file individually.
  Durability is close to per point, at about one file write per point.

Neither is wrong; they are two points on the same trade. At the default 60 second
timer a 1s series sits in the second regime, which for a write primary database is
usually the intent. Raise `write_ahead_size` or `flush_interval` to move the other
way.

A clean buffer still costs nothing to flush, so an idle series does not gain a
block of null fill every time anything flushes it.

### Changing settings while it runs

`/v1/config` reads and changes the settings worth tuning against a live workload:

```
$ curl -XPOST -H 'X-API-Key: $WRITE_KEY' 'localhost:8086/v1/config?flush_interval=15'
{"settings":{"flush_interval":15,"series_max_idle":900,…}}
```

`flush_interval`, `series_max_idle`, `maintenance_interval`, `max_points_per_read`,
`max_series_per_read`, `max_points_per_write` and `max_condense_scan`. The
maintenance thread re-reads its intervals every second, so a change takes effect on
the next tick rather than after the old interval expires.

A request that names one out-of-range value changes **nothing** — everything is
validated before anything is applied. Changes live in memory only; the
configuration file is not rewritten, so a restart returns to what is written there.
Everything else — the listening address, the data directory, the keys — still needs
a restart.

### Timestamps and how far a series can grow

A series created by a write is stamped with **that write's timestamp**, so the
first point is the start of the series. A batch assembled a moment before it is
sent is therefore accepted; stamping the series "now" instead meant the very write
creating it could be rejected for predating it.

Writing before an *existing* series' start is refused
(`timestamp_before_series_start`) rather than silently relocated. To backfill
history into a series that already exists, create it with `start=` set in the past.

Because series are dense, a point timestamped far beyond the end of a series would
null fill every interval in between. `max_grow_points` bounds how far one write may
reach; beyond it the write fails with `timestamp_too_far_ahead` rather than
allocating and writing gigabytes for a single bad timestamp.

### Where a write lands

A write is placed by flooring, not by rounding to the nearest slot:

    slot = (timestamp - series start) / interval        integer division

With a 60 second interval, a reading timestamped 59 seconds past a slot goes into
**that** slot, not the one a second away. The grid is anchored to the series' own
start timestamp, which for a series created by a write is the timestamp of its
first point, so the slot boundaries sit wherever that first reading fell.

Two consequences worth designing around:

* **Two readings can land in the same slot, and the later one silently replaces
  the earlier.** There is no error and no indication. With a sampler running at the
  series' own interval this only happens when a reading is late enough to cross a
  slot boundary, which then also leaves the slot it should have filled empty.
* Reads report `first_point_timestamp` as the true slot time of `output[i=0]`,
  which can be up to `interval - 1` seconds before the `start` that was asked for.
  Point `i` is at `first_point_timestamp + i * interval`; do not assume it is at
  `start + i * interval`.

If sub-interval placement matters for your data, the fix is a shorter interval,
not a rounding rule: a series stores one value per slot and cannot represent two
readings inside one.

### Condensing for charts

`max_points` and `condense` downsample a range into at most that many buckets. They
are **used as a pair** — a bucket count means nothing without saying how to combine
what falls in a bucket, and vice versa — and supplying one alone is refused.
`condense` is `min`, `max` or `average`. Both work on `/v1/series/{key}/data` and on
`/v1/data`.

```
$ curl -H 'X-API-Key: $READ_KEY' \
    'localhost:8086/v1/series/1/data?start=1700000000&end=1700300000&max_points=500&condense=average'
{"key":1,"type":"float64","datasize":8,"interval":600,"source_interval":1,
 "condense":"average","factor":600,"first_point_timestamp":1700000000,
 "n_points":500,"data":"...","real_points":500,"points_scanned":300000,
 "source_points":300000,"null_fill":"ffffffffffffffff"}
```

`interval` is the **bucket** width, so `first_point_timestamp + i * interval` still
gives point `i`'s time; `source_interval` is the series' own. `factor` is how many
slots went into each bucket. `real_points` counts buckets that contained at least
one reading, `points_scanned` is how many slots were walked and `source_points`
how many of those held a reading.

`min` and `max` hand back a stored point untouched, so they keep the series' type.
An average generally is not representable in it — the mean of two `uint8` readings
usually is not a `uint8` — so `condense=average` promotes to `float64`, which holds
every value of every narrower type exactly. The response's `type` and `datasize`
say which you got.

**A condensed read is windowed**, so it is not bounded by `max_points_per_read` the
way a raw read is: the range is walked a window at a time and a bucket's running
state carries across window boundaries. Condensing 300000 one second points into
500 buckets returns 8KB and holds a few MB while doing it. `max_condense_scan`
bounds the total slots a condensed request may walk, and `condense_window` sets how
many are held at once.

### Migrating a legacy series

A version 1 header records a point width and no datatype, so a one byte series
reads back as `uint8` with 255 for the fill. That leaves a prober's own sentinel
— 1 for "no reply" — sitting in the middle of the readings, where it is the
*smallest* value rather than the worst one. It drags an average down, never
appears in a maximum, and pins a minimum to itself forever.

```
$ curl -XPOST -H 'X-API-Key: $WRITE_KEY' 'localhost:8086/v1/lab/series/531/migrate'
{"key":531,"migrated":true,"from_version":1,"to_version":3,"points":15894380,
 "remapped":910173,"clamped":3209,"nulls":1780471,
 "no_reply":254,"reading_max":244,"null_fill":255}
```

Afterwards a `uint8` series means: **0 to 244** a reading, **245 to 253** spare
for another sentinel, **254** no reply, **255** nothing recorded. The admin page
offers the button on any series whose file is still version 1.

The migration does three things: it moves 1 to 254, pulls 245 to 254 down to 244,
and stamps a version 3 header recording `uint8` and a 255 fill. Both headers are
20 bytes, so the data does not move.

**245 to 254, not 245 to 253.** The range has to include the value the sentinel is
moving *into*, or a real reading of 254 becomes indistinguishable from a no reply.
On the series above that was 2,896 readings — small, but it is the difference
between 254 meaning one thing and meaning two.

**It cannot be undone**, and it is not re-runnable: the two steps are one pass in
that order, so a second run would clamp the sentinels the first one wrote. The
version field is the guard, and a series that is not a version 1 one byte unsigned
file is refused with a 409 rather than migrated into something meaningless.

The series is flushed and evicted first, then written to `<key>.migrating` and
renamed over the original, so an interrupted run leaves the original untouched.
That needs the file's size again in free space — worth checking on a router before
migrating a series of any size. The index lock is held throughout, so the database
is quiet while it runs.

What this buys, on five years of a real series at six buckets:

| | min | max | avg raw | avg `reserved=254` | loss |
|---|---|---|---|---|---|
| bucket 0 | 7 | 254 | 23.22 | 22.87 | 0.1% |
| bucket 5 | 8 | 254 | 90.87 | 23.10 | 27.1% |

Before the migration that last bucket averaged **14ms** and looked like the
fastest week in the series, because a quarter of its readings were a 1.

### Values that are not measurements

A prober that writes `1` for "no reply" is recording an outcome, not a latency.
Averaged with milliseconds it is worse than noise: on a real series here 11% of
readings were that `1`, and including them moved the mean **down** by 2.5ms, so a
link losing a third of its packets read as the fastest week on the chart.

`reserved` names such values, and they are then counted rather than aggregated.
It takes a comma separated list, so a prober can tell no reply from a timeout
from a DNS failure and still have all of them treated alike.

```
$ curl -H 'X-API-Key: $READ_KEY' \
    'localhost:8086/v1/lab/series/531/data?start=…&end=…&max_points=12&condense=average&reserved=1'
{"…","data":"…","reserved":[1],"reserved_mode":"exclude","reserved_threshold":0,
 "reserved_points":910173,"reserved_counts":"…"}
```

`reserved_counts` is one little endian `uint32` per bucket, so loss can be drawn
beside latency instead of hidden inside it. `reserved_points` is the total. Naming
no reserved value leaves the response byte for byte what it was.

**This is a property of the request, not of the series.** Whether `1` means "no
reply" is the prober's convention, and another series may store `1` as an ordinary
reading, so nothing about it is written to the file. It applies only to a
condensed read — a raw read hands back what was stored — and passing it to a raw
read is refused rather than quietly ignored.

**`reserved_mode=dominate` lets a reserved value take a maximum.** Only a maximum:
a minimum is the other end of the range, where "no reply" is exactly the wrong
answer, and an average of a sentinel is not a number.

`reserved_threshold` is the share of a bucket that has to be reserved before it
does, and it is what stops the feature being useless at scale. With a tenth of a
series reserved, every bucket of a year long window contains one, so the default
threshold of `0` — dominate on sight — paints the whole chart as an outage:

| bucket | 0 | 1 | … | 9 | 10 | 11 |
|---|---|---|---|---|---|---|
| loss | 0.1% | 0.1% | | 6.7% | 20.9% | 33.3% |
| `condense=max` | 254 | 254 | | 254 | 254 | 254 |
| `dominate`, threshold 0 | 1 | 1 | | 1 | 1 | 1 |
| `dominate`, threshold 0.25 | 254 | 254 | | 254 | 254 | 1 |

Threshold `0` is still the right answer when the value is genuinely rare; it is
the default because it is the least surprising reading of "let it dominate".

Reserved values are matched as doubles, which is exact for every type the database
stores except a 64 bit integer past 2^53. Sentinels are small by nature, so that
has not been worth a second comparison path.

### How missing data is kept out of the aggregate

A series is dense, so a month of one second slots contains a slot per second
whether anything was recorded or not. Feeding the empty ones to an aggregate would
be wrong three separate ways:

* **average** would be dragged towards the fill value by however many slots were
  empty, which for a sparsely written series is nearly all of them
* **max** over an unsigned series would return the fill itself, since the fill is
  the maximum value of the type
* **min** over a float series would return NaN, and one NaN poisons every
  comparison it takes part in

So empty slots take no part in any aggregate. A bucket with no readings at all
produces the null fill, which is what tells a chart to draw a gap rather than a
line through an invented value. Non finite floats are treated as missing too: the
float fill is a NaN, but a client can store a NaN or an infinity of its own and
either would wreck an aggregate the same way.

Over 600 slots holding `1..100`, then 100 empty, then `50` throughout:

```
condense=min      01 ff 32 32 32 32     1,   gap, 50 ...   the fill is not a minimum
condense=max      64 ff 32 32 32 32     100, gap, 50 ...   not 255, which is the fill
condense=average  50.5, NaN, 50, ...                       the true mean of 1..100
```

### Gaps in a response

`data` is the raw series content, so the points where nothing was recorded hold the
series' null fill. Every response carries `null_fill` as a hex pattern one point
wide — compare each point against it to find the gaps. In the example above the
last two points are gaps.

The consequence is worth being explicit about: **a real reading equal to the fill
is indistinguishable from a gap.** For float series the fill is a NaN and nothing
is lost. For integer series pick a fill your data cannot produce, in the series
definitions file. `real_points` counts the points that differ from the fill.

### Setting a database up from nothing

A database needs no configured keys to start. Where there is no write key the
server mints a one time bootstrap token and prints it:

```
bseriesd: this database has no write key yet.
  Create the first one with:

    curl -XPOST -H 'X-API-Key: 6f2c…' \
      '127.0.0.1:8086/v1/auth/keys?role=write&name=admin'

  This token works only for that, and only until a write key exists.
```

```
$ curl -XPOST -H 'X-API-Key: 6f2c…' '…/v1/auth/keys?role=write&name=admin'
{"name":"admin","role":"write","key":"bsw_7319a9bb…",
 "note":"this is the only time the key is shown; it is stored hashed"}
```

From there everything is an endpoint: mint more keys, create tables, create series,
write and read. Nothing else needs editing on disk.

The token is deliberately narrow. It is printed to the server's log, so it is
accepted **only** for creating a key — not for any data endpoint — and it stops
working the moment a write key exists. That closes the race where whoever reached a
fresh port first would own the database.

### Keys

Keys go in `X-API-Key` (`Authorization: Bearer` also works). A `read` key may call
the GET endpoints, a `write` key may call everything including key and table
management. Keys are compared in constant time.

Minted keys live in `auth.keys` in the data directory, **salted and hashed with
SHA-256, never in the clear**. The server therefore cannot show a key again after
minting it — a leaked keystore yields hashes to attack, not credentials. The file
is written 0600 through a temporary file and renamed, so an interrupted write
cannot leave a database with no way in.

Revoking the last write key is refused rather than allowed to lock the database out
of its own administration, unless a `write_key` in the configuration provides
another way in.

`read_key` and `write_key` in the configuration still work and sit alongside the
keystore, so an existing deployment's configuration stays valid.

`cors_origin` in the config allows a browser origin, and may be repeated; a single
`*` allows any. With no `cors_origin` line no CORS headers are sent, which blocks
browser callers and is the right default when the clients are server side.

There is no TLS. Bind to localhost or a trusted interface, and put a reverse proxy
in front for anything else.


## Gaps

Series are dense: every interval between the first and last point occupies space in
the file whether or not a point was recorded, so one value per type is reserved to
mean "nothing was recorded here". For floats that is a NaN and costs nothing. For
unsigned integers it is the maximum value, so a `uint8` series cannot store 255.
Signed integers have no clean sentinel. Set an explicit `null_fill` in the series
definition when the default collides with real data.


## File format

The 20 byte header records the version, the creation timestamp, the interval, a
typecode and a checksum. Three versions exist:

* **Version 1** stores a plain point width and no datatype. Written by releases
  before typed headers. On read the datatype is inferred from the width (1 byte as
  `uint8`, 4 bytes as `float32`, matching the only two types those releases
  supported).
* **Version 2** packs the datatype and the width into the same field, so the header
  stayed 20 bytes and existing files are read at unchanged offsets. Its fill comes
  from the datatype.
* **Version 3** additionally records the null fill byte, in what version 2 left
  spare. This is what makes a series file sufficient on its own. Before it, a
  custom fill lived only in the definitions file, so the same bytes meant different
  things depending on whether that file was present — a stored `02` was a gap with
  it and a reading without it. Written for every series created now.

There is no migration step and no format flag day. Version 1 and 2 files stay
readable at unchanged offsets, and the header is still 20 bytes.

The one thing this cannot fix retroactively: a version 2 series created with a
**custom** fill never recorded it, so it still needs its definition until it is
rewritten. A version 2 series using its type's default fill needs nothing.

The definitions file is therefore no longer something reads depend on. It decides
what shape to give a *new* series — worth keeping, since one line provisions ten
thousand of them — but an existing series is read correctly from its own header
alone.

The format is native endian and 32 bit timestamps cap it at 2106.
