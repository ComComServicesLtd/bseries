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

| Method | Path | Key | |
|---|---|---|---|
| GET | `/v1/health` | none | liveness |
| GET | `/v1/series?keys=` or `?limit=&after=` | read | list series |
| GET | `/v1/series/{key}` | read | header and size |
| POST | `/v1/series/{key}?type=&interval=&start=` | write | create |
| DELETE | `/v1/series/{key}` | write | delete |
| GET | `/v1/series/{key}/data?start=&end=` | read | read a range |
| GET | `/v1/data?keys=&start=&end=` | read | read a range across several series |
| POST | `/v1/data` | write | write points to several series |
| POST | `/v1/now` | write | push one reading into every series, at the nearest slot |
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

### Gaps in a response

`data` is the raw series content, so the points where nothing was recorded hold the
series' null fill. Every response carries `null_fill` as a hex pattern one point
wide — compare each point against it to find the gaps. In the example above the
last two points are gaps.

The consequence is worth being explicit about: **a real reading equal to the fill
is indistinguishable from a gap.** For float series the fill is a NaN and nothing
is lost. For integer series pick a fill your data cannot produce, in the series
definitions file. `real_points` counts the points that differ from the fill.

### Authentication and CORS

Keys go in `X-API-Key` (`Authorization: Bearer` also works). `read_key` may call
the GET endpoints, `write_key` may call everything; they must differ, and an unset
key disables that level of access. Keys are compared in constant time.

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
typecode and a checksum. Two versions exist:

* **Version 1** stores a plain point width and no datatype. Written by releases
  before series definitions, and still written today for series created without a
  matching definition. On read the datatype is inferred from the width (1 byte as
  `uint8`, 4 bytes as `float32`, matching the only two types those releases
  supported); a definition for the key corrects the guess.
* **Version 2** packs the datatype and the width into the same field, so the header
  stayed 20 bytes and existing files are read at unchanged offsets. Written for any
  series created with a definition.

There is no migration step and no format flag day: version 1 files stay readable,
and a database with no definitions file keeps producing them.

The format is native endian and 32 bit timestamps cap it at 2106.
