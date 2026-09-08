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

Every data path is rooted at a table (see **Tables** below); `{t}` stands for the
table name.

| Method | Path | Key | |
|---|---|---|---|
| GET | `/v1/health` | none | liveness |
| POST | `/v1/auth/keys?role=&name=` | write | mint an API key |
| GET | `/v1/auth/keys` | write | list keys (never the secrets) |
| DELETE | `/v1/auth/keys/{name}` | write | revoke a key |
| GET | `/v1/tables` | read | list tables |
| GET | `/v1/tables/{t}` | read | table info |
| POST | `/v1/tables/{t}` | write | create a table |
| DELETE | `/v1/tables/{t}?force=` | write | drop a table |
| GET | `/v1/{t}/series?keys=` or `?limit=&after=` | read | list series |
| GET | `/v1/{t}/series/{key}` | read | header and size |
| POST | `/v1/{t}/series/{key}?type=&interval=&start=` | write | create |
| DELETE | `/v1/{t}/series/{key}` | write | delete |
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

The buffer flushes once every `write_ahead_size` points per series, and on a clean
shutdown. Straight against the library, 100000 points into one series is 24 write
syscalls — one per 4096 points.

Getting there needs `seriesShape()` rather than `seriesInfo()` on any write path.
`seriesInfo()` reads the header off disk deliberately, so probing unknown keys
cannot grow the in memory index; calling it per point cost an open, two reads, a
seek and a close for something the open series already knew.

### Durability: what an unclean shutdown loses

The other side of that: points sit in memory until their buffer fills. A `kill -9`
or a power loss discards whatever has not flushed. Demonstrated — 3000 points
written and readable, the file still 20 bytes, and nothing left after a restart.

How long points sit there depends on the series interval, because a buffer holds
`write_ahead_size` **points**, not seconds:

| interval | points held before a flush | worst case age of unflushed data |
|---|---|---|
| 1s | 4096 | 1.1 hours |
| 10s | 4096 | 11.4 hours |
| 60s | 4096 | 68 hours |
| 5m | 4096 | 342 hours |

`SIGTERM` and `SIGINT` flush everything, so a normal restart loses nothing. The
maintenance thread flushes series that have gone idle for `series_max_idle`, but an
**actively written** series is never flushed early — it is the busy series whose
data is at risk, not the quiet one.

Lower `write_ahead_size` to trade syscalls for exposure: it is points per flush, so
halving it halves both the buffered window and the points per disk write.

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
