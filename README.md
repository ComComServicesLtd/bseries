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
| GET | `/v1/series?limit=&after=` | read | list series |
| GET | `/v1/series/{key}` | read | header and size |
| POST | `/v1/series/{key}?type=&interval=&start=` | write | create |
| DELETE | `/v1/series/{key}` | write | delete |
| GET | `/v1/series/{key}/data?start=&end=` | read | read a range |
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
