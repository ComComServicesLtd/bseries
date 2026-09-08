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
