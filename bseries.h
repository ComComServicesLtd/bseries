#ifndef BSERIES_H
#define BSERIES_H



#include <map>
#include <vector>
#include <string.h>
#include <thread>
#include <mutex>
#include <iostream>



#include "debug.h"
#include "bseries_types.h"


#define NO_ERROR 0
#define CREATE_NEW_HEADER_FAIL -1
#define HEADER_INVALID_CHECKSUM -2
#define WAL_MEMORY_ALLOCATION_FAILURE -3
#define WAL_WRITE_FAILURE -4
#define DATA_POINT_WRITE_FAILURE -5
#define INTERNAL_ERROR -6
#define FILE_OPEN_FAILURE -7
#define WRITE_BEFORE_SERIES_START -8
#define INVALID_SERIES_INTERVAL -9
#define SERIES_TYPE_MISMATCH -10
#define INVALID_SERIES_DEFINITION -11
#define DEFINITIONS_FILE_UNREADABLE -12
#define SERIES_ALREADY_EXISTS -13
#define SERIES_NOT_FOUND -14
#define SERIES_GROWTH_LIMIT -15


#define INVALID_TIME_RANGE -1
#define FAILED_TO_READ_HEADER -2
#define INVALID_HEADER_CHECKSUM -3
#define MEMORY_ALLOCATION_FAILED -4
#define FAILED_TO_OPEN_FILE -5

#define TOO_MANY_OPEN_FILES -99




using namespace std;



union BType {
    uint32_t code;
    struct {
        uint8_t datatype; // 0 = unsigned, 1 = signed, 2 = float
        uint8_t datasize; //
        uint8_t nc1; //
        uint8_t nc2; //
    } structure;
};


#define SERIES_VERSION_LEGACY   1
#define SERIES_VERSION_TYPED    2
#define SERIES_VERSION_FILLED   3
#define SERIES_VERSION_PROFILED 4

/// Header sizes on disk. This is the data offset, so it is part of the format:
/// point n lives at bsHeaderBytes(version) + n * datasize.
#define SERIES_HEADER_BYTES_V3  20
#define SERIES_HEADER_BYTES_V4  128

/// Field widths in a version 4 header.
#define SERIES_PROFILE_BYTES    16
#define SERIES_ADDRESS_BYTES    16
#define SERIES_NAME_BYTES       48

/// flags, version 4. The address family is two bits of a word that exists
/// anyway; these are the database's own values and deliberately not the
/// platform's AF_* constants, which differ between operating systems and would
/// make a file mean different things depending on what wrote it.
#define SERIES_ADDRESS_NONE     0
#define SERIES_ADDRESS_IPV4     1
#define SERIES_ADDRESS_IPV6     2
#define SERIES_ADDRESS_MASK     0x3


/// What a remap did, for the caller to report rather than have to infer.
///
/// Deliberately counts rather than describes: what the values *mean* lives in the
/// profiles the caller built the translation from, and the database only applies
/// it. An operator confirms the file held what they thought from these numbers,
/// which matters because the rewrite cannot be undone.

typedef struct {
    int64_t points;      // points in the file
    int64_t changed;     // values the translation moved
    int64_t unchanged;   // values it left where they were
    int64_t nulls;       // fill, never touched whatever the map says
} MIGRATION_REPORT;


/// On disk series header. Fixed at 20 bytes: every data point is addressed as
/// sizeof(SERIES) + point * datasize, so the size of this struct is part of the
/// file format and must not change.

/// The header as the rest of the database sees it, whatever version wrote it.
///
/// This is no longer the on disk layout. Versions 1 to 3 are 20 bytes and version
/// 4 is 128, and both are read and written field by field by bsReadHeader() and
/// bsWriteHeader() -- so the format does not depend on how a compiler happens to
/// lay this struct out, which it did for as long as the header was fwrite'd whole.
///
/// timestamp and interval are kept in seconds because every slot calculation in
/// the database is in seconds and there are some fifty of them. Version 4 records
/// milliseconds, and the two are held in step on read: a version 1 to 3 file has
/// its seconds multiplied up, a version 4 file has its milliseconds divided down.
/// Sub-second intervals are therefore expressible in the format but refused on
/// creation until that arithmetic moves to milliseconds, rather than silently
/// truncating an interval to zero.

typedef struct _SERIES
{
     uint32_t version;   // 1 = legacy, typecode is a plain byte width
                         // 2 = typecode packs the datatype class and the byte width
                         // 3 = typecode also carries the null fill byte
                         // 4 = 128 byte header: milliseconds, profile, name, address
     uint32_t timestamp; // First point timestamp (Unix Epoch seconds)
     uint32_t interval;  // seconds between points
     uint32_t typecode;  // see bsPackTypeCode(), was called datasize in version 1
     uint32_t checksum;  // versions 1-3 only; version 4 uses a CRC32 of the header

     // Version 4. Zero or empty on an older header.
     uint64_t timestamp_ms;
     uint64_t interval_ms;
     uint32_t flags;
     unsigned char null_fill_wide[8];              // fill at the point's full width
     char profile[SERIES_PROFILE_BYTES];           // which profile reads this series
     unsigned char address[SERIES_ADDRESS_BYTES];  // what it measures, if anything
     char name[SERIES_NAME_BYTES];                 // label, for people
} SERIES;


/// Bytes the header occupies on disk for this version, which is the offset of the
/// first point. Unknown versions answer 0, so a caller that forgets to check gets
/// an obviously wrong offset rather than a plausible one.

inline uint32_t bsHeaderBytes(uint32_t version){

    if(version >= SERIES_VERSION_LEGACY && version <= SERIES_VERSION_FILLED)
        return SERIES_HEADER_BYTES_V3;

    if(version == SERIES_VERSION_PROFILED)
        return SERIES_HEADER_BYTES_V4;

    return 0;
}


/// Reads and writes a header in whichever layout its version calls for. Both
/// return false on a short or malformed header; the file position is left after
/// the header on success.
bool bsReadHeader(FILE *file, SERIES *out);
bool bsWriteHeader(FILE *file, const SERIES *header);

/// Fills in the derived fields after the on disk ones have been set, and computes
/// the checksum. Call after building a header by hand.
void bsFinaliseHeader(SERIES *header);


/// Version 2 packs the datatype class and the byte width into the single 32 bit
/// field version 1 used for the byte width alone. A version 1 width of 1, 2, 4 or 8
/// therefore reads back as a version 2 code with width 0, which is not a storable
/// type, so the two are never confused even before the version field is consulted.
///
/// Version 3 additionally records the null fill byte, in what version 2 left spare.
/// That is the last piece of a series' meaning that used to live outside the file:
/// with the fill in the header, a series file alone is enough to read the series
/// correctly, and the definitions file goes back to being what shape to give new
/// series rather than something reads depend on. A version 2 header left the spare
/// byte zero, which is a legitimate fill value, so the version has to say whether
/// the byte means anything rather than the byte speaking for itself.

inline uint32_t bsPackTypeCode(uint8_t datatype, uint8_t datasize, unsigned char null_fill = 0){
    return (uint32_t)datatype | ((uint32_t)datasize << 8) | ((uint32_t)null_fill << 16);
}

inline uint8_t bsTypeCodeDataType(uint32_t typecode){
    return (uint8_t)(typecode & 0xFF);
}

inline uint8_t bsTypeCodeDataSize(uint32_t typecode){
    return (uint8_t)((typecode >> 8) & 0xFF);
}

inline unsigned char bsTypeCodeNullFill(uint32_t typecode){
    return (unsigned char)((typecode >> 16) & 0xFF);
}


/// Decoding a header, whatever version wrote it, in one place. Every caller that
/// wants a series' width, type or fill goes through these rather than testing the
/// version itself, which is how the fill came to be resolved three slightly
/// different ways before it lived in the header at all.

inline uint32_t bsHeaderDataSize(const SERIES *header){

    if(header->version == SERIES_VERSION_LEGACY)
        return header->typecode;      // version 1 stored a plain width

    return bsTypeCodeDataSize(header->typecode);
}

inline uint8_t bsHeaderDataType(const SERIES *header){

    if(header->version != SERIES_VERSION_LEGACY)
        return bsTypeCodeDataType(header->typecode);

    // Version 1 recorded the width but not what the points meant. The only widths
    // those releases wrote were 1 (unsigned char) and 4 (float).
    return (header->typecode == 4) ? BS_FLOAT : BS_UNSIGNED;
}


/// Declares the shape of a series before it exists on disk: how often a point is
/// recorded and what each point is. Keys in [key_first,key_last] use this shape.
///
/// A definition only applies at creation time. Once a series file exists its own
/// header is the authority, because the data already on disk was laid out to it.

typedef struct
{
     uint32_t key_first;
     uint32_t key_last;
     uint32_t interval;             // seconds per point
     uint8_t  datatype;             // BS_UNSIGNED / BS_SIGNED / BS_FLOAT
     uint8_t  datasize;             // bytes per point
     unsigned char null_fill_byte;  // byte written for "no point here"
     char name[32];                 // for diagnostics only
} SERIES_DEFINITION;


typedef struct
{
     SERIES header;
     mutex access;
     int64_t file_size;
     uint32_t last_write;
     uint32_t last_commit;
     uint32_t cache_start_timestamp;
     char* write_ahead_cache;

     /// Decoded from the header by bindHeader() once it has been loaded, so the
     /// point loops do not unpack the typecode on every point. Zero until then,
     /// which is how the cache allocation knows the header is not ready yet.
     uint32_t datasize;
     uint8_t  datatype;
     unsigned char null_fill_byte;

     /// How much of the write ahead buffer has been written since the last flush,
     /// and when the first of those points arrived. Together they let a flush
     /// write only the points that exist rather than the whole buffer, and let a
     /// timed flush know how long a series has been holding data.
     ///
     /// Zero points means the buffer is clean, so nothing is written for a series
     /// that has taken no writes since its last flush.
     int64_t buffer_points;
     uint32_t buffer_dirty_since;
} ENTRY;





//#define WRITE_AHEAD_SIZE 4096
//#define SECONDS_PER_POINT 10
//#define MAX_FILES 10000




class BSeries
{
public:
    BSeries();

    FILE* openFile(uint64_t key, bool writeMode);
    /// Writes points from the front of the write ahead buffer to the end of the
    /// file. points of -1 means however many have actually been written since the
    /// last flush, which is what every caller wants: flushing the whole buffer
    /// would advance the file past slots nothing has been written to yet, and
    /// every later write into that window would then take the direct path, one
    /// open and seek per point.
    bool flushBuffer(ENTRY *entry, FILE *file, int64_t points = -1);


    int createSeries(FILE *file, SERIES *series, uint32_t key, uint32_t datasize, uint32_t start_timestamp); // NO_ERROR, or negative
    uint32_t getChecksum(SERIES *series);
    bool bindHeader(ENTRY *entry, uint32_t key);

    /// The fill byte a series' gaps are written with. A version 3 header records
    /// it, so the file decides. Older headers predate the field, and fall back to
    /// a definition for the key, then to the default for the type, then to the
    /// database wide default.
    unsigned char resolveNullFill(uint32_t key, const SERIES *header);

    /// overwrote, when supplied, is set true if the slot this write lands in
    /// already held something other than the series' null fill, meaning a real
    /// reading was replaced. Checking costs a read of one point on the direct
    /// write path, so it is only done when a caller asks for it.
    int write(uint32_t key, void *value, uint32_t datasize, uint32_t timestamp = 0, bool *overwrote = NULL);
    int read(uint32_t key, int64_t start_time, int64_t end_time, int64_t *n_points, int64_t *r_points, int64_t *seconds_per_point, int64_t *first_point_timestamp, uint32_t *datasize, void **result, uint8_t *datatype = NULL);


    /// Series definitions. Declare the interval and datatype for a key range and
    /// any series created in that range takes that shape, instead of every series
    /// in the database sharing default_seconds_per_point and the datasize that
    /// happened to be passed to the first write.
    ///
    /// Ranges are searched in the order they were declared and the first match
    /// wins, so declare narrow ranges before wide ones. Keys matching no
    /// definition fall back to default_seconds_per_point and the caller's
    /// datasize, which is what the database did before definitions existed.
    ///
    /// Definitions are meant to be installed once at startup before any read or
    /// write. They are mutex guarded so that a later reload is not a data race,
    /// but a reload cannot retype series that already exist on disk.

    int defineSeries(uint32_t key_first, uint32_t key_last, uint32_t interval, uint8_t datatype, uint8_t datasize, const char *name = NULL, int null_fill_byte = -1);
    /// table selects which of the file's tables to install. A "table <name>"
    /// line switches which table the definitions after it belong to; those before
    /// any such line belong to "default". Pass NULL to install every definition
    /// in the file regardless, which is what a single table database wants.
    int loadDefinitions(const char *path, const char *table = NULL);
    bool definitionForKey(uint32_t key, SERIES_DEFINITION *out);
    void clearDefinitions();

    vector<SERIES_DEFINITION> definitions;
    mutex definitions_access;


    map<uint32_t,ENTRY> series_list;
    const char *data_directory;

    int write_ahead_size;
    int default_seconds_per_point;
    char default_null_fill_byte;

    /// The most points one write may null fill to reach its position. Series are
    /// dense, so a point timestamped far beyond the end of a series grows the file
    /// by every interval in between; without a ceiling, one write with a bad
    /// timestamp asks for an unbounded allocation and an unbounded file. Writes
    /// that would exceed this fail with SERIES_GROWTH_LIMIT. Zero disables it.
    int64_t max_grow_points;

    /// Series lifecycle, beyond the implicit "created by the first write" path.
    ///
    /// createSeriesFile() lays down a header for a series that does not exist yet
    /// with an explicit shape, so a caller can declare a series without having a
    /// point to write. deleteSeries() drops the series from memory without
    /// flushing it and unlinks its file. seriesInfo() reads a header straight from
    /// disk without touching series_list, so probing a key that does not exist
    /// cannot grow the in memory index.

    /// start_timestamp is the moment the series' first point sits at; pass 0 for
    /// now. Setting it in the past is how a series is prepared for a backfill,
    /// since a write before the series start is refused.
    int createSeriesFile(uint32_t key, uint32_t interval, uint8_t datatype, uint8_t datasize, uint32_t start_timestamp = 0);
    int deleteSeries(uint32_t key);
    int seriesInfo(uint32_t key, SERIES *header, int64_t *file_size);

    /// The header of a series that is already open, without touching the disk.
    /// Falls back to reading the file only when the series is not in memory, which
    /// on a write path means the first point after a cold start or an eviction.
    ///
    /// Use this rather than seriesInfo() wherever a write needs to know a series'
    /// shape: seriesInfo() opens, reads, seeks and closes every time it is called,
    /// which on a one point per series ingest is several syscalls per point for
    /// information the entry is already holding.
    bool seriesShape(uint32_t key, SERIES *header_out);

    /// Points a series is holding that its file does not have yet.
    ///
    /// A read already merges these over the file, so they are not "pending" in any
    /// sense a caller can observe by reading. What they are is missing from
    /// file_size, and so from the points_in_file derived from it, which is why a
    /// series can hold hours of data and still report nothing on disk.
    ///
    /// Zero for a series that is not open, since a series with nothing in memory
    /// has nothing buffered. Probing an unopened key must not grow the index, the
    /// same reason seriesInfo() reads the header straight from disk.
    int64_t bufferedPoints(uint32_t key);

    /// Rewrites a legacy uint8 series as version 3, remapping the values a prober
    /// reserved by convention into the range this database reserves by rule.
    ///
    /// A version 1 header records a width and no datatype, so a 1 byte series reads
    /// back as uint8 with 255 for the fill. That leaves a prober's own sentinel --
    /// 1 for "no reply" -- sitting in the middle of the real readings, where it is
    /// the smallest value rather than the worst one: it drags an average down,
    /// never shows up in a maximum, and pins a minimum to itself forever.
    ///
    /// This moves that sentinel to 254, immediately below the fill, so it sorts
    /// where it belongs. 245 to 254 are pulled down to 244 first, both to clear the
    /// target and to leave the top of the range meaning something rather than
    /// nothing. 255 is left alone: it is the fill, and a slot that holds it is one
    /// nothing was ever written to.
    ///
    /// The two steps are one pass in the order given, so running it twice would
    /// clamp the sentinels the first run wrote. The version field is the guard:
    /// only a version 1 file is accepted, and the header is stamped last.
    ///
    /// Not reversible -- readings at 245 to 254 are lost, and a genuine 1 becomes a
    /// no reply. The series is flushed and evicted first, then written to a
    /// temporary file and renamed over the original, so an interrupted run leaves
    /// the original exactly as it was. It needs the file's size again in free
    /// space, and holds the index lock throughout, so the database is quiet while
    /// it runs.
    /// Rewrites a one byte unsigned series through a 256 entry translation, and
    /// stamps a version 3 header while it is there.
    ///
    /// The translation is built by the caller from a source and a target profile,
    /// because what a value means is not something the database knows: this only
    /// applies the mapping and reports what it did. map256[v] is the value to
    /// store where v was stored; an entry equal to its own index is a value left
    /// alone. The null fill is never touched, whatever the map says.
    ///
    /// dry_run counts without writing, which matters because this cannot be
    /// undone and the counts are how an operator confirms the file held what they
    /// thought it did.
    ///
    /// The series is flushed and evicted first, then written to a temporary file
    /// and renamed over the original, so an interrupted run leaves the original
    /// exactly as it was. It needs the file's size again in free space, and holds
    /// the index lock throughout.
    int remapUint8(uint32_t key, const unsigned char *map256, const char *profile,
                   MIGRATION_REPORT *report, bool dry_run);

    /// Names the profile a series is read with, in its own header.
    ///
    /// Only a version 4 header has anywhere to put it. Rewrites the header in
    /// place -- 128 bytes, no data moved -- and refuses a name that would not
    /// survive being turned back into a path.
    int setSeriesProfile(uint32_t key, const char *profile);
    int listSeriesKeys(vector<uint32_t> *keys, uint32_t after, int limit);

    /// Flushes every series whose buffer has been holding points for at least
    /// max_age seconds, whether or not it is still being written to. The write
    /// ahead buffer holds a number of points rather than a span of time, so
    /// without this an actively written series can hold hours of data in memory.
    /// Returns how many series were flushed.
    int flushAged(uint32_t max_age);

    void flush();
    void close();

    void closeSeries(uint32_t max_age);
    bool trim();


    mutex index_access;


    bool shuttingDown;

    ~BSeries();
    bool validateWriteAheadCache(ENTRY *series);
};

#endif // BSERIES_H
