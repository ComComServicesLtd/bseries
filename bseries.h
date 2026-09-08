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


#define SERIES_VERSION_LEGACY 1
#define SERIES_VERSION_TYPED  2
#define SERIES_VERSION_FILLED 3


/// On disk series header. Fixed at 20 bytes: every data point is addressed as
/// sizeof(SERIES) + point * datasize, so the size of this struct is part of the
/// file format and must not change.

typedef struct _SERIES
{
     uint32_t version;   // 1 = legacy, typecode is a plain byte width
                         // 2 = typecode packs the datatype class and the byte width
                         // 3 = typecode also carries the null fill byte
     uint32_t timestamp; // First point timestamp (Unix Epoch)
     uint32_t interval;  // = 10 for every 10 seconds
     uint32_t typecode;  // see bsPackTypeCode(), was called datasize in version 1
     uint32_t checksum;  // = 1234567890 + ((version ^ timestamp) ^ (interval ^ typecode));
} SERIES;


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
} ENTRY;





//#define WRITE_AHEAD_SIZE 4096
//#define SECONDS_PER_POINT 10
//#define MAX_FILES 10000




class BSeries
{
public:
    BSeries();

    FILE* openFile(uint64_t key, bool writeMode);
    bool flushBuffer(ENTRY *entry, FILE *file);


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
    int listSeriesKeys(vector<uint32_t> *keys, uint32_t after, int limit);

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
