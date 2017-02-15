#ifndef BSERIES_H
#define BSERIES_H



#include <map>
#include <string.h>
#include <thread>
#include <mutex>
#include <iostream>



#include "debug.h"


#define NO_ERROR 0
#define CREATE_NEW_HEADER_FAIL -1
#define HEADER_INVALID_CHECKSUM -2
#define WAL_MEMORY_ALLOCATION_FAILURE -3
#define WAL_WRITE_FAILURE -4
#define DATA_POINT_WRITE_FAILURE -5
#define INTERNAL_ERROR -6


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



typedef struct _SERIES
{
     uint32_t version; // = 1, version code. only 1 currently valid
     uint32_t timestamp; // First point timestamp (Unix Epoch)
     uint32_t interval; // = 10 for every 10 seconds
     uint32_t datasize;
     uint32_t checksum; // = 1234567890 + ((version ^ timestamp) ^ (interval ^ datatype);
} SERIES;


typedef struct ENTRY
{
     SERIES header;
     mutex access;
     int64_t file_size;
     uint32_t last_write;
     uint32_t last_commit;
     uint32_t cache_start_timestamp;
     char* write_ahead_cache;
} ENTRY;





//#define WRITE_AHEAD_SIZE 4096
//#define SECONDS_PER_POINT 10
//#define MAX_FILES 10000




class BSeries
{
public:
    BSeries();

    FILE* openFile(uint64_t key);
    bool flushBuffer(ENTRY *entry, FILE *file);


    int createSeries(FILE *file, SERIES *series, uint32_t datasize);
    uint32_t getChecksum(SERIES *series);

    int write(uint32_t key, void *value, uint32_t datasize, uint32_t timestamp = 0);
    int read(uint32_t key, int64_t start_time, int64_t end_time, int64_t *n_points, int64_t *r_points, int64_t *seconds_per_point, int64_t *first_point_timestamp, uint32_t *datasize, void **result);

    map<uint32_t,ENTRY*> series_list;
    const char *data_directory;

    int write_ahead_size;
    int default_seconds_per_point;
    char default_null_fill_byte;

    void flush();
    void close();

    void closeSeries(uint32_t max_age);
    bool trim();


    mutex index_access;


    bool shuttingDown;

    ~BSeries();
};

#endif // BSERIES_H
