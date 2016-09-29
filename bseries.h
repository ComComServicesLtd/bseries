#ifndef BSERIES_H
#define BSERIES_H



#include <map>

#include <string.h>
#include <thread>
#include <mutex>



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


union BType {
    uint32_t code;
    struct {
        uint8_t datatype; // 0 = unsigned, 1 = signed, 2 = float
        uint8_t pointsize; //
        uint8_t nc1; //
        uint8_t nc2; //
    } structure;
};



typedef struct _SERIES
{
     uint32_t version; // = 1, version code. only 1 currently valid
     uint32_t timestamp; // First point timestamp (Unix Epoch)
     uint32_t interval; // = 10 for every 10 seconds
     BType datatype;
     uint32_t checksum; // = 1234567890 + ((version ^ timestamp) ^ (interval ^ datatype);
} SERIES;


typedef struct ENTRY
{
     FILE *file;
     SERIES header;
     mutex access;
     int64_t file_size;
     uint32_t last_write;
} ENTRY;






#define WRITE_AHEAD_SIZE 4096
#define SECONDS_PER_POINT 10


#define MAX_FILES 10000


using namespace std;


class BSeries
{
public:
    BSeries();

    int createSeries(FILE *file, SERIES *series, BType datatype);
    uint32_t getChecksum(SERIES *series);

    int write(uint32_t key, void *value, BType datatype, uint32_t timestamp = 0);
    int read(uint32_t key, int64_t start_time, int64_t end_time, int64_t *n_points, int64_t *r_points, int64_t *seconds_per_point, int64_t *first_point_timestamp, BType *datatype, void **result);

    map<uint32_t,ENTRY> series_list;
    char *data_directory;

    int default_Seconds_Per_Point;

    void flush();
    void close();

    void closeSeries(uint32_t max_age);
    bool trim();



    ~BSeries();
};

#endif // BSERIES_H
