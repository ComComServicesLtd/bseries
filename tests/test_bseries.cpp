#include "bseries.h"
#include "test_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>


int main(int argc, char**argv){
    (void)argc; (void)argv;
    const char *dir = testMakeDirectory();

    // ---- 1. float round trip through the write ahead cache (fix #6) ----
    printf("[1] float round trip through cache\n");
    {
        BSeries db; db.data_directory = dir; db.default_seconds_per_point = 1;
        uint32_t t0 = time(NULL);
        float vals[8] = {1.5f,2.5f,3.5f,4.5f,5.5f,6.5f,7.5f,8.5f};
        for(int i=0;i<8;i++) CHECK(db.write(100, &vals[i], 4, t0+i)==NO_ERROR, "write float");

        int64_t n=0, r=12345, spp=0, fpt=0; uint32_t ds=0; void *res=NULL;
        int rc = db.read(100, t0, t0+8, &n,&r,&spp,&fpt,&ds,&res);
        CHECK(rc==NO_ERROR, "read returned NO_ERROR");
        CHECK(r==8, "real_points is 8 (not caller garbage + 8)");
        CHECK(fpt==(int64_t)t0, "first_point_timestamp was filled in");
        CHECK(ds==4, "datasize reported");
        float *out = (float*)res;
        bool allgood = res && n>=8;
        for(int i=0;i<8 && allgood;i++) if(out[i]!=vals[i]){ printf("   out[%d]=%f want %f\n",i,out[i],vals[i]); allgood=false; }
        CHECK(allgood, "float values match (cache byte offset)");
        delete[] (char*)res;
        db.close();
    }

    // ---- 2. backdated write must not explode the file (fix #5) ----
    printf("[2] backdated write\n");
    {
        BSeries db; db.data_directory = dir; db.default_seconds_per_point = 1;
        unsigned char v = 42;
        uint32_t t0 = time(NULL);
        CHECK(db.write(200,&v,1,t0)==NO_ERROR, "seed write");
        int rc = db.write(200,&v,1,t0-5000);
        CHECK(rc==WRITE_BEFORE_SERIES_START, "backdated write rejected");
        db.close();
        char path[512]; snprintf(path,sizeof(path),"%s/200",dir);
        FILE*f=fopen(path,"rb"); fseek(f,0,SEEK_END); long sz=ftell(f); fclose(f);
        printf("   file size = %ld\n", sz);
        CHECK(sz < 1024*1024, "file did not balloon");
    }

    // ---- 3. failing write must not report success (fix #1) ----
    printf("[3] write failure reporting\n");
    {
        BSeries db; db.data_directory = "/nonexistent-dir-xyz"; db.default_seconds_per_point = 1;
        unsigned char v = 7;
        int rc = db.write(300,&v,1,0);
        printf("   rc = %d\n", rc);
        CHECK(rc != NO_ERROR, "unopenable file reports an error");
        db.close();
    }

    // ---- 4. closeSeries flushes instead of discarding (fixes #3 + #4) ----
    printf("[4] closeSeries flush + iteration\n");
    {
        BSeries db; db.data_directory = dir; db.default_seconds_per_point = 1;
        uint32_t t0 = time(NULL);
        unsigned char v = 99;
        // several keys so an odd/even map size would expose the double increment
        for(uint32_t k=400;k<405;k++) CHECK(db.write(k,&v,1,t0)==NO_ERROR, "write");
        // age every entry so they all qualify for eviction
        for(uint32_t k=400;k<405;k++) db.series_list[k].last_write = time(NULL) - 1000;
        db.closeSeries(10);                // evict everything older than 10s
        CHECK(db.series_list.size()==0, "closeSeries visited and erased every entry");
        // data must survive: read it back with a fresh instance
        BSeries db2; db2.data_directory = dir; db2.default_seconds_per_point = 1;
        for(uint32_t k=400;k<405;k++){
            int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; void*res=NULL;
            db2.read(k,t0,t0+2,&n,&r,&spp,&fpt,&ds,&res);
            CHECK(res && ((unsigned char*)res)[0]==99, "point survived closeSeries");
            delete[] (char*)res;
        }
        db2.close();
        db.close();
    }

    // ---- 5. degenerate read ranges must not corrupt the heap ----
    printf("[5] degenerate read ranges\n");
    {
        BSeries db; db.data_directory = dir; db.default_seconds_per_point = 1;
        uint32_t t0 = time(NULL);
        unsigned char v = 5;
        db.write(500,&v,1,t0);
        int64_t n,r,spp,fpt; uint32_t ds; void*res;
        int rc;
        n=r=spp=fpt=0; res=NULL; rc=db.read(500,t0,t0,&n,&r,&spp,&fpt,&ds,&res);
        printf("   zero width rc=%d n=%ld\n",rc,(long)n); delete[] (char*)res;
        n=r=spp=fpt=0; res=NULL; rc=db.read(500,1,t0+100000,&n,&r,&spp,&fpt,&ds,&res);
        printf("   huge range rc=%d n=%ld r=%ld\n",rc,(long)n,(long)r); delete[] (char*)res;
        n=r=spp=fpt=0; res=NULL; rc=db.read(500,t0+50,t0+10,&n,&r,&spp,&fpt,&ds,&res);
        CHECK(rc==INVALID_TIME_RANGE, "start > end rejected");
        CHECK(res==NULL, "no stale result pointer on failure");
        db.close();
    }

    // ---- 6. read on a key that has no file: must not unlock an unlocked mutex ----
    printf("[6] reads spanning the write ahead boundary\n");
    {
        // Regression: buffer_output_points subtracted one too many, so the last
        // point of the file region read back as null fill. Nothing under
        // write_ahead_size points ever touches the file, which is why smaller
        // round trips did not show it.
        BSeries db; db.data_directory = dir; db.default_seconds_per_point = 1;
        db.write_ahead_size = 64;                 // small, so the boundary comes fast
        uint32_t t0 = time(NULL);
        const int total = 64 * 5 + 7;             // several full buffers plus a tail

        for(int i = 0; i < total; i++){
            unsigned char v = (unsigned char)(i % 251);
            if(db.write(600, &v, 1, t0 + i) != NO_ERROR){ CHECK(false,"write"); break; }
        }

        int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; void *res=NULL;
        int rc = db.read(600, t0, t0 + total, &n,&r,&spp,&fpt,&ds,&res);
        CHECK(rc==NO_ERROR && res != NULL, "read back across the boundary");
        CHECK(n==total, "every requested point returned");

        int wrong = -1;
        unsigned char *out = (unsigned char*)res;
        for(int i = 0; i < total && res; i++){
            if(out[i] != (unsigned char)(i % 251)){ wrong = i; break; }
        }
        if(wrong >= 0) printf("   first wrong point: %d (got %u)\n", wrong, out[wrong]);
        CHECK(wrong < 0, "no point was dropped at a flush boundary");
        CHECK(r==total, "real_points counts them all");
        delete[] (char*)res;
        db.close();
    }

    printf("[7] point grid alignment\n");
    {
        // The grid is anchored to the series' own start, so a read whose start_time
        // falls mid slot must still label its points with their true slot times.
        BSeries db; db.data_directory = dir;
        const uint32_t T0 = 1700000000;
        db.createSeriesFile(700,60,BS_UNSIGNED,1,T0);
        for(int i=0;i<4;i++){ unsigned char v=(unsigned char)(100+i); db.write(700,&v,1,T0+i*60); }

        long offsets[] = {0, 1, 30, 59};
        for(unsigned k=0;k<4;k++){
            int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; void*res=NULL;
            int64_t s = (int64_t)T0 + offsets[k];
            db.read(700,s,s+240,&n,&r,&spp,&fpt,&ds,&res);
            char msg[128];
            snprintf(msg,sizeof(msg),"start T0+%ld reports the slot time, not the request",offsets[k]);
            CHECK(fpt == (int64_t)T0, msg);
            CHECK(res && ((unsigned char*)res)[0] == 100, "and output[0] is that slot's value");
            delete[] (char*)res;
        }

        // a request starting before the series keeps the same grid
        int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; void*res=NULL;
        db.read(700,(int64_t)T0-120,(int64_t)T0+240,&n,&r,&spp,&fpt,&ds,&res);
        CHECK(fpt == (int64_t)T0-120, "a request before the series start stays on the grid");
        CHECK(res && ((unsigned char*)res)[2] == 100, "and the series' first point lands two slots in");
        delete[] (char*)res;
        db.close();
    }

    printf("[8] read of a missing series\n");
    {
        BSeries db; db.data_directory = dir; db.default_seconds_per_point = 1;
        int64_t n,r,spp,fpt; uint32_t ds; void*res=NULL;
        n=r=spp=fpt=0;
        int rc = db.read(999999,time(NULL)-10,time(NULL),&n,&r,&spp,&fpt,&ds,&res);
        CHECK(rc==FAILED_TO_OPEN_FILE, "missing series reports FAILED_TO_OPEN_FILE");
        db.close();
    }

    printf("[8] timed flush\n");
    {
        BSeries db; db.data_directory = dir; db.default_seconds_per_point = 1;
        uint32_t t0 = 1700000000;
        db.write_ahead_size = 256;
        db.createSeriesFile(800,1,BS_UNSIGNED,1,t0);

        char path[512];
        snprintf(path,sizeof(path),"%s/800",dir);

        for(int i = 0; i < 100; i++){
            unsigned char v = (unsigned char)(i % 250);
            db.write(800,&v,1,t0+i);
        }

        FILE *f = fopen(path,"rb"); fseek(f,0,SEEK_END); long before = ftell(f); fclose(f);
        CHECK(before == (long)SERIES_HEADER_BYTES_V4, "100 points are still only in memory");

        CHECK(db.flushAged(3600) == 0, "a young buffer is not flushed");
        CHECK(db.flushAged(0) == 1, "an aged one is");
        CHECK(db.flushAged(0) == 0, "and is not flushed twice");

        f = fopen(path,"rb"); fseek(f,0,SEEK_END); long after = ftell(f); fclose(f);
        CHECK(after == (long)SERIES_HEADER_BYTES_V4 + db.write_ahead_size,
              "a partly filled block is padded out with null fill, not left short");

        // the rest of that block is already in the file, so those points are
        // written straight into it and the file does not grow
        for(int i = 100; i < db.write_ahead_size; i++){
            unsigned char v = (unsigned char)(i % 250);
            db.write(800,&v,1,t0+i);
        }

        f = fopen(path,"rb"); fseek(f,0,SEEK_END); long later = ftell(f); fclose(f);
        CHECK(later == after, "filling the rest of a padded block does not grow the file");

        // going past it starts a new block, which is buffered again
        for(int i = db.write_ahead_size; i < db.write_ahead_size + 50; i++){
            unsigned char v = (unsigned char)(i % 250);
            db.write(800,&v,1,t0+i);
        }

        f = fopen(path,"rb"); fseek(f,0,SEEK_END); long next = ftell(f); fclose(f);
        CHECK(next == later, "the next block is buffered until it is flushed");
        CHECK(db.flushAged(0) == 1, "and the timer catches it");
        f = fopen(path,"rb"); fseek(f,0,SEEK_END); long padded = ftell(f); fclose(f);
        CHECK(padded == later + db.write_ahead_size, "padded out to a whole block again");

        // and everything reads back
        int total = db.write_ahead_size + 50;
        int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; void *res=NULL;
        db.read(800,t0,t0+total,&n,&r,&spp,&fpt,&ds,&res);
        int wrong = -1;
        unsigned char *o = (unsigned char*)res;
        for(int i = 0; i < total && res; i++)
            if(o[i] != (unsigned char)(i % 250)){ wrong = i; break; }
        if(wrong >= 0) printf("   first wrong point: %d (got %u)\n", wrong, o[wrong]);
        CHECK(wrong < 0, "every point survives flushes mid stream");
        delete[] (char*)res;

        db.close();
    }

    printf("[9] a clean buffer costs nothing to flush\n");
    {
        // Flushing used to append a whole buffer of null fill whether or not
        // anything had been written, so an idle series grew on every flush.
        BSeries db; db.data_directory = dir; db.default_seconds_per_point = 1;
        uint32_t t0 = 1700000000;
        db.createSeriesFile(801,1,BS_UNSIGNED,1,t0);
        unsigned char v = 5;
        db.write(801,&v,1,t0);
        db.flushAged(0);

        char path[512];
        snprintf(path,sizeof(path),"%s/801",dir);
        FILE *f = fopen(path,"rb"); fseek(f,0,SEEK_END); long one = ftell(f); fclose(f);

        db.flushAged(0);
        db.flushAged(0);
        db.flush();

        f = fopen(path,"rb"); fseek(f,0,SEEK_END); long three = ftell(f); fclose(f);
        CHECK(one == (long)SERIES_HEADER_BYTES_V4 + db.write_ahead_size, "one point flushed writes its whole block");
        CHECK(three == one, "flushing a clean buffer again adds nothing");
        db.close();
    }

    return testReport();
}
