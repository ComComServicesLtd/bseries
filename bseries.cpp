#include "bseries.h"
#include "debug.h"

#include <new>




BSeries::BSeries()
{

    this->default_seconds_per_point = 10;
    this->default_null_fill_byte = 0xFF;
    this->write_ahead_size = 4096;

    this->shuttingDown = false;

}



uint32_t BSeries::getChecksum(SERIES *series){
    return 1234567890 + ((series->version ^ series->timestamp) ^ (series->interval ^ series->datasize));
}


int BSeries::createSeries(FILE *file, SERIES *series, uint32_t datasize){

    series->version = 1;
    series->timestamp = time(NULL);
    series->interval = default_seconds_per_point;
    series->datasize = datasize;
    series->checksum = getChecksum(series);

    fseek(file,0,SEEK_SET);
    int64_t size = fwrite((char*)series,sizeof(SERIES),1,file);


    if(size == 1)
        return 1;

    return 0;
}






//closes series that havent been written to in max_age (seconds)
//
// Entries currently in use are skipped: series_list hands out raw ENTRY pointers,
// so an entry whose access mutex is held may still be in the middle of a write and
// erasing it would leave that thread with a dangling pointer.

void BSeries::closeSeries(uint32_t max_age){

    uint32_t current_timestamp = time(NULL);

    index_access.lock();

    auto it = series_list.begin();
    while(it != series_list.end()){

        // Never evict an entry another thread is working on. This also has to come
        // before the age test: last_write is published under the entry lock, so
        // reading it here without that lock is a data race.
        if(!it->second.access.try_lock()){
            it++;
            continue;
        }

        int64_t age = (int64_t)current_timestamp - (int64_t)it->second.last_write;

        if(age <= (int64_t)max_age && it->second.last_write){
            it->second.access.unlock();
            it++;
            continue;
        }

        // Flush whatever is still sitting in the write ahead cache before dropping
        // it, otherwise closing a series silently discards up to write_ahead_size
        // points. last_write is only non-zero once a write has actually landed, so
        // entries that were only ever read have nothing to flush.
        if(it->second.write_ahead_cache != NULL){

            if(it->second.last_write){
                FILE *file = openFile(it->first,true);
                if(file != NULL){
                    flushBuffer(&it->second,file);
                    fclose(file);
                } else {
                    _ERROR("\t Failed to open %u to flush before closing\n",it->first);
                }
            }

            free(it->second.write_ahead_cache);
            it->second.write_ahead_cache = NULL;
        }

        it->second.access.unlock();

        it = series_list.erase(it);
    }

    index_access.unlock();
}




bool BSeries::trim(){
    // flush and close old series;
    return false; // not implemented yet, falling off the end here was undefined
}



FILE* BSeries::openFile(uint64_t key, bool writeMode){



    char filename[256];
    _DEBUG("\tOpening file %lu\n",(unsigned long)key);

    if(snprintf(filename,sizeof(filename),"%s/%lu",data_directory,(unsigned long)key) >= (int)sizeof(filename)){
        _ERROR("\t Path for key %lu does not fit in the filename buffer\n",(unsigned long)key);
        return NULL;
    }


    FILE *file = fopen(filename,"r+b");

    if(file == NULL && writeMode){ // Try Again, append binary
        file = fopen(filename,"ab");
    }


    return file;

}



bool BSeries::flushBuffer(ENTRY *series,FILE *file){

    if(file == NULL || series->write_ahead_cache == NULL)
        return false;


    _DEBUG("\tSeeking to end of file\n");
    // Seek to end of file
    fseek(file,0,SEEK_END);

    _DEBUG("\tFlushing current buffer\n");
    // Write are buffer to the file
    size_t size = fwrite(series->write_ahead_cache,series->header.datasize,write_ahead_size,file); // Write the data point

    if(size != write_ahead_size){
        _ERROR("\t Failed to flush write ahead buffer to file");
        return false;
    }
    series->file_size += write_ahead_size * series->header.datasize; // Our file has grown!
    _DEBUG("\tNew File Size = %d\n",series->file_size);


    // Reset our buffer with null fill
    memset(series->write_ahead_cache,default_null_fill_byte,write_ahead_size * series->header.datasize);

    return true;
}




bool BSeries::validateWriteAheadCache(ENTRY *series){

    if(series->header.datasize == 0){
        // The header has not been loaded yet. Allocating here would produce a zero
        // sized cache that is never resized, because the NULL check below would then
        // consider it valid forever.
        _ERROR("Cache requested before the header was loaded\n");
        return false;
    }

    // If our write ahead cache is NULL, malloc it and set it to our null fill
    if(series->write_ahead_cache == NULL){
        series->write_ahead_cache = (char*)malloc(write_ahead_size * series->header.datasize);

        if(series->write_ahead_cache == NULL){
            _ERROR("Cache Malloc Failed, Fatal!\n");
            return false;
        }
        _DEBUG("Cache Malloc Success\n");

        memset(series->write_ahead_cache,default_null_fill_byte,write_ahead_size * series->header.datasize);
    }

    return true;

}


/// This function takes a filename and value
/// it reads the header of the file and determins where the point needs to be written based on the current timestamp
/// if the files is missing a new file is created
/// if the headers checksum is bad the function returns
/// if the file dose not containe room to write our point write_ahead_size is automatically appended to it full of null points


/// Errors
/// -1 Could not write new header to file
/// -2 Header has invalid checksum
/// -3 Could not allocate memory for Write Ahead Log
/// -4 Could not append null write ahead data
/// -5 Could not write data point to file
/// -6 Invalid Write Position Detected, Write Failed for an unknown reason, INTERNAL ERROR




/// Any modifications to the map (New entries or deleted entries have to lock)

int BSeries::write(uint32_t key, void *value,uint32_t datasize, uint32_t timestamp){

    if(shuttingDown) // We lock mutexes after the database is shutdown, some writes could hang here trying to lock the mutex so we force return
        return -1;


    _DEBUG("Looking Up Key: %d\n",key);

    FILE *file = NULL;


    ENTRY *series;

    // The entry lock is taken while index_access is still held. series_list hands
    // out raw ENTRY pointers, so closeSeries() needs the entry mutex to tell that
    // an entry is in use; releasing index_access before locking leaves a window
    // where it could erase the entry and hand us a dangling pointer.
    index_access.lock();
    series = &series_list[key];
    series->access.lock();
    index_access.unlock();


    int status = NO_ERROR;

    if(!timestamp)
        timestamp = time(NULL);

    do {

        int size;

        _DEBUG("Writing to series %u\n",key);




        // Step 1, check if we have a valid header, if not read header from file
        // Step 2, check if our write is within the cached buffer or not



        // Check if we have a falid header, if not open the file and check for a valid header, if still no valid header create one

        if(series->header.checksum != getChecksum(&series->header)){// cached checksum header is invalid, read it from the file
            _DEBUG("\t INVALID CACHED HEADER, READING HEADER FROM FILE\n");


            file = openFile(key,true);

            if(file == NULL){
                _ERROR("\t Failed to open or create file");
                status = FILE_OPEN_FAILURE;
                break;
            }

            // read our header
            size = fread((char*)&series->header,sizeof(series->header),1,file);


            // If the header not read correctily, create the series
            if(size != 1){
                /// Create header and continue write
                if(!createSeries(file,&series->header,datasize)){ // attempt to create header, if failure, return error
                    fclose(file);
                    file = NULL; // the exit path below closes file, don't close it twice
                    _ERROR("\t Failed to create new File/header\n");
                    status = CREATE_NEW_HEADER_FAIL;
                    break;
                }
                _DEBUG("\t Created New File\n");
            }



            // check the checksum once more, if incorrect, close the file
            if(series->header.checksum != getChecksum(&series->header)){
                fclose(file);
                file = NULL; // the exit path below closes file, don't close it twice
                _ERROR("\t HEADER_INVALID_CHECKSUM\n");
                status = HEADER_INVALID_CHECKSUM;
                break;
            }


            // Get our file size, we get the file size whenver we open a new file, when we update a file we also update the filesize
            fseek(file,0,SEEK_END);
            series->file_size = ftell(file);
            _DEBUG("\tFile size = %u\n",series->file_size);
        }


        // Allocate write ahead cache if needed
        if(!validateWriteAheadCache(series)){

            _ERROR("\t WAL_MEMORY_ALLOCATION_FAILURE\n");
            status = WAL_MEMORY_ALLOCATION_FAILURE;
            break;
        }



        /// If we reach this point, we have a valid header and our pointer is on the first data point in the series

        if(series->header.interval == 0){ // the point calculation below divides by this
            _ERROR("\t INVALID_SERIES_INTERVAL\n");
            status = INVALID_SERIES_INTERVAL;
            break;
        }

        if(timestamp < series->header.timestamp){
            // Both fields are uint32_t. An unsigned subtraction here wraps to about
            // four billion, and the null fill below would then try to grow the file
            // by gigabytes to reach that point.
            _ERROR("\t WRITE_BEFORE_SERIES_START\n");
            status = WRITE_BEFORE_SERIES_START;
            break;
        }

retry:

        int64_t point = ((int64_t)timestamp - (int64_t)series->header.timestamp)/(int64_t)series->header.interval;
        // Point since start of file

        int64_t file_pos = point * series->header.datasize + sizeof(SERIES);


        if(file_pos >= series->file_size){ // Cached Write

            _DEBUG("\t ===== Performing Cached Write =======\n");

            // get total points in series
            int64_t pointsInBuffer = point - ((series->file_size - sizeof(SERIES)) / series->header.datasize);
            //a int64_t pointsInBuffer = pointsInSeries - point;

            _DEBUG("\t Absolute point in series: %d,  buffer pos: %d, buffer size: %d\n",point,pointsInBuffer,write_ahead_size);


            // Check if this is a cache write or memory write
            if(pointsInBuffer < write_ahead_size){
                // Write to buffer
                memcpy(series->write_ahead_cache + (pointsInBuffer * series->header.datasize),value,series->header.datasize);
                _DEBUG("\t Writing to buffer at pos: %d\n",pointsInBuffer);

                if(pointsInBuffer == (write_ahead_size-1)){ // If we've reached the end of our buffer, flush it.
                    // flush write ahead to file
                    _DEBUG("\t Buffer is full, flushing to disk\n");

                    // If file not already open, open it
                    if(file == NULL)
                        file = openFile(key,true);

                    if(file == NULL){
                        _ERROR("\t Failed to open or create file\n");
                        status = FILE_OPEN_FAILURE;
                        break;
                    }

                    if(!flushBuffer(series,file)){
                        _ERROR("\t WAL_WRITE_FAILURE, failed to flush buffer\n");
                        status = WAL_WRITE_FAILURE;
                        break;
                    }
                }

            } else {
                _WARN("\t Warning: Write position on series %u exceeds write ahead size, null filling\n",key);


                // If file not already open, open it
                if(file == NULL)
                    file = openFile(key,true);

                // Check if the file opened correctily
                if(file == NULL){
                    _ERROR("\t Failed to open or create file\n");
                    status = FILE_OPEN_FAILURE;
                    break;
                }



                // First flush our current buffer to disk because it could contain some valid points
                if(!flushBuffer(series,file)){
                    _ERROR("\t WAL_WRITE_FAILURE, failed to flush buffer\n");
                    status = WAL_WRITE_FAILURE;
                    break;
                }



                // How many null points do we need to insert?... Measure from end of write ahead pos to where we want to be
                int64_t grow_by = (pointsInBuffer - write_ahead_size); // Calculate how many bytes we are going to grow the file, we want write_ahead_size beyond the current requested write position
                _DEBUG("\t Adding %u null points\n",grow_by);


                // Create a temporary buffer to hold the data
                char *nullFill = (char*)malloc(grow_by * series->header.datasize); // Allocate a temporary memory buffer to contain the null data points that we will write
                if(nullFill == NULL){ // Could not allocate memory
                    _ERROR("\t WAL_MEMORY_ALLOCATION_FAILURE\n");
                    status = WAL_MEMORY_ALLOCATION_FAILURE;
                    break;
                }

                // Set the buffer to our null fill
                memset(nullFill,0xFF,grow_by * series->header.datasize); // Set to the null fill, for char a value of '0' is used, for float a value of 'FFFFFFFF' is used which represents 'Nan'

                _DEBUG("\Writing null fill to end of file\n");
                // Write it to disk
                if(fwrite(nullFill,series->header.datasize,grow_by,file) != (size_t)grow_by){ // Write the null points
                    free(nullFill);
                    _ERROR("\t WAL_WRITE_FAILURE\n");
                    status = WAL_WRITE_FAILURE;
                    break;
                }
                _DEBUG("\t freeing null buffer\n");
                free(nullFill);

                series->file_size += grow_by * series->header.datasize; // Our new filesize
                _DEBUG("\tNew File Size = %d\n",series->file_size);



                _DEBUG("\tRETRYING");

                goto retry;

            }



        } else {
            _DEBUG("\t ===== Performing Direct Write =======\n");
            // Direct Write

            if(file == NULL){
                file = openFile(key,true);
                if(file == NULL){
                    _ERROR("\tFailed to open file for direct writing\n");
                    status = FILE_OPEN_FAILURE;
                    break;
                }
            }

            _DEBUG("\t seeking to current writing position: %d\n",file_pos);
            fseek(file,file_pos,SEEK_SET); // Seek to the current writing position, this is based on the timestamp and interval
            _DEBUG("\t Datapoint Size = %d\n",series->header.datasize);
            size = fwrite(value,series->header.datasize,1,file); // Write the data point
            _DEBUG("\t Wrote %d points @ %d\n",size,file_pos);

            if(size != 1){ // Check that write completed with the correct number of bytes written
                fclose(file);
                file = NULL; // the exit path below closes file, don't close it twice
                _ERROR("\t Failed to direct write data point\n");
                status = DATA_POINT_WRITE_FAILURE;
                break;
            }
            _DEBUG("\t Success\n");
        }


        // Stamped for every successful write, cached or direct. closeSeries() evicts
        // on last_write, so leaving it unset on the cached path (the common one) made
        // it evict exactly the series that were busiest.
        series->last_write = time(NULL);

        status = NO_ERROR; // Return successful, don't close file
        break;

    } while (true);

    _DEBUG("\t Unlocking Series mutex\n");


    if(file != NULL)
        fclose(file);

    series->access.unlock();


    _DEBUG("\t Done\n");
    return status;


}


/// Read points from a series data file, if the end_time is not specified the current time is used
/// Returns the number of points read if successful and a result array with the total points requested as if all of the data points were present
///
/// On failure returns the following errors
/// -1 Invalid Time Range
/// -2 Could not read complete header
/// -3 Header Checksum Error
/// -4 Memory Allocation Failed
/// -5 Could not open file
///
///
/// NOTE: The returned result array is allocated from the stack and must be freed by the program using the function
///
/// n_points is the number of points in the output array
/// r_points is the number of real points in the output array (Points found in the time series)



int BSeries::read(uint32_t key, int64_t start_time, int64_t end_time, int64_t *n_points, int64_t *real_points, int64_t *seconds_per_point, int64_t *first_point_timestamp, uint32_t *datasize, void** result)
{ // Returns number of points if successful or -1 if error


    if(shuttingDown)
        return -1;

    _DEBUG("Reading from series %u where time > %lu and time < %lu\n",key,start_time,end_time);


    // Every out parameter is filled in or accumulated into below; callers cannot be
    // expected to pre-zero them, and on a failed read they must not be left holding
    // a stale result pointer.
    *n_points = 0;
    *real_points = 0;
    *seconds_per_point = 0;
    *first_point_timestamp = 0;
    *result = NULL;

    int status = NO_ERROR;
    FILE *file = NULL;
    ENTRY *series = NULL;





    do {


        _DEBUG("Looking Up Key: %d\n",key);

        // Same ordering as write(): take the entry lock while index_access is still
        // held, both so closeSeries() cannot erase the entry underneath us and so
        // that the exit path below always has a lock to release.
        index_access.lock();
        series = &series_list[key];
        series->access.lock();
        index_access.unlock();


        if(end_time <= 0)
            end_time = time(NULL);

        if(start_time > end_time){
            _ERROR("\t INVALID_TIME_RANGE,  Start time > end time\n");
            status = INVALID_TIME_RANGE;
            break;
        }


        if(!start_time){
            _ERROR("\t INVALID_TIME_RANGE,  Invliad start time\n");
            status = INVALID_TIME_RANGE;
            break;
        }






        // Open our file

        file = openFile(key,false);
        if(file == NULL){
            _ERROR("\t Failed to open file");
            status = FAILED_TO_OPEN_FILE;
            break;
        }




        // Check if our cached header is valid, if not, read it from file
        if(series->header.checksum != getChecksum(&series->header)){// cached checksum header is invalid, read it from the file
            _DEBUG("\t INVALID CACHED HEADER, READING HEADER FROM FILE\n");



            fseek(file,0,SEEK_SET); // Seek begining
            int size = fread((char*)&series->header,sizeof(series->header),1,file);

            if(size != 1){
                _ERROR("\t FAILED_TO_READ_HEADER\n");
                status = FAILED_TO_READ_HEADER;
                break;
            }

            _DEBUG("%u\n\tVersion: %lu\n\tTimestamp: %lu\n\tInverval: %lu\n\tDatasize: %lu\n\tChecksum: %lu\n\n   ",key,series->header.version,series->header.timestamp,series->header.interval,series->header.datasize,series->header.checksum);

            if(series->header.checksum != getChecksum(&series->header)){
                _ERROR("\t INVALID_HEADER_CHECKSUM\n");
                status = INVALID_HEADER_CHECKSUM;
                break;
            }

            // Get our file size, we get the file size whenver we open a new file, when we update a file we also update the filesizeNTRY *
            fseek(file,0,SEEK_END);
            series->file_size = ftell(file);
            _DEBUG("\tFile size = %u\n",series->file_size);

        }


        /// If we reach this point, we have a valid header and our pointer is on the first data point in the series


        /// Calculate the position in the file (Byte) where we are going to write the data point,
        /// this is based on the start timestampfor the series and the number of seconds between points,
        /// the position is also offseted to account for the header size
        if(series->header.interval == 0 || series->header.datasize == 0){ // both divide below
            _ERROR("\t INVALID_SERIES_INTERVAL\n");
            status = INVALID_SERIES_INTERVAL;
            break;
        }

        // Only now that the header is loaded is datasize known. Allocating the cache
        // any earlier sizes it as write_ahead_size * 0, and malloc(0) hands back a
        // non NULL pointer that this function then never grows.
        if(!validateWriteAheadCache(series)){
            _ERROR("\t WAL_MEMORY_ALLOCATION_FAILURE\n");
            status = WAL_MEMORY_ALLOCATION_FAILURE;
            break;
        }

        int64_t points = ( end_time - start_time) / series->header.interval;
        int64_t points_in_file = (series->file_size - (int64_t)sizeof(SERIES))/series->header.datasize;
        *seconds_per_point = series->header.interval;

        // output[0] corresponds to start_time: the mapping below places every file
        // and cache point at (its timestamp - start_time) / interval.
        *first_point_timestamp = start_time;

        char *output = new (std::nothrow) char[points*series->header.datasize];
        if(output == NULL){
            _ERROR("\t MEMORY_ALLOCATION_FAILED\n");
            status = MEMORY_ALLOCATION_FAILED;
            break;
        }
        memset(output,this->default_null_fill_byte,points*series->header.datasize); // Set to the null fill, for char a value of '0' is used, for float a value of 'FFFFFFFF' is used which represents 'Nan'


        {


            /// Map file to our output buffer


            int64_t file_start_point = 0;
            int64_t file_end_point = 0;
            int64_t buffer_output_pos = 0;
            int64_t buffer_output_points = 0;

            if(start_time <= series->header.timestamp){ // Have we requested data extending before our first file data point?
                file_start_point = 0;// ((start_time - series->header.timestamp)/series->header.interval);
            } else {
                file_start_point = (start_time - series->header.timestamp)/series->header.interval;
            }




            file_end_point = (end_time - series->header.timestamp) / series->header.interval;
            if(file_end_point > points_in_file){
                file_end_point = points_in_file;
            }

            buffer_output_points = file_end_point - file_start_point - 1;
            // THE FILE READ COMMAND WAS COPYING TOO MANY BYTES...

            int64_t  buffer_output_timestamp = series->header.timestamp + (file_start_point * series->header.interval);
            buffer_output_pos = (buffer_output_timestamp - start_time) / series->header.interval;



            /*
             *
         =MAPPING FILE=
        points_in_file: 31321
        file_start_point: 31967
        file_end_point: 31321
        buffer_output_pos: 0
        buffer_output_points: -646
        buffer_output_timestamp: 1486069751

*/

            _DEBUG("=MAPPING FILE=\n");
            _DEBUG("\tpoints_in_file: %d\n",points_in_file);
            _DEBUG("\tfile_start_point: %d\n",file_start_point);
            _DEBUG("\tfile_end_point: %d\n",file_end_point);
            _DEBUG("\tbuffer_output_pos: %d\n",buffer_output_pos);
            _DEBUG("\tbuffer_output_points: %d\n",buffer_output_points);
            _DEBUG("\tbuffer_output_timestamp: %d\n",buffer_output_timestamp);

            // Clamp to the output buffer. Without this a wide enough time range
            // freads past the end of output.
            if(buffer_output_pos < 0)
                buffer_output_pos = 0;
            if(buffer_output_pos + buffer_output_points > points)
                buffer_output_points = points - buffer_output_pos;

            if(buffer_output_points > 0 && file_start_point >= 0 && file_end_point >= 0){


                fseek(file,((file_start_point*series->header.datasize)+sizeof(SERIES)),SEEK_SET); // Read Points


                int64_t file_points = fread(output+(buffer_output_pos*series->header.datasize),series->header.datasize,buffer_output_points,file);

             //   int64_t file_points = 0;
                *real_points += file_points;
            }

        }



        {
            /// Map cache to our output buffer

            int64_t points_in_cache = this->write_ahead_size;
            int64_t cache_start_point = 0;
            int64_t cache_end_point = 0;
            int64_t buffer_output_start_pos = 0;
            int64_t buffer_output_end_pos = 0;
            int64_t buffer_output_points = 0;





            int64_t cache_start_time = (series->header.timestamp + (points_in_file * series->header.interval));




            if(start_time <= cache_start_time){ // Have we requested data extending before our first file data point?
                cache_start_point = 0;
            } else {
                cache_start_point = ((start_time - cache_start_time)/series->header.interval);
            }

            // cache start point is first point in the cache that we will copy to our buffer
            // if data extends before the cache to include file points this number will always be 0
            // otherwise it will be between 0 to $cache_length

            cache_end_point = (end_time - cache_start_time) / series->header.interval;

            if(cache_end_point < cache_start_point){
                cache_end_point = cache_start_point;
            } // make sure the end point cannot be greater then the start point

            if(cache_end_point > points_in_cache){
                cache_end_point = points_in_cache;
            }

            // cache end point is the last point in the cache that is within the requested time range
            // it will be between cache_start_point to $cachelength


            int64_t buffer_output_timestamp = cache_start_time + (cache_start_point * series->header.interval);
            // buffer_output_timestamp is the timestamp that the cache data starts at

            buffer_output_start_pos = (buffer_output_timestamp - start_time) / series->header.interval;
            // buffer output start pos is the position in the output buffer where we will copy our data too

            buffer_output_points = cache_end_point - cache_start_point;
            // number of points that lie in our cache (That fit within our requested range)

            buffer_output_end_pos = buffer_output_start_pos + buffer_output_points;
            // end point is start point plus numver of points


            if(buffer_output_end_pos > points){ // make sure the end pos does exceed the number of points in the external buffer
                buffer_output_end_pos = points;
            }


            buffer_output_points = buffer_output_end_pos - buffer_output_start_pos;
            // number of points to copy into the buffer





            _DEBUG("=MAPPING CACHE=\n");
            _DEBUG("\tfile_start_point: %d\n",series->header.timestamp);
            _DEBUG("\tpoints_in_cache: %d\n",points_in_cache);
            _DEBUG("\tcache_start_point: %d\n",cache_start_point);
            _DEBUG("\tcache_end_point: %d\n",cache_end_point);
            _DEBUG("\tbuffer_output_start_pos: %d\n",buffer_output_start_pos);
            _DEBUG("\tbuffer_output_end_pos: %d\n",buffer_output_end_pos);
            _DEBUG("\tbuffer_output_points: %d\n",buffer_output_points);
            _DEBUG("\tcache_start_time: %d\n",cache_start_time);

            if(buffer_output_points > 0 && series->write_ahead_cache != NULL && cache_start_point >= 0 && cache_end_point >= 0 && buffer_output_start_pos >= 0){
                // cache_start_point counts points, the cache is addressed in bytes.
                memcpy(output+(buffer_output_start_pos*series->header.datasize),series->write_ahead_cache + (cache_start_point*series->header.datasize),buffer_output_points*series->header.datasize);
                *real_points += buffer_output_points;
            }


        }


        *result = output;
        *n_points = points;
        *datasize = series->header.datasize;

        status = NO_ERROR;

    } while (false);



    if(file)
        fclose(file);


    if(series)
        series->access.unlock();

    return status;
}






void BSeries::flush()
{

    cout << "Flushing Database" << endl;

    _DEBUG("Flushing All Series..\n");
    this->index_access.lock();
    // Close all open files
    auto it = series_list.begin();
    while(it != series_list.end()){


            it->second.access.lock(); // this will wait for any current writes to complete

            FILE *file = openFile(it->first,true);
            if(file != NULL){
                _DEBUG("Flushing: %u\n",it->first);
                this->flushBuffer(&it->second,file);
                fclose(file);
            }
            it->second.access.unlock();
        it++;
    }
    this->index_access.unlock();
    cout << "\t Done Flushing Database" << endl;
}



void BSeries::close()
{

    cout << "Closing Database" << endl;

    this->shuttingDown = true;

    this->flush();


    this->index_access.lock();
    // Close all open files
    auto it = series_list.begin();
    while(it != series_list.end()){

            _DEBUG("Closing: %u\n",it->first);
            it->second.access.lock(); // Ensure nobody is accessing our resource
            if(it->second.write_ahead_cache != NULL){
                free(it->second.write_ahead_cache);
                it->second.write_ahead_cache = NULL; // don't leave a freed pointer behind
            }
            it->second.access.unlock(); // destroying a locked std::mutex is undefined

        it++;
    }
    this->index_access.unlock();

    cout << "\t Done Closing Database" << endl;
}





BSeries::~BSeries()
{
    if(!this->shuttingDown){
        _ERROR("CRITICAL!! INVALID USAGE! BSERIES SHOULD BE CLOSED BEFORE BEING DECONSTRUCTED!");
        this->close();
    }

}
