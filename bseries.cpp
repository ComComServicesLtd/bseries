#include "bseries.h"
#include "debug.h"




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

void BSeries::closeSeries(uint32_t max_age){

    int64_t age;
    int64_t age_counts;

    uint32_t current_timestamp = time(NULL);

    auto it = series_list.begin();
    while(it != series_list.end()){


        if(it->second != NULL){



            age = current_timestamp - it->second->last_write;
            if(age > max_age ||  !it->second->last_write){
                //if(it->second->file != NULL){
                //    fclose(it->second->file);
                //    delete it->second;
                // }

                // TODO, Flush buffer to disk, clear mutexes
                it = series_list.erase(it);
            } else {
                it++;
            }


            it++;




        } else { // invalid entry, erase it
            it = series_list.erase(it);
        }



    }
}




bool BSeries::trim(){
    // flush and close old series;
}



FILE* BSeries::openFile(uint64_t key){



    char filename[256];
    _DEBUG("\tOpening file %u\n",key);
    sprintf(filename,"%s/%u",data_directory,key);


    FILE *file = fopen(filename,"r+b");

    if(file == NULL){ // Try Again, append binary
        file = fopen(filename,"ab");
    }


    return file;

}



bool BSeries::flushBuffer(ENTRY *series,FILE *file){

    if(file == NULL)
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


    index_access.lock();
    ENTRY *series = series_list[key];
    if(series == NULL){
        _DEBUG("No Entry Found, Creating new one\n");
        series = (ENTRY*)malloc(sizeof(ENTRY));
        memset(series,0,sizeof(ENTRY));
        if(series == NULL){
            _ERROR("Series Malloc Failed, Fatal!\n");
            index_access.unlock();
            return -99999;
        }
        _DEBUG("Series Malloc Success\n");

        series_list[key] = series;

    }
    index_access.unlock();


    uint32_t status = NO_ERROR;

    if(!timestamp)
        timestamp = time(NULL);

    do {
        series->access.lock();

        int size;

        _DEBUG("Writing to series %u\n",key);




        // Step 1, check if we have a valid header, if not read header from file
        // Step 2, check if our write is within the cached buffer or not



        // Check if we have a falid header, if not open the file and check for a valid header, if still no valid header create one

        if(series->header.checksum != getChecksum(&series->header)){// cached checksum header is invalid, read it from the file
            _DEBUG("\t INVALID CACHED HEADER, READING HEADER FROM FILE\n");


            file = openFile(key);

            if(file == NULL){
                _ERROR("\t Failed to open or create file");
                break;
            }

            // read our header
            size = fread((char*)&series->header,sizeof(series->header),1,file);


            // If the header not read correctily, create the series
            if(size != 1){
                /// Create header and continue write
                if(!createSeries(file,&series->header,datasize)){ // attempt to create header, if failure, return error
                    fclose(file);
                    _ERROR("\t Failed to create new File/header\n");
                    status = CREATE_NEW_HEADER_FAIL;
                    break;
                }
                _DEBUG("\t Created New File\n");
            }



            // check the checksum once more, if incorrect, close the file
            if(series->header.checksum != getChecksum(&series->header)){
                fclose(file);
                _ERROR("\t HEADER_INVALID_CHECKSUM\n");
                status = HEADER_INVALID_CHECKSUM;
                break;
            }


            // Get our file size, we get the file size whenver we open a new file, when we update a file we also update the filesize
            fseek(file,0,SEEK_END);
            series->file_size = ftell(file);
            _DEBUG("\tFile size = %u\n",series->file_size);
        }



        // If our write ahead cache is NULL, malloc it and set it to our null fill
        if(series->write_ahead_cache == NULL){
            series->write_ahead_cache = (char*)malloc(write_ahead_size * series->header.datasize);

            if(series->write_ahead_cache == NULL){
                _ERROR("Cache Malloc Failed, Fatal!\n");
                index_access.unlock();
                return -99999;
            }
            _DEBUG("Cache Malloc Success\n");

            memset(series->write_ahead_cache,default_null_fill_byte,write_ahead_size * series->header.datasize);
        }




        /// If we reach this point, we have a valid header and our pointer is on the first data point in the series

retry:

        int64_t point = (timestamp - series->header.timestamp)/series->header.interval;
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
                        file = openFile(key);

                    if(!flushBuffer(series,file)){
                        _DEBUG("\t Failed to flush buffer");
                        break;
                    }
                }

            } else {
                _WARN("\t Warning: Write position on series %u exceeds write ahead size, null filling\n",key);


                // If file not already open, open it
                if(file == NULL)
                    file = openFile(key);

                // Check if the file opened correctily
                if(file == NULL){
                    _ERROR("\t Failed to open or create file");
                    break;
                }



                // First flush our current buffer to disk because it could contain some valid points
                if(!flushBuffer(series,file)){
                    _DEBUG("\t Failed to flush buffer");
                    break;
                }



                // How many null points do we need to insert?... Measure from end of write ahead pos to where we want to be
                int64_t grow_by = (pointsInBuffer - write_ahead_size); // Calculate how many bytes we are going to grow the file, we want write_ahead_size beyond the current requested write position
                _DEBUG("\t Adding %u null points\n",grow_by);


                // Create a temporary buffer to hold the data
                char *nullFill = (char*)malloc(grow_by * series->header.datasize); // Allocate a temporary memory buffer to contain the null data points that we will write
                if(nullFill == NULL){ // Could not allocate memory
                    _ERROR("\t WAL_MEMORY_ALLOCATION_FAILURE\n");
                    break;
                }

                // Set the buffer to our null fill
                memset(nullFill,0xFF,grow_by * series->header.datasize); // Set to the null fill, for char a value of '0' is used, for float a value of 'FFFFFFFF' is used which represents 'Nan'

                _DEBUG("\Writing null fill to end of file\n");
                // Write it to disk
                if(fwrite(nullFill,series->header.datasize,grow_by,file) != grow_by){ // Write the null points
                    free(nullFill);
                    _ERROR("\t WAL_WRITE_FAILURE\n");
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
                file = openFile(key);
                if(file == NULL){
                    _ERROR("\tFailed to open file for direct writing\n");
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
                _ERROR("\t Failed to direct write data point\n");
                status = DATA_POINT_WRITE_FAILURE;
                break;
            }
            _DEBUG("\t Success\n");
            series->last_write = time(NULL);
        }


        status = NO_ERROR; // Return successful, don't close file
        break;

    } while (true);

    _DEBUG("\t Unlocking Series mutex\n");

    series->access.unlock();

    if(file != NULL)
        fclose(file);

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


    uint32_t status = NO_ERROR;
    FILE *file = NULL;

    _DEBUG("Looking Up Key: %d\n",key);

    index_access.lock();
    ENTRY *series = series_list[key];
    if(series == NULL){
        _DEBUG("No Entry Found, Creating new one\n");
        series = (ENTRY*)malloc(sizeof(ENTRY));

        if(series == NULL){
            _ERROR("Malloc Failed, Fatal!\n");
            index_access.unlock();
            return -99999;
        }

        memset(series,0,sizeof(ENTRY));
        _DEBUG("Malloc Success\n");
        series_list[key] = series;
    }

    index_access.unlock();



    do {
        series->access.lock();


        if(end_time <=0)
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

        file = openFile(key);
        if(file == NULL){
            _ERROR("\t Failed to open file");
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

            // Get our file size, we get the file size whenver we open a new file, when we update a file we also update the filesize
            fseek(file,0,SEEK_END);
            series->file_size = ftell(file);
            _DEBUG("\tFile size = %u\n",series->file_size);

        }


        /// If we reach this point, we have a valid header and our pointer is on the first data point in the series


        /// Calculate the position in the file (Byte) where we are going to write the data point,
        /// this is based on the start timestampfor the series and the number of seconds between points,
        /// the position is also offseted to account for the header size
        int64_t points = ( end_time - start_time) / series->header.interval;
        int64_t points_in_file = (series->file_size - sizeof(SERIES))/series->header.datasize;
        *seconds_per_point = series->header.interval;
        // *first_point_timestamp = series->header.timestamp + (series->header.interval * (start_pos + offset));

        char *output = (char*)malloc(points*series->header.datasize);
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

            buffer_output_points = file_end_point - file_start_point;

            int64_t  buffer_output_timestamp = series->header.timestamp + (file_start_point * series->header.interval);
            buffer_output_pos = (buffer_output_timestamp - start_time) / series->header.interval;



            /*
             * MAPPING FILE=
        points_in_file: 31321
        file_start_point: 31967
        file_end_point: 31321
        buffer_output_pos: 0
        buffer_output_points: -646
        buffer_output_timestamp: 1486069751
        =MAPPING CACHE=

*/

            _DEBUG("=MAPPING FILE=\n");
            _DEBUG("\tpoints_in_file: %d\n",points_in_file);
            _DEBUG("\tfile_start_point: %d\n",file_start_point);
            _DEBUG("\tfile_end_point: %d\n",file_end_point);
            _DEBUG("\tbuffer_output_pos: %d\n",buffer_output_pos);
            _DEBUG("\tbuffer_output_points: %d\n",buffer_output_points);
            _DEBUG("\tbuffer_output_timestamp: %d\n",buffer_output_timestamp);

            if(buffer_output_points > 0 && file_start_point >= 0 && file_end_point >= 0){
                fseek(file,((file_start_point*series->header.datasize)+sizeof(SERIES)),SEEK_SET); // Read Points
                int64_t file_points  = fread(output+(buffer_output_pos*series->header.datasize),series->header.datasize,buffer_output_points,file);
                *real_points += buffer_output_points;
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

            cache_end_point = (end_time - cache_start_point) / series->header.interval;

            if(cache_end_point > points_in_cache){
                cache_end_point = points_in_cache;
            }


            int64_t buffer_output_timestamp = cache_start_time + (cache_start_point * series->header.interval);
            buffer_output_start_pos = (buffer_output_timestamp - start_time) / series->header.interval;


            buffer_output_points = cache_end_point - cache_start_point;
            buffer_output_end_pos = buffer_output_start_pos + buffer_output_points;


            if(buffer_output_end_pos > points){
                buffer_output_end_pos = points;
            }

            buffer_output_points = buffer_output_end_pos - buffer_output_start_pos;


            _DEBUG("=MAPPING CACHE=\n");
            _DEBUG("\tfile_start_point: %d\n",series->header.timestamp);
            _DEBUG("\tpoints_in_cache: %d\n",points_in_cache);
            _DEBUG("\tcache_start_point: %d\n",cache_start_point);
            _DEBUG("\tcache_end_point: %d\n",cache_end_point);
            _DEBUG("\tbuffer_output_start_pos: %d\n",buffer_output_start_pos);
            _DEBUG("\tbuffer_output_end_pos: %d\n",buffer_output_end_pos);
            _DEBUG("\tbuffer_output_points: %d\n",buffer_output_points);
            _DEBUG("\tcache_start_time: %d\n",cache_start_time);

            if(buffer_output_points > 0 && series->write_ahead_cache != NULL && cache_start_point >= 0 && cache_end_point >= 0 && buffer_output_points > 0){
                memcpy(output+(buffer_output_start_pos*series->header.datasize),series->write_ahead_cache + cache_start_point,buffer_output_points*series->header.datasize);
                *real_points += buffer_output_points;
            }
        }


        *result = output;
        *n_points = points;
        *datasize = series->header.datasize;

        status = NO_ERROR;
        break;




        break;
    } while (true);

    series->access.unlock();


    if(file != NULL)
        fclose(file);


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


        if(it->second != NULL){
            it->second->access.lock(); // this will wait for any current writes to complete

            FILE *file = openFile(it->first);
            if(file != NULL){
                _DEBUG("Flushing: %u\n",it->first);
                this->flushBuffer(it->second,file);
            }
            it->second->access.unlock();
        }
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
        if(it->second != NULL){

            _DEBUG("Closing: %u\n",it->first);
            it->second->access.lock(); // Ensure nobody is accessing our resource
            if(it->second->write_ahead_cache != NULL){
                free(it->second->write_ahead_cache);
            }

            free(it->second);

            // delete it->second;
        }
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
