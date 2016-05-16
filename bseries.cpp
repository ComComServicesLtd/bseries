#include "bseries.h"
#include "debug.h"




BSeries::BSeries()
{

    this->default_Seconds_Per_Point = SECONDS_PER_POINT;

}



uint32_t BSeries::getChecksum(SERIES *series){
    return 1234567890 + ((series->version ^ series->timestamp) ^ (series->interval ^ series->datatype));
}


int BSeries::createSeries(FILE *file, SERIES *series, uint32_t datatype){

    series->version = 1;
    series->timestamp = time(NULL);
    series->interval = default_Seconds_Per_Point;
    series->datatype = datatype;
    series->checksum = getChecksum(series);

    fseek(file,0,SEEK_SET);
    int64_t size = fwrite((char*)series,sizeof(SERIES),1,file);


    if(size == 1)
        return 1;

    return 0;
}




/// This function takes a filename and value
/// it reads the header of the file and determins where the point needs to be written based on the current timestamp
/// if the files is missing a new file is created
/// if the headers checksum is bad the function returns
/// if the file dose not containe room to write our point WRITE_AHEAD_SIZE is automatically appended to it full of null points


/// Errors
/// -1 Could not write new header to file
/// -2 Header has invalid checksum
/// -3 Could not allocate memory for Write Ahead Log
/// -4 Could not append null write ahead data
/// -5 Could not write data point to file
/// -6 Invalid Write Position Detected, Write Failed for an unknown reason, INTERNAL ERROR

int BSeries::write_u8(char *filename, uint8_t value, uint32_t timestamp){

    if(!timestamp)
        timestamp = time(NULL);



    FILE *file;
    char tries = 0;

retry:

    file = fopen(filename,"r+b");

    if(file != NULL){

        SERIES series;

        int size = fread((char*)&series,sizeof(series),1,file);

        if(size != 1){
            /// Create header and continue write
            if(!createSeries(file,&series)){ // attempt to create header, if failure, return error
                fclose(file);
                return -1;
            }
        }

        if(series.checksum != getChecksum(&series)){
            fclose(file);
            return -2;
        }

        /// If we reach this point, we have a valid header and our pointer is on the first data point in the series

        int64_t pos = sizeof(SERIES) + ((timestamp - series.timestamp)/series.interval);
        /// Calculate the position in the file (Byte) where we are going to write the data point,
        /// this is based on the start timestampfor the series and the number of seconds between points,
        /// the position is also offseted to account for the header size



        fseek(file,0,SEEK_END);

        int64_t filesize = ftell(file);

        //printf("Writing data @ %u\nfile size = %u\n",pos,filesize);
        if(pos >= filesize){ // projected write position greater than file length, append more null points :) :)

            //   fseek(file,SEEK_END); // First move to end of file

            int64_t grow_by = (pos - filesize) + WRITE_AHEAD_SIZE; // Calculate how many bytes we are going to grow the file, we want WRITE_AHEAD_SIZE beyond the current requested write position
            //  printf("Adding %u null points\n",grow_by);


            char *nullFill = (char*)malloc(grow_by); // Allocate a temporary memory buffer to contain the null data points that we will write
            if(nullFill == NULL){ // Could not allocate memory
                fclose(file);
                return -3;
            }

            memset(nullFill,0,grow_by); // Set to the null fill, for char a value of '0' is used, for float a value of 'FFFFFFFF' is used which represents 'Nan'

            if(fwrite(nullFill,1,grow_by,file) != grow_by){ // Write the null points
                fclose(file);
                free(nullFill);
                return -4;
            }

            free(nullFill); // Free our temporary buffer
        }

        if(!pos){ // Just a quick check that we have a valid writing position
            return -6;
        }

        fseek(file,pos,SEEK_SET); // Seek to the current writing position, this is based on the timestamp and interval

        size = fwrite(&value,sizeof(value),1,file); // Write the data point
        //  printf("%s:  Wrote %d points to %d\n",filename,size,pos);


        if(size != 1){ // Check that write completed with the correct number of bytes written
            fclose(file);
            return -5;
        }

        fclose(file); // Close the file
        return 1; // Return successful

    }

    file = fopen(filename,"ab");

    if(file == NULL){
        return -1;
    } else if(!tries){
        fclose(file);
        tries++;
        goto retry;
    }


    return 0; // Return error 0
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
/// r_poitns is the number of real points in the output array (Points found in the time series)


int BSeries::read_u8(char* filename, int64_t start_time, int64_t end_time, int64_t* n_points, int64_t *r_points, char** result){ // Returns number of points if successful or -1 if error


    if(!end_time)
        end_time = time(NULL);

    if(start_time > end_time)
        return -1;

    if(!start_time)
        return -1;


    FILE *file;

    file = fopen(filename,"r");

    if(file != NULL){
        SERIES series;

        int size = fread((char*)&series,sizeof(series),1,file);



        if(size != 1){
            fclose(file);
            return -2;
        }

        if(series.checksum != getChecksum(&series)){
            fclose(file);
            return -3;
        }

        /// If we reach this point, we have a valid header and our pointer is on the first data point in the series

        int64_t start_pos = ((start_time - series.timestamp)/series.interval);

        /// Calculate the position in the file (Byte) where we are going to write the data point,
        /// this is based on the start timestampfor the series and the number of seconds between points,
        /// the position is also offseted to account for the header size
        int64_t points = ( end_time - start_time) / series.interval;

        int64_t offset = 0;

        if(start_pos < 0){
            offset = start_pos * -1;
            start_pos = 0;
        }



        //  printf("start_time = %ld start_byte = %ld end_time = %ld end_byte = %ld points = %ld %d\n",start_time,start_pos,end_time,end_pos,points,offset);

        *result = (char*)malloc(points);

        if(*result == NULL)
            return -4;

        memset(*result,0,points); // Set to the null fill, for char a value of '0' is used, for float a value of 'FFFFFFFF' is used which represents 'Nan'

        fseek(file,(start_pos+sizeof(SERIES)),SEEK_SET);

        *r_points = fread(*result+offset,1,points-offset,file);
        *n_points = points;

        fclose(file);

        return 1;

    }

    return -5;


}







void BSeries::closeSeries(uint32_t max_age){

    int64_t age;
    int64_t age_counts;

    uint32_t current_timestamp = time(NULL);

    auto it = series_list.begin();
    while(it != series_list.end()){

        if(it->second.last_write){



            age = current_timestamp - it->second.last_write;
            if(age > max_age){
                if(it->second.file != NULL){
                    fclose(it->second.file);
                    it->second.file = NULL;
                }

                it = series_list.erase(it);
            } else {
                it++;
            }


            it++;




        } else { // invalid date, erase it

            if(it->second.file != NULL){
                fclose(it->second.file);
                it->second.file = NULL;

            }
            it = series_list.erase(it);
        }



    }
}




bool BSeries::trim(){

    if(series_list.size() < MAX_FILES)
        return true;
/*
    int target = MAX_FILES-(MAX_FILES/10);

    for(int i = 6; i >= 0; i--){

        closeSeries(i*10);

        if(series_list.size() < target){
            return true;
        }
    }

*/
    this->close(); // close all series
    return true;
}





/// This function takes a filename and value
/// it reads the header of the file and determins where the point needs to be written based on the current timestamp
/// if the files is missing a new file is created
/// if the headers checksum is bad the function returns
/// if the file dose not containe room to write our point WRITE_AHEAD_SIZE is automatically appended to it full of null points


/// Errors
/// -1 Could not write new header to file
/// -2 Header has invalid checksum
/// -3 Could not allocate memory for Write Ahead Log
/// -4 Could not append null write ahead data
/// -5 Could not write data point to file
/// -6 Invalid Write Position Detected, Write Failed for an unknown reason, INTERNAL ERROR



int BSeries::write_f32(uint32_t key, float value, uint32_t timestamp = 0){


    if(!timestamp)
        timestamp = time(NULL);

    char tries = 0;
    int size;

    debug("Writing to series %u, value=%f\n",key,value);

    ENTRY *series = &series_list[key];
    if(!trim()){
        error("\t TOO_MANY_OPEN_FILES\n");
        return TOO_MANY_OPEN_FILES;
    }


retry:

    if(series->file == NULL){
        char filename[256];
        sprintf(filename,"%s/%u",data_directory,key);
        debug("\t File not already open, opening file %s\n",filename);


        series->file = fopen(filename,"r+b");
    }


    if(series->file != NULL){

        if(series->header.checksum != getChecksum(&series->header)){// cached checksum header is invalid, read it from the file
            error("\t INVALID CACHED HEADER, READING HEADER FROM FILE\n");


            size = fread((char*)&series->header,sizeof(series->header),1,series->file);


            if(size != 1){
                /// Create header and continue write
                if(!createSeries(series->file,&series->header,FLOAT32)){ // attempt to create header, if failure, return error
                    fclose(series->file);
                    error("\t CREATE_NEW_HEADER_FAIL\n");
                    return CREATE_NEW_HEADER_FAIL;
                }
            }

            debug("\t Created Series\n");

            if(series->header.checksum != getChecksum(&series->header)){
                fclose(series->file);
                error("\t HEADER_INVALID_CHECKSUM\n");
                return HEADER_INVALID_CHECKSUM;
            }
        }


        /// If we reach this point, we have a valid header and our pointer is on the first data point in the series

        int64_t pos = sizeof(SERIES) + (((timestamp - series->header.timestamp)/series->header.interval)*4);
        /// Calculate the position in the file (Byte) where we are going to write the data point,
        /// this is based on the start timestampfor the series and the number of seconds between points,
        /// the position is also offseted to account for the header size


        if(pos >= series->file_size){ // Invalid file size or file to small, recheck size and update our local storage point
            debug("\t File size too small, rechecking file size, cur = %u\n",series->file_size);
            fseek(series->file,0,SEEK_END);
            series->file_size = ftell(series->file);
            debug("\t Updated file size = %u\n",series->file_size);
        }

        debug("\t Writing data @ %u\nfile size = %u\n",pos,series->file_size);
        if(pos >= series->file_size){ // projected write position greater than file length, append more null points :) :)

            int64_t grow_by = (pos - series->file_size) + WRITE_AHEAD_SIZE; // Calculate how many bytes we are going to grow the file, we want WRITE_AHEAD_SIZE beyond the current requested write position
            debug("\t Adding %u null bytes\n",grow_by);


            char *nullFill = (char*)malloc(grow_by); // Allocate a temporary memory buffer to contain the null data points that we will write
            if(nullFill == NULL){ // Could not allocate memory
                fclose(series->file);
                error("\t WAL_MEMORY_ALLOCATION_FAILURE\n");
                return WAL_MEMORY_ALLOCATION_FAILURE;
            }

            memset(nullFill,0xFF,grow_by); // Set to the null fill, for char a value of '0' is used, for float a value of 'FFFFFFFF' is used which represents 'Nan'

            if(fwrite(nullFill,1,grow_by,series->file) != grow_by){ // Write the null points
                fclose(series->file);
                free(nullFill);
                error("\t WAL_WRITE_FAILURE\n");
                return WAL_WRITE_FAILURE;
            }

            free(nullFill); // Free our temporary buffer
            series->file_size += grow_by;
        }



        if(!pos){ // Just a quick check that we have a valid writing position
            error("\t INTERNAL_ERROR\n");
            return INTERNAL_ERROR;
        }

        fseek(series->file,pos,SEEK_SET); // Seek to the current writing position, this is based on the timestamp and interval

        size = fwrite(&value,sizeof(value),1,series->file); // Write the data point
        debug("\t Wrote %d point @ %d\n",size,pos);


        if(size != 1){ // Check that write completed with the correct number of bytes written
            fclose(series->file);
            error("\t DATA_POINT_WRITE_FAILURE\n");
            return DATA_POINT_WRITE_FAILURE;
        }

        series->last_write = time(NULL);
        debug("\t Success\n");
        return NO_ERROR; // Return successful, don't close file

    } else {
        // file did not open, we will try to open it in append mode which will create the file if it does not exist, no existant file is likely why the last r+b open failed

        char filename[256];
        sprintf(filename,"%s/%u",data_directory,key);
        series->file = fopen(filename,"ab");

        if(series->file == NULL){
            warn("\t Failed to create file\n");
            return CREATE_NEW_HEADER_FAIL;
        } else if(!tries){
            fclose(series->file);
            series->file = NULL;
            tries++;
            warn("\t Attempting to create file\n");
            goto retry;
        }

    }

    return 0; // Return error 0
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



int BSeries::read_f32(uint32_t key, int64_t start_time, int64_t end_time, int64_t *n_points, int64_t *r_points, int64_t *seconds_per_point, int64_t *first_point_timestamp,  float** result){ // Returns number of points if successful or -1 if error


    debug("Reading from series %u where time > %lu and time < %lu\n",key,start_time,end_time);



    if(!end_time)
        end_time = time(NULL);

    if(start_time > end_time){
        error("\t INVALID_TIME_RANGE,  Start time > end time\n");
        return INVALID_TIME_RANGE;
    }


    if(!start_time){
        error("\t INVALID_TIME_RANGE,  Invliad start time\n");
        return INVALID_TIME_RANGE;
    }

    ENTRY *series = &series_list[key];
    if(!trim()){
        error("\t TOO_MANY_OPEN_FILES\n");
        return TOO_MANY_OPEN_FILES;
    }


    if(series->file == NULL){
        char filename[256];
        sprintf(filename,"%s/%u",data_directory,key);
        debug("\t File not already open, opening file %s\n",filename);
        series->file = fopen(filename,"r+b");
    }


    if(series->file != NULL){


        if(series->header.checksum != getChecksum(&series->header)){// cached checksum header is invalid, read it from the file
            error("\t INVALID CACHED HEADER, READING HEADER FROM FILE\n");

            int size = fread((char*)&series->header,sizeof(series->header),1,series->file);


            if(size != 1){
                fclose(series->file);
                error("\t FAILED_TO_READ_HEADER\n");
                return FAILED_TO_READ_HEADER;
            }

            if(series->header.checksum != getChecksum(&series->header)){
                fclose(series->file);
                error("\t INVALID_HEADER_CHECKSUM\n");
                return INVALID_HEADER_CHECKSUM;
            }

        }

        /// If we reach this point, we have a valid header and our pointer is on the first data point in the series

        int64_t start_pos = ((start_time - series->header.timestamp)/series->header.interval);
        /// Calculate the position in the file (Byte) where we are going to write the data point,
        /// this is based on the start timestampfor the series and the number of seconds between points,
        /// the position is also offseted to account for the header size
        int64_t points = ( end_time - start_time) / series->header.interval;

        int64_t offset = 0; // This variable is used when a request is made that extends prior to where we have data.

        if(start_pos < 0){ // If our start pos is behind our header we don't have data here, We will make sure when we read that we forward offset this amout of data points behind
            offset = start_pos * -1;
            start_pos = 0;
        }

        *seconds_per_point = series->header.interval;
        *first_point_timestamp = series->header.timestamp + (series->header.interval * (start_pos + offset));



          debug("start_time = %ld start_byte = %ld end_time = %ld points = %ld %d\n",start_time,start_pos,end_time,points,offset);

        float *output = (float*)malloc(points*sizeof(float));


        if(output == NULL){
            error("\t MEMORY_ALLOCATION_FAILED\n");
            return MEMORY_ALLOCATION_FAILED;
        }

        memset(output,0xff,points*sizeof(float)); // Set to the null fill, for char a value of '0' is used, for float a value of 'FFFFFFFF' is used which represents 'Nan'

        fseek(series->file,((start_pos*sizeof(float))+sizeof(SERIES)),SEEK_SET);
        *r_points  = fread(output+(offset*sizeof(float)),sizeof(float),(points-offset),series->file);

        *result = output;
        *n_points = points;

        return NO_ERROR;

    }

    return FAILED_TO_OPEN_FILE;


}






void BSeries::flush()
{
    // Close all open files
    auto it = series_list.begin();
    while(it != series_list.end()){
        if(it->second.file != NULL){
            fflush(it->second.file);
        }
        it++;
    }
}



void BSeries::close()
{
    // Close all open files
    auto it = series_list.begin();
    while(it != series_list.end()){
        if(it->second.file != NULL){
            fclose(it->second.file);
            it->second.file = NULL;

        }
        it++;
    }
}





BSeries::~BSeries()
{
    this->close();
}
