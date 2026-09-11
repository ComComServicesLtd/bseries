#include "bseries.h"
#include "debug.h"

#include <new>
#include <dirent.h>
#include <algorithm>
#include <stdlib.h>
#include <unistd.h>   // fsync, so a migrated file is on disk before the rename




BSeries::BSeries()
{

    this->default_seconds_per_point = 10;
    this->default_null_fill_byte = 0xFF;
    this->write_ahead_size = 1024;
    this->max_grow_points = 1000000;

    this->shuttingDown = false;

}



/// CRC32, for version 4 headers. The version 1 checksum is a sum of exclusive
/// ors over four fields, which a pair of matching bit flips cancels out; a header
/// with room for a real one should have a real one.

static uint32_t bsCrc32(const unsigned char *data, size_t length){

    static uint32_t table[256];
    static bool built = false;

    if(!built){
        for(uint32_t i = 0; i < 256; i++){
            uint32_t c = i;
            for(int k = 0; k < 8; k++)
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            table[i] = c;
        }
        built = true;
    }

    uint32_t crc = 0xFFFFFFFFu;

    for(size_t i = 0; i < length; i++)
        crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);

    return crc ^ 0xFFFFFFFFu;
}


/// Little endian field access, so the on disk layout is written down here rather
/// than left to whatever a compiler does with the struct.

static void putU32(unsigned char *p, uint32_t v){
    p[0]=(unsigned char)v; p[1]=(unsigned char)(v>>8); p[2]=(unsigned char)(v>>16); p[3]=(unsigned char)(v>>24);
}

static void putU64(unsigned char *p, uint64_t v){
    for(int i = 0; i < 8; i++) p[i] = (unsigned char)(v >> (8*i));
}

static uint32_t getU32(const unsigned char *p){
    return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
}

static uint64_t getU64(const unsigned char *p){
    uint64_t v = 0;
    for(int i = 0; i < 8; i++) v |= ((uint64_t)p[i]) << (8*i);
    return v;
}


/// Lays a header out on disk.
///
///   version 1-3   20 bytes, five little endian uint32
///   version 4    128 bytes, see bseries.h
///
/// A version 4 checksum covers every byte of the header but its own four, so a
/// change anywhere in it is caught -- including in the profile name, which decides
/// how the points are read.

static void packHeader(const SERIES *h, unsigned char *out, uint32_t bytes){

    memset(out,0,bytes);

    if(bytes == SERIES_HEADER_BYTES_V3){
        putU32(out +  0,h->version);
        putU32(out +  4,h->timestamp);
        putU32(out +  8,h->interval);
        putU32(out + 12,h->typecode);
        putU32(out + 16,h->checksum);
        return;
    }

    putU32(out +   0,h->version);
    putU32(out +   4,h->flags);
    putU64(out +   8,h->timestamp_ms);
    putU32(out +  16,(uint32_t)h->interval_ms);
    putU32(out +  20,h->typecode);
    memcpy(out +  24,h->null_fill_wide,8);
    memcpy(out +  32,h->profile,SERIES_PROFILE_BYTES);
    memcpy(out +  48,h->address,SERIES_ADDRESS_BYTES);
    memcpy(out +  64,h->name,SERIES_NAME_BYTES);
    // 112..123 spare, left zero
    putU32(out + 124,bsCrc32(out,124));
}


static bool unpackHeader(const unsigned char *in, uint32_t bytes, SERIES *h){

    memset(h,0,sizeof(*h));

    if(bytes == SERIES_HEADER_BYTES_V3){

        h->version   = getU32(in +  0);
        h->timestamp = getU32(in +  4);
        h->interval  = getU32(in +  8);
        h->typecode  = getU32(in + 12);
        h->checksum  = getU32(in + 16);

        // Kept in step so callers can use either without asking the version.
        h->timestamp_ms = (uint64_t)h->timestamp * 1000u;
        h->interval_ms  = (uint64_t)h->interval * 1000u;
        return true;
    }

    h->version      = getU32(in +  0);
    h->flags        = getU32(in +  4);
    h->timestamp_ms = getU64(in +  8);
    h->interval_ms  = getU32(in + 16);
    h->typecode     = getU32(in + 20);
    memcpy(h->null_fill_wide,in + 24,8);
    memcpy(h->profile,in + 32,SERIES_PROFILE_BYTES);
    memcpy(h->address,in + 48,SERIES_ADDRESS_BYTES);
    memcpy(h->name,   in + 64,SERIES_NAME_BYTES);
    h->checksum     = getU32(in + 124);

    if(h->checksum != bsCrc32(in,124))
        return false;

    // Every string field is used as a C string, so a file claiming 16 bytes of
    // profile name with no terminator must not read off the end of it.
    h->profile[SERIES_PROFILE_BYTES - 1] = 0;
    h->name[SERIES_NAME_BYTES - 1] = 0;

    h->timestamp = (uint32_t)(h->timestamp_ms / 1000u);
    h->interval  = (uint32_t)(h->interval_ms / 1000u);
    return true;
}


bool bsReadHeader(FILE *file, SERIES *out){

    unsigned char buffer[SERIES_HEADER_BYTES_V4];

    if(fseek(file,0,SEEK_SET) != 0)
        return false;

    if(fread(buffer,SERIES_HEADER_BYTES_V3,1,file) != 1)
        return false;

    uint32_t version = getU32(buffer);
    uint32_t bytes = bsHeaderBytes(version);

    if(bytes == 0)
        return false;                       // a version this build does not know

    if(bytes > SERIES_HEADER_BYTES_V3){

        size_t rest = bytes - SERIES_HEADER_BYTES_V3;

        if(fread(buffer + SERIES_HEADER_BYTES_V3,rest,1,file) != 1)
            return false;
    }

    return unpackHeader(buffer,bytes,out);
}


bool bsWriteHeader(FILE *file, const SERIES *header){

    uint32_t bytes = bsHeaderBytes(header->version);

    if(bytes == 0)
        return false;

    unsigned char buffer[SERIES_HEADER_BYTES_V4];
    packHeader(header,buffer,bytes);

    if(fseek(file,0,SEEK_SET) != 0)
        return false;

    return fwrite(buffer,bytes,1,file) == 1;
}


void bsFinaliseHeader(SERIES *header){

    if(header->version == SERIES_VERSION_PROFILED){

        if(header->interval_ms == 0)
            header->interval_ms = (uint64_t)header->interval * 1000u;

        if(header->timestamp_ms == 0)
            header->timestamp_ms = (uint64_t)header->timestamp * 1000u;

        header->timestamp = (uint32_t)(header->timestamp_ms / 1000u);
        header->interval  = (uint32_t)(header->interval_ms / 1000u);

        unsigned char buffer[SERIES_HEADER_BYTES_V4];
        packHeader(header,buffer,SERIES_HEADER_BYTES_V4);
        header->checksum = getU32(buffer + 124);
        return;
    }

    header->timestamp_ms = (uint64_t)header->timestamp * 1000u;
    header->interval_ms  = (uint64_t)header->interval * 1000u;
    header->checksum = 1234567890 + ((header->version ^ header->timestamp) ^ (header->interval ^ header->typecode));
}


uint32_t BSeries::getChecksum(SERIES *series){

    // Version 4 checksums the whole header with a CRC32, so the answer cannot be
    // computed from four fields the way the original could. Callers compare this
    // against header.checksum to decide whether a cached header is still good, so
    // it has to know which scheme the header uses or every version 4 header looks
    // corrupt.
    if(series->version == SERIES_VERSION_PROFILED){
        unsigned char buffer[SERIES_HEADER_BYTES_V4];
        packHeader(series,buffer,SERIES_HEADER_BYTES_V4);
        return getU32(buffer + 124);
    }

    return 1234567890 + ((series->version ^ series->timestamp) ^ (series->interval ^ series->typecode));
}


/// Lays down the header for a series that does not exist yet.
///
/// If a definition covers this key it decides the interval, the datatype and the
/// null fill. With no definition the width is what the caller is writing, the
/// datatype is inferred from it exactly as reading a version 1 file infers it, and
/// the fill is the database wide default.
///
/// Either way the result is a version 3 header, which records all of it. A series
/// is then readable correctly from its own file, with no configuration alongside.
///
/// start_timestamp is where the series' first point sits. A series created by a
/// write is stamped with that write's timestamp rather than with the current time:
/// the first point is by definition the start of the series, and stamping it "now"
/// meant the very write creating the series could be rejected for predating it,
/// which any batch assembled a moment before sending would trip over.
///
/// Returns NO_ERROR, or a negative error code.

int BSeries::createSeries(FILE *file, SERIES *series, uint32_t key, uint32_t datasize, uint32_t start_timestamp,
                          const CREATE_SHAPE *shape){

    SERIES_DEFINITION def;

    if(definitionForKey(key,&def)){

        if(datasize && datasize != def.datasize){
            _ERROR("\t Series %u is defined as %s but the write supplied %u byte points\n",
                   key,bsTypeName(def.datatype,def.datasize),datasize);
            return SERIES_TYPE_MISMATCH;
        }

        series->version = SERIES_VERSION_PROFILED;
        series->interval = def.interval;
        series->typecode = bsPackTypeCode(def.datatype,def.datasize,def.null_fill_byte);
        memset(series->null_fill_wide,def.null_fill_byte,sizeof(series->null_fill_wide));

        _DEBUG("\t Creating series %u as %s every %u seconds\n",key,bsTypeName(def.datatype,def.datasize),def.interval);

    } else {

        // The same inference reading a version 1 file applies, made explicit here
        // so it is recorded rather than repeated on every read. A caller that
        // says what it is writing overrides the guess.
        uint8_t inferred = (datasize == 4) ? BS_FLOAT : BS_UNSIGNED;
        uint32_t width = datasize;
        uint32_t interval = default_seconds_per_point;

        if(shape != NULL){
            if(shape->datatype != BS_TYPE_INVALID) inferred = shape->datatype;
            if(shape->datasize) width = shape->datasize;
            if(shape->interval) interval = shape->interval;
        }

        if(!bsTypeValid(inferred,(uint8_t)width)){
            _ERROR("\t Series %u has no definition and %u is not a storable point width\n",key,width);
            return SERIES_TYPE_MISMATCH;
        }

        series->version = SERIES_VERSION_PROFILED;
        series->interval = interval;
        series->typecode = bsPackTypeCode(inferred,(uint8_t)width,(unsigned char)default_null_fill_byte);
        memset(series->null_fill_wide,(unsigned char)default_null_fill_byte,sizeof(series->null_fill_wide));

        // Recorded in the header, so a later read resolves it without being told
        // and the caller needs no second request to set it.
        if(shape != NULL && shape->profile[0])
            snprintf(series->profile,sizeof(series->profile),"%s",shape->profile);
    }

    series->timestamp = start_timestamp ? start_timestamp : (uint32_t)time(NULL);
    bsFinaliseHeader(series);

    int64_t size = bsWriteHeader(file,series) ? 1 : 0;


    if(size == 1)
        return NO_ERROR;

    return CREATE_NEW_HEADER_FAIL;
}


unsigned char BSeries::resolveNullFill(uint32_t key, const SERIES *header){

    if(header->version >= SERIES_VERSION_FILLED)
        return bsTypeCodeNullFill(header->typecode);

    uint32_t datasize = bsHeaderDataSize(header);
    SERIES_DEFINITION def;

    if(definitionForKey(key,&def) && def.datasize == datasize)
        return def.null_fill_byte;

    if(header->version == SERIES_VERSION_TYPED)
        return bsTypeNullFill(bsHeaderDataType(header),(uint8_t)datasize);

    return (unsigned char)default_null_fill_byte;
}


/// Decodes a header that has just been loaded or created into the entry fields the
/// point loops use, and rejects a header describing something this build cannot
/// address. Must be called before an entry is used for anything.

bool BSeries::bindHeader(ENTRY *entry, uint32_t key){

    SERIES_DEFINITION def;
    bool have_def = definitionForKey(key,&def);

    uint8_t datatype;
    uint32_t datasize;

    if(entry->header.version == SERIES_VERSION_PROFILED ||
       entry->header.version == SERIES_VERSION_FILLED || entry->header.version == SERIES_VERSION_TYPED){

        datatype = bsTypeCodeDataType(entry->header.typecode);
        datasize = bsTypeCodeDataSize(entry->header.typecode);

    } else if(entry->header.version == SERIES_VERSION_LEGACY){

        // Version 1 recorded the point width but not what the points meant. The
        // only widths this database ever wrote were 1 (unsigned char) and 4
        // (float), so infer from the width and let a definition for the key
        // correct the guess where one has been installed.
        datasize = entry->header.typecode;
        datatype = (datasize == 4) ? BS_FLOAT : BS_UNSIGNED;

        if(have_def && def.datasize == datasize)
            datatype = def.datatype;

    } else {

        _ERROR("\t Series %u has header version %u, this build understands %d to %d\n",
               key,entry->header.version,SERIES_VERSION_LEGACY,SERIES_VERSION_PROFILED);
        return false;
    }

    if(!bsTypeValid(datatype,(uint8_t)datasize)){
        _ERROR("\t Series %u header describes an unstorable type (class %u, %u byte points)\n",key,datatype,datasize);
        return false;
    }

    if(entry->header.interval == 0){
        _ERROR("\t Series %u header has a zero interval\n",key);
        return false;
    }

    entry->datasize = datasize;
    entry->datatype = datatype;

    entry->null_fill_byte = resolveNullFill(key,&entry->header);

    // The file always wins over the definition: the points already on disk were
    // laid out to the header's interval and width, so honouring the definition
    // instead would misread every one of them. Say so loudly, it means the
    // definitions file and the data have drifted apart.
    if(have_def){

        if(def.interval != entry->header.interval)
            _WARN("\t Warning: series %u is defined with a %u second interval but its file says %u, using the file\n",
                  key,def.interval,entry->header.interval);

        if(def.datasize != datasize || def.datatype != datatype)
            _WARN("\t Warning: series %u is defined as %s but its file says %s, using the file\n",
                  key,bsTypeName(def.datatype,def.datasize),bsTypeName(datatype,(uint8_t)datasize));
    }

    return true;
}






/// ===========================================================================
/// Series definitions
///
/// Without these, every series in the database shares default_seconds_per_point
/// and takes whatever point width the first write happened to pass. A definition
/// declares the interval and the datatype for a range of keys up front, so one
/// database can hold one second ping bytes alongside, say, sixty second
/// temperature floats.
/// ===========================================================================


/// Copies a diagnostic label, truncating rather than failing. The name only ever
/// appears in log messages, so a long one being cut short costs nothing.

static void copyDefinitionName(char *destination, size_t size, const char *source){

    size_t length = strlen(source);

    if(length > size - 1)
        length = size - 1;

    memcpy(destination,source,length);
    destination[length] = 0;
}


/// Parses "1234", "1000-1999" or "*" into an inclusive key range.

static bool parseKeyRange(const char *text, uint32_t *first, uint32_t *last){

    if(strcmp(text,"*") == 0){
        *first = 0;
        *last = 0xFFFFFFFFu;
        return true;
    }

    char *end = NULL;
    unsigned long low = strtoul(text,&end,10);

    if(end == text || low > 0xFFFFFFFFu)
        return false;

    if(*end == 0){ // a single key
        *first = (uint32_t)low;
        *last = (uint32_t)low;
        return true;
    }

    if(*end != '-')
        return false;

    const char *high_text = end + 1;
    unsigned long high = strtoul(high_text,&end,10);

    if(end == high_text || *end != 0 || high > 0xFFFFFFFFu || high < low)
        return false;

    *first = (uint32_t)low;
    *last = (uint32_t)high;
    return true;
}


/// Parses "10", "30s", "5m" or "1h" into seconds.

static bool parseInterval(const char *text, uint32_t *seconds){

    char *end = NULL;
    unsigned long value = strtoul(text,&end,10);

    if(end == text || value == 0)
        return false;

    unsigned long multiplier = 1;

    if(*end == 's' && end[1] == 0)      multiplier = 1;
    else if(*end == 'm' && end[1] == 0) multiplier = 60;
    else if(*end == 'h' && end[1] == 0) multiplier = 3600;
    else if(*end != 0)                  return false;

    if(value > 0xFFFFFFFFuL / multiplier)
        return false;

    *seconds = (uint32_t)(value * multiplier);
    return true;
}


void BSeries::clearDefinitions(){

    definitions_access.lock();
    definitions.clear();
    definitions_access.unlock();
}


/// Declares the shape of every series whose key falls in [key_first,key_last].
/// Pass null_fill_byte as -1 to take the default for the type.

int BSeries::defineSeries(uint32_t key_first, uint32_t key_last, uint32_t interval, uint8_t datatype, uint8_t datasize, const char *name, int null_fill_byte){

    if(key_first > key_last){
        _ERROR("Series definition has an inverted key range %u-%u\n",key_first,key_last);
        return INVALID_SERIES_DEFINITION;
    }

    if(interval == 0){
        _ERROR("Series definition for keys %u-%u has a zero interval\n",key_first,key_last);
        return INVALID_SERIES_DEFINITION;
    }

    if(!bsTypeValid(datatype,datasize)){
        _ERROR("Series definition for keys %u-%u names an unstorable type (class %u, %u byte points)\n",key_first,key_last,datatype,datasize);
        return INVALID_SERIES_DEFINITION;
    }

    if(null_fill_byte > 255){
        _ERROR("Series definition for keys %u-%u has a null fill outside a byte\n",key_first,key_last);
        return INVALID_SERIES_DEFINITION;
    }

    SERIES_DEFINITION def;
    memset(&def,0,sizeof(def));

    def.key_first = key_first;
    def.key_last = key_last;
    def.interval = interval;
    def.datatype = datatype;
    def.datasize = datasize;
    def.null_fill_byte = (null_fill_byte < 0) ? bsTypeNullFill(datatype,datasize) : (unsigned char)null_fill_byte;

    if(name != NULL)
        copyDefinitionName(def.name,sizeof(def.name),name);

    definitions_access.lock();
    definitions.push_back(def);
    definitions_access.unlock();

    return NO_ERROR;
}


/// Returns the first definition covering key, so narrower ranges have to be
/// declared before wider ones.

bool BSeries::definitionForKey(uint32_t key, SERIES_DEFINITION *out){

    bool found = false;

    definitions_access.lock();

    for(size_t i = 0; i < definitions.size(); i++){
        if(key >= definitions[i].key_first && key <= definitions[i].key_last){
            if(out != NULL)
                *out = definitions[i];
            found = true;
            break;
        }
    }

    definitions_access.unlock();

    return found;
}


/// Loads a definitions file, replacing any definitions already installed.
///
/// One series family per line:
///
///     <name>  <keys>  <interval>  <type>  [null_fill]
///
/// keys is a single key, an inclusive N-M range, or * for everything. interval is
/// in seconds and may be suffixed s, m or h. type is a name from bseries_types.h
/// (uint8, int32, float32, ...). null_fill optionally overrides the default fill
/// byte for the type. Everything after a # is a comment.
///
/// A "table <name>" line switches which table the definitions after it belong to.
/// Definitions before any such line belong to the table named "default". When table
/// is given only that table's definitions are installed; pass NULL to install every
/// definition in the file, which is what a single table database wants.
///
/// Returns the number of definitions loaded, or a negative error code. A malformed
/// line fails the whole load rather than being skipped: a typo in a database's type
/// configuration must not quietly change how points are stored. That check runs on
/// every line, not only the ones being installed, so a typo under one table is
/// still reported when a different table is loaded.

int BSeries::loadDefinitions(const char *path, const char *table){

    FILE *file = fopen(path,"r");

    if(file == NULL){
        _ERROR("Could not open series definitions file %s\n",path);
        return DEFINITIONS_FILE_UNREADABLE;
    }

    vector<SERIES_DEFINITION> parsed;
    char line[512];
    int line_number = 0;
    int status = NO_ERROR;
    string current_table = "default";

    while(fgets(line,sizeof(line),file) != NULL){

        line_number++;

        char *comment = strchr(line,'#');
        if(comment != NULL)
            *comment = 0;

        char name[64], keys[64], interval_text[64], type_text[64], fill_text[64];

        int fields = sscanf(line,"%63s %63s %63s %63s %63s",name,keys,interval_text,type_text,fill_text);

        if(fields <= 0) // blank line, or a line that was nothing but a comment
            continue;

        if(strcmp(name,"table") == 0){

            if(fields != 2){
                _ERROR("%s line %d: expected 'table <name>'\n",path,line_number);
                status = INVALID_SERIES_DEFINITION;
                break;
            }

            current_table = keys; // the second token holds the table name
            continue;
        }

        bool wanted = (table == NULL) || (current_table == table);

        if(fields < 4){
            _ERROR("%s line %d: expected <name> <keys> <interval> <type> [null_fill]\n",path,line_number);
            status = INVALID_SERIES_DEFINITION;
            break;
        }

        uint32_t key_first, key_last, interval;
        uint8_t datatype, datasize;

        if(!parseKeyRange(keys,&key_first,&key_last)){
            _ERROR("%s line %d: '%s' is not a key, an N-M range or *\n",path,line_number,keys);
            status = INVALID_SERIES_DEFINITION;
            break;
        }

        if(!parseInterval(interval_text,&interval)){
            _ERROR("%s line %d: '%s' is not an interval, expected seconds optionally suffixed s, m or h\n",path,line_number,interval_text);
            status = INVALID_SERIES_DEFINITION;
            break;
        }

        if(!bsTypeFromName(type_text,&datatype,&datasize)){
            _ERROR("%s line %d: '%s' is not a known datatype\n",path,line_number,type_text);
            status = INVALID_SERIES_DEFINITION;
            break;
        }

        int null_fill = -1;

        if(fields >= 5){
            char *end = NULL;
            long value = strtol(fill_text,&end,0);
            if(end == fill_text || *end != 0 || value < 0 || value > 255){
                _ERROR("%s line %d: '%s' is not a byte value for the null fill\n",path,line_number,fill_text);
                status = INVALID_SERIES_DEFINITION;
                break;
            }
            null_fill = (int)value;
        }

        SERIES_DEFINITION def;
        memset(&def,0,sizeof(def));

        def.key_first = key_first;
        def.key_last = key_last;
        def.interval = interval;
        def.datatype = datatype;
        def.datasize = datasize;
        def.null_fill_byte = (null_fill < 0) ? bsTypeNullFill(datatype,datasize) : (unsigned char)null_fill;

        copyDefinitionName(def.name,sizeof(def.name),name);

        if(wanted)
            parsed.push_back(def);
    }

    fclose(file);

    if(status != NO_ERROR)
        return status;

    definitions_access.lock();
    definitions = parsed;
    definitions_access.unlock();

    _DEBUG("Loaded %d series definitions from %s\n",(int)parsed.size(),path);

    return (int)parsed.size();
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



bool BSeries::flushBuffer(ENTRY *series,FILE *file,int64_t points){

    if(file == NULL || series->write_ahead_cache == NULL)
        return false;

    // A block is always written whole. Where fewer points than that have been
    // recorded the rest go down as null fill, so the file only ever grows by whole
    // blocks and a partly filled one is padded ahead rather than left short.
    //
    // The consequence is worth knowing: the file now covers the whole block, so
    // later points inside it are direct writes rather than buffered ones. They
    // reach the disk as they arrive, which is why this costs syscalls and buys
    // durability.
    if(points < 0)
        points = write_ahead_size;

    if(points > write_ahead_size)
        points = write_ahead_size;

    if(series->buffer_points <= 0){
        // Nothing has been recorded since the last flush. Writing a block here is
        // how an idle series used to gain another block of null points every time
        // anything flushed it.
        series->buffer_points = 0;
        series->buffer_dirty_since = 0;
        return true;
    }

    _DEBUG("\tSeeking to end of file\n");
    // Seek to end of file
    fseek(file,0,SEEK_END);

    _DEBUG("\tFlushing a %ld point block, %ld of them recorded\n",(long)points,(long)series->buffer_points);
    size_t size = fwrite(series->write_ahead_cache,series->datasize,(size_t)points,file);

    if(size != (size_t)points){
        _ERROR("\t Failed to flush write ahead buffer to file");
        return false;
    }

    series->file_size += points * series->datasize; // Our file has grown!
    _DEBUG("\tNew File Size = %d\n",series->file_size);

    // Reset our buffer with null fill. The window has slid forward by a whole
    // block, so the next point in sequence lands at the front of it again.
    memset(series->write_ahead_cache,series->null_fill_byte,write_ahead_size * series->datasize);

    series->buffer_points = 0;
    series->buffer_dirty_since = 0;

    return true;
}


/// Flushes series that have been holding buffered points for too long.
///
/// A series still being written to is never flushed by closeSeries, which only
/// evicts idle ones, so without this it is the busiest series that hold the most
/// unflushed data. Entries another thread is using are skipped rather than waited
/// for; they will be caught on the next pass.

int BSeries::flushAged(uint32_t max_age){

    if(shuttingDown)
        return 0;

    uint32_t now = (uint32_t)time(NULL);
    int flushed = 0;

    index_access.lock();

    for(map<uint32_t,ENTRY>::iterator it = series_list.begin(); it != series_list.end(); ++it){

        if(!it->second.access.try_lock())
            continue;

        bool due = it->second.buffer_points > 0 &&
                   it->second.buffer_dirty_since != 0 &&
                   (int64_t)now - (int64_t)it->second.buffer_dirty_since >= (int64_t)max_age;

        if(!due){
            it->second.access.unlock();
            continue;
        }

        FILE *file = openFile(it->first,true);

        if(file != NULL){

            if(flushBuffer(&it->second,file))
                flushed++;
            else
                _ERROR("\t Failed to flush series %u on its timer\n",it->first);

            fclose(file);

        } else {
            _ERROR("\t Could not open series %u to flush it on its timer\n",it->first);
        }

        it->second.access.unlock();
    }

    index_access.unlock();

    return flushed;
}




bool BSeries::validateWriteAheadCache(ENTRY *series){

    if(series->datasize == 0){
        // The header has not been bound yet. Allocating here would produce a zero
        // sized cache that is never resized, because the NULL check below would then
        // consider it valid forever.
        _ERROR("Cache requested before the header was loaded\n");
        return false;
    }

    // If our write ahead cache is NULL, malloc it and set it to our null fill
    if(series->write_ahead_cache == NULL){
        series->write_ahead_cache = (char*)malloc(write_ahead_size * series->datasize);

        if(series->write_ahead_cache == NULL){
            _ERROR("Cache Malloc Failed, Fatal!\n");
            return false;
        }
        _DEBUG("Cache Malloc Success\n");

        memset(series->write_ahead_cache,series->null_fill_byte,write_ahead_size * series->datasize);
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

int BSeries::write(uint32_t key, void *value,uint32_t datasize, uint32_t timestamp, bool *overwrote,
                   const CREATE_SHAPE *shape){

    if(overwrote != NULL)
        *overwrote = false;

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
        unsigned char existing_point[8]; // widest point this build stores

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
            size = bsReadHeader(file,&series->header) ? 1 : 0;


            // If the header not read correctily, create the series
            if(size != 1){
                /// Create header and continue write
                int create_status = createSeries(file,&series->header,key,datasize,timestamp,shape);
                if(create_status != NO_ERROR){
                    fclose(file);
                    file = NULL; // the exit path below closes file, don't close it twice
                    _ERROR("\t Failed to create new File/header\n");
                    status = create_status; // keeps a type mismatch distinct from a write failure
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


            // Decode the header into the entry before anything reads its width
            if(!bindHeader(series,key)){
                status = SERIES_TYPE_MISMATCH;
                break;
            }


            // Get our file size, we get the file size whenver we open a new file, when we update a file we also update the filesize
            fseek(file,0,SEEK_END);
            series->file_size = ftell(file);
            _DEBUG("\tFile size = %u\n",series->file_size);
        }


        // The caller's idea of the point width has to match the series it is
        // writing to. Without this check a float written to a byte series is
        // silently truncated to its first byte.
        if(datasize != series->datasize){
            _ERROR("\t Series %u holds %s but the write supplied %u byte points\n",
                   key,bsTypeName(series->datatype,(uint8_t)series->datasize),datasize);
            status = SERIES_TYPE_MISMATCH;
            break;
        }


        // Allocate write ahead cache if needed
        if(!validateWriteAheadCache(series)){

            _ERROR("\t WAL_MEMORY_ALLOCATION_FAILURE\n");
            status = WAL_MEMORY_ALLOCATION_FAILURE;
            break;
        }



        /// If we reach this point, we have a valid header and our pointer is on the first data point in the series

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

        int64_t file_pos = point * series->datasize + bsHeaderBytes(series->header.version);


        if(file_pos >= series->file_size){ // Cached Write

            _DEBUG("\t ===== Performing Cached Write =======\n");

            // get total points in series
            int64_t pointsInBuffer = point - ((series->file_size - bsHeaderBytes(series->header.version)) / series->datasize);
            //a int64_t pointsInBuffer = pointsInSeries - point;

            _DEBUG("\t Absolute point in series: %d,  buffer pos: %d, buffer size: %d\n",point,pointsInBuffer,write_ahead_size);


            // Check if this is a cache write or memory write
            if(pointsInBuffer < write_ahead_size){

                char *slot = series->write_ahead_cache + (pointsInBuffer * series->datasize);

                if(overwrote != NULL){
                    // Anything other than the null fill means a real reading is
                    // about to be replaced.
                    for(uint32_t byte = 0; byte < series->datasize; byte++){
                        if((unsigned char)slot[byte] != series->null_fill_byte){
                            *overwrote = true;
                            break;
                        }
                    }
                }

                // Write to buffer
                memcpy(slot,value,series->datasize);

                if(pointsInBuffer + 1 > series->buffer_points)
                    series->buffer_points = pointsInBuffer + 1;

                if(series->buffer_dirty_since == 0)
                    series->buffer_dirty_since = (uint32_t)time(NULL);
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

                // A point timestamped far past the end of the series would null
                // fill every interval in between, so one write with a bad
                // timestamp could ask for gigabytes of memory and disk.
                if(max_grow_points > 0 && grow_by > max_grow_points){
                    _ERROR("\t Write to series %u is %ld points past the end of the file, the limit is %ld\n",
                           key,(long)grow_by,(long)max_grow_points);
                    status = SERIES_GROWTH_LIMIT;
                    break;
                }


                // Create a temporary buffer to hold the data
                char *nullFill = (char*)malloc(grow_by * series->datasize); // Allocate a temporary memory buffer to contain the null data points that we will write
                if(nullFill == NULL){ // Could not allocate memory
                    _ERROR("\t WAL_MEMORY_ALLOCATION_FAILURE\n");
                    status = WAL_MEMORY_ALLOCATION_FAILURE;
                    break;
                }

                // Set the buffer to our null fill
                memset(nullFill,series->null_fill_byte,grow_by * series->datasize); // the fill byte for this series' type, see bsTypeNullFill()

                _DEBUG("\Writing null fill to end of file\n");
                // Write it to disk
                if(fwrite(nullFill,series->datasize,grow_by,file) != (size_t)grow_by){ // Write the null points
                    free(nullFill);
                    _ERROR("\t WAL_WRITE_FAILURE\n");
                    status = WAL_WRITE_FAILURE;
                    break;
                }
                _DEBUG("\t freeing null buffer\n");
                free(nullFill);

                series->file_size += grow_by * series->datasize; // Our new filesize
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

            if(overwrote != NULL && series->datasize <= sizeof(existing_point)){

                fseek(file,file_pos,SEEK_SET);

                if(fread(existing_point,series->datasize,1,file) == 1){
                    for(uint32_t byte = 0; byte < series->datasize; byte++){
                        if(existing_point[byte] != series->null_fill_byte){
                            *overwrote = true;
                            break;
                        }
                    }
                }
            }

            _DEBUG("\t seeking to current writing position: %d\n",file_pos);
            fseek(file,file_pos,SEEK_SET); // Seek to the current writing position, this is based on the timestamp and interval
            _DEBUG("\t Datapoint Size = %d\n",series->datasize);
            size = fwrite(value,series->datasize,1,file); // Write the data point
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



int BSeries::read(uint32_t key, int64_t start_time, int64_t end_time, int64_t *n_points, int64_t *real_points, int64_t *seconds_per_point, int64_t *first_point_timestamp, uint32_t *datasize, void** result, uint8_t *datatype)
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
    *datasize = 0;

    if(datatype != NULL)
        *datatype = BS_TYPE_INVALID;

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
            int size = bsReadHeader(file,&series->header) ? 1 : 0;

            if(size != 1){
                _ERROR("\t FAILED_TO_READ_HEADER\n");
                status = FAILED_TO_READ_HEADER;
                break;
            }

            _DEBUG("%u\n\tVersion: %u\n\tTimestamp: %u\n\tInverval: %u\n\tTypecode: %u\n\tChecksum: %u\n\n   ",key,series->header.version,series->header.timestamp,series->header.interval,series->header.typecode,series->header.checksum);

            if(series->header.checksum != getChecksum(&series->header)){
                _ERROR("\t INVALID_HEADER_CHECKSUM\n");
                status = INVALID_HEADER_CHECKSUM;
                break;
            }

            // Decode the header into the entry before anything reads its width
            if(!bindHeader(series,key)){
                status = SERIES_TYPE_MISMATCH;
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
        if(series->header.interval == 0 || series->datasize == 0){ // both divide below
            _ERROR("\t INVALID_SERIES_INTERVAL\n"); // bindHeader should already have caught this
            status = INVALID_SERIES_INTERVAL;
            break;
        }

        // Only now that the header is bound is datasize known. Allocating the cache
        // any earlier sizes it as write_ahead_size * 0, and malloc(0) hands back a
        // non NULL pointer that this function then never grows.
        if(!validateWriteAheadCache(series)){
            _ERROR("\t WAL_MEMORY_ALLOCATION_FAILURE\n");
            status = WAL_MEMORY_ALLOCATION_FAILURE;
            break;
        }

        int64_t points = ( end_time - start_time) / series->header.interval;
        int64_t points_in_file = (series->file_size - (int64_t)bsHeaderBytes(series->header.version))/series->datasize;
        *seconds_per_point = series->header.interval;

        // The point grid is anchored to the series' own start, not to whatever
        // start_time was asked for, so output[0] is the slot that *contains*
        // start_time rather than start_time itself. Reporting the requested time
        // here would label every returned point up to interval-1 seconds later
        // than it really is. This mirrors the placement arithmetic below exactly.
        {
            int64_t first_slot;

            if(start_time <= (int64_t)series->header.timestamp)
                first_slot = -(((int64_t)series->header.timestamp - start_time) / (int64_t)series->header.interval);
            else
                first_slot = (start_time - (int64_t)series->header.timestamp) / (int64_t)series->header.interval;

            *first_point_timestamp = (int64_t)series->header.timestamp + first_slot * (int64_t)series->header.interval;
        }

        char *output = new (std::nothrow) char[points*series->datasize];
        if(output == NULL){
            _ERROR("\t MEMORY_ALLOCATION_FAILED\n");
            status = MEMORY_ALLOCATION_FAILED;
            break;
        }
        memset(output,series->null_fill_byte,points*series->datasize); // the fill byte for this series' type, see bsTypeNullFill()


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

            // file_end_point is exclusive, so the count is the plain difference.
            // This used to subtract one more, which dropped the last point of the
            // file region on every read: with a 4096 point write ahead buffer,
            // point 4095 of every flushed block read back as null fill instead of
            // its value. The subtraction was standing in for the missing bounds
            // check below, which now does that job properly.
            buffer_output_points = file_end_point - file_start_point;

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


                fseek(file,((file_start_point*series->datasize)+bsHeaderBytes(series->header.version)),SEEK_SET); // Read Points


                int64_t file_points = fread(output+(buffer_output_pos*series->datasize),series->datasize,buffer_output_points,file);

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
                memcpy(output+(buffer_output_start_pos*series->datasize),series->write_ahead_cache + (cache_start_point*series->datasize),buffer_output_points*series->datasize);
                *real_points += buffer_output_points;
            }


        }


        *result = output;
        *n_points = points;
        *datasize = series->datasize;

        if(datatype != NULL)
            *datatype = series->datatype;

        status = NO_ERROR;

    } while (false);



    if(file)
        fclose(file);


    if(series)
        series->access.unlock();

    return status;
}






/// ===========================================================================
/// Series lifecycle
/// ===========================================================================


/// Lays down a header for a series that does not exist yet, with an explicit shape
/// rather than one inferred from a write. Fails with SERIES_ALREADY_EXISTS rather
/// than touching a series that is already on disk, because rewriting a header
/// would misaddress every point already stored under the old one.

int BSeries::createSeriesFile(uint32_t key, uint32_t interval, uint8_t datatype, uint8_t datasize, uint32_t start_timestamp){

    if(shuttingDown)
        return INTERNAL_ERROR;

    if(interval == 0 || !bsTypeValid(datatype,datasize))
        return INVALID_SERIES_DEFINITION;

    ENTRY *series;

    index_access.lock();
    series = &series_list[key];
    series->access.lock();
    index_access.unlock();

    int status = NO_ERROR;
    FILE *file = NULL;

    do {

        file = openFile(key,false); // probe without creating

        if(file != NULL){
            status = SERIES_ALREADY_EXISTS;
            break;
        }

        file = openFile(key,true);

        if(file == NULL){
            _ERROR("\t Could not create series %u\n",key);
            status = FILE_OPEN_FAILURE;
            break;
        }

        SERIES header;
        memset(&header,0,sizeof(header));

        // A definition for this key supplies the fill; otherwise the type's own
        // default is used. Either way it is recorded in the header.
        SERIES_DEFINITION def;
        unsigned char fill = bsTypeNullFill(datatype,datasize);

        if(definitionForKey(key,&def) && def.datasize == datasize)
            fill = def.null_fill_byte;

        header.version = SERIES_VERSION_PROFILED;
        header.timestamp = start_timestamp ? start_timestamp : (uint32_t)time(NULL);
        header.interval = interval;
        header.typecode = bsPackTypeCode(datatype,datasize,fill);
        memset(header.null_fill_wide,fill,sizeof(header.null_fill_wide));
        bsFinaliseHeader(&header);

        fseek(file,0,SEEK_SET);

        if(!bsWriteHeader(file,&header)){
            _ERROR("\t Could not write the header for series %u\n",key);
            status = CREATE_NEW_HEADER_FAIL;
            break;
        }

        series->header = header;

        if(!bindHeader(series,key)){
            status = SERIES_TYPE_MISMATCH;
            break;
        }

        fseek(file,0,SEEK_END);
        series->file_size = ftell(file);

    } while(false);

    if(file != NULL)
        fclose(file);

    series->access.unlock();

    return status;
}


/// Drops a series from memory and unlinks its file.
///
/// The write ahead cache is freed without being flushed: the points in it are being
/// deleted along with everything else. A write racing this call can recreate the
/// series immediately afterwards, which is inherent to deleting something another
/// thread is still writing to.

int BSeries::deleteSeries(uint32_t key){

    if(shuttingDown)
        return INTERNAL_ERROR;

    char filename[256];

    if(snprintf(filename,sizeof(filename),"%s/%lu",data_directory,(unsigned long)key) >= (int)sizeof(filename)){
        _ERROR("\t Path for key %lu does not fit in the filename buffer\n",(unsigned long)key);
        return INTERNAL_ERROR;
    }

    index_access.lock();

    map<uint32_t,ENTRY>::iterator it = series_list.find(key);

    if(it != series_list.end()){

        // Everyone takes the entry lock while holding index_access, so blocking
        // here cannot deadlock and nobody can be waiting on this entry once we
        // hold it. That makes the erase below safe.
        it->second.access.lock();

        if(it->second.write_ahead_cache != NULL){
            free(it->second.write_ahead_cache);
            it->second.write_ahead_cache = NULL;
        }

        it->second.access.unlock();

        series_list.erase(it);
    }

    index_access.unlock();

    if(remove(filename) != 0)
        return SERIES_NOT_FOUND;

    return NO_ERROR;
}


/// Reads a header straight from disk without touching series_list, so probing keys
/// that do not exist cannot grow the in memory index. Callers exposing this over a
/// network want exactly that.

int BSeries::seriesInfo(uint32_t key, SERIES *header, int64_t *file_size){

    FILE *file = openFile(key,false);

    if(file == NULL)
        return SERIES_NOT_FOUND;

    int status = NO_ERROR;

    do {

        if(!bsReadHeader(file,header)){
            status = FAILED_TO_READ_HEADER;
            break;
        }

        if(header->checksum != getChecksum(header)){
            status = INVALID_HEADER_CHECKSUM;
            break;
        }

        if(file_size != NULL){
            fseek(file,0,SEEK_END);
            *file_size = ftell(file);
        }

    } while(false);

    fclose(file);

    return status;
}


int BSeries::remapUint8(uint32_t key, const unsigned char *map256, const char *profile,
                        MIGRATION_REPORT *report, bool dry_run){

    if(shuttingDown)
        return FAILED_TO_OPEN_FILE;

    if(report != NULL)
        memset(report,0,sizeof(*report));

    char path[256];
    char temp_path[288];

    if(snprintf(path,sizeof(path),"%s/%lu",data_directory,(unsigned long)key) >= (int)sizeof(path))
        return FAILED_TO_OPEN_FILE;

    snprintf(temp_path,sizeof(temp_path),"%s.migrating",path);

    // Held for the whole rewrite. Nothing may open this series while its file is
    // being replaced, and an admin operation that runs once is the right place to
    // pay for that with a quiet database rather than with a second locking scheme.
    index_access.lock();

    int status = NO_ERROR;
    FILE *in = NULL;
    FILE *out = NULL;

    do {

        // Flush and drop the series first: its write ahead cache holds points the
        // file does not, and its cached header would outlive the file it describes.
        map<uint32_t,ENTRY>::iterator it = series_list.find(key);

        if(it != series_list.end()){

            it->second.access.lock();

            if(it->second.write_ahead_cache != NULL){

                if(it->second.last_write){
                    FILE *file = openFile(key,true);
                    if(file != NULL){
                        flushBuffer(&it->second,file);
                        fclose(file);
                    } else {
                        _ERROR("\t Failed to open %lu to flush before migrating\n",(unsigned long)key);
                        it->second.access.unlock();
                        status = FAILED_TO_OPEN_FILE;
                        break;
                    }
                }

                free(it->second.write_ahead_cache);
                it->second.write_ahead_cache = NULL;
            }

            it->second.access.unlock();
            series_list.erase(it);
        }

        in = fopen(path,"rb");

        if(in == NULL){
            status = SERIES_NOT_FOUND;
            break;
        }

        SERIES header;

        if(!bsReadHeader(in,&header)){
            status = FAILED_TO_READ_HEADER;
            break;
        }

        if(header.checksum != getChecksum(&header)){
            status = INVALID_HEADER_CHECKSUM;
            break;
        }

        // A translation is one byte to one byte, so anything else is refused. The
        // version is not a guard any more: a remap moves between two conventions
        // the caller named, so it applies to a version 3 file being re-encoded
        // just as much as to a version 1 one being brought forward.
        // With no translation there is nothing width specific left -- the bytes
        // are copied verbatim -- so a header only upgrade works for any type,
        // which is the only way a float series reaches version 4.
        if(map256 != NULL && (bsHeaderDataSize(&header) != 1 || bsHeaderDataType(&header) != BS_UNSIGNED)){
            status = SERIES_TYPE_MISMATCH;
            break;
        }

        if(!dry_run){

            out = fopen(temp_path,"wb");

            if(out == NULL){
                _ERROR("\t Failed to open %s\n",temp_path);
                status = FAILED_TO_OPEN_FILE;
                break;
            }
        }

        uint32_t width = bsHeaderDataSize(&header);
        uint8_t kind = bsHeaderDataType(&header);
        const unsigned char fill = resolveNullFill(key,&header);

        // The rewrite already copies every byte, so bringing the header up to the
        // current version costs nothing extra here -- the data simply lands after
        // a 128 byte header instead of a 20 byte one. Doing it anywhere else would
        // mean moving every point in the file for its own sake.
        SERIES migrated = header;
        migrated.version = SERIES_VERSION_PROFILED;
        migrated.typecode = bsPackTypeCode(kind,(uint8_t)width,fill);
        memset(migrated.null_fill_wide,fill,sizeof(migrated.null_fill_wide));

        // The points are in the target profile's encoding now, so the header says
        // so: a later read resolves it without being told.
        memset(migrated.profile,0,sizeof(migrated.profile));

        if(profile != NULL)
            snprintf(migrated.profile,sizeof(migrated.profile),"%s",profile);

        bsFinaliseHeader(&migrated);

        if(!dry_run && !bsWriteHeader(out,&migrated)){
            status = CREATE_NEW_HEADER_FAIL;
            break;
        }

        unsigned char buffer[65536];
        size_t got;

        while((got = fread(buffer,1,sizeof(buffer),in)) > 0){

            // No translation means the bytes are copied untouched and nothing is
            // classified: an upgrade moves the header, and claiming to have
            // counted values it never decoded would be worse than saying nothing.
            if(map256 != NULL){

                for(size_t i = 0; i < got; i++){

                    unsigned char value = buffer[i];

                    // The fill is not a reading and is never translated, whatever
                    // the map says: a slot holding it was never written.
                    if(value == fill){
                        if(report) report->nulls++;
                        continue;
                    }

                    unsigned char mapped = map256[value];

                    if(mapped == value){
                        if(report) report->unchanged++;
                        continue;
                    }

                    buffer[i] = mapped;
                    if(report) report->changed++;
                }
            }

            if(!dry_run && fwrite(buffer,1,got,out) != got){
                status = DATA_POINT_WRITE_FAILURE;
                break;
            }

            if(report) report->points += (int64_t)got / (width ? width : 1);
        }

        if(status != NO_ERROR)
            break;

        if(ferror(in)){
            status = INTERNAL_ERROR;
            break;
        }

        if(dry_run)
            break;                    // counted, nothing written, original untouched

        // On disk before the rename, or a power cut could leave the name pointing
        // at a file whose contents never arrived.
        fflush(out);
        fsync(fileno(out));
        fclose(out);
        out = NULL;

        fclose(in);
        in = NULL;

        if(rename(temp_path,path) != 0){
            _ERROR("\t Failed to rename %s over %s\n",temp_path,path);
            status = DATA_POINT_WRITE_FAILURE;
            break;
        }

    } while(false);

    if(out != NULL){
        fclose(out);
        remove(temp_path);        // an interrupted run leaves the original alone
    }

    if(in != NULL)
        fclose(in);

    index_access.unlock();

    return status;
}


int BSeries::setSeriesProfile(uint32_t key, const char *profile){

    META meta;
    meta.profile = profile;
    meta.name = NULL;
    meta.address = NULL;
    meta.address_family = -1;

    return setSeriesMeta(key,meta);
}


int BSeries::setSeriesMeta(uint32_t key, const META &meta){

    if(shuttingDown)
        return FAILED_TO_OPEN_FILE;

    if(meta.profile != NULL && strlen(meta.profile) >= SERIES_PROFILE_BYTES)
        return INVALID_SERIES_DEFINITION;

    // 48 bytes with a guaranteed terminator, so 47 usable. Refused rather than
    // truncated: cutting UTF-8 to a byte count lands mid sequence and writes an
    // invalid string into the file.
    if(meta.name != NULL && strlen(meta.name) >= SERIES_NAME_BYTES)
        return INVALID_SERIES_DEFINITION;

    // The whole series is taken out of memory first: its cached header is about
    // to stop describing the file, and a reader holding it would resolve the old
    // profile for as long as the entry lived.
    index_access.lock();

    map<uint32_t,ENTRY>::iterator it = series_list.find(key);

    if(it != series_list.end()){

        it->second.access.lock();

        if(it->second.write_ahead_cache != NULL){

            if(it->second.last_write){
                FILE *f = openFile(key,true);
                if(f != NULL){ flushBuffer(&it->second,f); fclose(f); }
            }

            free(it->second.write_ahead_cache);
            it->second.write_ahead_cache = NULL;
        }

        it->second.access.unlock();
        series_list.erase(it);
    }

    FILE *file = openFile(key,true);

    if(file == NULL){
        index_access.unlock();
        return SERIES_NOT_FOUND;
    }

    SERIES header;
    int status = NO_ERROR;

    do {

        if(!bsReadHeader(file,&header)){
            status = FAILED_TO_READ_HEADER;
            break;
        }

        // Older headers have no field for it, and widening one would move every
        // point in the file -- which is a migration, not a rename.
        if(header.version != SERIES_VERSION_PROFILED){
            status = SERIES_TYPE_MISMATCH;
            break;
        }

        if(meta.profile != NULL){
            memset(header.profile,0,sizeof(header.profile));
            snprintf(header.profile,sizeof(header.profile),"%s",meta.profile);
        }

        if(meta.name != NULL){
            memset(header.name,0,sizeof(header.name));
            snprintf(header.name,sizeof(header.name),"%s",meta.name);
        }

        if(meta.address_family >= 0){

            memset(header.address,0,sizeof(header.address));

            if(meta.address != NULL && meta.address_family != SERIES_ADDRESS_NONE)
                memcpy(header.address,meta.address,sizeof(header.address));

            header.flags = (header.flags & ~(uint32_t)SERIES_ADDRESS_MASK) |
                           ((uint32_t)meta.address_family & SERIES_ADDRESS_MASK);
        }

        bsFinaliseHeader(&header);

        if(!bsWriteHeader(file,&header)){
            status = CREATE_NEW_HEADER_FAIL;
            break;
        }

        fflush(file);

    } while(false);

    fclose(file);
    index_access.unlock();

    return status;
}


int64_t BSeries::bufferedPoints(uint32_t key){

    if(shuttingDown)
        return 0;

    index_access.lock();

    map<uint32_t,ENTRY>::iterator it = series_list.find(key);

    if(it == series_list.end()){
        index_access.unlock();
        return 0;              // not open, so nothing is held for it
    }

    it->second.access.lock();
    index_access.unlock();

    int64_t points = it->second.buffer_points;

    it->second.access.unlock();

    return points;
}


bool BSeries::seriesShape(uint32_t key, SERIES *header_out){

    if(header_out == NULL || shuttingDown)
        return false;

    // The common case on a write path: the series is already open, so its header
    // is in memory and this costs a lock rather than a file.
    index_access.lock();

    map<uint32_t,ENTRY>::iterator it = series_list.find(key);

    if(it == series_list.end()){
        index_access.unlock();
        return seriesInfo(key,header_out,NULL) == NO_ERROR;
    }

    it->second.access.lock();
    index_access.unlock();

    bool bound = it->second.datasize != 0 &&
                 it->second.header.checksum == getChecksum(&it->second.header);

    if(bound)
        *header_out = it->second.header;

    it->second.access.unlock();

    if(bound)
        return true;

    return seriesInfo(key,header_out,NULL) == NO_ERROR;
}


/// Lists the series present in data_directory, in ascending key order, starting
/// after the given key. Returns the number of keys appended to the vector.
///
/// This walks the directory on every call. It is meant for administration, not for
/// a hot path.

int BSeries::listSeriesKeys(vector<uint32_t> *keys, uint32_t after, int limit){

    if(keys == NULL || limit <= 0)
        return 0;

    DIR *dir = opendir(data_directory);

    if(dir == NULL){
        _ERROR("Could not open the data directory %s\n",data_directory);
        return FAILED_TO_OPEN_FILE;
    }

    vector<uint32_t> found;
    struct dirent *entry;

    while((entry = readdir(dir)) != NULL){

        // Series files are named for their key and nothing else, so anything that
        // is not a plain unsigned number is somebody else's file.
        const char *name = entry->d_name;

        if(name[0] == 0)
            continue;

        char *end = NULL;
        unsigned long value = strtoul(name,&end,10);

        if(end == name || *end != 0 || value > 0xFFFFFFFFuL)
            continue;

        if(after != 0 && value <= after)
            continue;

        found.push_back((uint32_t)value);
    }

    closedir(dir);

    sort(found.begin(),found.end());

    int count = 0;

    for(size_t i = 0; i < found.size() && count < limit; i++){
        keys->push_back(found[i]);
        count++;
    }

    return count;
}




int BSeries::flushSeries(uint32_t key, int64_t *points_flushed){

    if(points_flushed)
        *points_flushed = 0;

    if(shuttingDown)
        return FAILED_TO_OPEN_FILE;

    index_access.lock();

    map<uint32_t,ENTRY>::iterator it = series_list.find(key);

    if(it == series_list.end()){
        index_access.unlock();
        return NO_ERROR;            // not open, so nothing is being held for it
    }

    it->second.access.lock();       // waits for a write in flight to finish
    index_access.unlock();

    int status = NO_ERROR;
    int64_t held = it->second.buffer_points;

    // last_write is only set once a write has landed, so a series that has only
    // ever been read has nothing to write out.
    if(held > 0 && it->second.write_ahead_cache != NULL && it->second.last_write){

        FILE *file = openFile(key,true);

        if(file == NULL){
            status = FAILED_TO_OPEN_FILE;
        } else {
            if(!flushBuffer(&it->second,file))
                status = WAL_WRITE_FAILURE;
            fclose(file);
        }
    }

    if(status == NO_ERROR && points_flushed)
        *points_flushed = held;

    it->second.access.unlock();

    return status;
}


int BSeries::flushOpen(int64_t *series_flushed, int64_t *points_flushed){

    if(series_flushed) *series_flushed = 0;
    if(points_flushed) *points_flushed = 0;

    if(shuttingDown)
        return FAILED_TO_OPEN_FILE;

    int status = NO_ERROR;

    index_access.lock();

    for(map<uint32_t,ENTRY>::iterator it = series_list.begin(); it != series_list.end(); it++){

        it->second.access.lock();

        int64_t held = it->second.buffer_points;

        if(held > 0 && it->second.write_ahead_cache != NULL && it->second.last_write){

            FILE *file = openFile(it->first,true);

            if(file == NULL){
                status = FAILED_TO_OPEN_FILE;
            } else {

                if(flushBuffer(&it->second,file)){
                    if(series_flushed) (*series_flushed)++;
                    if(points_flushed) (*points_flushed) += held;
                } else {
                    status = WAL_WRITE_FAILURE;
                }

                fclose(file);
            }
        }

        it->second.access.unlock();
    }

    index_access.unlock();

    return status;
}


void BSeries::flush()
{

    _WARN("Flushing database\n");

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
    _WARN("Done flushing database\n");
}



void BSeries::close()
{

    _WARN("Closing database\n");

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

    _WARN("Done closing database\n");
}





BSeries::~BSeries()
{
    if(!this->shuttingDown){
        _ERROR("CRITICAL!! INVALID USAGE! BSERIES SHOULD BE CLOSED BEFORE BEING DECONSTRUCTED!");
        this->close();
    }

}
