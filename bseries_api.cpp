#include "bseries_api.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>


// ===========================================================================
// Small helpers
// ===========================================================================

static const char HEX_DIGITS[] = "0123456789abcdef";


std::string apiToHex(const void *data, size_t length){

    const unsigned char *bytes = (const unsigned char*)data;
    std::string out;
    out.resize(length * 2);

    for(size_t i = 0; i < length; i++){
        out[i*2]     = HEX_DIGITS[bytes[i] >> 4];
        out[i*2 + 1] = HEX_DIGITS[bytes[i] & 0x0F];
    }

    return out;
}


static int hexValue(char c){

    if(c >= '0' && c <= '9') return c - '0';
    if(c >= 'a' && c <= 'f') return c - 'a' + 10;
    if(c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}


/// Decodes hex, ignoring whitespace so a body can be wrapped across lines.
/// Returns false on a stray character or an odd number of digits.

bool apiFromHex(const std::string &text, std::string *out){

    out->clear();
    out->reserve(text.size() / 2);

    int high = -1;

    for(size_t i = 0; i < text.size(); i++){

        char c = text[i];

        if(c == ' ' || c == '\t' || c == '\r' || c == '\n')
            continue;

        int value = hexValue(c);

        if(value < 0)
            return false;

        if(high < 0){
            high = value;
        } else {
            out->push_back((char)((high << 4) | value));
            high = -1;
        }
    }

    return high < 0; // an odd digit left over is a truncated byte
}


/// Escapes a string for inclusion in a JSON document.

static std::string jsonEscape(const std::string &text){

    std::string out;
    out.reserve(text.size() + 8);

    for(size_t i = 0; i < text.size(); i++){

        unsigned char c = (unsigned char)text[i];

        switch(c){
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b";  break;
            case '\f': out += "\\f";  break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:
                if(c < 0x20){
                    char escape[8];
                    snprintf(escape,sizeof(escape),"\\u%04x",c);
                    out += escape;
                } else {
                    out += (char)c;
                }
        }
    }

    return out;
}


static void jsonError(HTTP_RESPONSE &response, int status, const char *error, const std::string &message){

    response.status = status;
    response.body = "{\"error\":\"";
    response.body += jsonEscape(error);
    response.body += "\",\"message\":\"";
    response.body += jsonEscape(message);
    response.body += "\"}";
}


/// Maps a database status code onto an HTTP status and a stable error slug.

static void jsonDatabaseError(HTTP_RESPONSE &response, int code, const char *action){

    char message[256];
    snprintf(message,sizeof(message),"%s failed with database status %d",action,code);

    switch(code){
        case SERIES_NOT_FOUND:
        case FAILED_TO_OPEN_FILE:
            jsonError(response,404,"series_not_found",message);
            return;
        case SERIES_ALREADY_EXISTS:
            jsonError(response,409,"series_exists",message);
            return;
        case SERIES_TYPE_MISMATCH:
            jsonError(response,422,"type_mismatch",message);
            return;
        case WRITE_BEFORE_SERIES_START:
            jsonError(response,422,"timestamp_before_series_start",message);
            return;
        case INVALID_TIME_RANGE:
            jsonError(response,400,"invalid_time_range",message);
            return;
        case INVALID_SERIES_DEFINITION:
            jsonError(response,400,"invalid_series_definition",message);
            return;
        case INVALID_HEADER_CHECKSUM:
        case FAILED_TO_READ_HEADER:
        case INVALID_SERIES_INTERVAL:
            jsonError(response,500,"corrupt_series",message);
            return;
        default:
            jsonError(response,500,"database_error",message);
            return;
    }
}


/// Constant time comparison, so a wrong key cannot be found a byte at a time.

static bool secretsMatch(const std::string &a, const std::string &b){

    if(a.empty() || b.empty())
        return false;

    if(a.size() != b.size())
        return false;

    unsigned char difference = 0;

    for(size_t i = 0; i < a.size(); i++)
        difference |= (unsigned char)(a[i] ^ b[i]);

    return difference == 0;
}


static bool parseUnsigned(const std::string &text, unsigned long *out){

    if(text.empty())
        return false;

    char *end = NULL;
    errno = 0;
    unsigned long value = strtoul(text.c_str(),&end,10);

    if(errno != 0 || end == text.c_str() || *end != 0)
        return false;

    *out = value;
    return true;
}


static bool parseSigned(const std::string &text, long long *out){

    if(text.empty())
        return false;

    char *end = NULL;
    errno = 0;
    long long value = strtoll(text.c_str(),&end,10);

    if(errno != 0 || end == text.c_str() || *end != 0)
        return false;

    *out = value;
    return true;
}


/// Splits a path into its non empty segments.

static std::vector<std::string> splitPath(const std::string &path){

    std::vector<std::string> segments;
    size_t position = 0;

    while(position < path.size()){

        size_t slash = path.find('/',position);
        if(slash == std::string::npos)
            slash = path.size();

        if(slash > position)
            segments.push_back(path.substr(position,slash - position));

        position = slash + 1;
    }

    return segments;
}


// ===========================================================================
// Configuration
// ===========================================================================

void apiConfigDefaults(API_CONFIG *config){

    config->bind_address = "127.0.0.1";
    config->port = 8086;
    config->data_directory = "";
    config->definitions_path = "";
    config->read_key = "";
    config->write_key = "";
    config->cors_origins.clear();
    config->max_points_per_read = 1000000;
    config->max_body_bytes = 1024 * 1024;
    config->max_connections = 64;
    config->write_ahead_size = 4096;
    config->default_interval = 10;
    config->series_max_idle_seconds = 900;
    config->maintenance_interval_seconds = 60;
}


int apiLoadConfig(const char *path, API_CONFIG *config, std::string *error_out){

    FILE *file = fopen(path,"r");

    if(file == NULL){
        if(error_out) *error_out = std::string("could not open ") + path;
        return DEFINITIONS_FILE_UNREADABLE;
    }

    // The file holds API keys, so say something if anyone else can read it
    struct stat details;
    if(stat(path,&details) == 0 && (details.st_mode & 0077) != 0)
        _WARN("Warning: %s is readable by other users and holds API keys\n",path);

    char line[1024];
    int line_number = 0;
    int status = NO_ERROR;

    while(fgets(line,sizeof(line),file) != NULL){

        line_number++;

        char *comment = strchr(line,'#');
        if(comment != NULL)
            *comment = 0;

        char name[128], value[512];
        int fields = sscanf(line,"%127s %511s",name,value);

        if(fields <= 0)
            continue;

        if(fields < 2){
            char message[256];
            snprintf(message,sizeof(message),"%s line %d: '%s' has no value",path,line_number,name);
            if(error_out) *error_out = message;
            status = INVALID_SERIES_DEFINITION;
            break;
        }

        std::string key(name);
        std::string text(value);
        unsigned long number = 0;

        if(key == "listen")                            config->bind_address = text;
        else if(key == "data_directory")               config->data_directory = text;
        else if(key == "definitions")                  config->definitions_path = text;
        else if(key == "read_key")                     config->read_key = text;
        else if(key == "write_key")                    config->write_key = text;
        else if(key == "cors_origin")                  config->cors_origins.push_back(text);
        else if(key == "port"                     && parseUnsigned(text,&number)) config->port = (int)number;
        else if(key == "max_points_per_read"      && parseUnsigned(text,&number)) config->max_points_per_read = (int)number;
        else if(key == "max_body_bytes"           && parseUnsigned(text,&number)) config->max_body_bytes = (int)number;
        else if(key == "max_connections"          && parseUnsigned(text,&number)) config->max_connections = (int)number;
        else if(key == "write_ahead_size"         && parseUnsigned(text,&number)) config->write_ahead_size = (int)number;
        else if(key == "default_interval"         && parseUnsigned(text,&number)) config->default_interval = (int)number;
        else if(key == "series_max_idle"          && parseUnsigned(text,&number)) config->series_max_idle_seconds = (int)number;
        else if(key == "maintenance_interval"     && parseUnsigned(text,&number)) config->maintenance_interval_seconds = (int)number;
        else {
            char message[256];
            snprintf(message,sizeof(message),"%s line %d: unknown or malformed setting '%s'",path,line_number,name);
            if(error_out) *error_out = message;
            status = INVALID_SERIES_DEFINITION;
            break;
        }
    }

    fclose(file);

    return status;
}


// ===========================================================================
// API
// ===========================================================================

BSeriesApi::BSeriesApi(BSeries *database, const API_CONFIG *configuration)
{
    db = database;
    config = *configuration;
}


void BSeriesApi::handle(const HTTP_REQUEST &request, HTTP_RESPONSE &response, void *context){

    BSeriesApi *api = (BSeriesApi*)context;
    api->route(request,response);
}


void BSeriesApi::applyCors(const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    if(config.cors_origins.empty())
        return;

    std::string origin = httpHeader(request,"origin");

    if(origin.empty())
        return;

    std::string allowed;

    for(size_t i = 0; i < config.cors_origins.size(); i++){

        if(config.cors_origins[i] == "*"){
            // No cookies are involved, so the wildcard is safe to send literally
            allowed = "*";
            break;
        }

        if(config.cors_origins[i] == origin){
            allowed = origin;
            break;
        }
    }

    if(allowed.empty())
        return;

    HTTP_HEADER header;

    header.name = "Access-Control-Allow-Origin";
    header.value = allowed;
    response.headers.push_back(header);

    if(allowed != "*"){
        // caches must not serve one origin's response to another
        header.name = "Vary";
        header.value = "Origin";
        response.headers.push_back(header);
    }

    header.name = "Access-Control-Allow-Methods";
    header.value = "GET, POST, PUT, DELETE, OPTIONS";
    response.headers.push_back(header);

    header.name = "Access-Control-Allow-Headers";
    header.value = "X-API-Key, Authorization, Content-Type";
    response.headers.push_back(header);

    header.name = "Access-Control-Max-Age";
    header.value = "600";
    response.headers.push_back(header);
}


/// Read endpoints accept either key, write endpoints require the write key. An
/// unset key in the configuration disables that level of access rather than
/// allowing everything through.

bool BSeriesApi::authorise(const HTTP_REQUEST &request, bool needs_write, HTTP_RESPONSE &response){

    std::string presented = httpHeader(request,"x-api-key");

    if(presented.empty()){

        std::string authorization = httpHeader(request,"authorization");

        if(authorization.compare(0,7,"Bearer ") == 0)
            presented = authorization.substr(7);
    }

    if(presented.empty()){
        jsonError(response,401,"unauthorized","supply an API key in the X-API-Key header");
        return false;
    }

    bool has_write = secretsMatch(presented,config.write_key);

    if(has_write)
        return true;

    if(needs_write){
        // Deliberately the same whether the key was the read key or nonsense
        jsonError(response,403,"forbidden","this endpoint requires the write key");
        return false;
    }

    if(secretsMatch(presented,config.read_key))
        return true;

    jsonError(response,401,"unauthorized","unrecognised API key");
    return false;
}


void BSeriesApi::route(const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    applyCors(request,response);

    if(request.method == "OPTIONS"){
        response.status = 204;
        response.body.clear();
        return;
    }

    std::vector<std::string> segments = splitPath(request.path);

    if(segments.size() < 2 || segments[0] != "v1"){
        jsonError(response,404,"not_found","unknown endpoint, the API is rooted at /v1");
        return;
    }

    if(segments[1] == "health" && segments.size() == 2){
        handleHealth(request,response); // unauthenticated, exposes no data
        return;
    }

    if(segments[1] != "series"){
        jsonError(response,404,"not_found","unknown endpoint");
        return;
    }

    // /v1/series
    if(segments.size() == 2){

        if(request.method != "GET"){
            jsonError(response,405,"method_not_allowed","use GET to list series");
            return;
        }

        if(!authorise(request,false,response))
            return;

        handleListSeries(request,response);
        return;
    }

    unsigned long key_value = 0;

    if(!parseUnsigned(segments[2],&key_value) || key_value > 0xFFFFFFFFuL){
        jsonError(response,400,"bad_key","a series key is an unsigned 32 bit number");
        return;
    }

    uint32_t key = (uint32_t)key_value;

    // /v1/series/{key}
    if(segments.size() == 3){

        if(request.method == "GET"){
            if(!authorise(request,false,response)) return;
            handleSeriesInfo(key,response);
            return;
        }

        if(request.method == "POST" || request.method == "PUT"){
            if(!authorise(request,true,response)) return;
            handleCreateSeries(key,request,response);
            return;
        }

        if(request.method == "DELETE"){
            if(!authorise(request,true,response)) return;
            handleDeleteSeries(key,response);
            return;
        }

        jsonError(response,405,"method_not_allowed","GET, POST, PUT or DELETE");
        return;
    }

    // /v1/series/{key}/data
    if(segments.size() == 4 && segments[3] == "data"){

        if(request.method == "GET"){
            if(!authorise(request,false,response)) return;
            handleReadData(key,request,response);
            return;
        }

        if(request.method == "POST" || request.method == "PUT"){
            if(!authorise(request,true,response)) return;
            handleWriteData(key,request,response);
            return;
        }

        jsonError(response,405,"method_not_allowed","GET, POST or PUT");
        return;
    }

    jsonError(response,404,"not_found","unknown endpoint");
}


void BSeriesApi::handleHealth(const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    (void)request;

    response.status = 200;
    response.body = "{\"status\":\"ok\"}";
}


/// The hex pattern one point wide that stands in for "nothing was recorded here".

static std::string nullFillPattern(unsigned char fill_byte, uint32_t datasize){

    std::string pattern(datasize,(char)fill_byte);
    return apiToHex(pattern.data(),pattern.size());
}


static void appendSeriesJson(std::string &out, uint32_t key, const SERIES &header, int64_t file_size){

    uint8_t datatype;
    uint32_t datasize;

    if(header.version == SERIES_VERSION_TYPED){
        datatype = bsTypeCodeDataType(header.typecode);
        datasize = bsTypeCodeDataSize(header.typecode);
    } else {
        datasize = header.typecode;
        datatype = (datasize == 4) ? BS_FLOAT : BS_UNSIGNED;
    }

    int64_t points_in_file = 0;
    if(datasize > 0 && file_size >= (int64_t)sizeof(SERIES))
        points_in_file = (file_size - (int64_t)sizeof(SERIES)) / datasize;

    char buffer[512];

    snprintf(buffer,sizeof(buffer),
        "{\"key\":%lu,\"version\":%lu,\"type\":\"%s\",\"datasize\":%lu,\"interval\":%lu,"
        "\"created\":%lu,\"points_in_file\":%lld,\"file_size\":%lld}",
        (unsigned long)key,
        (unsigned long)header.version,
        bsTypeName(datatype,(uint8_t)datasize),
        (unsigned long)datasize,
        (unsigned long)header.interval,
        (unsigned long)header.timestamp,
        (long long)points_in_file,
        (long long)file_size);

    out += buffer;
}


void BSeriesApi::handleSeriesInfo(uint32_t key, HTTP_RESPONSE &response){

    SERIES header;
    int64_t file_size = 0;

    int rc = db->seriesInfo(key,&header,&file_size);

    if(rc != NO_ERROR){
        jsonDatabaseError(response,rc,"reading the series header");
        return;
    }

    response.status = 200;
    appendSeriesJson(response.body,key,header,file_size);
}


void BSeriesApi::handleListSeries(const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    unsigned long after = 0;
    unsigned long limit = 100;

    std::string after_text = httpQueryParam(request,"after");
    std::string limit_text = httpQueryParam(request,"limit");

    if(!after_text.empty() && !parseUnsigned(after_text,&after)){
        jsonError(response,400,"bad_parameter","after must be an unsigned number");
        return;
    }

    if(!limit_text.empty() && (!parseUnsigned(limit_text,&limit) || limit == 0 || limit > 10000)){
        jsonError(response,400,"bad_parameter","limit must be between 1 and 10000");
        return;
    }

    std::vector<uint32_t> keys;
    int rc = db->listSeriesKeys(&keys,(uint32_t)after,(int)limit);

    if(rc < 0){
        jsonDatabaseError(response,rc,"listing series");
        return;
    }

    response.status = 200;
    response.body = "{\"series\":[";

    for(size_t i = 0; i < keys.size(); i++){

        SERIES header;
        int64_t file_size = 0;

        if(db->seriesInfo(keys[i],&header,&file_size) != NO_ERROR)
            continue; // vanished or unreadable between the listing and now

        if(response.body[response.body.size()-1] != '[')
            response.body += ",";

        appendSeriesJson(response.body,keys[i],header,file_size);
    }

    char tail[128];
    snprintf(tail,sizeof(tail),"],\"count\":%lu,\"next\":%lu}",
             (unsigned long)keys.size(),
             (unsigned long)(keys.empty() ? 0 : keys[keys.size()-1]));

    response.body += tail;
}


void BSeriesApi::handleCreateSeries(uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    std::string type_text = httpQueryParam(request,"type");
    std::string interval_text = httpQueryParam(request,"interval");

    uint8_t datatype = 0, datasize = 0;
    unsigned long interval = 0;

    if(type_text.empty()){
        jsonError(response,400,"bad_parameter","type is required, for example type=float32");
        return;
    }

    if(!bsTypeFromName(type_text.c_str(),&datatype,&datasize)){
        jsonError(response,400,"bad_parameter","unknown type '" + type_text + "'");
        return;
    }

    if(interval_text.empty()){
        interval = (unsigned long)config.default_interval;
    } else if(!parseUnsigned(interval_text,&interval) || interval == 0 || interval > 0xFFFFFFFFuL){
        jsonError(response,400,"bad_parameter","interval must be a positive number of seconds");
        return;
    }

    // Where the series' first point sits. Defaults to now; set it in the past to
    // prepare a series for a backfill, since a write before the series start is
    // refused rather than silently relocated.
    std::string start_text = httpQueryParam(request,"start");
    unsigned long start_timestamp = 0;

    if(!start_text.empty() && (!parseUnsigned(start_text,&start_timestamp) || start_timestamp == 0 || start_timestamp > 0xFFFFFFFFuL)){
        jsonError(response,400,"bad_parameter","start must be a unix timestamp that fits in 32 bits");
        return;
    }

    int rc = db->createSeriesFile(key,(uint32_t)interval,datatype,datasize,(uint32_t)start_timestamp);

    if(rc != NO_ERROR){
        jsonDatabaseError(response,rc,"creating the series");
        return;
    }

    SERIES header;
    int64_t file_size = 0;

    if(db->seriesInfo(key,&header,&file_size) != NO_ERROR){
        jsonError(response,500,"database_error","the series was created but could not be read back");
        return;
    }

    response.status = 201;
    appendSeriesJson(response.body,key,header,file_size);
}


void BSeriesApi::handleDeleteSeries(uint32_t key, HTTP_RESPONSE &response){

    int rc = db->deleteSeries(key);

    if(rc != NO_ERROR){
        jsonDatabaseError(response,rc,"deleting the series");
        return;
    }

    char buffer[128];
    snprintf(buffer,sizeof(buffer),"{\"key\":%lu,\"deleted\":true}",(unsigned long)key);

    response.status = 200;
    response.body = buffer;
}


void BSeriesApi::handleReadData(uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    std::string start_text = httpQueryParam(request,"start");
    std::string end_text = httpQueryParam(request,"end");

    long long start_time = 0;
    long long end_time = 0;

    if(start_text.empty()){
        jsonError(response,400,"bad_parameter","start is required, as a unix timestamp");
        return;
    }

    if(!parseSigned(start_text,&start_time) || start_time <= 0){
        jsonError(response,400,"bad_parameter","start must be a positive unix timestamp");
        return;
    }

    if(end_text.empty()){
        end_time = (long long)time(NULL);
    } else if(!parseSigned(end_text,&end_time) || end_time <= 0){
        jsonError(response,400,"bad_parameter","end must be a positive unix timestamp");
        return;
    }

    if(end_time < start_time){
        jsonError(response,400,"invalid_time_range","end must not be before start");
        return;
    }

    // The series has to exist before read() is called: read() creates an in memory
    // entry for whatever key it is handed, so probing unknown keys over HTTP would
    // otherwise be a way to grow the index without bound.
    SERIES header;
    int64_t file_size = 0;

    int info = db->seriesInfo(key,&header,&file_size);

    if(info != NO_ERROR){
        jsonDatabaseError(response,info,"reading the series header");
        return;
    }

    // Bound the response before allocating anything. The library will happily
    // allocate a point for every interval in the range asked for.
    if(header.interval > 0){

        long long points = (end_time - start_time) / (long long)header.interval;

        if(points > (long long)config.max_points_per_read){
            char message[256];
            snprintf(message,sizeof(message),
                     "the range covers %lld points, the limit is %d; narrow start and end",
                     points,config.max_points_per_read);
            jsonError(response,413,"range_too_large",message);
            return;
        }
    }

    int64_t n_points = 0, real_points = 0, seconds_per_point = 0, first_point_timestamp = 0;
    uint32_t datasize = 0;
    uint8_t datatype = BS_TYPE_INVALID;
    void *result = NULL;

    int rc = db->read(key,(int64_t)start_time,(int64_t)end_time,
                      &n_points,&real_points,&seconds_per_point,&first_point_timestamp,
                      &datasize,&result,&datatype);

    if(rc != NO_ERROR){
        if(result) delete[] (char*)result;
        jsonDatabaseError(response,rc,"reading points");
        return;
    }

    unsigned char fill = 0xFF;
    {
        // The fill actually used for this series, taken from the entry the read
        // just bound rather than guessed from the type.
        SERIES_DEFINITION definition;
        if(db->definitionForKey(key,&definition) && definition.datasize == datasize)
            fill = definition.null_fill_byte;
        else if(header.version == SERIES_VERSION_TYPED)
            fill = bsTypeNullFill(datatype,(uint8_t)datasize);
        else
            fill = (unsigned char)db->default_null_fill_byte;
    }

    // real_points from the library counts every point the file and cache windows
    // cover, gaps included. Count the points that differ from the fill instead, so
    // the number means what a caller would expect it to mean.
    int64_t recorded = 0;

    if(result != NULL && datasize > 0){

        const unsigned char *bytes = (const unsigned char*)result;

        for(int64_t point = 0; point < n_points; point++){

            bool is_fill = true;

            for(uint32_t byte = 0; byte < datasize; byte++){
                if(bytes[point*datasize + byte] != fill){
                    is_fill = false;
                    break;
                }
            }

            if(!is_fill)
                recorded++;
        }
    }

    char head[512];

    snprintf(head,sizeof(head),
        "{\"key\":%lu,\"type\":\"%s\",\"datasize\":%lu,\"interval\":%lld,"
        "\"start\":%lld,\"end\":%lld,\"first_point_timestamp\":%lld,"
        "\"n_points\":%lld,\"real_points\":%lld,\"null_fill\":\"%s\",\"data\":\"",
        (unsigned long)key,
        bsTypeName(datatype,(uint8_t)datasize),
        (unsigned long)datasize,
        (long long)seconds_per_point,
        start_time,
        end_time,
        (long long)first_point_timestamp,
        (long long)n_points,
        (long long)recorded,
        nullFillPattern(fill,datasize).c_str());

    response.status = 200;
    response.body = head;
    response.body += apiToHex(result,(size_t)(n_points * datasize));
    response.body += "\"}";

    delete[] (char*)result;
}


void BSeriesApi::handleWriteData(uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    std::string timestamp_text = httpQueryParam(request,"timestamp");
    long long timestamp = 0;

    if(timestamp_text.empty()){
        timestamp = (long long)time(NULL);
    } else if(!parseSigned(timestamp_text,&timestamp) || timestamp <= 0 || timestamp > 0xFFFFFFFFLL){
        jsonError(response,400,"bad_parameter","timestamp must be a unix timestamp that fits in 32 bits");
        return;
    }

    std::string points;

    if(!apiFromHex(request.body,&points)){
        jsonError(response,400,"bad_hex","the body must be hex encoded point data");
        return;
    }

    if(points.empty()){
        jsonError(response,400,"empty_body","no point data was supplied");
        return;
    }

    // The point width has to be known before the body can be cut into points. For a
    // series that does not exist yet that comes from the definitions file, and a
    // body of exactly one point is the only unambiguous case.
    uint32_t datasize = 0;

    SERIES header;
    int64_t file_size = 0;

    if(db->seriesInfo(key,&header,&file_size) == NO_ERROR){

        datasize = (header.version == SERIES_VERSION_TYPED)
                 ? bsTypeCodeDataSize(header.typecode)
                 : header.typecode;

    } else {

        SERIES_DEFINITION definition;

        if(db->definitionForKey(key,&definition)){
            datasize = definition.datasize;
        } else {
            datasize = (uint32_t)points.size(); // a single point of whatever was sent
        }
    }

    if(datasize == 0){
        jsonError(response,500,"corrupt_series","the series has a zero point width");
        return;
    }

    if(points.size() % datasize != 0){
        char message[256];
        snprintf(message,sizeof(message),
                 "the body is %lu bytes, which is not a whole number of %lu byte points",
                 (unsigned long)points.size(),(unsigned long)datasize);
        jsonError(response,422,"bad_point_width",message);
        return;
    }

    int64_t count = (int64_t)(points.size() / datasize);
    int64_t interval = (file_size > 0) ? (int64_t)header.interval : 0;

    if(interval == 0){
        SERIES_DEFINITION definition;
        interval = db->definitionForKey(key,&definition) ? (int64_t)definition.interval
                                                         : (int64_t)config.default_interval;
    }

    if(count > 1 && interval <= 0){
        jsonError(response,500,"corrupt_series","the series has a zero interval");
        return;
    }

    int64_t written = 0;

    for(int64_t point = 0; point < count; point++){

        long long point_time = timestamp + point * interval;

        if(point_time > 0xFFFFFFFFLL){
            jsonError(response,422,"timestamp_overflow","the run of points passes the end of 32 bit time");
            return;
        }

        int rc = db->write(key,(void*)(points.data() + point * datasize),datasize,(uint32_t)point_time);

        if(rc != NO_ERROR){

            if(written > 0){
                // Some points already landed, so this is a partial write and the
                // caller has to be told exactly how far it got.
                char message[256];
                snprintf(message,sizeof(message),
                         "wrote %lld of %lld points before failing with database status %d",
                         (long long)written,(long long)count,rc);
                jsonError(response,500,"partial_write",message);
                return;
            }

            jsonDatabaseError(response,rc,"writing points");
            return;
        }

        written++;
    }

    char buffer[256];

    snprintf(buffer,sizeof(buffer),
        "{\"key\":%lu,\"points_written\":%lld,\"first_timestamp\":%lld,\"last_timestamp\":%lld,\"interval\":%lld}",
        (unsigned long)key,
        (long long)written,
        timestamp,
        (long long)(timestamp + (written - 1) * interval),
        (long long)interval);

    response.status = 200;
    response.body = buffer;
}
