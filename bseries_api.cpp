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


/// Hex encodes onto a stream a slice at a time, so a large blob never exists as a
/// second full sized copy in memory alongside the points themselves.

static bool streamHex(HttpStream *stream, const void *data, size_t length){

    const unsigned char *bytes = (const unsigned char*)data;
    char buffer[8192];
    size_t position = 0;

    while(position < length){

        size_t take = length - position;

        if(take > sizeof(buffer) / 2)
            take = sizeof(buffer) / 2;

        for(size_t i = 0; i < take; i++){
            unsigned char value = bytes[position + i];
            buffer[i*2]     = HEX_DIGITS[value >> 4];
            buffer[i*2 + 1] = HEX_DIGITS[value & 0x0F];
        }

        if(!stream->write(buffer,take * 2))
            return false;

        position += take;
    }

    return true;
}


// ===========================================================================
// Condensing
//
// Downsampling a range into at most max_points buckets, for plotting a span far
// wider than a chart has pixels.
//
// Missing data is the whole difficulty. A series is dense, so a range covering a
// month of one second slots contains a slot for every second whether anything was
// recorded or not, and the ones that were not hold the null fill. Feeding those to
// an aggregate would be wrong in three separate ways:
//
//   * average would be pulled towards the fill value by however many slots were
//     empty, which for a sparsely written series is nearly all of them
//   * max over an unsigned series would return the fill itself, since the fill is
//     the maximum value of the type
//   * min over a float series would return NaN, and one NaN poisons every
//     comparison it takes part in
//
// So empty slots are excluded from every aggregate rather than being treated as a
// value. A bucket with no real points in it produces the fill, meaning "nothing was
// recorded anywhere in this bucket", which is exactly what a chart needs in order
// to draw a gap rather than a line through zero.
//
// Non finite floats are treated as missing too. The float fill is a NaN, but a
// client can also store a NaN or an infinity of its own, and either would wreck an
// aggregate the same way.
// ===========================================================================

#define CONDENSE_NONE 0
#define CONDENSE_MIN  1
#define CONDENSE_MAX  2
#define CONDENSE_AVG  3

static bool condenseModeFromName(const std::string &name, int *mode){

    if(name == "min")                          { *mode = CONDENSE_MIN; return true; }
    if(name == "max")                          { *mode = CONDENSE_MAX; return true; }
    if(name == "avg" || name == "average")     { *mode = CONDENSE_AVG; return true; }

    return false;
}


static const char *condenseModeName(int mode){

    switch(mode){
        case CONDENSE_MIN: return "min";
        case CONDENSE_MAX: return "max";
        case CONDENSE_AVG: return "average";
        default:           return "none";
    }
}


/// Reads one stored point as a double, for averaging.

static double pointToDouble(const unsigned char *p, uint8_t datatype, uint32_t datasize){

    if(datatype == BS_FLOAT){
        if(datasize == 4){ float v;  memcpy(&v,p,4); return (double)v; }
        double v; memcpy(&v,p,8); return v;
    }

    if(datatype == BS_SIGNED){
        if(datasize == 1){ int8_t v;  memcpy(&v,p,1); return (double)v; }
        if(datasize == 2){ int16_t v; memcpy(&v,p,2); return (double)v; }
        if(datasize == 4){ int32_t v; memcpy(&v,p,4); return (double)v; }
        int64_t v; memcpy(&v,p,8); return (double)v;
    }

    if(datasize == 1){ uint8_t v;  memcpy(&v,p,1); return (double)v; }
    if(datasize == 2){ uint16_t v; memcpy(&v,p,2); return (double)v; }
    if(datasize == 4){ uint32_t v; memcpy(&v,p,4); return (double)v; }

    uint64_t v; memcpy(&v,p,8); return (double)v;
}


/// Orders two stored points in their own type, so min and max keep the exact bytes
/// rather than a value that has been through a double.

static int comparePoints(const unsigned char *a, const unsigned char *b, uint8_t datatype, uint32_t datasize){

    if(datatype == BS_UNSIGNED || datatype == BS_SIGNED || datatype == BS_FLOAT){

        double x = pointToDouble(a,datatype,datasize);
        double y = pointToDouble(b,datatype,datasize);

        // 64 bit integers can exceed what a double holds exactly, so those are
        // compared in their own width.
        if(datasize == 8 && datatype == BS_UNSIGNED){
            uint64_t ua, ub;
            memcpy(&ua,a,8); memcpy(&ub,b,8);
            return ua < ub ? -1 : (ua > ub ? 1 : 0);
        }

        if(datasize == 8 && datatype == BS_SIGNED){
            int64_t ia, ib;
            memcpy(&ia,a,8); memcpy(&ib,b,8);
            return ia < ib ? -1 : (ia > ib ? 1 : 0);
        }

        return x < y ? -1 : (x > y ? 1 : 0);
    }

    return 0;
}


/// True when a slot holds no reading: the series' null fill, or a float that is not
/// finite. Either would corrupt an aggregate it took part in.

static bool pointIsMissing(const unsigned char *p, uint8_t datatype, uint32_t datasize, unsigned char fill){

    bool all_fill = true;

    for(uint32_t i = 0; i < datasize; i++){
        if(p[i] != fill){ all_fill = false; break; }
    }

    if(all_fill)
        return true;

    if(datatype == BS_FLOAT){

        double value = pointToDouble(p,datatype,datasize);

        // NaN != NaN, and an infinity would drag an average to infinity
        if(value != value)
            return true;

        if(value > 1.7976931348623157e308 || value < -1.7976931348623157e308)
            return true;
    }

    return false;
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


/// A stable machine readable name for a database status code.

static const char *databaseErrorSlug(int code){

    switch(code){
        case NO_ERROR:                     return "ok";
        case SERIES_NOT_FOUND:
        case FAILED_TO_OPEN_FILE:          return "series_not_found";
        case SERIES_ALREADY_EXISTS:        return "series_exists";
        case SERIES_TYPE_MISMATCH:         return "type_mismatch";
        case WRITE_BEFORE_SERIES_START:    return "timestamp_before_series_start";
        case SERIES_GROWTH_LIMIT:          return "timestamp_too_far_ahead";
        case INVALID_TIME_RANGE:           return "invalid_time_range";
        case INVALID_SERIES_DEFINITION:    return "invalid_series_definition";
        case INVALID_HEADER_CHECKSUM:
        case FAILED_TO_READ_HEADER:
        case INVALID_SERIES_INTERVAL:      return "corrupt_series";
        default:                           return "database_error";
    }
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
        case SERIES_GROWTH_LIMIT:
            jsonError(response,422,"timestamp_too_far_ahead",message);
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


/// Parses "10500", "1-500" or "1-50,10500,20000" into an explicit key list.
///
/// A range is expanded, so it is checked against the cap before anything is
/// allocated: keys=0-4000000000 must not become four billion entries.

static bool parseKeyList(const std::string &text, int max_keys, std::vector<uint32_t> *keys, std::string *error_out){

    size_t position = 0;

    while(position <= text.size()){

        size_t comma = text.find(',',position);
        if(comma == std::string::npos)
            comma = text.size();

        std::string item = text.substr(position,comma - position);
        position = comma + 1;

        if(item.empty())
            continue;

        unsigned long first = 0, last = 0;
        size_t dash = item.find('-');

        if(dash == std::string::npos){

            if(!parseUnsigned(item,&first) || first > 0xFFFFFFFFuL){
                *error_out = "'" + item + "' is not a series key";
                return false;
            }

            last = first;

        } else {

            if(!parseUnsigned(item.substr(0,dash),&first) ||
               !parseUnsigned(item.substr(dash + 1),&last) ||
               first > 0xFFFFFFFFuL || last > 0xFFFFFFFFuL){
                *error_out = "'" + item + "' is not a key range";
                return false;
            }

            if(last < first){
                *error_out = "'" + item + "' runs backwards";
                return false;
            }
        }

        if(last - first + 1 > (unsigned long)max_keys || keys->size() + (last - first + 1) > (size_t)max_keys){
            char message[192];
            snprintf(message,sizeof(message),"more than %d series requested, ask for fewer",max_keys);
            *error_out = message;
            return false;
        }

        for(unsigned long key = first; key <= last; key++)
            keys->push_back((uint32_t)key);
    }

    if(keys->empty()){
        *error_out = "no series keys were given";
        return false;
    }

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
    config->max_series_per_read = 500;
    config->max_points_per_write = 500000;
    config->max_grow_points = 1000000;
    config->flush_interval = 60;
    config->max_condense_scan = 50000000;
    config->condense_window_points = 262144;
    config->auto_create_tables = false;
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
        else if(key == "keystore")                     config->keystore_path = text;
        else if(key == "cors_origin")                  config->cors_origins.push_back(text);
        else if(key == "port"                     && parseUnsigned(text,&number)) config->port = (int)number;
        else if(key == "max_points_per_read"      && parseUnsigned(text,&number)) config->max_points_per_read = (int)number;
        else if(key == "max_series_per_read"      && parseUnsigned(text,&number)) config->max_series_per_read = (int)number;
        else if(key == "max_points_per_write"     && parseUnsigned(text,&number)) config->max_points_per_write = (int)number;
        else if(key == "max_grow_points"          && parseUnsigned(text,&number)) config->max_grow_points = (int)number;
        else if(key == "flush_interval"           && parseUnsigned(text,&number)) config->flush_interval = (int)number;
        else if(key == "max_condense_scan"        && parseUnsigned(text,&number)) config->max_condense_scan = (int)number;
        else if(key == "condense_window"          && parseUnsigned(text,&number)) config->condense_window_points = (int)number;
        else if(key == "auto_create_tables"       && parseUnsigned(text,&number)) config->auto_create_tables = (number != 0);
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

BSeriesApi::BSeriesApi(TableSet *table_set, AuthStore *auth_store, RUNTIME_SETTINGS *settings, const API_CONFIG *configuration)
{
    tables = table_set;
    auth = auth_store;
    runtime = settings;
    config = *configuration;
}


void BSeriesApi::handleReadSettings(HTTP_RESPONSE &response){

    response.status = 200;
    response.body = "{\"settings\":{";

    for(unsigned i = 0; i < RUNTIME_SETTING_FIELD_COUNT; i++){

        char entry[192];
        snprintf(entry,sizeof(entry),"%s\"%s\":%d",
                 i > 0 ? "," : "",
                 RUNTIME_SETTING_FIELDS[i].name,
                 (runtime->*(RUNTIME_SETTING_FIELDS[i].field)).load());

        response.body += entry;
    }

    response.body += "}}";
}


/// Changes settings while the server runs. Every named setting is validated before
/// any is applied, so a request that names one bad value changes nothing rather
/// than leaving the server half configured.
///
/// These live only in memory; the configuration file is not rewritten, so a
/// restart returns to what is written there.

void BSeriesApi::handleUpdateSettings(const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    int wanted[16];
    bool given[16];
    unsigned count = RUNTIME_SETTING_FIELD_COUNT;
    unsigned named = 0;

    for(unsigned i = 0; i < count && i < 16; i++){

        given[i] = false;

        bool present = false;
        std::string text = httpQueryParam(request,RUNTIME_SETTING_FIELDS[i].name,&present);

        if(!present)
            continue;

        unsigned long value = 0;

        if(!parseUnsigned(text,&value) || (long long)value < RUNTIME_SETTING_FIELDS[i].minimum ||
           (long long)value > RUNTIME_SETTING_FIELDS[i].maximum){

            char message[256];
            snprintf(message,sizeof(message),"%s must be between %d and %d",
                     RUNTIME_SETTING_FIELDS[i].name,
                     RUNTIME_SETTING_FIELDS[i].minimum,
                     RUNTIME_SETTING_FIELDS[i].maximum);

            jsonError(response,400,"bad_parameter",message);
            return;
        }

        wanted[i] = (int)value;
        given[i] = true;
        named++;
    }

    if(named == 0){
        jsonError(response,400,"bad_parameter","name at least one setting to change");
        return;
    }

    for(unsigned i = 0; i < count && i < 16; i++){
        if(given[i])
            (runtime->*(RUNTIME_SETTING_FIELDS[i].field)).store(wanted[i]);
    }

    handleReadSettings(response);
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


/// Read endpoints accept either role, write endpoints require a write key.
///
/// Keys come from two places: the configuration file, which is how this worked
/// before keys could be managed over HTTP and still works, and the keystore, which
/// is where keys minted through the API live. An unset configuration key disables
/// that level of access rather than allowing everything through.

bool BSeriesApi::authorise(const HTTP_REQUEST &request, bool needs_write, HTTP_RESPONSE &response, bool allow_bootstrap){

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

    int granted = AUTH_ROLE_NONE;

    if(secretsMatch(presented,config.write_key))
        granted = AUTH_ROLE_WRITE;
    else if(secretsMatch(presented,config.read_key))
        granted = AUTH_ROLE_READ;

    if(granted == AUTH_ROLE_NONE && auth != NULL)
        granted = auth->roleFor(presented);

    // The bootstrap token exists only to create the first key. It is printed to
    // the server's log, so it is not accepted anywhere else.
    if(granted == AUTH_ROLE_NONE && allow_bootstrap && auth != NULL && auth->bootstrapTokenMatches(presented))
        granted = AUTH_ROLE_WRITE;

    if(granted == AUTH_ROLE_WRITE)
        return true;

    if(needs_write){
        // Deliberately the same whether a read key was presented or nonsense
        jsonError(response,403,"forbidden","this endpoint requires a write key");
        return false;
    }

    if(granted == AUTH_ROLE_READ)
        return true;

    jsonError(response,401,"unauthorized","unrecognised API key");
    return false;
}


void BSeriesApi::handleListKeys(HTTP_RESPONSE &response){

    std::vector<AUTH_KEY> keys;
    auth->list(&keys);

    response.status = 200;
    response.body = "{\"keys\":[";

    for(size_t i = 0; i < keys.size(); i++){

        char entry[320];
        snprintf(entry,sizeof(entry),"%s{\"name\":\"%s\",\"role\":\"%s\",\"created\":%lu}",
                 i > 0 ? "," : "",
                 jsonEscape(keys[i].name).c_str(),
                 AuthStore::roleName(keys[i].role),
                 (unsigned long)keys[i].created);

        response.body += entry;
    }

    char tail[128];
    snprintf(tail,sizeof(tail),"],\"count\":%lu}",(unsigned long)keys.size());
    response.body += tail;
}


/// Mints a key. The secret is in the response and nowhere else: it is stored
/// hashed, so this is the only time it can ever be read.

void BSeriesApi::handleCreateKey(const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    std::string name = httpQueryParam(request,"name");
    std::string role_text = httpQueryParam(request,"role");
    std::string error;

    if(!AuthStore::validKeyName(name,&error)){
        jsonError(response,400,"bad_parameter",error);
        return;
    }

    int role = AUTH_ROLE_NONE;

    if(!AuthStore::roleFromName(role_text,&role)){
        jsonError(response,400,"bad_parameter","role must be read or write");
        return;
    }

    std::string secret;
    int rc = auth->create(name,role,&secret);

    if(rc == SERIES_ALREADY_EXISTS){
        jsonError(response,409,"key_exists","a key named '" + name + "' already exists");
        return;
    }

    if(rc != NO_ERROR){
        jsonDatabaseError(response,rc,"creating the key");
        return;
    }

    char buffer[512];
    snprintf(buffer,sizeof(buffer),
        "{\"name\":\"%s\",\"role\":\"%s\",\"key\":\"%s\","
        "\"note\":\"this is the only time the key is shown; it is stored hashed\"}",
        jsonEscape(name).c_str(),AuthStore::roleName(role),secret.c_str());

    response.status = 201;
    response.body = buffer;
}


void BSeriesApi::handleRevokeKey(const std::string &name, HTTP_RESPONSE &response){

    int rc = auth->revoke(name,!config.write_key.empty());

    if(rc == SERIES_NOT_FOUND){
        jsonError(response,404,"key_not_found","no key named '" + name + "'");
        return;
    }

    if(rc == SERIES_ALREADY_EXISTS){
        jsonError(response,409,"last_write_key",
                  "this is the only write key; revoking it would lock the database out of its own administration");
        return;
    }

    if(rc != NO_ERROR){
        jsonDatabaseError(response,rc,"revoking the key");
        return;
    }

    char buffer[192];
    snprintf(buffer,sizeof(buffer),"{\"name\":\"%s\",\"revoked\":true}",jsonEscape(name).c_str());

    response.status = 200;
    response.body = buffer;
}


/// Resolves a table name to its database.
///
/// A write to a table that does not exist is a 404 unless auto_create_tables is
/// on, so a typo in a table name cannot quietly start a second copy of a
/// database that nobody is reading from.

BSeries *BSeriesApi::resolveTable(const std::string &name, bool for_write, HTTP_RESPONSE &response){

    std::string error;

    if(!TableSet::validName(name,&error)){
        jsonError(response,400,"bad_table",error);
        return NULL;
    }

    BSeries *db = tables->open(name);

    if(db != NULL)
        return db;

    if(for_write && config.auto_create_tables){

        int rc = tables->create(name);

        if(rc == NO_ERROR || rc == SERIES_ALREADY_EXISTS)
            db = tables->open(name);

        if(db != NULL)
            return db;
    }

    jsonError(response,404,"table_not_found",
              "no table '" + name + "'; create it with POST /v1/tables/" + name);
    return NULL;
}


void BSeriesApi::handleListTables(const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    (void)request;

    std::vector<std::string> names;

    if(tables->list(&names) < 0){
        jsonError(response,500,"database_error","could not list tables");
        return;
    }

    response.status = 200;
    response.body = "{\"tables\":[";

    for(size_t i = 0; i < names.size(); i++){

        if(i > 0)
            response.body += ",";

        response.body += "\"" + jsonEscape(names[i]) + "\"";
    }

    char tail[64];
    snprintf(tail,sizeof(tail),"],\"count\":%lu}",(unsigned long)names.size());
    response.body += tail;
}


void BSeriesApi::handleTableInfo(const std::string &name, HTTP_RESPONSE &response){

    std::string error;

    if(!TableSet::validName(name,&error)){
        jsonError(response,400,"bad_table",error);
        return;
    }

    if(!tables->exists(name)){
        jsonError(response,404,"table_not_found","no table '" + name + "'");
        return;
    }

    BSeries *db = tables->open(name);
    std::vector<uint32_t> keys;
    int count = (db != NULL) ? db->listSeriesKeys(&keys,0,1000000) : 0;

    char buffer[256];
    snprintf(buffer,sizeof(buffer),"{\"table\":\"%s\",\"series\":%d}",
             jsonEscape(name).c_str(), count < 0 ? 0 : count);

    response.status = 200;
    response.body = buffer;
}


void BSeriesApi::handleCreateTable(const std::string &name, HTTP_RESPONSE &response){

    std::string error;

    if(!TableSet::validName(name,&error)){
        jsonError(response,400,"bad_table",error);
        return;
    }

    int rc = tables->create(name);

    if(rc == SERIES_ALREADY_EXISTS){
        jsonError(response,409,"table_exists","table '" + name + "' already exists");
        return;
    }

    if(rc != NO_ERROR){
        jsonDatabaseError(response,rc,"creating the table");
        return;
    }

    char buffer[192];
    snprintf(buffer,sizeof(buffer),"{\"table\":\"%s\",\"created\":true}",jsonEscape(name).c_str());

    response.status = 201;
    response.body = buffer;
}


/// Dropping a table removes every series in it, so it refuses unless the table is
/// already empty. force=1 says to remove the series too, and is the only way to
/// destroy data through this endpoint.

void BSeriesApi::handleDropTable(const std::string &name, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    std::string error;

    if(!TableSet::validName(name,&error)){
        jsonError(response,400,"bad_table",error);
        return;
    }

    if(name == DEFAULT_TABLE_NAME){
        jsonError(response,400,"bad_table","the default table is the data directory itself and cannot be dropped");
        return;
    }

    bool force = false;
    std::string force_text = httpQueryParam(request,"force");

    if(!force_text.empty() && force_text != "0" && force_text != "false")
        force = true;

    int rc = tables->drop(name,force);

    if(rc == SERIES_NOT_FOUND){
        jsonError(response,404,"table_not_found","no table '" + name + "'");
        return;
    }

    if(rc == SERIES_ALREADY_EXISTS){
        jsonError(response,409,"table_not_empty",
                  "table '" + name + "' still holds series; pass force=1 to delete them with it");
        return;
    }

    if(rc != NO_ERROR){
        jsonDatabaseError(response,rc,"dropping the table");
        return;
    }

    char buffer[192];
    snprintf(buffer,sizeof(buffer),"{\"table\":\"%s\",\"dropped\":true}",jsonEscape(name).c_str());

    response.status = 200;
    response.body = buffer;
}


/// The endpoints inside one table. rest is the path after the table name.

void BSeriesApi::routeTable(BSeries *db, const std::vector<std::string> &rest, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    if(rest.empty()){
        jsonError(response,404,"not_found","expected /series, /data or /now under a table");
        return;
    }

    // <table>/now - one value per series, into the slot nearest the server clock
    if(rest[0] == "now" && rest.size() == 1){

        if(request.method != "POST" && request.method != "PUT"){
            jsonError(response,405,"method_not_allowed","use POST to push a reading into every series");
            return;
        }

        if(!authorise(request,true,response))
            return;

        handleWriteNow(db,request,response);
        return;
    }

    // <table>/data - several series in one request, read or write
    if(rest[0] == "data" && rest.size() == 1){

        if(request.method == "GET"){
            if(!authorise(request,false,response)) return;
            handleMultiRead(db,request,response);
            return;
        }

        if(request.method == "POST" || request.method == "PUT"){
            if(!authorise(request,true,response)) return;
            handleBatchWrite(db,request,response);
            return;
        }

        jsonError(response,405,"method_not_allowed","GET to read, POST to write");
        return;
    }

    if(rest[0] != "series"){
        jsonError(response,404,"not_found","unknown endpoint");
        return;
    }

    // <table>/series
    if(rest.size() == 1){

        if(request.method != "GET"){
            jsonError(response,405,"method_not_allowed","use GET to list series");
            return;
        }

        if(!authorise(request,false,response))
            return;

        handleListSeries(db,request,response);
        return;
    }

    unsigned long key_value = 0;

    if(!parseUnsigned(rest[1],&key_value) || key_value > 0xFFFFFFFFuL){
        jsonError(response,400,"bad_key","a series key is an unsigned 32 bit number");
        return;
    }

    uint32_t key = (uint32_t)key_value;

    // <table>/series/{key}
    if(rest.size() == 2){

        if(request.method == "GET"){
            if(!authorise(request,false,response)) return;
            handleSeriesInfo(db,key,response);
            return;
        }

        if(request.method == "POST" || request.method == "PUT"){
            if(!authorise(request,true,response)) return;
            handleCreateSeries(db,key,request,response);
            return;
        }

        if(request.method == "DELETE"){
            if(!authorise(request,true,response)) return;
            handleDeleteSeries(db,key,response);
            return;
        }

        jsonError(response,405,"method_not_allowed","GET, POST, PUT or DELETE");
        return;
    }

    // <table>/series/{key}/data
    if(rest.size() == 3 && rest[2] == "data"){

        if(request.method == "GET"){
            if(!authorise(request,false,response)) return;
            handleReadData(db,key,request,response);
            return;
        }

        if(request.method == "POST" || request.method == "PUT"){
            if(!authorise(request,true,response)) return;
            handleWriteData(db,key,request,response);
            return;
        }

        jsonError(response,405,"method_not_allowed","GET, POST or PUT");
        return;
    }

    jsonError(response,404,"not_found","unknown endpoint");
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

    // /v1/config - settings that can change while the server runs
    if(segments[1] == "config" && segments.size() == 2){

        if(request.method == "GET"){
            if(!authorise(request,false,response)) return;
            handleReadSettings(response);
            return;
        }

        if(request.method == "POST" || request.method == "PUT" || request.method == "PATCH"){
            if(!authorise(request,true,response)) return;
            handleUpdateSettings(request,response);
            return;
        }

        jsonError(response,405,"method_not_allowed","GET to read, POST to change");
        return;
    }

    // /v1/auth - the API keys themselves
    if(segments[1] == "auth"){

        if(segments.size() < 3 || segments[2] != "keys"){
            jsonError(response,404,"not_found","unknown endpoint");
            return;
        }

        if(segments.size() == 3){

            if(request.method == "GET"){
                if(!authorise(request,true,response)) return;
                handleListKeys(response);
                return;
            }

            if(request.method == "POST" || request.method == "PUT"){
                // The one place the bootstrap token is accepted
                if(!authorise(request,true,response,true)) return;
                handleCreateKey(request,response);
                return;
            }

            jsonError(response,405,"method_not_allowed","GET to list, POST to create");
            return;
        }

        if(segments.size() == 4 && request.method == "DELETE"){
            if(!authorise(request,true,response)) return;
            handleRevokeKey(segments[3],response);
            return;
        }

        jsonError(response,404,"not_found","unknown endpoint");
        return;
    }

    // /v1/tables - the tables themselves
    if(segments[1] == "tables"){

        if(segments.size() == 2){

            if(request.method != "GET"){
                jsonError(response,405,"method_not_allowed","use GET to list tables");
                return;
            }

            if(!authorise(request,false,response))
                return;

            handleListTables(request,response);
            return;
        }

        if(segments.size() == 3){

            if(request.method == "GET"){
                if(!authorise(request,false,response)) return;
                handleTableInfo(segments[2],response);
                return;
            }

            if(request.method == "POST" || request.method == "PUT"){
                if(!authorise(request,true,response)) return;
                handleCreateTable(segments[2],response);
                return;
            }

            if(request.method == "DELETE"){
                if(!authorise(request,true,response)) return;
                handleDropTable(segments[2],request,response);
                return;
            }

            jsonError(response,405,"method_not_allowed","GET, POST, PUT or DELETE");
            return;
        }

        jsonError(response,404,"not_found","unknown endpoint");
        return;
    }

    std::vector<std::string> rest;
    std::string table;

    if(segments[1] == "series" || segments[1] == "data" || segments[1] == "now"){

        // The unqualified paths that predate tables, which address the default
        // table. Kept so existing clients keep working against existing data.
        table = DEFAULT_TABLE_NAME;
        rest.assign(segments.begin() + 1,segments.end());

    } else {

        table = segments[1];
        rest.assign(segments.begin() + 2,segments.end());
    }

    // Whether this request writes decides whether an unknown table may be created
    bool writes = (request.method == "POST" || request.method == "PUT" || request.method == "DELETE");

    BSeries *db = resolveTable(table,writes,response);

    if(db == NULL)
        return;

    routeTable(db,rest,request,response);
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

    uint8_t datatype = bsHeaderDataType(&header);
    uint32_t datasize = bsHeaderDataSize(&header);

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


void BSeriesApi::handleSeriesInfo(BSeries *db, uint32_t key, HTTP_RESPONSE &response){

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


void BSeriesApi::handleListSeries(BSeries *db, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    std::vector<uint32_t> keys;
    bool selected = false;

    // keys= selects specific series, the same selector every read endpoint takes:
    // a comma separated list which may contain N-M ranges. Without it the data
    // directory is walked in key order.
    std::string keys_text = httpQueryParam(request,"keys");

    if(!keys_text.empty()){

        std::string parse_error;

        if(!parseKeyList(keys_text,runtime->max_series_per_read.load(),&keys,&parse_error)){
            jsonError(response,400,"bad_parameter",parse_error);
            return;
        }

        selected = true;

    } else {

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

        int rc = db->listSeriesKeys(&keys,(uint32_t)after,(int)limit);

        if(rc < 0){
            jsonDatabaseError(response,rc,"listing series");
            return;
        }
    }

    bool skip_missing = false;
    std::string skip_text = httpQueryParam(request,"skip_missing");

    if(!skip_text.empty() && skip_text != "0" && skip_text != "false")
        skip_missing = true;

    HttpStream *stream = response.stream;

    stream->begin(200,"application/json",response.headers);
    stream->write("{\"series\":[",11);

    size_t returned = 0;
    uint32_t last_key = 0;

    for(size_t i = 0; i < keys.size(); i++){

        SERIES header;
        int64_t file_size = 0;

        int info = db->seriesInfo(keys[i],&header,&file_size);

        // A key from a directory walk that has vanished since is simply gone; one
        // the caller named explicitly is reported unless they asked otherwise.
        if(info != NO_ERROR && (skip_missing || !selected))
            continue;

        if(returned > 0)
            stream->write(",",1);

        if(info != NO_ERROR){

            char entry[256];
            snprintf(entry,sizeof(entry),"{\"key\":%lu,\"error\":\"%s\",\"status\":%d}",
                     (unsigned long)keys[i],databaseErrorSlug(info),info);
            stream->write(entry,strlen(entry));

        } else {

            std::string object;
            appendSeriesJson(object,keys[i],header,file_size);
            stream->write(object);
        }

        last_key = keys[i];
        returned++;
    }

    char tail[128];

    if(selected)
        snprintf(tail,sizeof(tail),"],\"count\":%lu}",(unsigned long)returned);
    else
        snprintf(tail,sizeof(tail),"],\"count\":%lu,\"next\":%lu}",
                 (unsigned long)returned,(unsigned long)last_key);

    stream->write(tail,strlen(tail));
}





void BSeriesApi::handleCreateSeries(BSeries *db, uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

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


void BSeriesApi::handleDeleteSeries(BSeries *db, uint32_t key, HTTP_RESPONSE &response){

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

/// Streams the JSON object for one series over one time range, data blob and all.
///
/// The whole range comes back as a single hex blob rather than a point per array
/// element: a day of one second points is 86400 values, and wrapping each in JSON
/// punctuation costs several times what the data itself does. The array in a multi
/// series response holds one of these objects per series, not one per point.
///
/// Writes the object's fields without the surrounding braces, so the single series
/// response can carry them flattened alongside the range while the multi series
/// response wraps each set in its own object.
///
/// Returns a database status. Nothing is written to the stream unless it is
/// NO_ERROR, so a caller can still turn a failure into an error entry.

int BSeriesApi::streamSeriesData(BSeries *db, uint32_t key, long long start_time, long long end_time, HttpStream *stream){

    SERIES header;
    int64_t file_size = 0;

    // The series has to exist before read() is called: read() creates an in memory
    // entry for whatever key it is handed, so probing unknown keys over HTTP would
    // otherwise be a way to grow the index without bound.
    int info = db->seriesInfo(key,&header,&file_size);

    if(info != NO_ERROR)
        return info;

    int64_t n_points = 0, real_points = 0, seconds_per_point = 0, first_point_timestamp = 0;
    uint32_t datasize = 0;
    uint8_t datatype = BS_TYPE_INVALID;
    void *result = NULL;

    int rc = db->read(key,(int64_t)start_time,(int64_t)end_time,
                      &n_points,&real_points,&seconds_per_point,&first_point_timestamp,
                      &datasize,&result,&datatype);

    if(rc != NO_ERROR){
        if(result) delete[] (char*)result;
        return rc;
    }

    unsigned char fill = db->resolveNullFill(key,&header);

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
        "\"key\":%lu,\"type\":\"%s\",\"datasize\":%lu,\"interval\":%lld,"
        "\"first_point_timestamp\":%lld,\"n_points\":%lld,\"real_points\":%lld,"
        "\"null_fill\":\"%s\",\"data\":\"",
        (unsigned long)key,
        bsTypeName(datatype,(uint8_t)datasize),
        (unsigned long)datasize,
        (long long)seconds_per_point,
        (long long)first_point_timestamp,
        (long long)n_points,
        (long long)recorded,
        nullFillPattern(fill,datasize).c_str());

    stream->write(head,strlen(head));
    streamHex(stream,result,(size_t)(n_points * datasize));
    stream->write("\"",1);

    delete[] (char*)result;

    return NO_ERROR;
}



/// Appends one finished bucket's value.
///
/// A bucket with no real samples in it produces the null fill, which is what tells
/// a chart to draw a gap rather than a line through some invented value.

static void emitCondensedBucket(std::string &out, int mode, long long samples, double sum,
                                const unsigned char *best, uint8_t src_datatype, uint32_t src_datasize,
                                uint32_t out_datasize, unsigned char out_fill){

    (void)src_datatype;
    (void)src_datasize;

    if(samples == 0){
        out.append((size_t)out_datasize,(char)out_fill);
        return;
    }

    if(mode == CONDENSE_AVG){
        double average = sum / (double)samples;
        char bytes[8];
        memcpy(bytes,&average,8);
        out.append(bytes,8);
        return;
    }

    out.append((const char*)best,(size_t)out_datasize);
}


/// Streams one series condensed into at most max_points buckets.
///
/// The range is walked a window at a time rather than read whole, so a condensed
/// read of a year of one second points costs a window of memory, not a year of it.
/// A bucket's running state carries across window boundaries, so the window size
/// does not have to be a multiple of anything.
///
/// Returns a database status. Nothing is written unless it is NO_ERROR.

int BSeriesApi::streamCondensedSeries(BSeries *db, uint32_t key, long long start_time, long long end_time,
                                      int mode, long long max_points, HttpStream *stream,
                                      long long *scanned_out){

    SERIES header;
    int64_t file_size = 0;

    int info = db->seriesInfo(key,&header,&file_size);

    if(info != NO_ERROR)
        return info;

    if(header.interval == 0)
        return INVALID_SERIES_INTERVAL;

    long long interval = (long long)header.interval;

    // Align to the series' own grid, the same way a raw read does, so bucket
    // boundaries land on slot boundaries.
    long long base;
    {
        long long first_slot = (start_time <= (long long)header.timestamp)
            ? -(((long long)header.timestamp - start_time) / interval)
            : (start_time - (long long)header.timestamp) / interval;

        base = (long long)header.timestamp + first_slot * interval;
    }

    long long total_slots = (end_time - start_time) / interval;

    if(total_slots < 1)
        total_slots = 1;

    long long factor = (total_slots + max_points - 1) / max_points;

    if(factor < 1)
        factor = 1;

    long long bucket_count = (total_slots + factor - 1) / factor;
    long long bucket_interval = interval * factor;

    // min and max hand back a stored point untouched, so the output keeps the
    // source's type and fill. An average is not generally representable in the
    // source type, so it is promoted to float64, which holds every value of every
    // narrower type exactly; 0xFF over a float64 is a NaN, which is the sentinel
    // that costs nothing.
    uint8_t src_datatype = bsHeaderDataType(&header);
    uint32_t src_datasize = bsHeaderDataSize(&header);
    unsigned char src_fill = db->resolveNullFill(key,&header);

    uint8_t out_datatype = (mode == CONDENSE_AVG) ? BS_FLOAT : src_datatype;
    uint32_t out_datasize = (mode == CONDENSE_AVG) ? 8 : src_datasize;
    unsigned char out_fill = (mode == CONDENSE_AVG) ? 0xFF : src_fill;


    // Everything below only appends, so build the head first.
    char head[640];

    snprintf(head,sizeof(head),
        "\"key\":%lu,\"type\":\"%s\",\"datasize\":%lu,\"interval\":%lld,"
        "\"source_interval\":%lld,\"condense\":\"%s\",\"factor\":%lld,"
        "\"first_point_timestamp\":%lld,\"n_points\":%lld,",
        (unsigned long)key,
        bsTypeName(out_datatype,(uint8_t)out_datasize),
        (unsigned long)out_datasize,
        bucket_interval,
        interval,
        condenseModeName(mode),
        factor,
        base,
        bucket_count);

    std::string prefix = head;

    // The counts are only known once every bucket has been walked, and the blob is
    // streamed as it is produced, so they are reported after it rather than before.
    stream->write(prefix);
    stream->write("\"data\":\"",8);

    long long buckets_with_data = 0;
    long long points_scanned = 0;
    long long real_points = 0;

    // State of the bucket currently being filled, carried across windows.
    long long bucket_index = 0;
    long long bucket_samples = 0;
    double bucket_sum = 0.0;
    unsigned char bucket_best[8];
    bool bucket_has_best = false;

    std::string out_bytes;
    out_bytes.reserve(4096);

    long long slot = 0;
    int status = NO_ERROR;

    while(slot < total_slots && status == NO_ERROR){

        long long window_slots = (long long)config.condense_window_points;

        if(window_slots < 1)
            window_slots = 262144;

        if(window_slots > total_slots - slot)
            window_slots = total_slots - slot;

        long long window_start = base + slot * interval;
        long long window_end = window_start + window_slots * interval;

        int64_t n=0, r=0, spp=0, fpt=0;
        uint32_t ds=0;
        uint8_t dt=0;
        void *result = NULL;

        int rc = db->read(key,window_start,window_end,&n,&r,&spp,&fpt,&ds,&result,&dt);

        if(rc != NO_ERROR){
            if(result) delete[] (char*)result;
            status = rc;
            break;
        }

        const unsigned char *bytes = (const unsigned char*)result;

        for(int64_t i = 0; i < n && slot + i < total_slots; i++){

            long long global_slot = slot + i;
            long long belongs_to = global_slot / factor;

            if(belongs_to != bucket_index){

                // Emit the bucket that just closed
                emitCondensedBucket(out_bytes,mode,bucket_samples,bucket_sum,
                                    bucket_has_best ? bucket_best : NULL,
                                    src_datatype,src_datasize,out_datasize,out_fill);

                if(bucket_samples > 0)
                    buckets_with_data++;

                bucket_index = belongs_to;
                bucket_samples = 0;
                bucket_sum = 0.0;
                bucket_has_best = false;

                if(out_bytes.size() >= 4096){
                    streamHex(stream,out_bytes.data(),out_bytes.size());
                    out_bytes.clear();
                }
            }

            const unsigned char *point = bytes + i * ds;
            points_scanned++;

            if(pointIsMissing(point,src_datatype,ds,src_fill))
                continue;   // an empty slot takes no part in the aggregate

            real_points++;
            bucket_samples++;

            if(mode == CONDENSE_AVG){

                bucket_sum += pointToDouble(point,src_datatype,ds);

            } else if(!bucket_has_best){

                memcpy(bucket_best,point,ds);
                bucket_has_best = true;

            } else {

                int order = comparePoints(point,bucket_best,src_datatype,ds);

                if((mode == CONDENSE_MIN && order < 0) || (mode == CONDENSE_MAX && order > 0))
                    memcpy(bucket_best,point,ds);
            }
        }

        delete[] (char*)result;
        slot += window_slots;
    }

    if(status != NO_ERROR)
        return status;   // the head is already out, the caller reports it in the body

    // The final bucket never sees a boundary, so it is closed here.
    emitCondensedBucket(out_bytes,mode,bucket_samples,bucket_sum,
                        bucket_has_best ? bucket_best : NULL,
                        src_datatype,src_datasize,out_datasize,out_fill);

    if(bucket_samples > 0)
        buckets_with_data++;

    if(!out_bytes.empty())
        streamHex(stream,out_bytes.data(),out_bytes.size());

    char tail[320];

    snprintf(tail,sizeof(tail),
        "\",\"real_points\":%lld,\"points_scanned\":%lld,\"source_points\":%lld,\"null_fill\":\"%s\"",
        buckets_with_data,
        points_scanned,
        real_points,
        nullFillPattern(out_fill,out_datasize).c_str());

    stream->write(tail,strlen(tail));

    if(scanned_out != NULL)
        *scanned_out = points_scanned;

    return NO_ERROR;
}


/// Reads max_points and condense from the query string.
///
/// They are a pair: a bucket count means nothing without saying how to combine
/// what falls in a bucket, and an operation means nothing without a bucket count.
/// Supplying one without the other is refused rather than guessed at.
///
/// Sets mode to CONDENSE_NONE when neither is present.

bool BSeriesApi::readCondenseOptions(const HTTP_REQUEST &request, HTTP_RESPONSE &response, int *mode, long long *max_points){

    *mode = CONDENSE_NONE;
    *max_points = 0;

    std::string max_text = httpQueryParam(request,"max_points");
    std::string condense_text = httpQueryParam(request,"condense");

    if(max_text.empty() && condense_text.empty())
        return true;

    if(max_text.empty() || condense_text.empty()){
        jsonError(response,400,"bad_parameter",
                  "max_points and condense are used together; supply both or neither");
        return false;
    }

    unsigned long value = 0;

    if(!parseUnsigned(max_text,&value) || value == 0){
        jsonError(response,400,"bad_parameter","max_points must be a positive number of buckets");
        return false;
    }

    if(value > (unsigned long)runtime->max_points_per_read.load()){
        char message[192];
        snprintf(message,sizeof(message),"max_points may not exceed max_points_per_read, which is %d",
                 runtime->max_points_per_read.load());
        jsonError(response,400,"bad_parameter",message);
        return false;
    }

    if(!condenseModeFromName(condense_text,mode)){
        jsonError(response,400,"bad_parameter","condense must be min, max or average");
        return false;
    }

    *max_points = (long long)value;
    return true;
}


/// Reads start and end from the query string. end defaults to now.

bool BSeriesApi::readTimeRange(const HTTP_REQUEST &request, HTTP_RESPONSE &response, long long *start_time, long long *end_time){

    std::string start_text = httpQueryParam(request,"start");
    std::string end_text = httpQueryParam(request,"end");

    if(start_text.empty()){
        jsonError(response,400,"bad_parameter","start is required, as a unix timestamp");
        return false;
    }

    if(!parseSigned(start_text,start_time) || *start_time <= 0){
        jsonError(response,400,"bad_parameter","start must be a positive unix timestamp");
        return false;
    }

    if(end_text.empty()){
        *end_time = (long long)time(NULL);
    } else if(!parseSigned(end_text,end_time) || *end_time <= 0){
        jsonError(response,400,"bad_parameter","end must be a positive unix timestamp");
        return false;
    }

    if(*end_time < *start_time){
        jsonError(response,400,"invalid_time_range","end must not be before start");
        return false;
    }

    return true;
}


/// How many points the range covers for one series, or 0 if it cannot be read.

int64_t BSeriesApi::pointsInRange(BSeries *db, uint32_t key, long long start_time, long long end_time){

    SERIES header;
    int64_t file_size = 0;

    if(db->seriesInfo(key,&header,&file_size) != NO_ERROR)
        return 0;

    if(header.interval == 0)
        return 0;

    return (int64_t)((end_time - start_time) / (long long)header.interval);
}


void BSeriesApi::handleReadData(BSeries *db, uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    long long start_time = 0, end_time = 0;

    if(!readTimeRange(request,response,&start_time,&end_time))
        return;

    int condense_mode = CONDENSE_NONE;
    long long max_points = 0;

    if(!readCondenseOptions(request,response,&condense_mode,&max_points))
        return;

    int64_t points = pointsInRange(db,key,start_time,end_time);

    if(condense_mode == CONDENSE_NONE){

        // Bound the response before allocating anything. The library will happily
        // allocate a point for every interval in the range asked for.
        if(points > (int64_t)runtime->max_points_per_read.load()){
            char message[256];
            snprintf(message,sizeof(message),
                     "the range covers %lld points, the limit is %d; narrow start and end, "
                     "or pass max_points and condense to downsample it",
                     (long long)points,runtime->max_points_per_read.load());
            jsonError(response,413,"range_too_large",message);
            return;
        }

    } else if(points > (int64_t)runtime->max_condense_scan.load()){

        // A condensed read walks the range a window at a time, so the response is
        // bounded by max_points rather than by the range. The work still is not,
        // hence a separate and much larger ceiling on what may be scanned.
        char message[256];
        snprintf(message,sizeof(message),
                 "condensing would walk %lld points, the limit is %d; narrow start and end",
                 (long long)points,runtime->max_condense_scan.load());
        jsonError(response,413,"range_too_large",message);
        return;
    }

    // Everything that could turn this into an error has to be settled before the
    // response head goes out, because streaming cannot take it back. A series that
    // cannot be read at all is caught here.
    SERIES probe;
    int64_t probe_size = 0;
    int info = db->seriesInfo(key,&probe,&probe_size);

    if(info != NO_ERROR){
        jsonDatabaseError(response,info,"reading the series header");
        return;
    }

    HttpStream *stream = response.stream;

    char head[256];
    snprintf(head,sizeof(head),"{\"start\":%lld,\"end\":%lld,",start_time,end_time);

    stream->begin(200,"application/json",response.headers);
    stream->write(head,strlen(head));

    int rc = (condense_mode == CONDENSE_NONE)
           ? streamSeriesData(db,key,start_time,end_time,stream)
           : streamCondensedSeries(db,key,start_time,end_time,condense_mode,max_points,stream,NULL);

    if(rc != NO_ERROR){
        // The head is already on the wire, so a failure this late can only be
        // reported inside the body.
        char entry[256];
        snprintf(entry,sizeof(entry),"\"key\":%lu,\"error\":\"%s\",\"status\":%d",
                 (unsigned long)key,databaseErrorSlug(rc),rc);
        stream->write(entry,strlen(entry));
    }

    stream->write("}",1);
}


/// Several series over one time range, as an array of dataset objects.
///
/// A series that cannot be read becomes an entry carrying an error rather than
/// failing the whole request, because one missing key out of two hundred should
/// not cost the caller the other hundred and ninety nine.

void BSeriesApi::handleMultiRead(BSeries *db, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    long long start_time = 0, end_time = 0;

    if(!readTimeRange(request,response,&start_time,&end_time))
        return;

    int condense_mode = CONDENSE_NONE;
    long long max_points = 0;

    if(!readCondenseOptions(request,response,&condense_mode,&max_points))
        return;

    std::string keys_text = httpQueryParam(request,"keys");

    if(keys_text.empty()){
        jsonError(response,400,"bad_parameter","keys is required, for example keys=10500,10501 or keys=1-500");
        return;
    }

    std::vector<uint32_t> keys;
    std::string parse_error;

    if(!parseKeyList(keys_text,runtime->max_series_per_read.load(),&keys,&parse_error)){
        jsonError(response,400,"bad_parameter",parse_error);
        return;
    }

    bool skip_missing = false;
    std::string skip_text = httpQueryParam(request,"skip_missing");

    if(!skip_text.empty() && skip_text != "0" && skip_text != "false")
        skip_missing = true;

    // The budget is shared across the whole request. Applying it per series would
    // let a caller multiply it by the number of series they ask for.
    int64_t total_points = 0;

    for(size_t i = 0; i < keys.size(); i++)
        total_points += pointsInRange(db,keys[i],start_time,end_time);

    if(condense_mode == CONDENSE_NONE && total_points > (int64_t)runtime->max_points_per_read.load()){
        char message[256];
        snprintf(message,sizeof(message),
                 "the request covers %lld points across %lu series, the limit is %d; "
                 "narrow the range, ask for fewer series, or pass max_points and condense",
                 (long long)total_points,(unsigned long)keys.size(),runtime->max_points_per_read.load());
        jsonError(response,413,"range_too_large",message);
        return;
    }

    if(condense_mode != CONDENSE_NONE && total_points > (int64_t)runtime->max_condense_scan.load()){
        char message[256];
        snprintf(message,sizeof(message),
                 "condensing would walk %lld points across %lu series, the limit is %d",
                 (long long)total_points,(unsigned long)keys.size(),runtime->max_condense_scan.load());
        jsonError(response,413,"range_too_large",message);
        return;
    }

    HttpStream *stream = response.stream;

    char head[256];
    snprintf(head,sizeof(head),"{\"start\":%lld,\"end\":%lld,\"series\":[",start_time,end_time);

    stream->begin(200,"application/json",response.headers);
    stream->write(head,strlen(head));

    size_t returned = 0;

    for(size_t i = 0; i < keys.size(); i++){

        // Probed before anything is written, because a separator cannot be taken
        // back once it is on the wire and skip_missing has to omit the entry
        // entirely rather than leave a hole in the array.
        SERIES probe;
        int64_t probe_size = 0;
        int info = db->seriesInfo(keys[i],&probe,&probe_size);

        if(info != NO_ERROR && skip_missing)
            continue;

        if(returned > 0)
            stream->write(",",1);

        stream->write("{",1);

        int rc = info;

        if(info == NO_ERROR)
            rc = (condense_mode == CONDENSE_NONE)
               ? streamSeriesData(db,keys[i],start_time,end_time,stream)
               : streamCondensedSeries(db,keys[i],start_time,end_time,condense_mode,max_points,stream,NULL);

        if(rc != NO_ERROR){
            char entry[256];
            snprintf(entry,sizeof(entry),
                     "\"key\":%lu,\"error\":\"%s\",\"status\":%d",
                     (unsigned long)keys[i],databaseErrorSlug(rc),rc);
            stream->write(entry,strlen(entry));
        }

        stream->write("}",1);
        returned++;
    }

    char tail[128];
    snprintf(tail,sizeof(tail),"],\"count\":%lu}",(unsigned long)returned);
    stream->write(tail,strlen(tail));
}



/// Works out how wide a point is for this series and how far apart points sit.
///
/// The series' own header is the authority. For a series that does not exist yet
/// the definitions file decides, and failing that the caller's payload is taken to
/// be a single point, which is the only unambiguous reading available.

bool BSeriesApi::resolveWriteShape(BSeries *db, uint32_t key, size_t body_bytes, uint32_t *datasize, int64_t *interval, std::string *error){

    SERIES header;

    // seriesShape rather than seriesInfo: this runs once per point on an ingest
    // path, and the open/read/seek/close of reading the header back off disk each
    // time is several syscalls for something the open series already knows.
    if(db->seriesShape(key,&header)){

        *datasize = bsHeaderDataSize(&header);
        *interval = (int64_t)header.interval;

    } else {

        SERIES_DEFINITION definition;

        if(db->definitionForKey(key,&definition)){
            *datasize = definition.datasize;
            *interval = (int64_t)definition.interval;
        } else {
            *datasize = (uint32_t)body_bytes;
            *interval = (int64_t)config.default_interval;
        }
    }

    if(*datasize == 0){
        *error = "the series has a zero point width";
        return false;
    }

    if(body_bytes % *datasize != 0){
        char message[192];
        snprintf(message,sizeof(message),
                 "%lu bytes is not a whole number of %lu byte points",
                 (unsigned long)body_bytes,(unsigned long)*datasize);
        *error = message;
        return false;
    }

    if(body_bytes / *datasize > 1 && *interval <= 0){
        *error = "the series has a zero interval, so consecutive points cannot be placed";
        return false;
    }

    return true;
}


/// Writes consecutive points starting at timestamp. Returns a database status and
/// sets written to how many points landed before any failure.

int BSeriesApi::writePoints(BSeries *db, uint32_t key, const std::string &points, uint32_t datasize, int64_t interval, long long timestamp, int64_t *written, int64_t *overwritten){

    int64_t count = (int64_t)(points.size() / datasize);
    *written = 0;

    if(overwritten != NULL)
        *overwritten = 0;

    for(int64_t point = 0; point < count; point++){

        long long point_time = timestamp + point * interval;

        if(point_time > 0xFFFFFFFFLL)
            return WRITE_BEFORE_SERIES_START; // ran off the end of 32 bit time

        bool replaced = false;
        int rc = db->write(key,(void*)(points.data() + point * datasize),datasize,(uint32_t)point_time,
                           overwritten != NULL ? &replaced : NULL);

        if(rc != NO_ERROR)
            return rc;

        if(replaced && overwritten != NULL)
            (*overwritten)++;

        (*written)++;
    }

    return NO_ERROR;
}


/// The slot nearest a given time, as a timestamp.
///
/// Writes floor onto the grid, so handing the library the nearest slot's own
/// timestamp is what places a reading in it. A series that does not exist yet has
/// no grid to snap to, so the time is returned unchanged and becomes its start.

bool BSeriesApi::nearestSlotTime(BSeries *db, uint32_t key, long long when, long long *slot_time){

    SERIES header;

    if(!db->seriesShape(key,&header) || header.interval == 0){
        *slot_time = when;
        return false;
    }

    long long interval = (long long)header.interval;
    long long start = (long long)header.timestamp;

    if(when <= start){
        *slot_time = start;
        return true;
    }

    // Rounding half up, so a reading exactly between two slots takes the later.
    long long slot = (when - start + interval / 2) / interval;

    *slot_time = start + slot * interval;
    return true;
}


void BSeriesApi::handleWriteData(BSeries *db, uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

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

    uint32_t datasize = 0;
    int64_t interval = 0;
    std::string shape_error;

    if(!resolveWriteShape(db,key,points.size(),&datasize,&interval,&shape_error)){
        jsonError(response,422,"bad_point_width",shape_error);
        return;
    }

    int64_t count = (int64_t)(points.size() / datasize);

    if(count > (int64_t)runtime->max_points_per_write.load()){
        char message[192];
        snprintf(message,sizeof(message),"%lld points in one request, the limit is %d",
                 (long long)count,runtime->max_points_per_write.load());
        jsonError(response,413,"too_many_points",message);
        return;
    }

    int64_t written = 0, overwritten = 0;
    int rc = writePoints(db,key,points,datasize,interval,timestamp,&written,&overwritten);

    if(rc != NO_ERROR){

        if(written > 0){
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

    char buffer[256];

    snprintf(buffer,sizeof(buffer),
        "{\"key\":%lu,\"points_written\":%lld,\"overwritten\":%lld,"
        "\"first_timestamp\":%lld,\"last_timestamp\":%lld,\"interval\":%lld}",
        (unsigned long)key,
        (long long)written,
        (long long)overwritten,
        timestamp,
        (long long)(timestamp + (written - 1) * interval),
        (long long)interval);

    response.status = 200;
    response.body = buffer;
}


/// One record of a batch write.

typedef struct {
    int line;
    uint32_t key;
    long long timestamp;
    std::string points;    // already decoded from hex
    uint32_t datasize;
    int64_t interval;
    int64_t count;
    int64_t written;
    int64_t overwritten;
    int status;
    bool attempted;
} BATCH_RECORD;


/// Splits a line on whitespace into at most max tokens. Returns how many it found;
/// anything past max is left attached to the last token, which never happens for a
/// well formed record and produces a clear error when it does.

static int splitTokens(const std::string &line, std::string *tokens, int max){

    int count = 0;
    size_t position = 0;

    while(position < line.size() && count < max){

        while(position < line.size() && isspace((unsigned char)line[position]))
            position++;

        if(position >= line.size())
            break;

        size_t start = position;

        while(position < line.size() && !isspace((unsigned char)line[position]))
            position++;

        tokens[count++] = line.substr(start,position - start);
    }

    // anything left over means the line had more fields than the grammar allows
    while(position < line.size() && isspace((unsigned char)line[position]))
        position++;

    if(position < line.size())
        return max + 1;

    return count;
}


/// One value per series, into the slot nearest the server's clock.
///
///     POST /v1/now
///     <key> <hex>
///     <key> <hex>
///
/// This is the shape a poller wants: it has just sampled a thousand devices and
/// does not care which slot each reading lands in, only that they all land in the
/// one nearest now. No timestamp appears in the body at all, so a client with a
/// wrong clock cannot scatter readings across the grid.
///
/// Unlike the general batch endpoint this rounds to the nearest slot rather than
/// flooring, which halves the worst case placement error. It also reports which
/// series already held a reading in that slot, since rounding to nearest makes two
/// samples sharing a slot more likely, and a series can only hold one.

void BSeriesApi::handleWriteNow(BSeries *db, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    long long now = (long long)time(NULL);

    // An explicit time is allowed so a run can be replayed or tested; the point of
    // the endpoint is that the body carries no timestamps, not that the clock is
    // untouchable.
    std::string at_text = httpQueryParam(request,"at");

    if(!at_text.empty() && (!parseSigned(at_text,&now) || now <= 0 || now > 0xFFFFFFFFLL)){
        jsonError(response,400,"bad_parameter","at must be a unix timestamp that fits in 32 bits");
        return;
    }

    std::vector<BATCH_RECORD> records;
    size_t position = 0;
    int line_number = 0;

    while(position <= request.body.size()){

        size_t newline = request.body.find('\n',position);
        if(newline == std::string::npos)
            newline = request.body.size();

        std::string line = request.body.substr(position,newline - position);
        position = newline + 1;
        line_number++;

        while(!line.empty() && isspace((unsigned char)line[line.size()-1]))
            line.erase(line.size()-1);

        size_t first = line.find_first_not_of(" \t");
        if(first == std::string::npos || line[first] == '#')
            continue;

        std::string tokens[2];
        int found = splitTokens(line,tokens,2);

        if(found != 2){
            char message[192];
            snprintf(message,sizeof(message),"line %d: expected <key> <hex>",line_number);
            jsonError(response,400,"bad_record",message);
            return;
        }

        BATCH_RECORD record;
        record.line = line_number;
        record.written = 0;
        record.overwritten = 0;
        record.status = NO_ERROR;
        record.attempted = false;

        unsigned long key_value = 0;

        if(!parseUnsigned(tokens[0],&key_value) || key_value > 0xFFFFFFFFuL){
            char message[192];
            snprintf(message,sizeof(message),"line %d: '%s' is not a series key",line_number,tokens[0].c_str());
            jsonError(response,400,"bad_record",message);
            return;
        }

        record.key = (uint32_t)key_value;

        if(!apiFromHex(tokens[1],&record.points) || record.points.empty()){
            char message[192];
            snprintf(message,sizeof(message),"line %d: the value is not valid hex",line_number);
            jsonError(response,400,"bad_hex",message);
            return;
        }

        std::string shape_error;

        if(!resolveWriteShape(db,record.key,record.points.size(),&record.datasize,&record.interval,&shape_error)){
            char message[256];
            snprintf(message,sizeof(message),"line %d: %s",line_number,shape_error.c_str());
            jsonError(response,422,"bad_point_width",shape_error);
            return;
        }

        // One value per series is the whole contract here. A line carrying more
        // than one point is a client using the wrong endpoint.
        if(record.points.size() != record.datasize){
            char message[256];
            snprintf(message,sizeof(message),
                     "line %d: %lu bytes is %lu points; this endpoint takes one value per series, use /v1/data for a run",
                     line_number,(unsigned long)record.points.size(),
                     (unsigned long)(record.points.size() / record.datasize));
            jsonError(response,422,"expected_single_point",message);
            return;
        }

        record.count = 1;
        records.push_back(record);

        if(records.size() > (size_t)runtime->max_series_per_read.load()){
            char message[192];
            snprintf(message,sizeof(message),"more than %d series in one request",runtime->max_series_per_read.load());
            jsonError(response,413,"too_many_points",message);
            return;
        }
    }

    if(records.empty()){
        jsonError(response,400,"empty_body","no records were supplied");
        return;
    }

    size_t records_written = 0;
    size_t records_failed = 0;
    size_t overwritten = 0;

    for(size_t i = 0; i < records.size(); i++){

        // Each series has its own grid, so the nearest slot is worked out per
        // series rather than once for the batch.
        long long slot_time = now;
        nearestSlotTime(db,records[i].key,now,&slot_time);
        records[i].timestamp = slot_time;

        records[i].attempted = true;
        records[i].status = writePoints(db,records[i].key,records[i].points,records[i].datasize,
                                        records[i].interval,slot_time,
                                        &records[i].written,&records[i].overwritten);

        if(records[i].status == NO_ERROR){
            records_written++;
            if(records[i].overwritten > 0)
                overwritten++;
        } else {
            records_failed++;
        }
    }

    bool verbose = false;
    std::string verbose_text = httpQueryParam(request,"verbose");

    if(!verbose_text.empty() && verbose_text != "0" && verbose_text != "false")
        verbose = true;

    char head[320];

    snprintf(head,sizeof(head),
        "{\"now\":%lld,\"records\":%lu,\"records_written\":%lu,\"records_failed\":%lu,\"overwritten\":%lu",
        now,
        (unsigned long)records.size(),
        (unsigned long)records_written,
        (unsigned long)records_failed,
        (unsigned long)overwritten);

    response.body = head;

    // Only the records worth looking at: the ones that failed, and the ones that
    // replaced a reading already in their slot.
    if(records_failed > 0 || overwritten > 0 || verbose){

        response.body += ",\"results\":[";
        bool first = true;

        for(size_t i = 0; i < records.size(); i++){

            bool interesting = records[i].status != NO_ERROR || records[i].overwritten > 0;

            if(!interesting && !verbose)
                continue;

            char entry[384];

            snprintf(entry,sizeof(entry),
                "%s{\"line\":%d,\"key\":%lu,\"state\":\"%s\",\"error\":\"%s\",\"slot\":%lld,\"overwritten\":%s}",
                first ? "" : ",",
                records[i].line,
                (unsigned long)records[i].key,
                records[i].status == NO_ERROR ? "written" : "failed",
                databaseErrorSlug(records[i].status),
                (long long)records[i].timestamp,
                records[i].overwritten > 0 ? "true" : "false");

            response.body += entry;
            first = false;
        }

        response.body += "]";
    }

    if(records_failed > 0){
        response.status = 500;
        response.body += (records_written == 0)
            ? ",\"error\":\"write_failed\",\"message\":\"no record could be written, see results\"}"
            : ",\"error\":\"partial_write\",\"message\":\"the batch was applied in part, see results\"}";
        return;
    }

    response.status = 200;
    response.body += "}";
}




/// Points for several series in one request.
///
/// The body is one record per line:
///
///     <key> <timestamp|now> <hex>
///
/// The hex is one or more consecutive points, placed the same way the single series
/// endpoint places them. Blank lines and lines beginning with # are ignored.
///
/// The whole body is parsed and validated before anything is written, so a typo on
/// line four hundred cannot leave the first three hundred and ninety nine applied.
/// The database has no transactions, so a failure during the write pass still
/// leaves a partial batch; that is reported per record rather than glossed over.

void BSeriesApi::handleBatchWrite(BSeries *db, const HTTP_REQUEST &request, HTTP_RESPONSE &response){

    std::vector<BATCH_RECORD> records;
    int64_t total_points = 0;
    long long now = (long long)time(NULL);

    size_t position = 0;
    int line_number = 0;

    while(position <= request.body.size()){

        size_t newline = request.body.find('\n',position);
        if(newline == std::string::npos)
            newline = request.body.size();

        std::string line = request.body.substr(position,newline - position);
        position = newline + 1;
        line_number++;

        while(!line.empty() && (line[line.size()-1] == '\r' || isspace((unsigned char)line[line.size()-1])))
            line.erase(line.size()-1);

        size_t first = line.find_first_not_of(" \t");
        if(first == std::string::npos)
            continue;                       // blank

        if(line[first] == '#')
            continue;                       // comment

        std::string tokens[3];
        int found = splitTokens(line,tokens,3);

        if(found != 3){
            char message[192];
            snprintf(message,sizeof(message),
                     "line %d: expected <key> <timestamp|now> <hex>",line_number);
            jsonError(response,400,"bad_record",message);
            return;
        }

        BATCH_RECORD record;
        record.line = line_number;
        record.written = 0;
        record.overwritten = 0;
        record.status = NO_ERROR;
        record.attempted = false;

        unsigned long key_value = 0;

        if(!parseUnsigned(tokens[0],&key_value) || key_value > 0xFFFFFFFFuL){
            char message[192];
            snprintf(message,sizeof(message),"line %d: '%s' is not a series key",line_number,tokens[0].c_str());
            jsonError(response,400,"bad_record",message);
            return;
        }

        record.key = (uint32_t)key_value;

        if(tokens[1] == "now"){
            record.timestamp = now;
        } else if(!parseSigned(tokens[1],&record.timestamp) || record.timestamp <= 0 || record.timestamp > 0xFFFFFFFFLL){
            char message[192];
            snprintf(message,sizeof(message),
                     "line %d: '%s' is not a unix timestamp that fits in 32 bits",line_number,tokens[1].c_str());
            jsonError(response,400,"bad_record",message);
            return;
        }

        if(!apiFromHex(tokens[2],&record.points) || record.points.empty()){
            char message[192];
            snprintf(message,sizeof(message),"line %d: the point data is not valid hex",line_number);
            jsonError(response,400,"bad_hex",message);
            return;
        }

        std::string shape_error;

        if(!resolveWriteShape(db,record.key,record.points.size(),&record.datasize,&record.interval,&shape_error)){
            char message[256];
            snprintf(message,sizeof(message),"line %d: %s",line_number,shape_error.c_str());
            jsonError(response,422,"bad_point_width",message);
            return;
        }

        record.count = (int64_t)(record.points.size() / record.datasize);
        total_points += record.count;

        if(total_points > (int64_t)runtime->max_points_per_write.load()){
            char message[192];
            snprintf(message,sizeof(message),
                     "more than %d points in one request; split the batch",runtime->max_points_per_write.load());
            jsonError(response,413,"too_many_points",message);
            return;
        }

        records.push_back(record);
    }

    if(records.empty()){
        jsonError(response,400,"empty_body","no records were supplied");
        return;
    }

    // Everything parsed. Apply it.
    //
    // Every record is attempted even after one fails. These are independent
    // series, and a single device with a bad clock must not cost the other
    // thousand their points. The database has no transactions, so this is
    // reported honestly per record rather than presented as all or nothing.
    int64_t written_points = 0;
    int64_t overwritten_points = 0;
    size_t records_written = 0;
    size_t records_failed = 0;

    for(size_t i = 0; i < records.size(); i++){

        records[i].attempted = true;
        records[i].status = writePoints(db,records[i].key,records[i].points,records[i].datasize,
                                        records[i].interval,records[i].timestamp,
                                        &records[i].written,&records[i].overwritten);

        written_points += records[i].written;
        overwritten_points += records[i].overwritten;

        if(records[i].status == NO_ERROR)
            records_written++;
        else
            records_failed++;
    }

    bool verbose = false;
    std::string verbose_text = httpQueryParam(request,"verbose");

    if(!verbose_text.empty() && verbose_text != "0" && verbose_text != "false")
        verbose = true;

    char head[320];

    snprintf(head,sizeof(head),
        "{\"records\":%lu,\"records_written\":%lu,\"records_failed\":%lu,"
        "\"points_written\":%lld,\"points_expected\":%lld,\"overwritten\":%lld",
        (unsigned long)records.size(),
        (unsigned long)records_written,
        (unsigned long)records_failed,
        (long long)written_points,
        (long long)total_points,
        (long long)overwritten_points);

    response.body = head;

    // Only the records worth looking at, which for a two thousand device batch is
    // the difference between a line of JSON and a megabyte of it. verbose=1 lists
    // every record instead.
    if(records_failed > 0 || overwritten_points > 0 || verbose){

        response.body += ",\"results\":[";
        bool first = true;

        for(size_t i = 0; i < records.size(); i++){

            bool interesting = records[i].status != NO_ERROR || records[i].overwritten > 0;

            if(!interesting && !verbose)
                continue;

            const char *state = !records[i].attempted ? "not_attempted"
                              : (records[i].status == NO_ERROR ? "written" : "failed");

            char entry[448];

            snprintf(entry,sizeof(entry),
                "%s{\"line\":%d,\"key\":%lu,\"state\":\"%s\",\"error\":\"%s\","
                "\"points_written\":%lld,\"points_expected\":%lld,\"overwritten\":%lld,"
                "\"first_timestamp\":%lld}",
                first ? "" : ",",
                records[i].line,
                (unsigned long)records[i].key,
                state,
                databaseErrorSlug(records[i].status),
                (long long)records[i].written,
                (long long)records[i].count,
                (long long)records[i].overwritten,
                records[i].timestamp);

            response.body += entry;
            first = false;
        }

        response.body += "]";
    }

    if(records_failed > 0){

        // Nothing landing at all is a failed batch, not a partial one. Saying
        // "partial" when zero points were written would be a lie a caller could
        // act on.
        response.status = 500;

        if(records_written == 0)
            response.body += ",\"error\":\"write_failed\",\"message\":\"no record could be written, see results\"}";
        else
            response.body += ",\"error\":\"partial_write\",\"message\":\"the batch was applied in part, see results\"}";

        return;
    }

    response.status = 200;
    response.body += "}";
}
