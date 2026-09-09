// End to end tests for the HTTP API. Starts a real server on an ephemeral port
// and talks to it over a real socket.

#include <sys/stat.h>
#include "bseries.h"
#include "bseries_api.h"
#include "table_set.h"
#include "auth_store.h"
#include "runtime_settings.h"
#include "http_server.h"
#include "test_util.h"
#include <time.h>

#include <string>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

static int g_port = 0;

typedef struct {
    int status;
    std::string body;
    std::string head;
    bool chunked;
} REPLY;


// Reassembles a chunked body. Returns false if the framing is malformed, which is
// itself worth catching: the server writes these chunk headers by hand.
static bool decodeChunked(const std::string &raw, std::string *out){

    size_t position = 0;

    for(;;){

        size_t line_end = raw.find("\r\n",position);
        if(line_end == std::string::npos)
            return false;

        std::string size_line = raw.substr(position,line_end - position);

        char *end = NULL;
        unsigned long size = strtoul(size_line.c_str(),&end,16);

        if(end == size_line.c_str())
            return false;

        position = line_end + 2;

        if(size == 0)
            return true;                      // terminating chunk

        if(position + size > raw.size())
            return false;

        out->append(raw,position,size);
        position += size + 2;                 // the chunk's own trailing CRLF
    }
}


static REPLY request(const char *method, const std::string &path, const char *api_key,
                     const std::string &body = "", const char *origin = NULL,
                     const char *extra_header = NULL){

    REPLY reply;
    reply.status = -1;
    reply.chunked = false;

    int fd = socket(AF_INET,SOCK_STREAM,0);
    if(fd < 0) return reply;

    struct sockaddr_in address;
    memset(&address,0,sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(g_port);
    address.sin_addr.s_addr = inet_addr("127.0.0.1");

    if(connect(fd,(struct sockaddr*)&address,sizeof(address)) != 0){ close(fd); return reply; }

    char head[2048];
    int length = snprintf(head,sizeof(head),
        "%s %s HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: %lu\r\n",
        method,path.c_str(),(unsigned long)body.size());

    std::string out(head,(size_t)length);
    if(api_key) out += std::string("X-API-Key: ") + api_key + "\r\n";
    if(origin)  out += std::string("Origin: ") + origin + "\r\n";
    if(extra_header) out += std::string(extra_header) + "\r\n";
    out += "\r\n";
    out += body;

    size_t sent = 0;
    while(sent < out.size()){
        ssize_t wrote = send(fd,out.data()+sent,out.size()-sent,0);
        if(wrote <= 0) break;
        sent += (size_t)wrote;
    }

    std::string raw;
    char buffer[4096];
    ssize_t got;
    while((got = recv(fd,buffer,sizeof(buffer),0)) > 0)
        raw.append(buffer,(size_t)got);

    close(fd);

    if(raw.compare(0,5,"HTTP/") == 0)
        reply.status = atoi(raw.c_str() + 9);

    size_t split = raw.find("\r\n\r\n");

    if(split != std::string::npos){

        reply.head = raw.substr(0,split);
        std::string body = raw.substr(split + 4);

        reply.chunked = reply.head.find("Transfer-Encoding: chunked") != std::string::npos;

        if(reply.chunked){
            if(!decodeChunked(body,&reply.body)){
                printf("  (malformed chunked framing in a response)\n");
                test_failures++;
            }
        } else {
            reply.body = body;
        }
    }

    return reply;
}


static bool bodyHas(const REPLY &reply, const char *needle){
    return reply.body.find(needle) != std::string::npos;
}

static bool headHas(const REPLY &reply, const char *needle){
    return reply.head.find(needle) != std::string::npos;
}


int main(int argc, char **argv){

    (void)argc; (void)argv;
    const char *dir = testMakeDirectory();

    API_CONFIG config;
    apiConfigDefaults(&config);
    config.bind_address = "127.0.0.1";
    config.port = 0;                       // let the OS pick
    config.data_directory = dir;
    config.read_key  = "read-only-key";
    config.write_key = "read-write-key";
    config.cors_origins.push_back("https://dashboard.example.com");
    // Large enough that a streamed response exceeds the stream's own buffer, so
    // the chunking is actually exercised, and still small enough to test the caps.
    config.max_points_per_read = 100000;
    config.max_body_bytes = 262144;
    config.default_interval = 10;
    config.condense_window_points = 64;   // small, so condensing crosses many windows

    TableSet tables;
    tables.configure(dir,4096,config.default_interval,1000000);

    AuthStore auth;
    auth.load(std::string(dir) + "/auth.keys");

    RUNTIME_SETTINGS runtime;
    runtime.flush_interval.store(60);
    runtime.series_max_idle.store(900);
    runtime.maintenance_interval.store(60);
    runtime.max_points_per_read.store(config.max_points_per_read);
    runtime.max_series_per_read.store(config.max_series_per_read);
    runtime.max_points_per_write.store(config.max_points_per_write);
    runtime.max_condense_scan.store(config.max_condense_scan);

    BSeriesApi api(&tables,&auth,&runtime,&config);

    HttpServer server;
    server.max_body_bytes = config.max_body_bytes;

    if(!server.start(config.bind_address.c_str(),config.port)){
        printf("could not start the server\n");
        return 1;
    }

    g_port = server.boundPort();
    std::thread serving([&]{ server.run(BSeriesApi::handle,&api); });

    printf("[1] health and routing\n");
    {
        REPLY r = request("GET","/v1/health",NULL);
        CHECK(r.status==200 && bodyHas(r,"\"ok\""), "health needs no key");
        r = request("GET","/v1/nonsense","read-only-key");
        CHECK(r.status==404, "unknown endpoint is 404");
        r = request("GET","/v1/series/notanumber","read-only-key");
        CHECK(r.status==400 && bodyHas(r,"bad_key"), "non numeric key is 400");
    }

    printf("[2] authentication\n");
    {
        REPLY r = request("GET","/v1/series",NULL);
        CHECK(r.status==401, "no key is 401");
        r = request("GET","/v1/series","wrong-key");
        CHECK(r.status==401, "bad key is 401");
        r = request("GET","/v1/series","read-only-key");
        CHECK(r.status==200, "read key may list");
        r = request("POST","/v1/series/1?type=uint8","read-only-key");
        CHECK(r.status==403 && bodyHas(r,"forbidden"), "read key may not write");
        r = request("DELETE","/v1/series/1","read-only-key");
        CHECK(r.status==403, "read key may not delete");
    }

    printf("[3] create and inspect\n");
    {
        REPLY r = request("POST","/v1/series/10500?type=float32&interval=60&start=1700000000","read-write-key");
        CHECK(r.status==201 && bodyHas(r,"\"float32\"") && bodyHas(r,"\"interval\":60"), "create float32 at 60s");
        CHECK(bodyHas(r,"\"created\":1700000000"), "series start set in the past for backfill");
        r = request("POST","/v1/series/10500?type=float32&interval=60","read-write-key");
        CHECK(r.status==409 && bodyHas(r,"series_exists"), "creating twice is 409");
        r = request("GET","/v1/series/10500","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"datasize\":4"), "info reports the shape");
        r = request("GET","/v1/series/999999","read-only-key");
        CHECK(r.status==404 && bodyHas(r,"series_not_found"), "unknown series is 404");
        r = request("POST","/v1/series/11000?type=nosuchtype","read-write-key");
        CHECK(r.status==400, "unknown type is 400");
    }

    printf("[4] write and read hex\n");
    {
        // three float32 values: 20.5, 21.25, 22.0  (little endian on x86/arm)
        float values[3] = {20.5f,21.25f,22.0f};
        std::string hex = apiToHex(values,sizeof(values));

        char path[256];
        snprintf(path,sizeof(path),"/v1/series/10500/data?timestamp=%ld",(long)1700000000);

        REPLY r = request("POST",path,"read-write-key",hex);
        CHECK(r.status==200 && bodyHas(r,"\"points_written\":3"), "three points written from one body");

        r = request("GET","/v1/series/10500/data?start=1700000000&end=1700000300","read-only-key");
        CHECK(r.status==200, "read back");
        CHECK(bodyHas(r,"\"type\":\"float32\""), "type reported");
        CHECK(bodyHas(r,"\"null_fill\":\"ffffffff\""), "null fill pattern reported");
        CHECK(bodyHas(r,"\"real_points\":3"), "three real points counted, gaps excluded");
        CHECK(bodyHas(r,hex.c_str()), "the written points appear in the data blob");
        CHECK(bodyHas(r,"ffffffff"), "the gap after them is the fill pattern");

        r = request("GET","/v1/series/10500/data","read-only-key");
        CHECK(r.status==400 && bodyHas(r,"start"), "start is required");
        r = request("GET","/v1/series/10500/data?start=1700000300&end=1700000000","read-only-key");
        CHECK(r.status==400 && bodyHas(r,"invalid_time_range"), "end before start is 400");
    }

    printf("[5] input validation\n");
    {
        REPLY r = request("POST","/v1/series/10500/data?timestamp=1700000000","read-write-key","nothex!!");
        CHECK(r.status==400 && bodyHas(r,"bad_hex"), "non hex body is 400");
        r = request("POST","/v1/series/10500/data?timestamp=1700000000","read-write-key","abc");
        CHECK(r.status==400 && bodyHas(r,"bad_hex"), "odd hex digit count is 400");
        r = request("POST","/v1/series/10500/data?timestamp=1700000000","read-write-key","0000a44100");
        CHECK(r.status==422 && bodyHas(r,"bad_point_width"), "a partial point is 422");
        r = request("POST","/v1/series/10500/data?timestamp=1700000000","read-write-key","");
        CHECK(r.status==400 && bodyHas(r,"empty_body"), "empty body is 400");

        r = request("GET","/v1/series/10500/data?start=1&end=2000000000","read-only-key");
        CHECK(r.status==413 && bodyHas(r,"range_too_large"), "an oversized range is refused before allocating");

        std::string huge(300000,'a');
        r = request("POST","/v1/series/10500/data?timestamp=1700000000","read-write-key",huge);
        CHECK(r.status==413, "an oversized body is refused");
    }

    printf("[6] CORS\n");
    {
        REPLY r = request("OPTIONS","/v1/series/10500/data",NULL,"","https://dashboard.example.com");
        CHECK(r.status==204, "preflight succeeds without a key");
        CHECK(headHas(r,"Access-Control-Allow-Origin: https://dashboard.example.com"), "allowed origin echoed");
        CHECK(headHas(r,"Access-Control-Allow-Headers: X-API-Key"), "the key header is allowed");
        CHECK(headHas(r,"Vary: Origin"), "Vary set so caches do not cross origins");

        r = request("GET","/v1/series/10500","read-only-key","","https://evil.example.com");
        CHECK(r.status==200 && !headHas(r,"Access-Control-Allow-Origin"), "an unlisted origin gets no CORS header");
    }

    printf("[7] uint8 series and a configured null fill\n");
    {
        REPLY r = request("POST","/v1/series/30001?type=uint8&interval=5&start=1700000000","read-write-key");
        CHECK(r.status==201, "create uint8");
        char path[256];
        snprintf(path,sizeof(path),"/v1/series/30001/data?timestamp=%ld",(long)1700000000);
        r = request("POST",path,"read-write-key","0105");
        CHECK(r.status==200 && bodyHas(r,"\"points_written\":2"), "two byte points written");
        r = request("GET","/v1/series/30001/data?start=1700000000&end=1700000030","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"null_fill\":\"ff\""), "uint8 fill is one byte");
        CHECK(bodyHas(r,"\"real_points\":2"), "gaps not counted as real");
    }

    printf("[8] multi series read\n");
    {
        // a second float32 series alongside 10500, plus a uint8 one
        REPLY r = request("POST","/v1/series/10501?type=float32&interval=60&start=1700000000","read-write-key");
        CHECK(r.status==201, "create a second float32 series");
        r = request("POST","/v1/series/10501/data?timestamp=1700000000","read-write-key","0000c8420000ca42");
        CHECK(r.status==200 && bodyHas(r,"\"points_written\":2"), "seed it");

        r = request("GET","/v1/data?keys=10500,10501&start=1700000000&end=1700000300","read-only-key");
        CHECK(r.status==200, "multi read succeeds");
        CHECK(bodyHas(r,"\"count\":2"), "two datasets returned");
        CHECK(bodyHas(r,"\"key\":10500") && bodyHas(r,"\"key\":10501"), "both keys present");
        CHECK(bodyHas(r,"0000a4410000aa410000b041"), "first dataset's blob");
        CHECK(bodyHas(r,"0000c8420000ca42"), "second dataset's blob");
        // one blob per dataset, not one JSON value per point
        CHECK(!bodyHas(r,"\"points\":["), "points are not expanded into an array");

        r = request("GET","/v1/data?keys=1-3&start=1700000000&end=1700000300","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"count\":3"), "a key range expands");
        CHECK(bodyHas(r,"series_not_found"), "missing series become error entries");

        r = request("GET","/v1/data?keys=1-3,10500&start=1700000000&end=1700000300&skip_missing=1","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"count\":1"), "skip_missing drops the absent ones");
        CHECK(!bodyHas(r,"series_not_found"), "and reports no errors");

        r = request("GET","/v1/data?start=1700000000","read-only-key");
        CHECK(r.status==400 && bodyHas(r,"keys"), "keys is required");
        r = request("GET","/v1/data?keys=notakey&start=1700000000","read-only-key");
        CHECK(r.status==400, "a bad key is 400");
        r = request("GET","/v1/data?keys=9-1&start=1700000000","read-only-key");
        CHECK(r.status==400, "a backwards range is 400");
        r = request("GET","/v1/data?keys=0-4000000000&start=1700000000","read-only-key");
        CHECK(r.status==400, "a range past the cap is refused, not expanded");

        // the point budget is shared, not per series
        r = request("GET","/v1/data?keys=10500,10501&start=1&end=1700000000","read-only-key");
        CHECK(r.status==413 && bodyHas(r,"range_too_large"), "the shared budget applies across series");

        r = request("GET","/v1/data?keys=10500","read-only-key");
        CHECK(r.status==400 && bodyHas(r,"start"), "start still required");
        r = request("POST","/v1/data?keys=10500&start=1700000000","read-write-key");
        CHECK(r.status==400 && bodyHas(r,"empty_body"), "POST to the same path is a batch write, not a read");
        r = request("GET","/v1/data?keys=10500&start=1700000000&end=1700000300",NULL);
        CHECK(r.status==401, "multi read needs a key");
    }

    printf("[9] batch write\n");
    {
        // 10502 is new, 10500 and 10501 already exist
        REPLY r = request("POST","/v1/series/10502?type=float32&interval=60&start=1700000000","read-write-key");
        CHECK(r.status==201, "create a third series");

        std::string batch =
            "# one record per line\n"
            "10500 1700000600 0000a4410000aa41\n"
            "\n"
            "10501 1700000600 0000b041\n"
            "10502 1700000600 0000c8410000ca410000cc41\n";

        r = request("POST","/v1/data","read-write-key",batch);
        CHECK(r.status==200, "batch accepted");
        CHECK(bodyHas(r,"\"records\":3"), "three records");
        CHECK(bodyHas(r,"\"points_written\":6"), "six points across three series");
        CHECK(!bodyHas(r,"\"results\""), "no per record detail unless asked");

        r = request("POST","/v1/data?verbose=1","read-write-key","10500 1700001200 0000a441\n");
        CHECK(r.status==200 && bodyHas(r,"\"results\""), "verbose returns per record detail");

        // the points really landed, in the right series
        r = request("GET","/v1/data?keys=10500-10502&start=1700000600&end=1700000780","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"0000a4410000aa41"), "10500 got its two points");
        CHECK(bodyHas(r,"0000c8410000ca410000cc41"), "10502 got its three points");

        r = request("POST","/v1/data","read-write-key","");
        CHECK(r.status==400 && bodyHas(r,"empty_body"), "an empty batch is 400");
        r = request("POST","/v1/data","read-write-key","# just a comment\n\n");
        CHECK(r.status==400 && bodyHas(r,"empty_body"), "comments alone are 400");
        r = request("POST","/v1/data","read-write-key","10500 1700000600\n");
        CHECK(r.status==400 && bodyHas(r,"bad_record"), "a short record is 400");
        r = request("POST","/v1/data","read-write-key","10500 1700000600 0000a441 extra\n");
        CHECK(r.status==400 && bodyHas(r,"bad_record"), "a long record is 400");
        r = request("POST","/v1/data","read-write-key","notakey 1700000600 0000a441\n");
        CHECK(r.status==400 && bodyHas(r,"line 1"), "a bad key names its line");
        r = request("POST","/v1/data","read-write-key","10500 notatime 0000a441\n");
        CHECK(r.status==400 && bodyHas(r,"line 1"), "a bad timestamp names its line");
        r = request("POST","/v1/data","read-write-key","10500 1700000600 zzz\n");
        CHECK(r.status==400 && bodyHas(r,"bad_hex"), "bad hex is 400");

        // nothing is written when a later line is malformed
        r = request("GET","/v1/series/10500/data?start=1700002000&end=1700002200","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"real_points\":0"), "the range is empty before the test");
        r = request("POST","/v1/data","read-write-key",
                    "10500 1700002000 0000a441\n10500 badtime 0000a441\n");
        CHECK(r.status==400, "a batch with a bad second line is rejected");
        r = request("GET","/v1/series/10500/data?start=1700002000&end=1700002200","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"real_points\":0"), "and the good first line was not applied");

        r = request("POST","/v1/data","read-write-key","10500 1700000600 0000a4\n");
        CHECK(r.status==422 && bodyHas(r,"bad_point_width"), "a partial point is 422 and names the line");
        r = request("POST","/v1/data","read-only-key","10500 1700000600 0000a441\n");
        CHECK(r.status==403, "batch write needs the write key");
        r = request("GET","/v1/data?keys=10500&start=1700000600&end=1700000780","read-write-key");
        CHECK(r.status==200, "GET on the same path still reads");

        // "now" is accepted in place of a timestamp
        r = request("POST","/v1/data?verbose=1","read-write-key","1 now 07\n");
        CHECK(r.status==200 && bodyHas(r,"\"points_written\":1"), "now writes at the current time");

        // one bad record must not cost the others their points
        r = request("POST","/v1/data","read-write-key",
                    "10500 1699999000 0000a441\n10500 1700003000 0000aa41\n");
        CHECK(r.status==500 && bodyHas(r,"partial_write"), "a mixed batch reports partial_write");
        CHECK(bodyHas(r,"\"records_written\":1") && bodyHas(r,"\"records_failed\":1"), "counted both ways");
        CHECK(bodyHas(r,"timestamp_before_series_start"), "the failing record names its error");
        CHECK(bodyHas(r,"\"state\":\"failed\"") && !bodyHas(r,"\"state\":\"written\""),
              "results lists only the interesting records");

        r = request("POST","/v1/data?verbose=1","read-write-key",
                    "10500 1699999000 0000a441\n10500 1700003060 0000aa41\n");
        CHECK(bodyHas(r,"\"state\":\"written\"") && bodyHas(r,"\"state\":\"failed\""),
              "verbose=1 lists every record");
        r = request("GET","/v1/series/10500/data?start=1700003000&end=1700003100","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"real_points\":1"), "the good record was still applied");

        // a series created by a write starts at that write's timestamp, so a
        // batch assembled a moment before it is sent is not rejected wholesale
        long long moments_ago = (long long)time(NULL) - 5;
        char line[128];
        snprintf(line,sizeof(line),"40000 %lld 42\n",moments_ago);
        r = request("POST","/v1/data","read-write-key",line);
        CHECK(r.status==200 && bodyHas(r,"\"points_written\":1"), "a new series accepts a slightly stale timestamp");

        // and a point far past the end of a series is refused rather than
        // null filling every interval in between
        snprintf(line,sizeof(line),"40000 %lld 42\n",(long long)time(NULL) + 900000000LL);
        r = request("POST","/v1/data","read-write-key",line);
        CHECK(r.status==500 && bodyHas(r,"timestamp_too_far_ahead"), "a wildly future timestamp is refused");

        // nothing landing at all is a failed batch, not a partial one
        r = request("POST","/v1/data","read-write-key",
                    "10500 1699999000 0000a441\n10501 1699999000 0000aa41\n");
        CHECK(r.status==500 && bodyHas(r,"write_failed"), "no points written is write_failed, not partial_write");
        CHECK(bodyHas(r,"\"records_written\":0"), "and no record counted as written");
    }

    printf("[10] delete and list\n");
    {
        REPLY r = request("GET","/v1/series?limit=100","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"10500") && bodyHas(r,"30001"), "both series listed");

        r = request("DELETE","/v1/series/30001","read-write-key");
        CHECK(r.status==200 && bodyHas(r,"\"deleted\":true"), "delete succeeds");
        r = request("GET","/v1/series/30001","read-only-key");
        CHECK(r.status==404, "deleted series is gone");
        r = request("DELETE","/v1/series/30001","read-write-key");
        CHECK(r.status==404, "deleting twice is 404");
    }

    printf("[11] keys selector on every read, and streaming\n");
    {
        // keys=1,4,5 style selection on the listing endpoint
        REPLY r = request("GET","/v1/series?keys=10500,10502","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"count\":2"), "keys= selects specific series to list");
        CHECK(bodyHas(r,"\"key\":10500") && bodyHas(r,"\"key\":10502"), "the named ones");
        CHECK(!bodyHas(r,"\"key\":10501"), "and only those");

        r = request("GET","/v1/series?keys=10500,999999","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"series_not_found"), "a named key that is missing is reported");
        r = request("GET","/v1/series?keys=10500,999999&skip_missing=1","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"count\":1") && !bodyHas(r,"series_not_found"), "skip_missing omits it");
        r = request("GET","/v1/series?keys=notakey","read-only-key");
        CHECK(r.status==400, "a bad selector is 400");

        // the same selector shape on the data endpoint
        r = request("GET","/v1/data?keys=10500,10502&start=1700000000&end=1700000300","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"count\":2"), "keys=a,b on the data endpoint");

        // responses are chunked, and survive a size that would not fit one buffer
        r = request("GET","/v1/series?keys=10500","read-only-key");
        CHECK(headHas(r,"Transfer-Encoding: chunked"), "reads are streamed chunked");
        CHECK(!headHas(r,"Content-Length"), "and carry no Content-Length");

        // a body larger than the stream buffer must reassemble exactly
        r = request("POST","/v1/series/50000?type=uint8&interval=1&start=1700000000","read-write-key");
        CHECK(r.status==201, "create a byte series for a large read");
        std::string big;
        for(int i = 0; i < 40000; i++) big += "ab";      // 40000 points
        r = request("POST","/v1/series/50000/data?timestamp=1700000000","read-write-key",big);
        CHECK(r.status==200 && bodyHas(r,"\"points_written\":40000"), "40000 points written");
        r = request("GET","/v1/series/50000/data?start=1700000000&end=1700040000","read-only-key");
        CHECK(r.status==200, "large read succeeds");
        size_t data_at = r.body.find("\"data\":\"");
        std::string blob;
        if(data_at != std::string::npos){
            size_t from = data_at + 8;
            size_t to = r.body.find('"',from);
            if(to != std::string::npos) blob = r.body.substr(from,to - from);
        }
        CHECK(blob.size() == 80000, "the streamed blob is exactly the expected length");
        CHECK(blob.compare(0,80000,big) == 0, "and its bytes survived chunking intact");
    }

    printf("[12] push a reading into every series at now\n");
    {
        const long long T = 1700000000;

        // 60 second series, so slot boundaries are 60s apart from T
        REPLY r = request("POST","/v1/series/60001?type=uint8&interval=60&start=1700000000","read-write-key");
        CHECK(r.status==201, "create a 60s series");
        r = request("POST","/v1/series/60002?type=uint8&interval=60&start=1700000000","read-write-key");
        CHECK(r.status==201, "and another");

        char path[256];

        // 29s past a slot rounds back to it; 31s past rounds forward to the next
        snprintf(path,sizeof(path),"/v1/now?at=%lld&verbose=1",T + 29);
        r = request("POST",path,"read-write-key","60001 11\n");
        CHECK(r.status==200 && bodyHas(r,"\"slot\":1700000000"), "29s past a slot rounds back to it");

        snprintf(path,sizeof(path),"/v1/now?at=%lld&verbose=1",T + 31);
        r = request("POST",path,"read-write-key","60002 22\n");
        CHECK(r.status==200 && bodyHas(r,"\"slot\":1700000060"), "31s past a slot rounds up to the next");

        snprintf(path,sizeof(path),"/v1/now?at=%lld&verbose=1",T + 30);
        r = request("POST",path,"read-write-key","60002 23\n");
        CHECK(r.status==200 && bodyHas(r,"\"slot\":1700000060"), "exactly halfway takes the later slot");

        // several series in one request, no timestamps in the body at all
        snprintf(path,sizeof(path),"/v1/now?at=%lld",T + 121);
        r = request("POST",path,"read-write-key","60001 31\n60002 32\n");
        CHECK(r.status==200 && bodyHas(r,"\"records_written\":2"), "two series pushed in one request");
        CHECK(bodyHas(r,"\"overwritten\":0"), "nothing was replaced");
        CHECK(!bodyHas(r,"\"results\""), "and no detail when nothing is interesting");

        // pushing again into the same slot replaces a real reading, and says so
        snprintf(path,sizeof(path),"/v1/now?at=%lld",T + 122);
        r = request("POST",path,"read-write-key","60001 41\n60002 42\n");
        CHECK(r.status==200 && bodyHas(r,"\"overwritten\":2"), "a second push into the same slot is reported");
        CHECK(bodyHas(r,"\"results\"") && bodyHas(r,"\"overwritten\":true"), "and named per series");

        // the replacement really is what is stored
        r = request("GET","/v1/series/60001/data?start=1700000120&end=1700000180","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"data\":\"41\""), "the later value is the one kept");

        // a fresh slot is not an overwrite
        snprintf(path,sizeof(path),"/v1/now?at=%lld",T + 300);
        r = request("POST",path,"read-write-key","60001 51\n");
        CHECK(r.status==200 && bodyHas(r,"\"overwritten\":0"), "an empty slot is not an overwrite");

        // one value per series is the contract
        snprintf(path,sizeof(path),"/v1/now?at=%lld",T + 400);
        r = request("POST",path,"read-write-key","60001 5152\n");
        CHECK(r.status==422 && bodyHas(r,"expected_single_point"), "more than one point per line is refused");

        r = request("POST","/v1/now","read-write-key","");
        CHECK(r.status==400 && bodyHas(r,"empty_body"), "an empty body is 400");
        r = request("POST","/v1/now","read-write-key","60001 11 1700000000\n");
        CHECK(r.status==400 && bodyHas(r,"bad_record"), "a stray timestamp is refused");
        r = request("POST","/v1/now","read-write-key","60001 zz\n");
        CHECK(r.status==400 && bodyHas(r,"bad_hex"), "bad hex is 400");
        r = request("POST","/v1/now","read-only-key","60001 11\n");
        CHECK(r.status==403, "it needs the write key");
        r = request("GET","/v1/now","read-write-key");
        CHECK(r.status==405, "GET is 405");
        r = request("POST","/v1/now?at=notatime","read-write-key","60001 11\n");
        CHECK(r.status==400, "a bad at= is 400");

        // the general batch endpoint reports overwrites too
        r = request("POST","/v1/data","read-write-key","60001 1700000120 61\n");
        CHECK(r.status==200 && bodyHas(r,"\"overwritten\":1"), "the batch endpoint reports overwrites");
        r = request("POST","/v1/series/60001/data?timestamp=1700000120","read-write-key","62");
        CHECK(r.status==200 && bodyHas(r,"\"overwritten\":1"), "so does the single series write");
    }

    printf("[13] condensing for visualisation\n");
    {
        const long long T = 1700000000;

        // 600 one second slots: 0-99 hold 1..100, 100-199 are never written,
        // 200-599 all hold 50. The empty stretch is the interesting part.
        REPLY r = request("POST","/v1/series/70001?type=uint8&interval=1&start=1700000000","read-write-key");
        CHECK(r.status==201, "create a 1s uint8 series");

        std::string batch;
        char line[128];
        for(int i = 0; i < 600; i++){
            if(i >= 100 && i < 200) continue;          // deliberate gap
            snprintf(line,sizeof(line),"70001 %lld %02x\n", T + i, (i < 100) ? (i + 1) : 50);
            batch += line;
        }
        r = request("POST","/v1/data","read-write-key",batch);
        CHECK(r.status==200 && bodyHas(r,"\"records_written\":500"), "500 of 600 slots written");

        // min: the empty slots must not become the minimum
        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700000600&max_points=6&condense=min","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"condense\":\"min\"") && bodyHas(r,"\"factor\":100"), "min condenses to 6 buckets");
        CHECK(bodyHas(r,"\"type\":\"uint8\""), "min keeps the series type");
        CHECK(bodyHas(r,"\"data\":\"01ff32323232\""), "min: 1, gap, then 50s - the fill is not a minimum");

        // max: the fill is 0xff, which is also the largest uint8, so this is the
        // case where including empty slots would be silently wrong
        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700000600&max_points=6&condense=max","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"data\":\"64ff32323232\""), "max: 100 not 255 - empty slots excluded");

        // average: 1..100 averages 50.5, and the empty slots must not pull it down
        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700000600&max_points=6&condense=average","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"type\":\"float64\""), "average promotes to float64");
        CHECK(bodyHas(r,"\"datasize\":8"), "and says so");
        {
            size_t at = r.body.find("\"data\":\"");
            std::string blob;
            if(at != std::string::npos){
                size_t from = at + 8, to = r.body.find('"',at + 8);
                if(to != std::string::npos) blob = r.body.substr(from,to - from);
            }
            CHECK(blob.size() == 6 * 16, "six float64 buckets");
            double values[6];
            bool decoded = blob.size() == 96;
            for(int b = 0; b < 6 && decoded; b++){
                unsigned char bytes[8];
                for(int j = 0; j < 8; j++)
                    bytes[j] = (unsigned char)strtoul(blob.substr(b*16 + j*2,2).c_str(),NULL,16);
                memcpy(&values[b],bytes,8);
            }
            CHECK(decoded && values[0] > 50.4 && values[0] < 50.6, "bucket 0 averages 50.5, the true mean of 1..100");
            CHECK(decoded && values[1] != values[1], "the empty bucket is NaN, not 0 and not 255");
            CHECK(decoded && values[5] > 49.9 && values[5] < 50.1, "the last bucket averages 50");
        }

        // a bucket wider than the condense window, so bucket state must survive
        // several window reads (the window is 64 points in this test)
        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700000600&max_points=2&condense=max","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"factor\":300"), "two buckets of 300 slots each");
        CHECK(bodyHas(r,"\"data\":\"6432\""), "max survives buckets spanning several windows");

        // partial buckets: some real points, some empty
        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700000600&max_points=4&condense=average","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"n_points\":4"), "uneven bucketing still produces max_points buckets");

        // reported counts
        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700000600&max_points=6&condense=min","read-only-key");
        CHECK(bodyHas(r,"\"real_points\":5"), "five of six buckets contain data");
        CHECK(bodyHas(r,"\"points_scanned\":600"), "all 600 slots were walked");
        CHECK(bodyHas(r,"\"source_points\":500"), "500 of them held a reading");
        CHECK(bodyHas(r,"\"interval\":100") && bodyHas(r,"\"source_interval\":1"), "bucket and source intervals both given");

        // the pair rule
        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700000600&max_points=6","read-only-key");
        CHECK(r.status==400 && bodyHas(r,"together"), "max_points without condense is refused");
        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700000600&condense=min","read-only-key");
        CHECK(r.status==400 && bodyHas(r,"together"), "condense without max_points is refused");
        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700000600&max_points=6&condense=median","read-only-key");
        CHECK(r.status==400, "an unknown operation is refused");
        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700000600&max_points=0&condense=min","read-only-key");
        CHECK(r.status==400, "max_points=0 is refused");

        // condensing works on the multi series endpoint too
        r = request("POST","/v1/series/70002?type=uint8&interval=1&start=1700000000","read-write-key");
        r = request("POST","/v1/data","read-write-key","70002 1700000000 0a141e28\n");
        CHECK(r.status==200, "seed a second series");
        r = request("GET","/v1/data?keys=70001,70002&start=1700000000&end=1700000600&max_points=6&condense=max","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"count\":2"), "batch read condenses too");
        CHECK(bodyHas(r,"\"condense\":\"max\""), "and reports the operation per series");

        // The page is in the binary, so upgrading the binary is what changes it.
        // Without a validator a browser caches it heuristically and an upgraded
        // server serves a page the browser will not refetch.
        {
            REPLY r = request("GET","/admin",NULL);
            CHECK(r.status==200, "the admin page needs no key, it is where one is typed");
            CHECK(headHas(r,"Cache-Control: no-cache"), "it asks to be revalidated, not reused blind");
            CHECK(headHas(r,"ETag: \""), "and carries a tag derived from its bytes");

            // Pull the tag back out of the head to revalidate with it.
            std::string tag;
            {
                size_t at = r.head.find("ETag: ");
                if(at != std::string::npos){
                    size_t end = r.head.find("\r\n",at);
                    tag = r.head.substr(at + 6, end - (at + 6));
                }
            }
            CHECK(!tag.empty(), "the tag is readable");

            r = request("GET","/admin",NULL,"",NULL,("If-None-Match: " + tag).c_str());
            CHECK(r.status==304 && r.body.empty(), "an unchanged page revalidates to 304 with no body");

            r = request("GET","/admin",NULL,"",NULL,"If-None-Match: \"stale\"");
            CHECK(r.status==200 && !r.body.empty(), "a tag that no longer matches gets the page");

            // Different bytes, different tag, so upgrading always invalidates.
            REPLY js = request("GET","/admin/vue.global.prod.js",NULL);
            CHECK(js.status==200 && headHas(js,"ETag: \""), "the script is tagged too");
            CHECK(js.head.find(tag) == std::string::npos, "and its tag is not the page's");
        }

        // Profiles: what the stored values mean.
        {
            char profile_dir[512], profile_path[600];
            snprintf(profile_dir,sizeof(profile_dir),"%s/profiles",dir);
            mkdir(profile_dir,0755);
            snprintf(profile_path,sizeof(profile_path),"%s/pingt",profile_dir);

            FILE *f = fopen(profile_path,"w");
            CHECK(f != NULL, "write a profile file");
            if(f){
                fputs("literal  0-244  ms          #0000FF,#FF0000\n",f);
                fputs("bucket   245    245-500     \"over 244 ms\"   #FF8C00\n",f);
                fputs("bucket   246    500-1000    \"over 500 ms\"   #FF4500\n",f);
                fputs("state    254    no_reply    \"No reply\"      #000000\n",f);
                fclose(f);
            }

            REPLY r = request("GET","/v1/profiles","read-only-key");
            CHECK(r.status==200 && bodyHas(r,"\"pingt\""), "it is listed");

            r = request("GET","/v1/profiles/pingt","read-only-key");
            CHECK(r.status==200 && bodyHas(r,"\"kind\":\"bucket\""), "and readable");
            CHECK(headHas(r,"immutable"), "cached forever, because a profile never changes");

            r = request("GET","/v1/profiles/does_not_exist","read-only-key");
            CHECK(r.status==404, "a missing profile is a 404");

            // 10, 20, 245 (bucket 245-500), 246 (bucket 500-1000), 254 (no reply)
            r = request("POST","/v1/series/70005?type=uint8&interval=1&start=1700000000","read-write-key");
            CHECK(r.status==201, "a series to profile");
            r = request("POST","/v1/data","read-write-key","70005 1700000000 0a14f5f6fe\n");
            CHECK(r.status==200, "five points covering every kind");

            const char *RANGE = "/v1/series/70005/data?start=1700000000&end=1700000005&max_points=1";

            // A bucket's maximum is the top of the range it stands for, which a
            // uint8 cannot hold -- so a profiled read answers in magnitudes.
            r = request("GET",(std::string(RANGE) + "&condense=max&profile=pingt").c_str(),"read-only-key");
            CHECK(r.status==200 && bodyHas(r,"\"type\":\"float64\""), "profiled output is promoted");
            CHECK(bodyHas(r,"\"literal_points\":2"), "two exact readings");
            CHECK(bodyHas(r,"\"bucketed_points\":2"), "two known only to a range");
            CHECK(bodyHas(r,"\"reserved_points\":1"), "one that is not a reading at all");
            CHECK(bodyHas(r,"\"lower_bound\":true"), "and the answer is flagged as a floor");

            {
                size_t at = r.body.find("\"data\":\"");
                double value = 0;
                if(at != std::string::npos){
                    unsigned char bytes[8];
                    for(int j = 0; j < 8; j++)
                        bytes[j] = (unsigned char)strtoul(r.body.substr(at + 8 + j*2,2).c_str(),NULL,16);
                    memcpy(&value,bytes,8);
                }
                CHECK(value > 999.9 && value < 1000.1, "max is the high edge of the top bucket, 1000");
            }

            r = request("GET",(std::string(RANGE) + "&condense=min&profile=pingt").c_str(),"read-only-key");
            {
                size_t at = r.body.find("\"data\":\"");
                double value = 0;
                if(at != std::string::npos){
                    unsigned char bytes[8];
                    for(int j = 0; j < 8; j++)
                        bytes[j] = (unsigned char)strtoul(r.body.substr(at + 8 + j*2,2).c_str(),NULL,16);
                    memcpy(&value,bytes,8);
                }
                CHECK(value > 9.9 && value < 10.1, "min is the low edge, 10");
            }

            // (10 + 20 + 245 + 500) / 4, the bucket low edges, no reserved point
            r = request("GET",(std::string(RANGE) + "&condense=average&profile=pingt").c_str(),"read-only-key");
            {
                size_t at = r.body.find("\"data\":\"");
                double value = 0;
                if(at != std::string::npos){
                    unsigned char bytes[8];
                    for(int j = 0; j < 8; j++)
                        bytes[j] = (unsigned char)strtoul(r.body.substr(at + 8 + j*2,2).c_str(),NULL,16);
                    memcpy(&value,bytes,8);
                }
                CHECK(value > 193.7 && value < 193.8, "average uses the low edges and excludes the state");
            }

            // The profile comes back with the data, so nothing needs a second call.
            CHECK(bodyHas(r,"\"profiles\":{\"pingt\":"), "the profile is returned with the data");
            CHECK(bodyHas(r,"\"colours\":[\"#0000FF\",\"#FF0000\"]"), "including what a client needs to draw");

            r = request("GET","/v1/data?keys=70005,70001&start=1700000000&end=1700000005&max_points=1&condense=max&profile=pingt","read-only-key");
            CHECK(r.status==200, "a bulk read takes a profile too");
            {
                size_t first = r.body.find("\"entries\":[");
                size_t second = first == std::string::npos ? std::string::npos
                                                           : r.body.find("\"entries\":[",first + 1);
                CHECK(first != std::string::npos && second == std::string::npos,
                      "and sends it once for the request, not once per series");
            }

            r = request("GET",(std::string(RANGE) + "&condense=max&profile=pingt&reserved=254").c_str(),"read-only-key");
            CHECK(r.status==400, "profile and reserved together are refused, not silently ranked");

            r = request("GET","/v1/series/70005/data?start=1700000000&end=1700000005&profile=pingt","read-only-key");
            CHECK(r.status==400, "a profile on a raw read is refused; raw data stays as stored");

            r = request("GET",(std::string(RANGE) + "&condense=max&profile=absent").c_str(),"read-only-key");
            CHECK(r.status==404, "a profile that cannot be read fails the request rather than being ignored");

            r = request("GET",(std::string(RANGE) + "&condense=max&profile=../../etc/passwd").c_str(),"read-only-key");
            CHECK(r.status==404, "and a traversal never reaches the disk");

            // A table is any directory in the data directory, so the one holding
            // profiles has to be spoken for or it turns up as a table nobody made.
            r = request("GET","/v1/tables","read-only-key");
            CHECK(!bodyHas(r,"\"profiles\""), "the profiles directory is not listed as a table");
            r = request("POST","/v1/tables/profiles","read-write-key");
            CHECK(r.status==400, "and a table cannot be created with that name");
        }

        // Migrating a legacy series.
        //
        // Written as a version 1 file by hand, since nothing in this build produces
        // one any more: a 20 byte header whose typecode is a plain byte width, then
        // one of every value so each branch of the remapping is exercised.
        {
            char legacy_path[512];
            snprintf(legacy_path,sizeof(legacy_path),"%s/70004",dir);

            uint32_t head[5];
            head[0] = 1;                 // version 1
            head[1] = 1700000000;        // timestamp
            head[2] = 1;                 // interval
            head[3] = 1;                 // typecode: a plain width of one byte
            head[4] = 1234567890 + ((head[0] ^ head[1]) ^ (head[2] ^ head[3]));

            FILE *f = fopen(legacy_path,"wb");
            CHECK(f != NULL, "write a version 1 file by hand");
            if(f){
                fwrite(head,sizeof(head),1,f);
                for(int v = 0; v < 256; v++){       // 0..255, one of each
                    unsigned char b = (unsigned char)v;
                    fwrite(&b,1,1,f);
                }
                fclose(f);
            }

            REPLY r = request("GET","/v1/series/70004","read-only-key");
            CHECK(r.status==200 && bodyHas(r,"\"version\":1"), "it reads back as version 1");
            CHECK(bodyHas(r,"\"type\":\"uint8\""), "with the type inferred from the width");

            r = request("POST","/v1/series/70004/migrate","read-only-key");
            CHECK(r.status==403, "migrating needs a write key");

            r = request("POST","/v1/series/70004/migrate","read-write-key");
            CHECK(r.status==200 && bodyHas(r,"\"migrated\":true"), "the migration runs");
            CHECK(bodyHas(r,"\"points\":256"), "every point was walked");
            CHECK(bodyHas(r,"\"remapped\":1"), "the single 1 became the no reply value");
            CHECK(bodyHas(r,"\"clamped\":10"), "245 to 254 inclusive were pulled down, which is ten values");
            CHECK(bodyHas(r,"\"nulls\":1"), "the single 255 was left as fill");

            r = request("GET","/v1/series/70004","read-only-key");
            CHECK(bodyHas(r,"\"version\":3"), "the header is now version 3");

            // 0 and 2..243 untouched, 244 holds itself plus the ten clamped, 254
            // is the sentinel, 255 is still the fill.
            r = request("GET","/v1/series/70004/data?start=1700000000&end=1700000256","read-only-key");
            CHECK(r.status==200, "the migrated series reads");
            {
                size_t at = r.body.find("\"data\":\"");
                std::string blob;
                if(at != std::string::npos){
                    size_t from = at + 8, to = r.body.find('"',at + 8);
                    if(to != std::string::npos) blob = r.body.substr(from,to - from);
                }
                CHECK(blob.size() == 512, "256 one byte points");
                auto at_index = [&](int i){
                    return blob.size() == 512 ? (int)strtoul(blob.substr(i*2,2).c_str(),NULL,16) : -1;
                };
                CHECK(at_index(0) == 0,     "0 is still 0");
                CHECK(at_index(1) == 254,   "1 became the no reply value");
                CHECK(at_index(2) == 2,     "2 is untouched");
                CHECK(at_index(243) == 243, "243 is untouched");
                CHECK(at_index(244) == 244, "244 is untouched");
                CHECK(at_index(245) == 244, "245 was clamped");
                CHECK(at_index(253) == 244, "253 was clamped");
                CHECK(at_index(254) == 244, "254 was clamped, which is what clears the sentinel");
                CHECK(at_index(255) == 255, "255 is still the fill");
            }

            r = request("POST","/v1/series/70004/migrate","read-write-key");
            CHECK(r.status==409 && bodyHas(r,"already version 3"), "running it twice is refused");

            // A version 3 series was never a candidate.
            r = request("POST","/v1/series/70001/migrate","read-write-key");
            CHECK(r.status==409, "a series this build created is not migrated");
            r = request("POST","/v1/series/99999/migrate","read-write-key");
            CHECK(r.status==404, "a series that does not exist is a 404");
        }

        // points_in_file counts slots the file holds, so a series written to since
        // the last flush reports nothing on disk while reading back perfectly.
        // buffered_points is the difference, and points is what is actually there.
        r = request("POST","/v1/series/70003?type=uint8&interval=1&start=1700000000","read-write-key");
        CHECK(r.status==201 && bodyHas(r,"\"points_in_file\":0"), "a new series has nothing on disk");
        CHECK(bodyHas(r,"\"buffered_points\":0") && bodyHas(r,"\"points\":0"), "and nothing buffered either");

        r = request("POST","/v1/data","read-write-key","70003 1700000000 0a0b0c0d\n");
        CHECK(r.status==200, "write four points without flushing");

        r = request("GET","/v1/series/70003","read-only-key");
        CHECK(bodyHas(r,"\"points_in_file\":0"), "still nothing on disk");
        CHECK(bodyHas(r,"\"buffered_points\":4"), "the four are reported as buffered");
        CHECK(bodyHas(r,"\"points\":4"), "and counted in the series total");

        // The point of the distinction: they are readable regardless.
        r = request("GET","/v1/series/70003/data?start=1700000000&end=1700000004","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"data\":\"0a0b0c0d\""), "unflushed points read back");
        CHECK(bodyHas(r,"\"real_points\":4"), "and count as real, not as fill");

        // Reserved values.
        //
        // Bucket 0 holds 1..100, so reserving 1 removes exactly one point from it
        // and leaves every other bucket alone: the arithmetic is checkable by hand.
        const char *RANGE = "/v1/series/70001/data?start=1700000000&end=1700000600&max_points=6";

        r = request("GET",(std::string(RANGE) + "&condense=min").c_str(),"read-only-key");
        CHECK(bodyHas(r,"\"data\":\"01ff32323232\""), "without reserved, 1 is the minimum");

        r = request("GET",(std::string(RANGE) + "&condense=min&reserved=1").c_str(),"read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"data\":\"02ff32323232\""), "reserved 1 is not a minimum, 2 is");
        CHECK(bodyHas(r,"\"reserved\":[1]"), "the request's reserved list is echoed");
        CHECK(bodyHas(r,"\"reserved_mode\":\"exclude\""), "exclude is the default mode");
        CHECK(bodyHas(r,"\"reserved_points\":1"), "one reserved point in the whole range");
        CHECK(bodyHas(r,"\"reserved_counts\":\"010000000000000000000000000000000000000000000000\""),
              "and it is counted against bucket 0 alone");

        // 1..100 averages 50.5; take the 1 out and 2..100 averages 51 exactly.
        r = request("GET",(std::string(RANGE) + "&condense=average&reserved=1").c_str(),"read-only-key");
        CHECK(r.status==200, "average accepts reserved");
        {
            size_t at = r.body.find("\"data\":\"");
            double first = 0;
            if(at != std::string::npos){
                unsigned char bytes[8];
                for(int j = 0; j < 8; j++)
                    bytes[j] = (unsigned char)strtoul(r.body.substr(at + 8 + j*2,2).c_str(),NULL,16);
                memcpy(&first,bytes,8);
            }
            CHECK(first > 50.99 && first < 51.01, "the mean of 2..100 is 51, not the 50.5 that included the 1");
        }

        // A tenth of a real series was a reserved value, so dominating on sight
        // paints every bucket as an outage; the threshold is what makes it usable.
        r = request("GET",(std::string(RANGE) + "&condense=max&reserved=1&reserved_mode=dominate").c_str(),"read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"data\":\"01ff32323232\""), "one reserved point takes the bucket at threshold 0");

        r = request("GET",(std::string(RANGE) + "&condense=max&reserved=1&reserved_mode=dominate&reserved_threshold=0.5").c_str(),"read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"data\":\"64ff32323232\""), "one point in a hundred does not clear a half threshold");

        r = request("GET",(std::string(RANGE) + "&condense=max&reserved=1,50").c_str(),"read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"reserved\":[1,50]"), "a comma separated list is accepted");
        // 50 occurs once inside 1..100 and again in all 400 later slots, plus the
        // single 1.
        CHECK(bodyHas(r,"\"reserved_points\":402"), "both values are counted");

        // A response that named none must be exactly what it always was.
        r = request("GET",(std::string(RANGE) + "&condense=min").c_str(),"read-only-key");
        CHECK(!bodyHas(r,"reserved"), "no reserved parameter means no reserved fields");

        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700000600&reserved=1","read-only-key");
        CHECK(r.status==400, "reserved on a raw read is refused rather than ignored");
        r = request("GET",(std::string(RANGE) + "&condense=min&reserved=nope").c_str(),"read-only-key");
        CHECK(r.status==400, "a non numeric reserved value is refused");
        r = request("GET",(std::string(RANGE) + "&condense=min&reserved=1&reserved_mode=sometimes").c_str(),"read-only-key");
        CHECK(r.status==400, "an unknown reserved_mode is refused");
        r = request("GET",(std::string(RANGE) + "&condense=min&reserved=1&reserved_threshold=0.5").c_str(),"read-only-key");
        CHECK(r.status==400, "a threshold without dominate is refused");
        r = request("GET",(std::string(RANGE) + "&condense=min&reserved=1&reserved_mode=dominate&reserved_threshold=7").c_str(),"read-only-key");
        CHECK(r.status==400, "a threshold outside 0..1 is refused");

        // a condensed range may exceed the raw read cap, since it is windowed
        r = request("GET","/v1/series/70001/data?start=1&end=1700000600","read-only-key");
        CHECK(r.status==413 && bodyHas(r,"max_points"), "a huge raw range is refused and suggests condensing");
    }

    printf("[14] tables\n");
    {
        REPLY r = request("GET","/v1/tables","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"default\""), "the data directory itself is the default table");

        r = request("POST","/v1/tables/network","read-write-key");
        CHECK(r.status==201 && bodyHas(r,"\"created\":true"), "create a table");
        r = request("POST","/v1/tables/network","read-write-key");
        CHECK(r.status==409 && bodyHas(r,"table_exists"), "creating it twice is 409");
        r = request("GET","/v1/tables","read-only-key");
        CHECK(bodyHas(r,"\"network\"") && bodyHas(r,"\"count\":2"), "it is listed");

        // the same key in two tables is two different series
        r = request("POST","/v1/network/series/1?type=float32&interval=60&start=1700000000","read-write-key");
        CHECK(r.status==201, "create series 1 in network");
        r = request("POST","/v1/network/data","read-write-key","1 1700000000 0000a441\n");
        CHECK(r.status==200, "write to network series 1");
        r = request("GET","/v1/network/series/1/data?start=1700000000&end=1700000120","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"data\":\"0000a441ffffffff\""), "read it back from network");

        r = request("POST","/v1/tables/power","read-write-key");
        CHECK(r.status==201, "create a second table");
        r = request("GET","/v1/power/series/1","read-only-key");
        CHECK(r.status==404 && bodyHas(r,"series_not_found"), "series 1 in power is a different series");

        // the unqualified paths still address the default table
        r = request("GET","/v1/series/10500","read-only-key");
        CHECK(r.status==200, "the legacy path still works");
        r = request("GET","/v1/default/series/10500","read-only-key");
        CHECK(r.status==200, "and names the same series through the default table");
        r = request("GET","/v1/network/series/10500","read-only-key");
        CHECK(r.status==404, "which is not the same series as in another table");

        // unknown tables
        r = request("GET","/v1/nosuchtable/series","read-only-key");
        CHECK(r.status==404 && bodyHas(r,"table_not_found"), "an unknown table is 404");
        r = request("POST","/v1/nosuchtable/data","read-write-key","1 1700000000 0a\n");
        CHECK(r.status==404 && bodyHas(r,"table_not_found"), "and a write to one does not create it");

        // a table name is a directory name, so it must not be able to escape
        const char *escapes[] = {
            "/v1/../series", "/v1/..%2f..%2fetc/series", "/v1/.%2e/series",
            "/v1/a%2fb/series", "/v1/%2e%2e/series", "/v1/foo.bar/series",
            "/v1/-lead/series", "/v1/9numeric/series", "/v1/net2/series"
        };
        for(unsigned i=0;i<sizeof(escapes)/sizeof(escapes[0]);i++){
            r = request("GET",escapes[i],"read-only-key");
            char msg[160];
            snprintf(msg,sizeof(msg),"refused: %s",escapes[i]);
            CHECK(r.status==400 || r.status==404, msg);
        }

        r = request("POST","/v1/tables/data","read-write-key");
        CHECK(r.status==400 && bodyHas(r,"reserved"), "an endpoint name cannot be a table");
        r = request("POST","/v1/tables/health","read-write-key");
        CHECK(r.status==400, "nor can health");

        // dropping
        r = request("DELETE","/v1/tables/power","read-write-key");
        CHECK(r.status==200 && bodyHas(r,"\"dropped\":true"), "an empty table drops");
        r = request("DELETE","/v1/tables/network","read-write-key");
        CHECK(r.status==409 && bodyHas(r,"table_not_empty"), "one holding series does not");
        r = request("GET","/v1/network/series/1","read-only-key");
        CHECK(r.status==200, "and its series survived the refusal");
        r = request("DELETE","/v1/tables/network?force=1","read-write-key");
        CHECK(r.status==200, "force drops it with its series");
        r = request("GET","/v1/network/series","read-only-key");
        CHECK(r.status==404, "and it is gone");
        r = request("DELETE","/v1/tables/default?force=1","read-write-key");
        CHECK(r.status==400, "the default table cannot be dropped, it is the data directory");

        r = request("GET","/v1/tables/network","read-only-key");
        CHECK(r.status==404, "info on a dropped table is 404");
        r = request("POST","/v1/tables/network","read-only-key");
        CHECK(r.status==403, "creating a table needs the write key");
    }

    printf("[15] keys over the API\n");
    {
        REPLY r = request("POST","/v1/auth/keys?role=write&name=minted","read-write-key");
        CHECK(r.status==201 && bodyHas(r,"\"key\":\"bsw_"), "mint a write key through the API");

        std::string minted;
        size_t at = r.body.find("\"key\":\"");
        if(at != std::string::npos){
            size_t from = at + 7, to = r.body.find('"',from);
            if(to != std::string::npos) minted = r.body.substr(from,to - from);
        }
        CHECK(minted.size() > 8, "and it came back in the response");

        r = request("GET","/v1/tables",minted.c_str());
        CHECK(r.status==200, "the minted key works on the API");
        r = request("POST","/v1/auth/keys?role=read&name=minted_reader",minted.c_str());
        CHECK(r.status==201, "and can mint others");

        std::string reader;
        at = r.body.find("\"key\":\"");
        if(at != std::string::npos){
            size_t from = at + 7, to = r.body.find('"',from);
            if(to != std::string::npos) reader = r.body.substr(from,to - from);
        }
        r = request("GET","/v1/tables",reader.c_str());
        CHECK(r.status==200, "a minted read key reads");
        r = request("POST","/v1/tables/blocked",reader.c_str());
        CHECK(r.status==403, "but does not write");

        r = request("GET","/v1/auth/keys","read-write-key");
        CHECK(r.status==200 && bodyHas(r,"\"minted\"") && bodyHas(r,"\"minted_reader\""), "keys are listed");
        CHECK(!bodyHas(r,"bsw_") && !bodyHas(r,"bsr_"), "and the listing never contains a secret");

        r = request("GET","/v1/auth/keys",reader.c_str());
        CHECK(r.status==403, "listing keys needs a write key");

        r = request("POST","/v1/auth/keys?role=write&name=minted","read-write-key");
        CHECK(r.status==409 && bodyHas(r,"key_exists"), "duplicate names are refused");
        r = request("POST","/v1/auth/keys?role=admin&name=x","read-write-key");
        CHECK(r.status==400, "an unknown role is refused");
        r = request("POST","/v1/auth/keys?role=write&name=has%20space","read-write-key");
        CHECK(r.status==400, "a bad key name is refused");
        r = request("POST","/v1/auth/keys?role=write&name=nokey",NULL);
        CHECK(r.status==401, "minting needs credentials");

        r = request("DELETE","/v1/auth/keys/minted_reader","read-write-key");
        CHECK(r.status==200 && bodyHas(r,"\"revoked\":true"), "revoke a key");
        r = request("GET","/v1/tables",reader.c_str());
        CHECK(r.status==401, "and it stops working immediately");
        r = request("DELETE","/v1/auth/keys/minted_reader","read-write-key");
        CHECK(r.status==404, "revoking it twice is 404");

        // the config keys keep working alongside the store
        r = request("GET","/v1/tables","read-only-key");
        CHECK(r.status==200, "a configured key still works");
        r = request("DELETE","/v1/auth/keys/minted","read-write-key");
        CHECK(r.status==200, "the store's only write key may go while a config one exists");
    }

    printf("[16] runtime settings\n");
    {
        REPLY r = request("GET","/v1/config","read-only-key");
        CHECK(r.status==200 && bodyHas(r,"\"flush_interval\":60"), "settings are readable");
        CHECK(bodyHas(r,"\"series_max_idle\":900"), "all of them");

        r = request("POST","/v1/config?flush_interval=15","read-write-key");
        CHECK(r.status==200 && bodyHas(r,"\"flush_interval\":15"), "the flush interval can be changed");
        CHECK(runtime.flush_interval.load()==15, "and the change reaches the maintenance thread");

        r = request("GET","/v1/config","read-only-key");
        CHECK(bodyHas(r,"\"flush_interval\":15"), "and is what a later read returns");

        r = request("POST","/v1/config?flush_interval=0","read-write-key");
        CHECK(r.status==200 && runtime.flush_interval.load()==0, "0 disables the timer");
        r = request("POST","/v1/config?flush_interval=60","read-write-key");
        CHECK(runtime.flush_interval.load()==60, "and it can be turned back on");

        // several at once, and the request limits are live too
        r = request("POST","/v1/config?max_points_per_read=1234&series_max_idle=300","read-write-key");
        CHECK(r.status==200 && runtime.max_points_per_read.load()==1234, "several settings in one call");
        CHECK(runtime.series_max_idle.load()==300, "both applied");
        r = request("GET","/v1/series/70001/data?start=1700000000&end=1700010000","read-only-key");
        CHECK(r.status==413, "a lowered read ceiling takes effect immediately");
        r = request("POST","/v1/config?max_points_per_read=100000","read-write-key");
        CHECK(runtime.max_points_per_read.load()==100000, "put back");

        // validation, and all-or-nothing
        int before = runtime.flush_interval.load();
        r = request("POST","/v1/config?flush_interval=5&max_points_per_read=0","read-write-key");
        CHECK(r.status==400, "an out of range value is refused");
        CHECK(runtime.flush_interval.load()==before, "and nothing in that request was applied");
        r = request("POST","/v1/config?flush_interval=notanumber","read-write-key");
        CHECK(r.status==400, "a non numeric value is refused");
        r = request("POST","/v1/config?nosuchsetting=1","read-write-key");
        CHECK(r.status==400 && bodyHas(r,"at least one"), "an unknown setting changes nothing");
        r = request("POST","/v1/config","read-write-key");
        CHECK(r.status==400, "naming no setting is refused");

        r = request("POST","/v1/config?flush_interval=30","read-only-key");
        CHECK(r.status==403, "changing settings needs a write key");
        r = request("GET","/v1/config",NULL);
        CHECK(r.status==401, "and reading them needs a key");
        r = request("DELETE","/v1/config","read-write-key");
        CHECK(r.status==405, "DELETE is 405");
    }

    printf("[17] method handling\n");
    {
        REPLY r = request("DELETE","/v1/series","read-write-key");
        CHECK(r.status==405, "DELETE on the collection is 405");
        r = request("PATCH","/v1/series/10500","read-write-key");
        CHECK(r.status==405, "unsupported method is 405");
    }

    server.stop();
    serving.join();
    tables.closeAll();

    return testReport();
}
