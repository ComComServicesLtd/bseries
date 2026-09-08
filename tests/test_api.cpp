// End to end tests for the HTTP API. Starts a real server on an ephemeral port
// and talks to it over a real socket.

#include "bseries.h"
#include "bseries_api.h"
#include "table_set.h"
#include "auth_store.h"
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
                     const std::string &body = "", const char *origin = NULL){

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

    BSeriesApi api(&tables,&auth,&config);

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

    printf("[16] method handling\n");
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
