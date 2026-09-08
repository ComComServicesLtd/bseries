// End to end tests for the HTTP API. Starts a real server on an ephemeral port
// and talks to it over a real socket.

#include "bseries.h"
#include "bseries_api.h"
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
} REPLY;


static REPLY request(const char *method, const std::string &path, const char *api_key,
                     const std::string &body = "", const char *origin = NULL){

    REPLY reply;
    reply.status = -1;

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
        reply.body = raw.substr(split + 4);
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
    config.max_points_per_read = 10000;
    config.max_body_bytes = 4096;
    config.default_interval = 10;

    BSeries db;
    db.data_directory = dir;
    db.default_seconds_per_point = config.default_interval;

    BSeriesApi api(&db,&config);

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

        std::string huge(9000,'a');
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
        CHECK(bodyHas(r,"\"state\":\"written\"") && bodyHas(r,"\"state\":\"failed\""), "each record's state given");
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

    printf("[11] method handling\n");
    {
        REPLY r = request("DELETE","/v1/series","read-write-key");
        CHECK(r.status==405, "DELETE on the collection is 405");
        r = request("PATCH","/v1/series/10500","read-write-key");
        CHECK(r.status==405, "unsupported method is 405");
    }

    server.stop();
    serving.join();
    db.close();

    return testReport();
}
