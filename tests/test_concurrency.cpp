#include "bseries.h"
#include "test_util.h"
#include <stdio.h>
#include <atomic>
#include <chrono>
#include <vector>
#include <thread>

static std::atomic<bool> stop(false);
static std::atomic<long> writes(0), reads(0), evicts(0);

int main(int argc,char**argv){
    (void)argc; (void)argv;
    const char *dir = testMakeDirectory();
    BSeries db; db.data_directory = dir; db.default_seconds_per_point = 1;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);

    std::vector<std::thread> th;
    for(int w=0; w<4; w++) th.emplace_back([&,w]{
        unsigned char v = (unsigned char)w;
        unsigned int seed = w*7919+1;
        while(!stop.load()){
            uint32_t key = (seed = seed*1103515245+12345) % 12;
            db.write(key,&v,1,0);
            writes++;
        }
    });
    for(int r=0; r<2; r++) th.emplace_back([&]{
        while(!stop.load()){
            int64_t n=0,rp=0,spp=0,fpt=0; uint32_t ds=0; void*res=NULL;
            int64_t now = time(NULL);
            if(db.read(now%12, now-30, now, &n,&rp,&spp,&fpt,&ds,&res)==NO_ERROR)
                delete[] (char*)res;
            reads++;
        }
    });
    th.emplace_back([&]{
        while(!stop.load()){ db.closeSeries(0); evicts++; }
    });

    while(std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
    stop.store(true);
    for(auto&t:th) t.join();
    db.close();
    printf("  writes=%ld reads=%ld evict passes=%ld\n", writes.load(), reads.load(), evicts.load());
    return testReport();
}
