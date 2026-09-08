/// bseriesd - HTTP front end for a bseries database.
///
/// Depends on nothing but libc, pthreads and the POSIX socket API, so it builds
/// for arm, arm64 and x86 with the same makefile and no libraries to chase.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <thread>
#include <atomic>
#include <vector>
#include <string>

#include "bseries.h"
#include "bseries_api.h"
#include "http_server.h"
#include "table_set.h"


static HttpServer *g_server = NULL;
static std::atomic<bool> g_stopping(false);


static void handleSignal(int number){
    (void)number;
    g_stopping.store(true);
    if(g_server != NULL)
        g_server->stop();
}


/// Flushes and drops series that have gone quiet. Without this a long running
/// server holds an entry, and a write ahead buffer, for every key it has ever
/// touched, and buffered points sit in memory indefinitely.

static void maintenanceThread(TableSet *tables, int idle_seconds, int interval_seconds){

    if(idle_seconds <= 0 || interval_seconds <= 0)
        return;

    while(!g_stopping.load()){

        for(int waited = 0; waited < interval_seconds && !g_stopping.load(); waited++){
            struct timespec pause = { 1, 0 };
            nanosleep(&pause,NULL);
        }

        if(g_stopping.load())
            break;

        tables->maintain((uint32_t)idle_seconds);
    }
}


static void usage(const char *program){
    fprintf(stderr,
        "usage: %s -c <config file>\n"
        "\n"
        "  -c <path>   configuration file, see bseriesd.conf.example\n"
        "  -h          this message\n",
        program);
}


int main(int argc, char **argv){

    const char *config_path = NULL;

    for(int i = 1; i < argc; i++){

        if(strcmp(argv[i],"-c") == 0 && i + 1 < argc){
            config_path = argv[++i];
        } else if(strcmp(argv[i],"-h") == 0){
            usage(argv[0]);
            return 0;
        } else {
            usage(argv[0]);
            return 2;
        }
    }

    if(config_path == NULL){
        usage(argv[0]);
        return 2;
    }

    API_CONFIG config;
    apiConfigDefaults(&config);

    std::string error;

    if(apiLoadConfig(config_path,&config,&error) != NO_ERROR){
        fprintf(stderr,"bseriesd: %s\n",error.c_str());
        return 1;
    }

    if(config.data_directory.empty()){
        fprintf(stderr,"bseriesd: data_directory is not set in %s\n",config_path);
        return 1;
    }

    if(config.read_key.empty() && config.write_key.empty()){
        fprintf(stderr,"bseriesd: neither read_key nor write_key is set, nothing could authenticate\n");
        return 1;
    }

    if(config.read_key == config.write_key && !config.read_key.empty()){
        fprintf(stderr,"bseriesd: read_key and write_key are the same, the read only key would grant writes\n");
        return 1;
    }

    if(config.write_key.empty())
        fprintf(stderr,"bseriesd: no write_key set, the API is read only\n");

    TableSet tables;
    tables.configure(config.data_directory,config.write_ahead_size,config.default_interval,config.max_grow_points);
    tables.auto_create = config.auto_create_tables;

    if(!config.definitions_path.empty()){

        std::string error;
        int loaded = tables.loadDefinitions(config.definitions_path.c_str(),&error);

        if(loaded < 0){
            fprintf(stderr,"bseriesd: %s\n",error.c_str());
            tables.closeAll();
            return 1;
        }

        fprintf(stderr,"bseriesd: loaded %d series definitions\n",loaded);
    }

    {
        std::vector<std::string> names;
        tables.list(&names);
        fprintf(stderr,"bseriesd: %lu table%s\n",(unsigned long)names.size(),names.size() == 1 ? "" : "s");
    }

    BSeriesApi api(&tables,&config);

    HttpServer server;
    server.max_connections = config.max_connections;
    server.max_body_bytes = config.max_body_bytes;

    g_server = &server;

    signal(SIGINT,handleSignal);
    signal(SIGTERM,handleSignal);

    if(!server.start(config.bind_address.c_str(),config.port)){
        tables.closeAll();
        return 1;
    }

    fprintf(stderr,"bseriesd: listening on %s port %d, data in %s\n",
            config.bind_address.c_str(),config.port,config.data_directory.c_str());

    std::thread maintenance(maintenanceThread,&tables,config.series_max_idle_seconds,config.maintenance_interval_seconds);

    server.run(BSeriesApi::handle,&api);

    g_stopping.store(true);
    maintenance.join();

    fprintf(stderr,"bseriesd: shutting down\n");

    tables.closeAll(); // flushes every table's write ahead buffers to disk

    return 0;
}
