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
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include "bseries.h"
#include "bseries_api.h"
#include "http_server.h"
#include "table_set.h"
#include "auth_store.h"
#include "runtime_settings.h"


static HttpServer *g_server = NULL;
static std::atomic<bool> g_stopping(false);


static void handleSignal(int number){
    (void)number;
    g_stopping.store(true);
    if(g_server != NULL)
        g_server->stop();
}


/// Two jobs, on their own schedules.
///
/// Flushing bounds how long a point can sit in memory. The write ahead buffer
/// holds a number of points rather than a span of time, so on a slow series an
/// actively written buffer can hold days of data; the flush timer is what turns
/// that into a number of seconds an operator can state.
///
/// Eviction drops series that have gone quiet, so a long running server does not
/// hold an entry and a buffer for every key it has ever touched.
///
/// The loop ticks every second and re-reads its intervals each time, so a change
/// made through /v1/config takes effect on the next tick rather than after the
/// old interval has run out.

static void maintenanceThread(TableSet *tables, RUNTIME_SETTINGS *runtime){

    time_t last_flush = time(NULL);
    time_t last_evict = time(NULL);

    while(!g_stopping.load()){

        struct timespec pause = { 1, 0 };
        nanosleep(&pause,NULL);

        if(g_stopping.load())
            break;

        time_t now = time(NULL);

        int flush_interval = runtime->flush_interval.load();

        if(flush_interval > 0 && now - last_flush >= (time_t)flush_interval){
            tables->flushAged((uint32_t)flush_interval);
            last_flush = now;
        }

        int idle = runtime->series_max_idle.load();
        int tick = runtime->maintenance_interval.load();

        if(tick < 1)
            tick = 1;

        if(idle > 0 && now - last_evict >= (time_t)tick){
            tables->maintain((uint32_t)idle);
            last_evict = now;
        }
    }
}


static void usage(const char *program){
    fprintf(stderr,
        "usage: %s [-c <config file>] [--health]\n"
        "\n"
        "  -c <path>   configuration file, see bseriesd.conf.example.\n"
        "              Optional: every setting also reads from the environment as\n"
        "              BSERIES_<SETTING>, which is what a container usually wants.\n"
        "  --health    connect to the configured address and exit 0 if the server\n"
        "              is answering, 1 if it is not. Meant for a container health\n"
        "              check, so the image needs no curl.\n"
        "  -h          this message\n",
        program);
}


/// Asks a running server whether it is alive, using nothing but sockets so that a
/// minimal image does not have to carry an HTTP client just to be health checked.

static int healthCheck(const API_CONFIG &config){

    char port_text[16];
    snprintf(port_text,sizeof(port_text),"%d",config.port);

    // An address the server binds to may be a wildcard, which is not a useful
    // thing to connect to; from inside the container the loopback is.
    std::string host = config.bind_address;

    if(host.empty() || host == "0.0.0.0" || host == "*")
        host = "127.0.0.1";
    else if(host == "::")
        host = "::1";

    struct addrinfo hints;
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *results = NULL;

    if(getaddrinfo(host.c_str(),port_text,&hints,&results) != 0)
        return 1;

    int fd = -1;

    for(struct addrinfo *candidate = results; candidate != NULL; candidate = candidate->ai_next){

        fd = socket(candidate->ai_family,candidate->ai_socktype,candidate->ai_protocol);

        if(fd < 0)
            continue;

        struct timeval timeout;
        timeout.tv_sec = 5;
        timeout.tv_usec = 0;
        setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));
        setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&timeout,sizeof(timeout));

        if(connect(fd,candidate->ai_addr,candidate->ai_addrlen) == 0)
            break;

        close(fd);
        fd = -1;
    }

    freeaddrinfo(results);

    if(fd < 0)
        return 1;

    const char *request =
        "GET /v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

    size_t length = strlen(request);
    size_t sent = 0;

    while(sent < length){
        ssize_t wrote = send(fd,request + sent,length - sent,0);
        if(wrote <= 0){ close(fd); return 1; }
        sent += (size_t)wrote;
    }

    char reply[512];
    ssize_t got = recv(fd,reply,sizeof(reply)-1,0);
    close(fd);

    if(got <= 0)
        return 1;

    reply[got] = 0;

    return strstr(reply,"200") != NULL ? 0 : 1;
}


int main(int argc, char **argv){

    // Whatever does reach stdout goes out a line at a time. Under docker logs it
    // is a pipe, and a block buffered pipe means output that appears late or, on
    // a hard kill, not at all. Diagnostics go to stderr, which is unbuffered.
    setvbuf(stdout,NULL,_IOLBF,0);

    const char *config_path = getenv("BSERIES_CONFIG");
    bool health_only = false;

    for(int i = 1; i < argc; i++){

        if(strcmp(argv[i],"-c") == 0 && i + 1 < argc){
            config_path = argv[++i];
        } else if(strcmp(argv[i],"--health") == 0){
            health_only = true;
        } else if(strcmp(argv[i],"-h") == 0){
            usage(argv[0]);
            return 0;
        } else {
            usage(argv[0]);
            return 2;
        }
    }

    API_CONFIG config;
    apiConfigDefaults(&config);

    std::string error;

    // The file is optional. A container is usually configured entirely from its
    // environment, and requiring a file would mean baking one into every image.
    if(config_path != NULL && apiLoadConfig(config_path,&config,&error) != NO_ERROR){
        fprintf(stderr,"bseriesd: %s\n",error.c_str());
        return 1;
    }

    // Environment last, so it wins over a file baked into an image.
    if(apiApplyEnvironment(&config,&error) != NO_ERROR){
        fprintf(stderr,"bseriesd: %s\n",error.c_str());
        return 1;
    }

    if(health_only)
        return healthCheck(config);

    if(config.data_directory.empty()){
        fprintf(stderr,"bseriesd: data_directory is not set; give it in a config file or as BSERIES_DATA_DIRECTORY\n");
        return 1;
    }

    if(config.read_key == config.write_key && !config.read_key.empty()){
        fprintf(stderr,"bseriesd: read_key and write_key are the same, the read only key would grant writes\n");
        return 1;
    }

    if(config.keystore_path.empty())
        config.keystore_path = config.data_directory + "/auth.keys";

    AuthStore auth;

    if(auth.load(config.keystore_path) < 0){
        fprintf(stderr,"bseriesd: could not read the keystore at %s\n",config.keystore_path.c_str());
        return 1;
    }

    // A database with no write key anywhere cannot be administered, and letting
    // whoever reaches the port first mint the first key would be a race worth
    // losing. So a one time token is minted and printed here, where it takes
    // access to the console or the logs to read it, and is accepted in place of a
    // key only for creating that first key.
    if(config.write_key.empty() && !auth.hasRole(AUTH_ROLE_WRITE)){

        std::string token = auth.mintBootstrapToken();

        if(token.empty()){
            fprintf(stderr,"bseriesd: no write key, and no system randomness to mint a bootstrap token\n");
            return 1;
        }

        // A wildcard bind address is not something anyone can curl, and printing
        // it is the first thing a container user would try and fail with.
        const char *reachable = config.bind_address.c_str();

        if(config.bind_address.empty() || config.bind_address == "0.0.0.0" || config.bind_address == "*")
            reachable = "localhost";
        else if(config.bind_address == "::")
            reachable = "[::1]";

        fprintf(stderr,
            "\nbseriesd: this database has no write key yet.\n"
            "  Create the first one with:\n\n"
            "    curl -XPOST -H 'X-API-Key: %s' \\\n"
            "      '%s:%d/v1/auth/keys?role=write&name=admin'\n\n"
            "  This token works only for that, and only until a write key exists.\n\n",
            token.c_str(),reachable,config.port);
    }

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

    RUNTIME_SETTINGS runtime;
    runtime.flush_interval.store(config.flush_interval);
    runtime.series_max_idle.store(config.series_max_idle_seconds);
    runtime.maintenance_interval.store(config.maintenance_interval_seconds);
    runtime.max_points_per_read.store(config.max_points_per_read);
    runtime.max_series_per_read.store(config.max_series_per_read);
    runtime.max_points_per_write.store(config.max_points_per_write);
    runtime.max_condense_scan.store(config.max_condense_scan);

    BSeriesApi api(&tables,&auth,&runtime,&config);

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

    if(config.flush_interval > 0)
        fprintf(stderr,"bseriesd: buffered points are flushed after at most %d seconds\n",config.flush_interval);
    else
        fprintf(stderr,"bseriesd: flush timer disabled; points sit in memory until their buffer fills\n");

    std::thread maintenance(maintenanceThread,&tables,&runtime);

    server.run(BSeriesApi::handle,&api);

    g_stopping.store(true);
    maintenance.join();

    fprintf(stderr,"bseriesd: shutting down\n");

    tables.closeAll(); // flushes every table's write ahead buffers to disk

    return 0;
}
