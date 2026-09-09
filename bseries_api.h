#ifndef BSERIES_API_H
#define BSERIES_API_H

#include <string>
#include <vector>

#include "bseries.h"
#include "http_server.h"
#include "table_set.h"
#include "auth_store.h"
#include "runtime_settings.h"


/// HTTP CRUD interface to a BSeries database.
///
/// Point data crosses the wire as hex, because a series holds raw binary of
/// whatever width the series was defined with and hex survives being pasted into a
/// shell, a log or a JSON string without an encoding argument.
///
/// Gaps are not marked separately. A series is dense, so every interval between the
/// first and last point occupies space whether or not anything was recorded, and
/// the bytes standing in for "nothing was recorded" are the series' null fill.
/// Every read response carries null_fill as a hex pattern one point wide, so a
/// client can find the gaps by comparing each point against it. Note the
/// consequence: a real reading equal to the fill is indistinguishable from a gap.
/// For float series the fill is a NaN and nothing is lost; for integer series pick
/// a fill your data cannot produce, in the series definitions file.


typedef struct {

    std::string bind_address;
    int port;

    std::string data_directory;
    std::string definitions_path;

    /// When false, a write to a table that does not exist is a 404 rather than a
    /// new table. Off by default: a typo in a table name should not quietly fork
    /// a database's data into a second copy nobody is reading.
    bool auto_create_tables;

    /// Static keys from the configuration file. Optional now that keys can be
    /// managed over HTTP; when set they work alongside the keystore, which is what
    /// keeps an existing deployment's configuration valid.
    std::string read_key;
    std::string write_key;

    /// Where minted keys are kept. Defaults to auth.keys in the data directory.
    std::string keystore_path;

    /// Origins allowed to call the API from a browser. A single entry of "*"
    /// allows any origin. Empty means no CORS headers are sent at all, which
    /// blocks browser callers and is the right default for a server side client.
    std::vector<std::string> cors_origins;

    /// Starting values for the settings that can later be changed over HTTP; see
    /// runtime_settings.h. Once the server is up, RUNTIME_SETTINGS is the truth.
    int max_points_per_read;   // shared across every series in one request
    int max_series_per_read;
    int max_condense_scan;     // input points a condensed request may walk
    int condense_window_points; // input points held at once while condensing
    int flush_interval;        // seconds a series may hold buffered points
    int max_points_per_write;  // total points one write request may carry
    int max_grow_points;       // how far past the end of a series one write may reach
    int max_body_bytes;
    int max_connections;
    int write_ahead_size;
    int default_interval;

    /// A series untouched for this long is flushed and dropped from memory by the
    /// maintenance thread. Zero disables it.
    int series_max_idle_seconds;
    int maintenance_interval_seconds;

} API_CONFIG;


void apiConfigDefaults(API_CONFIG *config);

/// Reads a "key value" configuration file. Returns NO_ERROR, or a negative error.
/// Unknown keys are an error rather than a silent no-op, so a typo in a key name
/// cannot leave the server listening with settings the operator did not intend.
int apiLoadConfig(const char *path, API_CONFIG *config, std::string *error_out);

/// Applies BSERIES_* environment variables over whatever is already in config.
///
/// Every setting the file understands has an environment twin: listen becomes
/// BSERIES_LISTEN, max_points_per_read becomes BSERIES_MAX_POINTS_PER_READ, and so
/// on. This is what lets the server run from `docker run -e` with no file at all,
/// which is how a container expects to be configured.
///
/// Environment wins over the file, since the file is baked into an image and the
/// environment is what the operator sets at run time.
int apiApplyEnvironment(API_CONFIG *config, std::string *error_out);


/// Values that are recorded, but are not measurements.
///
/// A prober that writes 1 for "no reply" is storing an outcome, not a latency,
/// and averaging it with milliseconds is meaningless: on a real series here, 11%
/// of readings were that 1, and including them moved the mean *down* by 2.5ms,
/// making a lossy link look faster than a clean one.
///
/// Which value carries that meaning is the caller's business, not the file's --
/// another series may use 1 as an ordinary reading -- so this arrives per request
/// and nothing about it is stored. Naming none leaves condensing exactly as it
/// was.
///
/// Reserved points never join an average or a minimum; they are counted per
/// bucket instead, and that count is returned so the loss can be charted beside
/// the latency rather than hidden inside it.

typedef struct {
    std::vector<double> values;   // empty when the caller named none
    bool dominate;                // let them win a maximum, rather than be skipped
    double threshold;             // share of a bucket needed before they do
} CONDENSE_RESERVED;


/// HTTP interface to a set of tables.
///
/// Every data path is rooted at a table: /v1/<table>/series, /v1/<table>/data and
/// so on. The unqualified paths that predate tables still work and address the
/// table named "default", which is the data directory itself.

class BSeriesApi
{
public:
    BSeriesApi(TableSet *table_set, AuthStore *auth_store, RUNTIME_SETTINGS *settings, const API_CONFIG *config);

    /// The HTTP_HANDLER entry point. Pass the BSeriesApi instance as context.
    static void handle(const HTTP_REQUEST &request, HTTP_RESPONSE &response, void *context);

    TableSet *tables;
    AuthStore *auth;
    RUNTIME_SETTINGS *runtime;
    API_CONFIG config;

private:
    void route(const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void routeTable(BSeries *db, const std::vector<std::string> &rest, const HTTP_REQUEST &request, HTTP_RESPONSE &response);

    /// Resolves a table name to its database, answering the request with the right
    /// error and returning NULL when it cannot.
    BSeries *resolveTable(const std::string &name, bool for_write, HTTP_RESPONSE &response);

    void handleListTables(const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleTableInfo(const std::string &name, HTTP_RESPONSE &response);
    void handleCreateTable(const std::string &name, HTTP_RESPONSE &response);
    void handleDropTable(const std::string &name, const HTTP_REQUEST &request, HTTP_RESPONSE &response);

    void handleListKeys(HTTP_RESPONSE &response);
    void handleCreateKey(const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleRevokeKey(const std::string &name, HTTP_RESPONSE &response);

    void handleReadSettings(HTTP_RESPONSE &response);
    void handleUpdateSettings(const HTTP_REQUEST &request, HTTP_RESPONSE &response);

    /// allow_bootstrap is true only for creating the first key. The bootstrap
    /// token is printed to the server's log, so it must not double as a general
    /// write credential for the data endpoints.
    bool authorise(const HTTP_REQUEST &request, bool needs_write, HTTP_RESPONSE &response, bool allow_bootstrap = false);
    void applyCors(const HTTP_REQUEST &request, HTTP_RESPONSE &response);

    void handleHealth(const HTTP_REQUEST &request, HTTP_RESPONSE &response);

    /// The admin page. Unauthenticated of necessity: it is the thing a key is
    /// typed into, so requiring one to fetch it would leave nowhere to type it.
    /// It carries no data of its own -- every byte it shows comes from a later
    /// authenticated call.
    bool handleWebAsset(const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleListSeries(BSeries *db, const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleSeriesInfo(BSeries *db, uint32_t key, HTTP_RESPONSE &response);
    void handleCreateSeries(BSeries *db, uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleDeleteSeries(BSeries *db, uint32_t key, HTTP_RESPONSE &response);
    void handleMigrateSeries(BSeries *db, uint32_t key, HTTP_RESPONSE &response);
    void handleReadData(BSeries *db, uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleMultiRead(BSeries *db, const HTTP_REQUEST &request, HTTP_RESPONSE &response);

    int streamSeriesData(BSeries *db, uint32_t key, long long start_time, long long end_time, HttpStream *stream);
    int streamCondensedSeries(BSeries *db, uint32_t key, long long start_time, long long end_time,
                              int mode, long long max_points, const CONDENSE_RESERVED &reserved,
                              HttpStream *stream, long long *scanned_out);
    bool readCondenseOptions(const HTTP_REQUEST &request, HTTP_RESPONSE &response, int *mode,
                             long long *max_points, CONDENSE_RESERVED *reserved);
    bool readReservedOptions(const HTTP_REQUEST &request, HTTP_RESPONSE &response, CONDENSE_RESERVED *reserved);
    bool readTimeRange(const HTTP_REQUEST &request, HTTP_RESPONSE &response, long long *start_time, long long *end_time);
    int64_t pointsInRange(BSeries *db, uint32_t key, long long start_time, long long end_time);
    void handleWriteData(BSeries *db, uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleBatchWrite(BSeries *db, const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleWriteNow(BSeries *db, const HTTP_REQUEST &request, HTTP_RESPONSE &response);

    bool resolveWriteShape(BSeries *db, uint32_t key, size_t body_bytes, uint32_t *datasize, int64_t *interval, std::string *error);
    int writePoints(BSeries *db, uint32_t key, const std::string &points, uint32_t datasize, int64_t interval, long long timestamp, int64_t *written, int64_t *overwritten = NULL);
    bool nearestSlotTime(BSeries *db, uint32_t key, long long when, long long *slot_time);
};


/// Hex helpers, exposed for testing.
std::string apiToHex(const void *data, size_t length);
bool apiFromHex(const std::string &text, std::string *out);


#endif // BSERIES_API_H
