#ifndef BSERIES_API_H
#define BSERIES_API_H

#include <string>
#include <vector>

#include "bseries.h"
#include "http_server.h"


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

    /// read_key may call the GET endpoints. write_key may call everything.
    /// An empty key disables that level of access entirely.
    std::string read_key;
    std::string write_key;

    /// Origins allowed to call the API from a browser. A single entry of "*"
    /// allows any origin. Empty means no CORS headers are sent at all, which
    /// blocks browser callers and is the right default for a server side client.
    std::vector<std::string> cors_origins;

    int max_points_per_read;   // shared across every series in one request
    int max_series_per_read;
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


class BSeriesApi
{
public:
    BSeriesApi(BSeries *database, const API_CONFIG *config);

    /// The HTTP_HANDLER entry point. Pass the BSeriesApi instance as context.
    static void handle(const HTTP_REQUEST &request, HTTP_RESPONSE &response, void *context);

    BSeries *db;
    API_CONFIG config;

private:
    void route(const HTTP_REQUEST &request, HTTP_RESPONSE &response);

    bool authorise(const HTTP_REQUEST &request, bool needs_write, HTTP_RESPONSE &response);
    void applyCors(const HTTP_REQUEST &request, HTTP_RESPONSE &response);

    void handleHealth(const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleListSeries(const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleSeriesInfo(uint32_t key, HTTP_RESPONSE &response);
    void handleCreateSeries(uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleDeleteSeries(uint32_t key, HTTP_RESPONSE &response);
    void handleReadData(uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleMultiRead(const HTTP_REQUEST &request, HTTP_RESPONSE &response);

    int streamSeriesData(uint32_t key, long long start_time, long long end_time, HttpStream *stream);
    bool readTimeRange(const HTTP_REQUEST &request, HTTP_RESPONSE &response, long long *start_time, long long *end_time);
    int64_t pointsInRange(uint32_t key, long long start_time, long long end_time);
    void handleWriteData(uint32_t key, const HTTP_REQUEST &request, HTTP_RESPONSE &response);
    void handleBatchWrite(const HTTP_REQUEST &request, HTTP_RESPONSE &response);

    bool resolveWriteShape(uint32_t key, size_t body_bytes, uint32_t *datasize, int64_t *interval, std::string *error);
    int writePoints(uint32_t key, const std::string &points, uint32_t datasize, int64_t interval, long long timestamp, int64_t *written);
};


/// Hex helpers, exposed for testing.
std::string apiToHex(const void *data, size_t length);
bool apiFromHex(const std::string &text, std::string *out);


#endif // BSERIES_API_H
