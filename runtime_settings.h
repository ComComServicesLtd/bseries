#ifndef RUNTIME_SETTINGS_H
#define RUNTIME_SETTINGS_H

#include <atomic>
#include <string>


/// The settings that can be changed while the server is running.
///
/// These are the ones an operator tunes against a live workload rather than
/// decides once: how long points may sit unflushed, how often maintenance runs,
/// and the ceilings a single request may reach. Everything else — the listening
/// address, the data directory, the keys — needs a restart, and stays in
/// API_CONFIG.
///
/// Atomic because the maintenance thread reads them on its own schedule while a
/// request handler may be writing them, and a torn int would be a real bug rather
/// than a theoretical one.

typedef struct {

    /// Seconds a series may hold buffered points before being flushed, whether or
    /// not it is still being written to. 0 disables the timer, which puts the
    /// exposure back to a whole buffer's worth of points.
    std::atomic<int> flush_interval;

    /// Seconds of no writes before a series is flushed and dropped from memory.
    std::atomic<int> series_max_idle;

    /// How often the maintenance thread considers either of the above.
    std::atomic<int> maintenance_interval;

    /// Per request ceilings.
    std::atomic<int> max_points_per_read;
    std::atomic<int> max_series_per_read;
    std::atomic<int> max_points_per_write;
    std::atomic<int> max_condense_scan;

} RUNTIME_SETTINGS;


/// The name/value pairs the settings endpoint accepts, so the endpoint and the
/// configuration file cannot drift apart.
typedef struct {
    const char *name;
    std::atomic<int> RUNTIME_SETTINGS::*field;
    int minimum;                 // inclusive
    int maximum;                 // inclusive
    const char *description;
} RUNTIME_SETTING_FIELD;

extern const RUNTIME_SETTING_FIELD RUNTIME_SETTING_FIELDS[];
extern const unsigned RUNTIME_SETTING_FIELD_COUNT;


#endif // RUNTIME_SETTINGS_H
