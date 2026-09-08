#include "runtime_settings.h"

const RUNTIME_SETTING_FIELD RUNTIME_SETTING_FIELDS[] = {
    {"flush_interval",       &RUNTIME_SETTINGS::flush_interval,       0, 86400,
     "seconds a series may hold buffered points before being flushed; 0 disables the timer"},
    {"series_max_idle",      &RUNTIME_SETTINGS::series_max_idle,      0, 604800,
     "seconds of no writes before a series is flushed and dropped from memory; 0 disables"},
    {"maintenance_interval", &RUNTIME_SETTINGS::maintenance_interval, 1, 3600,
     "how often the maintenance thread considers flushing and eviction"},
    {"max_points_per_read",  &RUNTIME_SETTINGS::max_points_per_read,  1, 1000000000,
     "points one read request may return"},
    {"max_series_per_read",  &RUNTIME_SETTINGS::max_series_per_read,  1, 1000000,
     "series one request may name in keys="},
    {"max_points_per_write", &RUNTIME_SETTINGS::max_points_per_write, 1, 1000000000,
     "points one write request may carry"},
    {"max_condense_scan",    &RUNTIME_SETTINGS::max_condense_scan,    1, 2000000000,
     "stored slots one condensed read may walk"}
};

const unsigned RUNTIME_SETTING_FIELD_COUNT =
    sizeof(RUNTIME_SETTING_FIELDS) / sizeof(RUNTIME_SETTING_FIELDS[0]);
