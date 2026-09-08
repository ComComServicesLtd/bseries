#ifndef TABLE_SET_H
#define TABLE_SET_H

#include <string>
#include <vector>
#include <map>
#include <mutex>

#include "bseries.h"


/// A set of independent series namespaces, each living in its own directory under
/// one root, so a database can hold several unrelated collections of series
/// without their keys colliding: device 1 in "network" and device 1 in "power" are
/// different series.
///
/// A table is exactly a directory of series files, and a BSeries instance pointed
/// at it. Nothing about the storage format changes; a table is a place, not a new
/// kind of thing.
///
/// The table named "default" is the root directory itself rather than a
/// subdirectory of it. That is what lets a database written before tables existed
/// keep working untouched: its files stay where they are and are reachable both as
/// the default table and through the original unqualified paths.


typedef struct {
    BSeries db;
    std::string directory;
    std::string name;
} TABLE;


#define DEFAULT_TABLE_NAME "default"


class TableSet
{
public:
    TableSet();
    ~TableSet();

    /// root is the data directory. Settings are copied onto every table opened.
    void configure(const std::string &root, int write_ahead_size, int default_interval, int64_t max_grow_points);

    /// Table names must start with a letter and continue with letters, digits,
    /// underscore or hyphen, up to 64 characters.
    ///
    /// Leading letter is not cosmetic: the default table is the root directory, so
    /// an all digit table name would create a directory that the default table's
    /// own series listing would mistake for a series file. The character set also
    /// makes the name safe to use as a path component, which matters because it
    /// arrives from a URL.
    static bool validName(const std::string &name, std::string *error);

    bool exists(const std::string &table);
    int create(const std::string &table);
    int drop(const std::string &table, bool force);
    int list(std::vector<std::string> *out);

    /// The BSeries for a table, opening it on first use. NULL when the table does
    /// not exist on disk. The returned pointer stays valid until closeAll().
    BSeries *open(const std::string &table);

    /// Installs the definitions belonging to each table. Definitions before any
    /// "table" line in the file belong to the default table.
    int loadDefinitions(const char *path, std::string *error);

    void maintain(uint32_t idle_seconds);

    /// Flushes buffered points older than max_age across every open table.
    /// Returns how many series were flushed.
    int flushAged(uint32_t max_age);
    void closeAll();

    std::string root;
    std::string definitions_path;

    int write_ahead_size;
    int default_interval;
    int64_t max_grow_points;

    /// When false, writing to a table that does not exist is a 404 rather than a
    /// new table. Off by default: a typo in a table name should not quietly fork
    /// a database's data into a second copy nobody is reading.
    bool auto_create;

private:
    std::string directoryFor(const std::string &table);

    std::map<std::string,TABLE*> open_tables;
    std::mutex access;
};


#endif // TABLE_SET_H
