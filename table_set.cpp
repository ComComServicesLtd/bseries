#include "table_set.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>

#include "debug.h"


/// First path segment names that belong to the API itself and so cannot be a
/// table. Without this, POST /v1/data would be ambiguous between the batch write
/// endpoint and a table called "data".

static const char *RESERVED_TABLE_NAMES[] = {
    "health", "tables", "series", "data", "now", "v1"
};


TableSet::TableSet()
{
    write_ahead_size = 4096;
    default_interval = 10;
    max_grow_points = 1000000;
    auto_create = false;
}


TableSet::~TableSet()
{
    closeAll();
}


void TableSet::configure(const std::string &data_root, int wal_size, int interval, int64_t grow_limit){

    root = data_root;
    write_ahead_size = wal_size;
    default_interval = interval;
    max_grow_points = grow_limit;
}


bool TableSet::validName(const std::string &name, std::string *error){

    if(name.empty()){
        if(error) *error = "a table name may not be empty";
        return false;
    }

    if(name.size() > 64){
        if(error) *error = "a table name may be at most 64 characters";
        return false;
    }

    // A leading letter keeps an all digit name from colliding with the series
    // files in the default table's directory, and rules out "." and ".." along
    // with every other way of walking out of the data directory.
    if(!((name[0] >= 'a' && name[0] <= 'z') || (name[0] >= 'A' && name[0] <= 'Z'))){
        if(error) *error = "a table name must start with a letter";
        return false;
    }

    for(size_t i = 0; i < name.size(); i++){

        char c = name[i];
        bool allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                       (c >= '0' && c <= '9') || c == '_' || c == '-';

        if(!allowed){
            if(error) *error = "a table name may only contain letters, digits, underscore and hyphen";
            return false;
        }
    }

    for(unsigned i = 0; i < sizeof(RESERVED_TABLE_NAMES)/sizeof(RESERVED_TABLE_NAMES[0]); i++){
        if(name == RESERVED_TABLE_NAMES[i]){
            if(error) *error = "'" + name + "' is reserved by the API and cannot be a table name";
            return false;
        }
    }

    return true;
}


/// The default table is the root itself, which is what keeps a database written
/// before tables existed readable without moving a single file.

std::string TableSet::directoryFor(const std::string &table){

    if(table == DEFAULT_TABLE_NAME)
        return root;

    return root + "/" + table;
}


bool TableSet::exists(const std::string &table){

    if(!validName(table,NULL))
        return false;

    struct stat details;

    if(stat(directoryFor(table).c_str(),&details) != 0)
        return false;

    return S_ISDIR(details.st_mode);
}


int TableSet::create(const std::string &table){

    std::string error;

    if(!validName(table,&error))
        return INVALID_SERIES_DEFINITION;

    if(table == DEFAULT_TABLE_NAME)
        return SERIES_ALREADY_EXISTS; // it is the root, it is always there

    std::string path = directoryFor(table);

    if(mkdir(path.c_str(),0755) != 0){

        if(errno == EEXIST)
            return SERIES_ALREADY_EXISTS;

        _ERROR("Could not create table directory %s: %s\n",path.c_str(),strerror(errno));
        return FILE_OPEN_FAILURE;
    }

    return NO_ERROR;
}


/// Removes a table. Without force the directory has to be empty already, so
/// dropping a table cannot destroy series nobody meant to destroy.

int TableSet::drop(const std::string &table, bool force){

    if(!validName(table,NULL))
        return INVALID_SERIES_DEFINITION;

    if(table == DEFAULT_TABLE_NAME)
        return INVALID_SERIES_DEFINITION; // it is the data directory itself

    if(!exists(table))
        return SERIES_NOT_FOUND;

    std::string path = directoryFor(table);

    // Anything still open has to be closed and flushed before the files go
    access.lock();

    std::map<std::string,TABLE*>::iterator it = open_tables.find(table);

    if(it != open_tables.end()){
        it->second->db.close();
        delete it->second;
        open_tables.erase(it);
    }

    access.unlock();

    if(force){

        DIR *dir = opendir(path.c_str());

        if(dir != NULL){

            struct dirent *entry;

            while((entry = readdir(dir)) != NULL){

                // Only series files, which are named for their key and nothing
                // else. Anything else in there is not ours to delete.
                const char *name = entry->d_name;
                char *end = NULL;
                strtoul(name,&end,10);

                if(end == name || *end != 0)
                    continue;

                std::string file = path + "/" + name;
                remove(file.c_str());
            }

            closedir(dir);
        }
    }

    if(rmdir(path.c_str()) != 0){

        if(errno == ENOTEMPTY || errno == EEXIST)
            return SERIES_ALREADY_EXISTS; // still holds series, force was not given

        _ERROR("Could not remove table directory %s: %s\n",path.c_str(),strerror(errno));
        return INTERNAL_ERROR;
    }

    return NO_ERROR;
}


int TableSet::list(std::vector<std::string> *out){

    DIR *dir = opendir(root.c_str());

    if(dir == NULL){
        _ERROR("Could not open the data directory %s\n",root.c_str());
        return FAILED_TO_OPEN_FILE;
    }

    struct dirent *entry;

    while((entry = readdir(dir)) != NULL){

        std::string name = entry->d_name;

        if(name == "." || name == "..")
            continue;

        if(!validName(name,NULL))
            continue;

        struct stat details;
        std::string path = root + "/" + name;

        if(stat(path.c_str(),&details) == 0 && S_ISDIR(details.st_mode))
            out->push_back(name);
    }

    closedir(dir);

    std::sort(out->begin(),out->end());

    // The root is always a table, and sorts first so it reads as the base case
    out->insert(out->begin(),DEFAULT_TABLE_NAME);

    return (int)out->size();
}


BSeries *TableSet::open(const std::string &table){

    if(!validName(table,NULL))
        return NULL;

    access.lock();

    std::map<std::string,TABLE*>::iterator it = open_tables.find(table);

    if(it != open_tables.end()){
        BSeries *found = &it->second->db;
        access.unlock();
        return found;
    }

    access.unlock();

    // Checked outside the lock because it touches the filesystem
    if(!exists(table))
        return NULL;

    access.lock();

    // Another thread may have opened it while we were not holding the lock
    it = open_tables.find(table);

    if(it != open_tables.end()){
        BSeries *found = &it->second->db;
        access.unlock();
        return found;
    }

    TABLE *entry = new TABLE;

    entry->name = table;
    entry->directory = directoryFor(table);
    entry->db.data_directory = entry->directory.c_str();
    entry->db.write_ahead_size = write_ahead_size;
    entry->db.default_seconds_per_point = default_interval;
    entry->db.max_grow_points = max_grow_points;

    if(!definitions_path.empty())
        entry->db.loadDefinitions(definitions_path.c_str(),table.c_str());

    open_tables[table] = entry;

    BSeries *opened = &entry->db;

    access.unlock();

    return opened;
}


/// Installs each table's definitions. The file is read once per table, which costs
/// nothing at startup and keeps one parser rather than two.

int TableSet::loadDefinitions(const char *path, std::string *error){

    definitions_path = path;

    std::vector<std::string> tables;

    if(list(&tables) < 0){
        if(error) *error = "could not list tables";
        return FAILED_TO_OPEN_FILE;
    }

    int total = 0;

    for(size_t i = 0; i < tables.size(); i++){

        BSeries *db = open(tables[i]);

        if(db == NULL)
            continue;

        int loaded = db->loadDefinitions(path,tables[i].c_str());

        if(loaded < 0){
            if(error) *error = std::string("could not load series definitions from ") + path;
            return loaded;
        }

        total += loaded;
    }

    return total;
}


void TableSet::maintain(uint32_t idle_seconds){

    // The table list is copied so the map is not held while each table does its
    // own flushing, which can take a while.
    std::vector<BSeries*> databases;

    access.lock();

    for(std::map<std::string,TABLE*>::iterator it = open_tables.begin(); it != open_tables.end(); ++it)
        databases.push_back(&it->second->db);

    access.unlock();

    for(size_t i = 0; i < databases.size(); i++)
        databases[i]->closeSeries(idle_seconds);
}


void TableSet::closeAll(){

    access.lock();

    for(std::map<std::string,TABLE*>::iterator it = open_tables.begin(); it != open_tables.end(); ++it){
        it->second->db.close();
        delete it->second;
    }

    open_tables.clear();

    access.unlock();
}
