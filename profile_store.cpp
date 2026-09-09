#include "profile_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>


ProfileStore::ProfileStore(){
}


void ProfileStore::configure(const std::string &directory){
    this->directory = directory;
}


/// Whitelisted rather than sanitised. The name arrives from a query string, and
/// once the header carries it, from a file -- so it is untrusted input to a path
/// either way, and trying to strip ".." out of an attacker's string is the losing
/// half of that game.

bool ProfileStore::nameIsSafe(const std::string &name){

    if(name.empty() || name.size() > 64)
        return false;

    for(size_t i = 0; i < name.size(); i++){

        char c = name[i];

        bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                  (c >= '0' && c <= '9') || c == '_' || c == '-';

        if(!ok)
            return false;
    }

    return true;
}


/// Splits a line into tokens, keeping a double quoted run together so a label may
/// contain spaces. No escapes: a label is a legend entry, and a format nobody can
/// mis-quote is worth more here than one that can express every string.

static bool tokenise(const std::string &line, std::vector<std::string> *out, std::string *error){

    size_t at = 0;

    while(at < line.size()){

        while(at < line.size() && (line[at] == ' ' || line[at] == '\t'))
            at++;

        if(at >= line.size())
            break;

        if(line[at] == '"'){

            size_t end = line.find('"',at + 1);

            if(end == std::string::npos){
                *error = "unterminated quoted string";
                return false;
            }

            out->push_back(line.substr(at + 1,end - (at + 1)));
            at = end + 1;
            continue;
        }

        size_t end = at;

        while(end < line.size() && line[end] != ' ' && line[end] != '\t')
            end++;

        out->push_back(line.substr(at,end - at));
        at = end;
    }

    return true;
}


static bool parseNumber(const std::string &text, double *out){

    if(text.empty())
        return false;

    char *stop = NULL;
    double value = strtod(text.c_str(),&stop);

    if(stop == text.c_str() || *stop != '\0' || value != value)
        return false;

    *out = value;
    return true;
}


/// "245-500", and "0-244". The separator is searched for from index 1 so a
/// leading minus belongs to the first number rather than splitting it.

static bool parseRange(const std::string &text, double *first, double *last){

    if(text.size() < 2)
        return false;

    size_t dash = text.find('-',1);

    if(dash == std::string::npos || dash + 1 >= text.size())
        return false;

    return parseNumber(text.substr(0,dash),first) &&
           parseNumber(text.substr(dash + 1),last);
}


static void parseColours(const std::string &text, std::vector<std::string> *out){

    size_t at = 0;

    while(at <= text.size()){

        size_t comma = text.find(',',at);
        std::string item = text.substr(at,comma == std::string::npos ? std::string::npos : comma - at);

        if(!item.empty())
            out->push_back(item);

        if(comma == std::string::npos)
            break;

        at = comma + 1;
    }
}


static bool isColour(const std::string &token){
    return !token.empty() && token[0] == '#';
}


/// Rejects rather than warns. Every check here is a case that would otherwise
/// produce a wrong number rather than an error: a value claimed twice, buckets
/// whose stored order disagrees with their magnitudes so a maximum picks the
/// wrong one, a range that reads backwards.

static bool validate(const PROFILE &profile, std::string *error){

    char message[256];

    for(size_t i = 0; i < profile.entries.size(); i++){

        const PROFILE_ENTRY &e = profile.entries[i];

        if(e.kind == BS_PROFILE_LITERAL && e.first > e.last){
            *error = "a literal range runs backwards";
            return false;
        }

        if(e.kind == BS_PROFILE_BUCKET && !(e.low < e.high)){
            snprintf(message,sizeof(message),"bucket %g needs low < high",e.first);
            *error = message;
            return false;
        }

        for(size_t j = i + 1; j < profile.entries.size(); j++){

            const PROFILE_ENTRY &f = profile.entries[j];

            // Two entries may not claim the same stored value; a reader would
            // have to pick one and would sometimes pick differently.
            bool overlap = !(e.last < f.first || f.last < e.first);

            if(overlap){
                snprintf(message,sizeof(message),
                         "values %g-%g and %g-%g both claim the same stored value",
                         e.first,e.last,f.first,f.last);
                *error = message;
                return false;
            }

            // Bounds are inclusive at both ends: a reading belongs to the bucket
            // where low <= it <= high. So 245-500 and 501-1000 are the way to
            // write two adjacent ranges, and 245-500 with 500-1000 is an overlap
            // -- 500 belongs to both, and whichever the loop reached last would
            // win, silently.
            if(e.kind == BS_PROFILE_BUCKET && f.kind == BS_PROFILE_BUCKET){

                if(e.low <= f.high && f.low <= e.high){
                    snprintf(message,sizeof(message),
                             "buckets %g and %g cover overlapping ranges, %g-%g and %g-%g",
                             e.first,f.first,e.low,e.high,f.low,f.high);
                    *error = message;
                    return false;
                }
            }

            // A literal reading and a bucket claiming the same magnitude has the
            // same problem: the value is exact and a floor at once.
            if(e.kind == BS_PROFILE_LITERAL && f.kind == BS_PROFILE_BUCKET){

                double lo = e.first * e.scale, hi = e.last * e.scale;

                if(lo <= f.high && f.low <= hi){
                    snprintf(message,sizeof(message),
                             "the literal range reaches %g, which bucket %g already covers",
                             hi,f.first);
                    *error = message;
                    return false;
                }
            }

            if(e.kind == BS_PROFILE_BUCKET && f.kind == BS_PROFILE_BUCKET){

                bool stored_ascends = e.first < f.first;
                bool bounds_ascend  = e.low   < f.low;

                if(stored_ascends != bounds_ascend){
                    snprintf(message,sizeof(message),
                             "buckets %g and %g are not in the same order as their bounds",
                             e.first,f.first);
                    *error = message;
                    return false;
                }
            }
        }
    }

    return true;
}


bool ProfileStore::parse(const std::string &text, PROFILE *out, std::string *error_out){

    std::string error;
    size_t line_number = 0;
    size_t at = 0;

    out->entries.clear();

    while(at <= text.size()){

        size_t end = text.find('\n',at);
        std::string line = text.substr(at,end == std::string::npos ? std::string::npos : end - at);

        line_number++;
        at = (end == std::string::npos) ? text.size() + 1 : end + 1;

        // A comment is a line starting with #, which is why an entry line never
        // starts with one -- colours do, and share the character.
        size_t first = line.find_first_not_of(" \t\r");

        if(first == std::string::npos || line[first] == '#')
            continue;

        std::vector<std::string> token;

        if(!tokenise(line,&token,&error)){
            char message[256];
            snprintf(message,sizeof(message),"line %u: %s",(unsigned)line_number,error.c_str());
            if(error_out) *error_out = message;
            return false;
        }

        if(token.empty())
            continue;

        PROFILE_ENTRY entry;
        entry.kind = BS_PROFILE_LITERAL;
        entry.first = entry.last = entry.low = entry.high = 0;
        entry.scale = 1.0;

        bool ok = true;

        if(token[0] == "literal"){

            // literal <a>-<b> <unit> [<scale>] [<colours>]
            entry.kind = BS_PROFILE_LITERAL;

            ok = token.size() >= 3 && parseRange(token[1],&entry.first,&entry.last);

            if(ok){
                entry.unit = token[2];

                for(size_t i = 3; i < token.size(); i++){
                    if(isColour(token[i]))
                        parseColours(token[i],&entry.colours);
                    else if(!parseNumber(token[i],&entry.scale))
                        ok = false;
                }
            }

        } else if(token[0] == "bucket"){

            // bucket <value> <low>-<high> "<label>" [<colour>]
            entry.kind = BS_PROFILE_BUCKET;

            ok = token.size() >= 3 &&
                 parseNumber(token[1],&entry.first) &&
                 parseRange(token[2],&entry.low,&entry.high);

            entry.last = entry.first;

            if(ok){
                if(token.size() >= 4) entry.label = token[3];
                if(token.size() >= 5 && isColour(token[4])) entry.colours.push_back(token[4]);
            }

        } else if(token[0] == "state"){

            // state <value> <code> "<label>" [<colour>]
            entry.kind = BS_PROFILE_STATE;

            ok = token.size() >= 3 && parseNumber(token[1],&entry.first);
            entry.last = entry.first;

            if(ok){
                entry.code = token[2];
                if(token.size() >= 4) entry.label = token[3];
                if(token.size() >= 5 && isColour(token[4])) entry.colours.push_back(token[4]);
            }

        } else {

            char message[256];
            snprintf(message,sizeof(message),"line %u: expected literal, bucket or state, got \"%s\"",
                     (unsigned)line_number,token[0].c_str());
            if(error_out) *error_out = message;
            return false;
        }

        if(!ok){
            char message[256];
            snprintf(message,sizeof(message),"line %u: malformed %s entry",
                     (unsigned)line_number,token[0].c_str());
            if(error_out) *error_out = message;
            return false;
        }

        out->entries.push_back(entry);
    }

    if(out->entries.empty()){
        if(error_out) *error_out = "a profile with no entries says nothing";
        return false;
    }

    if(!validate(*out,&error)){
        if(error_out) *error_out = error;
        return false;
    }

    return true;
}


const PROFILE_ENTRY *ProfileStore::classify(const PROFILE *profile, double value){

    if(profile == NULL)
        return NULL;

    for(size_t i = 0; i < profile->entries.size(); i++){

        const PROFILE_ENTRY &e = profile->entries[i];

        if(value >= e.first && value <= e.last)
            return &e;
    }

    return NULL;
}


const PROFILE *ProfileStore::get(const std::string &name, std::string *error_out){

    if(!nameIsSafe(name)){
        if(error_out) *error_out = "a profile name is letters, digits, underscore and dash";
        return NULL;
    }

    std::lock_guard<std::mutex> hold(access);

    std::map<std::string,PROFILE>::iterator found = cache.find(name);

    if(found != cache.end())
        return &found->second;

    // Remembered so a missing or malformed profile does not cost a file open on
    // every point read of a series that names it.
    std::map<std::string,std::string>::iterator bad = failures.find(name);

    if(bad != failures.end()){
        if(error_out) *error_out = bad->second;
        return NULL;
    }

    std::string path = directory + "/" + name;

    FILE *file = fopen(path.c_str(),"rb");

    if(file == NULL){
        std::string message = "no profile named \"" + name + "\"";
        failures[name] = message;
        if(error_out) *error_out = message;
        return NULL;
    }

    std::string text;
    char buffer[4096];
    size_t got;

    while((got = fread(buffer,1,sizeof(buffer),file)) > 0){

        text.append(buffer,got);

        // A profile is a handful of lines. Anything this size is not one, and a
        // router has no memory to spare finding out what it is instead.
        if(text.size() > 65536){
            fclose(file);
            std::string message = "profile \"" + name + "\" is larger than 64KB";
            failures[name] = message;
            if(error_out) *error_out = message;
            return NULL;
        }
    }

    fclose(file);

    PROFILE profile;
    profile.name = name;
    std::string error;

    if(!parse(text,&profile,&error)){
        std::string message = "profile \"" + name + "\": " + error;
        failures[name] = message;
        if(error_out) *error_out = message;
        return NULL;
    }

    cache[name] = profile;
    return &cache[name];
}


bool ProfileStore::list(std::vector<std::string> *names){

    DIR *dir = opendir(directory.c_str());

    if(dir == NULL)
        return false;

    struct dirent *entry;

    while((entry = readdir(dir)) != NULL){
        if(nameIsSafe(entry->d_name))
            names->push_back(entry->d_name);
    }

    closedir(dir);
    return true;
}
