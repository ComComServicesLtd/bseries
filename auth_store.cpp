#include "auth_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>

#include "bseries.h"
#include "debug.h"


// ===========================================================================
// SHA-256
//
// Hand written because the project takes no dependencies, and storing API keys in
// the clear when a hash is a hundred lines would be a poor default. FIPS 180-4.
// ===========================================================================

typedef struct {
    uint32_t state[8];
    uint64_t length;
    unsigned char buffer[64];
    size_t buffered;
} SHA256;

static const uint32_t SHA256_K[64] = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
};

static inline uint32_t rotr32(uint32_t v, int n){ return (v >> n) | (v << (32 - n)); }

static void sha256Block(SHA256 *c, const unsigned char *block){

    uint32_t w[64];

    for(int i = 0; i < 16; i++)
        w[i] = ((uint32_t)block[i*4] << 24) | ((uint32_t)block[i*4+1] << 16) |
               ((uint32_t)block[i*4+2] << 8) | (uint32_t)block[i*4+3];

    for(int i = 16; i < 64; i++){
        uint32_t s0 = rotr32(w[i-15],7) ^ rotr32(w[i-15],18) ^ (w[i-15] >> 3);
        uint32_t s1 = rotr32(w[i-2],17) ^ rotr32(w[i-2],19) ^ (w[i-2] >> 10);
        w[i] = w[i-16] + s0 + w[i-7] + s1;
    }

    uint32_t a=c->state[0], b=c->state[1], d=c->state[2], e=c->state[3];
    uint32_t f=c->state[4], g=c->state[5], h=c->state[6], i2=c->state[7];

    for(int i = 0; i < 64; i++){
        uint32_t S1 = rotr32(f,6) ^ rotr32(f,11) ^ rotr32(f,25);
        uint32_t ch = (f & g) ^ ((~f) & h);
        uint32_t t1 = i2 + S1 + ch + SHA256_K[i] + w[i];
        uint32_t S0 = rotr32(a,2) ^ rotr32(a,13) ^ rotr32(a,22);
        uint32_t maj = (a & b) ^ (a & d) ^ (b & d);
        uint32_t t2 = S0 + maj;

        i2 = h; h = g; g = f; f = e + t1;
        e = d; d = b; b = a; a = t1 + t2;
    }

    c->state[0]+=a; c->state[1]+=b; c->state[2]+=d; c->state[3]+=e;
    c->state[4]+=f; c->state[5]+=g; c->state[6]+=h; c->state[7]+=i2;
}

static void sha256Init(SHA256 *c){
    c->state[0]=0x6a09e667; c->state[1]=0xbb67ae85; c->state[2]=0x3c6ef372; c->state[3]=0xa54ff53a;
    c->state[4]=0x510e527f; c->state[5]=0x9b05688c; c->state[6]=0x1f83d9ab; c->state[7]=0x5be0cd19;
    c->length = 0;
    c->buffered = 0;
}

static void sha256Update(SHA256 *c, const unsigned char *data, size_t length){

    c->length += (uint64_t)length * 8;

    while(length > 0){

        size_t take = 64 - c->buffered;
        if(take > length) take = length;

        memcpy(c->buffer + c->buffered,data,take);
        c->buffered += take;
        data += take;
        length -= take;

        if(c->buffered == 64){
            sha256Block(c,c->buffer);
            c->buffered = 0;
        }
    }
}

static void sha256Final(SHA256 *c, unsigned char out[32]){

    uint64_t bits = c->length;

    unsigned char pad = 0x80;
    sha256Update(c,&pad,1);

    unsigned char zero = 0;
    while(c->buffered != 56)
        sha256Update(c,&zero,1);

    unsigned char length_bytes[8];
    for(int i = 0; i < 8; i++)
        length_bytes[i] = (unsigned char)(bits >> (56 - i*8));

    sha256Update(c,length_bytes,8);

    for(int i = 0; i < 8; i++){
        out[i*4]   = (unsigned char)(c->state[i] >> 24);
        out[i*4+1] = (unsigned char)(c->state[i] >> 16);
        out[i*4+2] = (unsigned char)(c->state[i] >> 8);
        out[i*4+3] = (unsigned char)(c->state[i]);
    }
}


static const char HEX[] = "0123456789abcdef";

static std::string toHex(const unsigned char *bytes, size_t length){

    std::string out;
    out.resize(length * 2);

    for(size_t i = 0; i < length; i++){
        out[i*2] = HEX[bytes[i] >> 4];
        out[i*2+1] = HEX[bytes[i] & 0x0F];
    }

    return out;
}

static bool fromHex(const std::string &text, std::string *out){

    if(text.size() % 2 != 0)
        return false;

    out->clear();

    for(size_t i = 0; i < text.size(); i += 2){

        int high = -1, low = -1;

        for(int d = 0; d < 16; d++){
            if(HEX[d] == text[i]) high = d;
            if(HEX[d] == text[i+1]) low = d;
        }

        if(high < 0 || low < 0)
            return false;

        out->push_back((char)((high << 4) | low));
    }

    return true;
}


std::string authSha256Hex(const std::string &data){

    SHA256 context;
    sha256Init(&context);
    sha256Update(&context,(const unsigned char*)data.data(),data.size());

    unsigned char digest[32];
    sha256Final(&context,digest);

    return toHex(digest,32);
}


std::string authRandomHex(size_t bytes){

    FILE *source = fopen("/dev/urandom","rb");

    if(source == NULL){
        _ERROR("No /dev/urandom; refusing to invent randomness for a credential\n");
        return std::string();
    }

    std::string raw;
    raw.resize(bytes);

    size_t got = fread(&raw[0],1,bytes,source);
    fclose(source);

    if(got != bytes){
        _ERROR("Short read from /dev/urandom; refusing to invent randomness for a credential\n");
        return std::string();
    }

    return toHex((const unsigned char*)raw.data(),bytes);
}


static std::string hashSecret(const std::string &salt_bytes, const std::string &secret){

    SHA256 context;
    sha256Init(&context);
    sha256Update(&context,(const unsigned char*)salt_bytes.data(),salt_bytes.size());
    sha256Update(&context,(const unsigned char*)secret.data(),secret.size());

    unsigned char digest[32];
    sha256Final(&context,digest);

    return toHex(digest,32);
}


/// Constant time comparison, so a wrong key cannot be found a character at a time.

static bool digestsMatch(const std::string &a, const std::string &b){

    if(a.empty() || b.empty() || a.size() != b.size())
        return false;

    unsigned char difference = 0;

    for(size_t i = 0; i < a.size(); i++)
        difference |= (unsigned char)(a[i] ^ b[i]);

    return difference == 0;
}


AuthStore::AuthStore()
{
}


const char *AuthStore::roleName(int role){

    if(role == AUTH_ROLE_READ)  return "read";
    if(role == AUTH_ROLE_WRITE) return "write";
    return "none";
}


bool AuthStore::roleFromName(const std::string &name, int *role){

    if(name == "read"){  *role = AUTH_ROLE_READ;  return true; }
    if(name == "write"){ *role = AUTH_ROLE_WRITE; return true; }
    return false;
}


bool AuthStore::validKeyName(const std::string &name, std::string *error){

    if(name.empty() || name.size() > 64){
        if(error) *error = "a key name must be between 1 and 64 characters";
        return false;
    }

    for(size_t i = 0; i < name.size(); i++){

        char c = name[i];
        bool allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                       (c >= '0' && c <= '9') || c == '_' || c == '-';

        if(!allowed){
            if(error) *error = "a key name may only contain letters, digits, underscore and hyphen";
            return false;
        }
    }

    return true;
}


int AuthStore::load(const std::string &keystore_path){

    access.lock();

    path = keystore_path;
    keys.clear();

    FILE *file = fopen(path.c_str(),"r");

    if(file == NULL){
        // No keystore yet is the normal state of a database nobody has set up
        access.unlock();
        return NO_ERROR;
    }

    struct stat details;
    if(stat(path.c_str(),&details) == 0 && (details.st_mode & 0077) != 0)
        _WARN("Warning: %s is readable by other users and holds API key hashes\n",path.c_str());

    char line[1024];
    int line_number = 0;

    while(fgets(line,sizeof(line),file) != NULL){

        line_number++;

        char *comment = strchr(line,'#');
        if(comment) *comment = 0;

        char name[128], role[32], salt[256], hash[256];
        unsigned long created = 0;

        int fields = sscanf(line,"%127s %31s %255s %255s %lu",name,role,salt,hash,&created);

        if(fields <= 0)
            continue;

        if(fields < 4){
            _ERROR("%s line %d: malformed key entry\n",path.c_str(),line_number);
            fclose(file);
            access.unlock();
            return INVALID_SERIES_DEFINITION;
        }

        AUTH_KEY key;
        key.name = name;
        key.salt = salt;
        key.hash = hash;
        key.created = (uint32_t)created;

        if(!roleFromName(role,&key.role)){
            _ERROR("%s line %d: '%s' is not a role\n",path.c_str(),line_number,role);
            fclose(file);
            access.unlock();
            return INVALID_SERIES_DEFINITION;
        }

        keys.push_back(key);
    }

    fclose(file);

    access.unlock();

    return (int)keys.size();
}


/// Rewrites the keystore through a temporary file, so an interrupted write cannot
/// leave a database with no way in.

int AuthStore::save(){

    std::string temporary = path + ".new";

    FILE *file = fopen(temporary.c_str(),"w");

    if(file == NULL){
        _ERROR("Could not write %s: %s\n",temporary.c_str(),strerror(errno));
        return FILE_OPEN_FAILURE;
    }

    // Before anything is in it, since it holds credentials
    if(chmod(temporary.c_str(),0600) != 0)
        _WARN("Could not restrict permissions on %s\n",temporary.c_str());

    fprintf(file,"# bseries API keys. Hashes, not secrets; a lost key cannot be recovered.\n");
    fprintf(file,"# <name> <role> <salt> <sha256(salt+secret)> <created>\n");

    for(size_t i = 0; i < keys.size(); i++)
        fprintf(file,"%s %s %s %s %lu\n",
                keys[i].name.c_str(),roleName(keys[i].role),
                keys[i].salt.c_str(),keys[i].hash.c_str(),
                (unsigned long)keys[i].created);

    if(fflush(file) != 0 || fsync(fileno(file)) != 0){
        fclose(file);
        remove(temporary.c_str());
        return FILE_OPEN_FAILURE;
    }

    fclose(file);

    if(rename(temporary.c_str(),path.c_str()) != 0){
        _ERROR("Could not replace %s: %s\n",path.c_str(),strerror(errno));
        remove(temporary.c_str());
        return FILE_OPEN_FAILURE;
    }

    return NO_ERROR;
}


int AuthStore::roleFor(const std::string &secret){

    if(secret.empty())
        return AUTH_ROLE_NONE;

    access.lock();

    int granted = AUTH_ROLE_NONE;

    // Every key is checked even after one matches, so the work done does not
    // depend on which key was presented.
    for(size_t i = 0; i < keys.size(); i++){

        std::string salt_bytes;

        if(!fromHex(keys[i].salt,&salt_bytes))
            continue;

        std::string candidate = hashSecret(salt_bytes,secret);

        if(digestsMatch(candidate,keys[i].hash) && granted == AUTH_ROLE_NONE)
            granted = keys[i].role;
    }

    access.unlock();

    return granted;
}


int AuthStore::create(const std::string &name, int role, std::string *secret_out){

    std::string error;

    if(!validKeyName(name,&error))
        return INVALID_SERIES_DEFINITION;

    if(role != AUTH_ROLE_READ && role != AUTH_ROLE_WRITE)
        return INVALID_SERIES_DEFINITION;

    std::string salt = authRandomHex(16);
    std::string body = authRandomHex(32);

    if(salt.empty() || body.empty())
        return INTERNAL_ERROR;

    std::string secret = std::string(role == AUTH_ROLE_WRITE ? "bsw_" : "bsr_") + body;

    std::string salt_bytes;
    fromHex(salt,&salt_bytes);

    access.lock();

    for(size_t i = 0; i < keys.size(); i++){
        if(keys[i].name == name){
            access.unlock();
            return SERIES_ALREADY_EXISTS;
        }
    }

    AUTH_KEY key;
    key.name = name;
    key.role = role;
    key.salt = salt;
    key.hash = hashSecret(salt_bytes,secret);
    key.created = (uint32_t)time(NULL);

    keys.push_back(key);

    int rc = save();

    if(rc != NO_ERROR){
        keys.pop_back();
        access.unlock();
        return rc;
    }

    // The first write key ends the bootstrap window
    if(role == AUTH_ROLE_WRITE)
        bootstrap_token.clear();

    access.unlock();

    *secret_out = secret;

    return NO_ERROR;
}


int AuthStore::revoke(const std::string &name, bool other_write_credential){

    access.lock();

    size_t found = keys.size();
    size_t write_keys = 0;

    for(size_t i = 0; i < keys.size(); i++){

        if(keys[i].name == name)
            found = i;

        if(keys[i].role == AUTH_ROLE_WRITE)
            write_keys++;
    }

    if(found == keys.size()){
        access.unlock();
        return SERIES_NOT_FOUND;
    }

    // Revoking the last write key would lock the database out of its own
    // administration, with no way back in short of editing files. A write key in
    // the configuration file is a way back in, so it lifts the restriction.
    if(keys[found].role == AUTH_ROLE_WRITE && write_keys <= 1 && !other_write_credential){
        access.unlock();
        return SERIES_ALREADY_EXISTS;
    }

    AUTH_KEY removed = keys[found];
    keys.erase(keys.begin() + found);

    int rc = save();

    if(rc != NO_ERROR){
        keys.insert(keys.begin() + found,removed);
        access.unlock();
        return rc;
    }

    access.unlock();

    return NO_ERROR;
}


void AuthStore::list(std::vector<AUTH_KEY> *out){

    access.lock();

    for(size_t i = 0; i < keys.size(); i++){
        AUTH_KEY copy = keys[i];
        copy.hash.clear();   // never leaves the process
        copy.salt.clear();
        out->push_back(copy);
    }

    access.unlock();
}


bool AuthStore::hasRole(int role){

    access.lock();

    bool found = false;

    for(size_t i = 0; i < keys.size(); i++){
        if(keys[i].role == role){
            found = true;
            break;
        }
    }

    access.unlock();

    return found;
}


bool AuthStore::empty(){

    access.lock();
    bool none = keys.empty();
    access.unlock();

    return none;
}


std::string AuthStore::mintBootstrapToken(){

    access.lock();

    bool have_write = false;

    for(size_t i = 0; i < keys.size(); i++){
        if(keys[i].role == AUTH_ROLE_WRITE){
            have_write = true;
            break;
        }
    }

    if(have_write){
        bootstrap_token.clear();
        access.unlock();
        return std::string();
    }

    bootstrap_token = authRandomHex(32);

    std::string token = bootstrap_token;

    access.unlock();

    return token;
}


bool AuthStore::bootstrapTokenMatches(const std::string &presented){

    access.lock();

    bool ok = !bootstrap_token.empty() && digestsMatch(bootstrap_token,presented);

    access.unlock();

    return ok;
}
