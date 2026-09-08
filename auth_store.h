#ifndef AUTH_STORE_H
#define AUTH_STORE_H

#include <string>
#include <vector>
#include <mutex>
#include <stdint.h>


/// API keys, kept in a file beside the data so a database can be set up entirely
/// over HTTP rather than by editing configuration and restarting.
///
/// Keys are stored salted and hashed, never in the clear. The server therefore
/// cannot show a key again after creating it, which is the point: a leaked
/// keystore does not hand anyone the credentials, only a set of hashes to attack.
///
/// A key is one of two roles. A read key may call the GET endpoints; a write key
/// may call everything, including managing keys and tables.
///
/// Bootstrapping: a database with no write key anywhere cannot be administered, and
/// letting anyone who reaches the port first create the first key would be a race
/// worth losing. So the server mints a one time bootstrap token at startup, prints
/// it where only someone with access to the console or the logs can read it, and
/// accepts it in place of a key until the first write key exists.


#define AUTH_ROLE_NONE  0
#define AUTH_ROLE_READ  1
#define AUTH_ROLE_WRITE 2


typedef struct {
    std::string name;
    int role;
    std::string salt;      // hex
    std::string hash;      // hex, sha256(salt_bytes + secret)
    uint32_t created;
} AUTH_KEY;


class AuthStore
{
public:
    AuthStore();

    /// Loads the keystore, creating nothing. A missing file is not an error; it
    /// means the database has no keys yet.
    int load(const std::string &path);

    /// The role a presented secret grants, or AUTH_ROLE_NONE. Comparison is
    /// constant time, and every stored key is checked whether or not an earlier
    /// one matched, so timing does not reveal which key was closest.
    int roleFor(const std::string &secret);

    /// Mints a key, writes it to the store and returns the secret. The secret is
    /// returned exactly once and cannot be recovered afterwards.
    int create(const std::string &name, int role, std::string *secret_out);

    /// Refuses to remove the last write key, which would lock the database out of
    /// its own administration. other_write_credential says a write key exists
    /// outside the store, in the configuration file, in which case there is no
    /// lockout to prevent.
    int revoke(const std::string &name, bool other_write_credential = false);

    void list(std::vector<AUTH_KEY> *out);

    bool hasRole(int role);
    bool empty();

    /// A one time token accepted as a write key while no write key exists.
    /// Returns empty once the store holds a write key.
    std::string mintBootstrapToken();
    bool bootstrapTokenMatches(const std::string &presented);

    static bool validKeyName(const std::string &name, std::string *error);
    static const char *roleName(int role);
    static bool roleFromName(const std::string &name, int *role);

    std::string path;

private:
    int save();

    std::vector<AUTH_KEY> keys;
    std::string bootstrap_token;
    std::mutex access;
};


/// SHA-256 of a string, as lowercase hex. Exposed so the hand written
/// implementation can be checked against the published test vectors.
std::string authSha256Hex(const std::string &data);

/// Random bytes from the system, as lowercase hex. Returns an empty string if the
/// system has no randomness to give, which callers must treat as fatal rather than
/// falling back to something predictable.
std::string authRandomHex(size_t bytes);


#endif // AUTH_STORE_H
