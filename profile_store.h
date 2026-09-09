#ifndef PROFILE_STORE_H
#define PROFILE_STORE_H

#include <string>
#include <vector>
#include <map>
#include <mutex>

/// What the values in a series mean.
///
/// A series file records how to *decode* its points -- the width, the datatype,
/// the byte that means nothing was recorded. It does not record what a reading
/// stands for, and that gap is not academic: a prober writing 1 for "no reply"
/// produced a series where a link losing a third of its packets read as the
/// fastest week on the chart, because every consumer averaged the sentinel as a
/// millisecond.
///
/// A profile closes that. It names three kinds of value:
///
///   literal  a reading, taken at face value (optionally scaled)
///   bucket   a reading, but only known to lie in a range -- how a one byte
///            series records a 30 second timeout
///   state    not a reading at all: no reply, an ICMP error
///
/// Profiles are immutable by convention. Changing what a value means is a new
/// profile, not an edit, so a chart drawn last year still means what it said. The
/// loader therefore resolves a name once and keeps it: a new profile is a new
/// file and is picked up without a restart, while an edit to one already in use
/// is ignored, which is exactly what immutability asks for.

#define BS_PROFILE_LITERAL 0
#define BS_PROFILE_BUCKET  1
#define BS_PROFILE_STATE   2


typedef struct {

    int kind;

    // The stored value, or the inclusive range of them for a literal.
    double first;
    double last;

    // What a bucket's value stands for: the reading was at least low and less
    // than high. Parsed as a double regardless of the series' width, because the
    // whole point of a bucket is to record a magnitude the type cannot hold.
    double low;
    double high;

    double scale;                       // literal: multiply the stored value by this
    std::string unit;                   // literal: "ms", "bytes"

    std::string code;                   // state: no_reply, icmp_error
    std::string label;                  // human text, for a legend
    std::vector<std::string> colours;   // opaque to this database, passed through

} PROFILE_ENTRY;


typedef struct {
    std::string name;
    std::vector<PROFILE_ENTRY> entries;
} PROFILE;


class ProfileStore
{
public:

    ProfileStore();

    /// Where profiles live. A file per profile, named for the profile.
    void configure(const std::string &directory);

    /// Resolves a profile, loading it on first use and caching it thereafter.
    /// Returns NULL when there is no such profile, which callers must treat as
    /// "the numbers may be wrong" rather than "no reserved values": silently
    /// falling back to reading everything literally is the original bug.
    const PROFILE *get(const std::string &name, std::string *error_out = NULL);

    /// Profile names reachable in the directory, for listing.
    bool list(std::vector<std::string> *names);

    /// Parses profile text. Public so a test can drive it without a filesystem.
    /// Returns false and fills error_out on anything malformed.
    static bool parse(const std::string &text, PROFILE *out, std::string *error_out);

    /// True when a name is safe to turn into a path. The name reaches this from a
    /// query string and, once the header carries it, from a file -- so it is
    /// untrusted either way and is whitelisted rather than sanitised.
    static bool nameIsSafe(const std::string &name);

    /// Which entry covers a stored value, or NULL. Values are matched as doubles,
    /// which is exact for every type this database stores except a 64 bit integer
    /// past 2^53.
    static const PROFILE_ENTRY *classify(const PROFILE *profile, double value);

private:

    std::string directory;
    std::mutex access;
    std::map<std::string,PROFILE> cache;
    std::map<std::string,std::string> failures;   // names known to be bad, and why
};


#endif // PROFILE_STORE_H
