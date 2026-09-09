#include "profile_store.h"
#include "test_util.h"

#include <stdio.h>
#include <string.h>


static bool parses(const char *text, PROFILE *out){
    std::string error;
    return ProfileStore::parse(text,out,&error);
}


static bool rejects(const char *text, const char *because){
    PROFILE p;
    std::string error;
    bool ok = ProfileStore::parse(text,&p,&error);
    if(ok) printf("      (accepted, expected a rejection: %s)\n",because);
    return !ok;
}


int main(int argc, char **argv){

    (void)argc; (void)argv;
    const char *dir = testMakeDirectory();

    static const char *PING =
        "# a ping profile\n"
        "literal  0-244  ms          #0000FF,#00FFFF,#00FF00,#FFFF00,#FF0000\n"
        "bucket   245    245-500     \"over 244 ms\"   #FF8C00\n"
        "bucket   246    501-1000    \"over 500 ms\"   #FF4500\n"
        "bucket   247    1001-30000  \"over 1000 ms\"  #B22222\n"
        "state    254    no_reply    \"No reply\"      #000000\n";

    printf("[1] parsing\n");
    {
        PROFILE p;
        CHECK(parses(PING,&p), "the documented format parses");
        CHECK(p.entries.size() == 5, "five entries, comments and blanks skipped");

        const PROFILE_ENTRY &lit = p.entries[0];
        CHECK(lit.kind == BS_PROFILE_LITERAL, "the first entry is a literal range");
        CHECK(lit.first == 0 && lit.last == 244, "covering 0 to 244");
        CHECK(lit.unit == "ms", "with a unit");
        CHECK(lit.scale == 1.0, "and a default scale of 1");
        CHECK(lit.colours.size() == 5, "a gradient may have more than two stops");

        const PROFILE_ENTRY &bucket = p.entries[3];
        CHECK(bucket.kind == BS_PROFILE_BUCKET, "buckets parse");
        CHECK(bucket.first == 247, "the stored value");
        CHECK(bucket.low == 1001 && bucket.high == 30000, "and the magnitude it stands for");
        CHECK(bucket.label == "over 1000 ms", "a quoted label keeps its spaces");
        CHECK(bucket.colours.size() == 1, "and carries one colour");

        const PROFILE_ENTRY &state = p.entries[4];
        CHECK(state.kind == BS_PROFILE_STATE, "states parse");
        CHECK(state.code == "no_reply", "with a code");
        CHECK(state.label == "No reply", "and a label");
    }

    printf("[2] a bound may exceed the type it is stored in\n");
    {
        PROFILE p;
        CHECK(parses("literal 0-250 ms\nbucket 251 1000-4000000000 \"very slow\"\n",&p),
              "a bound far past uint8 is accepted");
        CHECK(p.entries[1].high == 4000000000.0, "and kept at full magnitude");
    }

    printf("[3] scale\n");
    {
        PROFILE p;
        CHECK(parses("literal 0-244 ms 10\n",&p), "a scale may follow the unit");
        CHECK(p.entries[0].scale == 10.0, "and is read as a number, not a colour");
    }

    printf("[4] classification\n");
    {
        PROFILE p;
        parses(PING,&p);
        const PROFILE_ENTRY *e;

        e = ProfileStore::classify(&p,17);
        CHECK(e && e->kind == BS_PROFILE_LITERAL, "17 is a literal reading");

        e = ProfileStore::classify(&p,244);
        CHECK(e && e->kind == BS_PROFILE_LITERAL, "244 is still literal, the top of the range");

        e = ProfileStore::classify(&p,246);
        CHECK(e && e->kind == BS_PROFILE_BUCKET && e->low == 501, "246 is the 501-1000ms bucket");

        e = ProfileStore::classify(&p,254);
        CHECK(e && e->kind == BS_PROFILE_STATE, "254 is a state");

        e = ProfileStore::classify(&p,255);
        CHECK(e == NULL, "255 is claimed by nothing; the header owns the fill");

        e = ProfileStore::classify(&p,248);
        CHECK(e == NULL, "an unclaimed value classifies as nothing rather than guessing");
    }

    printf("[5] what must be rejected\n");
    {
        CHECK(rejects("literal 244-0 ms\n","a backwards range"),
              "a literal range that runs backwards");
        CHECK(rejects("literal 0-244 ms\nbucket 245 500-500 \"x\"\n","low == high"),
              "a bucket whose bounds are not a range");
        CHECK(rejects("literal 0-244 ms\nstate 244 dup \"x\"\n","244 claimed twice"),
              "two entries claiming the same stored value");
        CHECK(rejects("literal 0-100 ms\nbucket 200 1000-2000 \"a\"\nbucket 201 100-200 \"b\"\n",
                      "bounds descend while stored values ascend"),
              "buckets whose order disagrees with their bounds, which would break max");
        // Bounds are inclusive at both ends, so adjacent ranges step by one and a
        // shared edge is a real overlap.
        {
            PROFILE p;
            CHECK(parses("literal 0-244 ms\nbucket 245 245-500 \"a\"\n"
                         "bucket 246 501-1000 \"b\"\nbucket 247 1001-30000 \"c\"\n",&p),
                  "adjacent ranges written 245-500, 501-1000, 1001-30000");
        }
        CHECK(rejects("literal 0-244 ms\nbucket 245 245-500 \"a\"\nbucket 246 500-1000 \"b\"\n",
                      "500 belongs to both"),
              "a shared edge, since 500 would be in two buckets at once");
        CHECK(rejects("literal 0-244 ms\nbucket 245 245-600 \"a\"\nbucket 246 500-1000 \"b\"\n",
                      "245-600 and 500-1000 share 500-600"),
              "and ranges that overlap outright");
        CHECK(rejects("literal 0-300 ms\nbucket 245 245-500 \"a\"\n",
                      "the literal range reaches into the bucket"),
              "a literal range and a bucket claiming the same magnitude");
        CHECK(rejects("literal 0-100 ms 10\nbucket 245 500-1000 \"a\"\n",
                      "scaled, the literal reaches 1000"),
              "and the same once the scale is applied");
        CHECK(rejects("nonsense 1 2 3\n","unknown keyword"), "an unknown entry kind");
        CHECK(rejects("bucket 245 \"unterminated\n","unterminated quote"), "an unterminated label");
        CHECK(rejects("# only a comment\n","no entries"), "a profile with no entries");
    }

    printf("[6] names are whitelisted, not sanitised\n");
    {
        CHECK(ProfileStore::nameIsSafe("ping"), "a plain name is fine");
        CHECK(ProfileStore::nameIsSafe("ping-v2_1"), "dash and underscore are allowed");
        CHECK(!ProfileStore::nameIsSafe("../../etc/passwd"), "traversal is refused");
        CHECK(!ProfileStore::nameIsSafe("ping/../x"), "so is a slash anywhere");
        CHECK(!ProfileStore::nameIsSafe(""), "an empty name is refused");
        CHECK(!ProfileStore::nameIsSafe(std::string(65,'a')), "and an absurdly long one");
    }

    printf("[7] loading from a directory\n");
    {
        char path[512];
        snprintf(path,sizeof(path),"%s/ping",dir);
        FILE *f = fopen(path,"w");
        fputs(PING,f);
        fclose(f);

        ProfileStore store;
        store.configure(dir);

        std::string error;
        const PROFILE *p = store.get("ping",&error);
        CHECK(p != NULL, "a profile loads from its file");
        CHECK(p && p->entries.size() == 5, "with its entries");

        const PROFILE *again = store.get("ping",&error);
        CHECK(again == p, "and is cached, not reparsed");

        CHECK(store.get("absent",&error) == NULL, "a missing profile is NULL, not empty");
        CHECK(error.find("no profile") != std::string::npos, "and says so");

        CHECK(store.get("../../etc/passwd",&error) == NULL, "a traversal never reaches the disk");

        std::vector<std::string> names;
        store.list(&names);
        bool found = false;
        for(size_t i = 0; i < names.size(); i++) if(names[i] == "ping") found = true;
        CHECK(found, "listing finds it");
    }

    return testReport();
}
