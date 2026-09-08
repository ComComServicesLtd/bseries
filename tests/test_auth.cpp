// The key store: hashing, verification, revocation and the bootstrap window.

#include "auth_store.h"
#include "bseries.h"
#include "test_util.h"

#include <string>
#include <vector>

int main(int argc, char **argv){

    (void)argc; (void)argv;
    const char *dir = testMakeDirectory();
    std::string path = std::string(dir) + "/auth.keys";

    printf("[1] sha256 against the published vectors\n");
    {
        CHECK(authSha256Hex("") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","empty");
        CHECK(authSha256Hex("abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad","abc");
        CHECK(authSha256Hex("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")
              == "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1","56 bytes, the padding boundary");
        CHECK(authSha256Hex(std::string(64,'a'))
              == "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb","exactly one block");
        CHECK(authSha256Hex(std::string(1000,'a'))
              == "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3","many blocks");
    }

    printf("[2] minting and verifying\n");
    {
        AuthStore store;
        CHECK(store.load(path)==0,"an absent keystore is not an error");
        CHECK(store.empty(),"and starts empty");

        std::string write_key, read_key;
        CHECK(store.create("admin",AUTH_ROLE_WRITE,&write_key)==NO_ERROR,"mint a write key");
        CHECK(store.create("dash",AUTH_ROLE_READ,&read_key)==NO_ERROR,"mint a read key");
        CHECK(write_key.compare(0,4,"bsw_")==0,"a write key is recognisable");
        CHECK(read_key.compare(0,4,"bsr_")==0,"so is a read key");
        CHECK(write_key != read_key,"and they differ");

        CHECK(store.roleFor(write_key)==AUTH_ROLE_WRITE,"the write key grants write");
        CHECK(store.roleFor(read_key)==AUTH_ROLE_READ,"the read key grants read");
        CHECK(store.roleFor("nonsense")==AUTH_ROLE_NONE,"nonsense grants nothing");
        CHECK(store.roleFor("")==AUTH_ROLE_NONE,"nor does an empty string");
        CHECK(store.roleFor(write_key.substr(0,write_key.size()-1))==AUTH_ROLE_NONE,"nor a truncated key");

        CHECK(store.create("admin",AUTH_ROLE_READ,&read_key)==SERIES_ALREADY_EXISTS,"names are unique");

        // the secret must not be recoverable from what was written
        FILE *f = fopen(path.c_str(),"rb");
        std::string contents;
        char buffer[512];
        size_t got;
        while((got = fread(buffer,1,sizeof(buffer),f)) > 0) contents.append(buffer,got);
        fclose(f);
        CHECK(contents.find(write_key) == std::string::npos,"the keystore does not contain the write key");
        CHECK(contents.find(read_key) == std::string::npos,"nor the read key");
        CHECK(contents.find("admin") != std::string::npos,"only the names");

        // and it survives a reload
        AuthStore again;
        CHECK(again.load(path)==2,"two keys reload");
        CHECK(again.roleFor(write_key)==AUTH_ROLE_WRITE,"and still verify");
        CHECK(again.roleFor(read_key)==AUTH_ROLE_READ,"both of them");
    }

    printf("[3] revocation\n");
    {
        AuthStore store;
        store.load(path);
        std::string second;
        CHECK(store.create("second",AUTH_ROLE_WRITE,&second)==NO_ERROR,"a second write key");
        CHECK(store.revoke("dash")==NO_ERROR,"revoke the read key");
        CHECK(store.roleFor("dash")==AUTH_ROLE_NONE,"it stops working");
        CHECK(store.revoke("dash")==SERIES_NOT_FOUND,"revoking it twice is not found");
        CHECK(store.revoke("admin")==NO_ERROR,"one write key may go while another remains");
        CHECK(store.revoke("second")==SERIES_ALREADY_EXISTS,"the last write key may not, that would lock the database out");
        CHECK(store.roleFor(second)==AUTH_ROLE_WRITE,"and it still works after the refusal");
        CHECK(store.revoke("second",true)==NO_ERROR,"unless a write key exists outside the store");
    }

    printf("[4] the bootstrap window\n");
    {
        std::string fresh_path = std::string(dir) + "/fresh.keys";
        AuthStore store;
        store.load(fresh_path);

        std::string token = store.mintBootstrapToken();
        CHECK(!token.empty(),"a store with no write key mints a token");
        CHECK(token.size()==64,"32 random bytes of it");
        CHECK(store.bootstrapTokenMatches(token),"which matches itself");
        CHECK(!store.bootstrapTokenMatches("nonsense"),"and nothing else");
        CHECK(store.roleFor(token)==AUTH_ROLE_NONE,"the token is not a key");

        std::string first;
        CHECK(store.create("admin",AUTH_ROLE_WRITE,&first)==NO_ERROR,"mint the first write key with it");
        CHECK(!store.bootstrapTokenMatches(token),"the token stops working immediately");
        CHECK(store.mintBootstrapToken().empty(),"and no new one is issued while a write key exists");

        // a read key alone does not close the window
        std::string reader_path = std::string(dir) + "/reader.keys";
        AuthStore reader;
        reader.load(reader_path);
        std::string r;
        reader.create("r",AUTH_ROLE_READ,&r);
        std::string t2 = reader.mintBootstrapToken();
        CHECK(!t2.empty(),"a store with only a read key still needs bootstrapping");
    }

    printf("[5] names and roles\n");
    {
        AuthStore store;
        store.load(std::string(dir) + "/names.keys");
        std::string secret;
        CHECK(store.create("",AUTH_ROLE_WRITE,&secret)==INVALID_SERIES_DEFINITION,"an empty name is refused");
        CHECK(store.create("has space",AUTH_ROLE_WRITE,&secret)==INVALID_SERIES_DEFINITION,"a space is refused");
        CHECK(store.create("../escape",AUTH_ROLE_WRITE,&secret)==INVALID_SERIES_DEFINITION,"a path is refused");
        CHECK(store.create("ok",AUTH_ROLE_NONE,&secret)==INVALID_SERIES_DEFINITION,"an unknown role is refused");
        int role;
        CHECK(AuthStore::roleFromName("read",&role) && role==AUTH_ROLE_READ,"read parses");
        CHECK(AuthStore::roleFromName("write",&role) && role==AUTH_ROLE_WRITE,"write parses");
        CHECK(!AuthStore::roleFromName("admin",&role),"anything else does not");
    }

    return testReport();
}
