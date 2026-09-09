#include "bseries.h"
#include "test_util.h"
#include <stdio.h>
#include <math.h>


static const char *writeDefinitions(const char *dir){
    static char path[512];
    snprintf(path,sizeof(path),"%s/series.conf",dir);
    FILE *f = fopen(path,"w");
    fputs("ping        1-9999          1s   uint8\n",f);
    fputs("temperature 10000-19999     60s  float32\n",f);
    fputs("power_draw  20000-29999     10s  uint32\n",f);
    fputs("door_state  30000-30999     5s   uint8   0x02\n",f);
    fclose(f);
    return path;
}


// Versions 1 to 3 are twenty bytes on disk: five little endian uint32. The
// in-memory SERIES is a decoded header and is much larger, so these are written
// out field by field rather than by dropping the struct on the disk -- which is
// what the database itself now does, and the reason this helper exists.
static void writeLegacyHeader(const char *path, uint32_t version, uint32_t timestamp,
                              uint32_t interval, uint32_t typecode, const unsigned char *points,
                              size_t count){

    uint32_t checksum = 1234567890 + ((version ^ timestamp) ^ (interval ^ typecode));
    uint32_t field[5] = { version, timestamp, interval, typecode, checksum };

    FILE *w = fopen(path,"wb");
    for(int i = 0; i < 5; i++){
        unsigned char b[4];
        b[0]=(unsigned char)field[i];      b[1]=(unsigned char)(field[i]>>8);
        b[2]=(unsigned char)(field[i]>>16);b[3]=(unsigned char)(field[i]>>24);
        fwrite(b,4,1,w);
    }
    if(count) fwrite(points,1,count,w);
    fclose(w);
}

int main(int argc,char**argv){
    (void)argc; (void)argv;
    const char *dir = testMakeDirectory();
    const char *conf = writeDefinitions(dir);

    // ---- config parser ----
    printf("[A] definitions file\n");
    {
        BSeries db; db.data_directory=dir;
        int n = db.loadDefinitions(conf);
        printf("   loaded %d\n", n);
        CHECK(n==4, "loaded 4 definitions");
        SERIES_DEFINITION d;
        CHECK(db.definitionForKey(5000,&d) && d.interval==1 && d.datatype==BS_UNSIGNED && d.datasize==1, "ping -> 1s uint8");
        CHECK(db.definitionForKey(10500,&d) && d.interval==60 && d.datatype==BS_FLOAT && d.datasize==4, "temperature -> 60s float32");
        CHECK(db.definitionForKey(20001,&d) && d.interval==10 && d.datatype==BS_UNSIGNED && d.datasize==4, "power_draw -> 10s uint32");
        CHECK(db.definitionForKey(30001,&d) && d.null_fill_byte==0x02, "explicit null fill honoured");
        CHECK(!db.definitionForKey(999999,NULL), "unmatched key has no definition");
        CHECK(db.loadDefinitions("/no/such/file")==DEFINITIONS_FILE_UNREADABLE, "missing file reported");
        db.close();
    }

    // ---- a bad line fails the whole load ----
    printf("[B] malformed definitions rejected\n");
    {
        const char *bad[] = {
            "ping 1-9999 1s notatype\n",
            "ping 1-9999 0s uint8\n",
            "ping 9999-1 1s uint8\n",
            "ping 1-9999 1s uint8 0x1FF\n",
            "ping 1-9999 1s\n",
            "ping 1..9999 1s uint8\n",
            "ping 1-9999 1x uint8\n",
            "ping 1-9999 1s float16\n",
        };
        for(unsigned i=0;i<sizeof(bad)/sizeof(bad[0]);i++){
            char path[512]; snprintf(path,sizeof(path),"%s/bad.conf",dir);
            FILE*f=fopen(path,"w"); fputs("good 1-10 1s uint8\n",f); fputs(bad[i],f); fclose(f);
            BSeries db; db.data_directory=dir;
            int rc = db.loadDefinitions(path);
            char msg[160]; snprintf(msg,sizeof(msg),"rejected: %.*s",(int)strcspn(bad[i],"\n"),bad[i]);
            CHECK(rc<0, msg);
            CHECK(!db.definitionForKey(5,NULL), "  and installed nothing");
            db.close();
        }
    }

    // ---- a defined float32 series round trips at its own interval ----
    printf("[C] float32 series from a definition\n");
    {
        BSeries db; db.data_directory=dir; db.loadDefinitions(conf);
        uint32_t t0=time(NULL);
        float v[3]={20.5f,21.25f,-3.75f};
        for(int i=0;i<3;i++) CHECK(db.write(10500,&v[i],4,t0+i*60)==NO_ERROR,"write float32");
        int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; uint8_t dt=0; void*res=NULL;
        CHECK(db.read(10500,t0,t0+180,&n,&r,&spp,&fpt,&ds,&res,&dt)==NO_ERROR,"read");
        CHECK(spp==60,"interval came from the definition, not the global default");
        CHECK(ds==4 && dt==BS_FLOAT,"datatype reported as float32");
        float*o=(float*)res;
        CHECK(res && o[0]==v[0] && o[1]==v[1] && o[2]==v[2],"values round trip");
        delete[] (char*)res;
        db.close();
    }

    // ---- writing the wrong width is refused ----
    printf("[D] type mismatch\n");
    {
        BSeries db; db.data_directory=dir; db.loadDefinitions(conf);
        unsigned char b=1;
        CHECK(db.write(10500,&b,1,0)==SERIES_TYPE_MISMATCH,"byte write to a float32 series refused");
        float f=1.0f;
        CHECK(db.write(5000,&f,4,0)==SERIES_TYPE_MISMATCH,"float write to a uint8 series refused");
        db.close();
    }

    // ---- per type null fill ----
    printf("[E] null fill by type\n");
    {
        BSeries db; db.data_directory=dir; db.loadDefinitions(conf);
        uint32_t t0=time(NULL);
        float v=7.5f;
        db.write(10600,&v,4,t0);
        int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; void*res=NULL;
        db.read(10600,t0,t0+300,&n,&r,&spp,&fpt,&ds,&res);
        float*o=(float*)res;
        CHECK(res && o[0]==7.5f, "written point present");
        CHECK(res && n>=2 && isnan(o[1]), "gap reads back as NaN for float32");
        delete[] (char*)res;

        unsigned char d=1;
        db.write(30001,&d,1,t0);
        n=r=spp=fpt=0; res=NULL;
        db.read(30001,t0,t0+30,&n,&r,&spp,&fpt,&ds,&res);
        unsigned char*u=(unsigned char*)res;
        CHECK(res && u[0]==1, "door_state point present");
        CHECK(res && n>=2 && u[1]==0x02, "gap reads back as the configured 0x02");
        delete[] (char*)res;
        db.close();
    }

    // ---- version 1 files written before definitions existed still read ----
    printf("[F] older header versions still read\n");
    {
        // Version 3 is what gets written now: the null fill lives in the header, so
        // a series is readable correctly from its own file with no config.
        uint32_t t0=time(NULL);
        {
            BSeries db; db.data_directory=dir; db.default_seconds_per_point=10;
            unsigned char b=77;
            CHECK(db.write(777,&b,1,t0)==NO_ERROR,"write with no definition");
            db.close();
        }
        char path[512]; snprintf(path,sizeof(path),"%s/777",dir);
        FILE*f=fopen(path,"rb"); SERIES h; size_t got=bsReadHeader(f,&h)?1:0; fclose(f);
        CHECK(got==1 && h.version==SERIES_VERSION_PROFILED,"a new series is written as version 4");
        CHECK(bsHeaderBytes(SERIES_VERSION_FILLED)==20 && bsHeaderBytes(SERIES_VERSION_PROFILED)==128,
              "versions 1-3 are 20 bytes on disk, version 4 is 128");
        CHECK(bsTypeCodeDataSize(h.typecode)==1,"width recorded");
        CHECK(bsTypeCodeDataType(h.typecode)==BS_UNSIGNED,"type recorded");
        CHECK(bsTypeCodeNullFill(h.typecode)==0xFF,"fill recorded");

        // Version 4 carries fields the older headers had nowhere to put. Written
        // and read back through the serialiser, because the struct is no longer
        // the format and a round trip is the only thing that proves the layout.
        {
            snprintf(path,sizeof(path),"%s/790",dir);

            SERIES h;
            memset(&h,0,sizeof(h));
            h.version = SERIES_VERSION_PROFILED;
            h.timestamp = 1700000000;
            h.interval = 10;
            h.typecode = bsPackTypeCode(BS_UNSIGNED,1,0xFF);
            h.flags = SERIES_ADDRESS_IPV4;
            h.address[0]=8; h.address[1]=8; h.address[2]=8; h.address[3]=8;
            snprintf(h.profile,sizeof(h.profile),"ping");
            snprintf(h.name,sizeof(h.name),"gateway latency");
            bsFinaliseHeader(&h);

            FILE *w = fopen(path,"wb");
            CHECK(bsWriteHeader(w,&h),"a version 4 header writes");
            unsigned char point = 33; fwrite(&point,1,1,w);
            fclose(w);

            FILE *rd = fopen(path,"rb");
            fseek(rd,0,SEEK_END);
            CHECK(ftell(rd)==129,"128 byte header plus one point");
            SERIES back;
            memset(&back,0,sizeof(back));
            bool ok = bsReadHeader(rd,&back);
            fclose(rd);

            CHECK(ok,"and reads back");
            CHECK(back.version==SERIES_VERSION_PROFILED,"as version 4");
            CHECK(back.timestamp==1700000000,"its timestamp in seconds");
            CHECK(back.timestamp_ms==1700000000000ULL,"and in milliseconds");
            CHECK(back.interval==10 && back.interval_ms==10000,"its interval both ways");
            CHECK(strcmp(back.profile,"ping")==0,"the profile it is read with");
            CHECK(strcmp(back.name,"gateway latency")==0,"its name");
            CHECK((back.flags & SERIES_ADDRESS_MASK)==SERIES_ADDRESS_IPV4,"the address family");
            CHECK(back.address[0]==8 && back.address[3]==8,"and the address");
            CHECK(bsTypeCodeNullFill(back.typecode)==0xFF,"the fill still decodes");

            // The point sits after 128 bytes, not 20.
            BSeries db; db.data_directory=dir; db.default_seconds_per_point=10;
            int64_t n=0,r2=0,spp=0,fpt=0; uint32_t ds=0; uint8_t dt=0; void*res=NULL;
            CHECK(db.read(790,1700000000,1700000030,&n,&r2,&spp,&fpt,&ds,&res,&dt)==NO_ERROR,
                  "a version 4 series reads");
            CHECK(res && ((unsigned char*)res)[0]==33,"and its point is found at the new offset");
            delete[] (char*)res;
            db.close();
        }

        // A corrupted version 4 header must be refused, not read as though the
        // damage were data: the checksum covers every byte including the profile
        // name, which decides how the points are interpreted.
        {
            snprintf(path,sizeof(path),"%s/791",dir);
            SERIES h;
            memset(&h,0,sizeof(h));
            h.version = SERIES_VERSION_PROFILED;
            h.timestamp = 1700000000;
            h.interval = 10;
            h.typecode = bsPackTypeCode(BS_UNSIGNED,1,0xFF);
            snprintf(h.profile,sizeof(h.profile),"ping");
            bsFinaliseHeader(&h);
            FILE *w = fopen(path,"wb"); bsWriteHeader(w,&h); fclose(w);

            // flip one byte of the profile name
            FILE *rw = fopen(path,"r+b");
            fseek(rw,32,SEEK_SET);
            unsigned char c = 'X'; fwrite(&c,1,1,rw);
            fclose(rw);

            SERIES back;
            FILE *rd = fopen(path,"rb");
            bool ok = bsReadHeader(rd,&back);
            fclose(rd);
            CHECK(!ok,"a single flipped byte in the profile name fails the checksum");
        }

        // Hand build a genuine version 1 header, as releases before typed headers
        // wrote them, and check it still reads.
        {
            snprintf(path,sizeof(path),"%s/778",dir);
            unsigned char point = 42;
            writeLegacyHeader(path,SERIES_VERSION_LEGACY,t0,10,1,&point,1);
        }
        {
            BSeries db; db.data_directory=dir; db.default_seconds_per_point=10;
            int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; uint8_t dt=0; void*res=NULL;
            CHECK(db.read(778,t0,t0+30,&n,&r,&spp,&fpt,&ds,&res,&dt)==NO_ERROR,"a version 1 file still reads");
            CHECK(ds==1 && dt==BS_UNSIGNED,"its width and inferred type");
            CHECK(spp==10,"its interval");
            CHECK(res && ((unsigned char*)res)[0]==42,"and its point");
            delete[] (char*)res;
            db.close();
        }

        // A genuine version 2 header: typed, but with the fill byte left spare.
        // Its fill must come from the type, not be read as 0x00.
        {
            SERIES v2;
            memset(&v2,0,sizeof(v2));
            v2.version = SERIES_VERSION_TYPED;
            v2.timestamp = t0;
            v2.interval = 10;
            v2.typecode = (uint32_t)BS_UNSIGNED | (1u << 8);   // spare byte zero

            snprintf(path,sizeof(path),"%s/779",dir);
            unsigned char point = 7;
            writeLegacyHeader(path,SERIES_VERSION_TYPED,t0,10,v2.typecode,&point,1);

            BSeries tmp; tmp.data_directory = dir;
            CHECK(tmp.resolveNullFill(779,&v2)==0xFF,"a version 2 fill comes from the type, not the spare byte");
            tmp.close();
        }
        {
            BSeries db; db.data_directory=dir; db.default_seconds_per_point=10;
            int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; void*res=NULL;
            CHECK(db.read(779,t0,t0+30,&n,&r,&spp,&fpt,&ds,&res)==NO_ERROR,"a version 2 file still reads");
            CHECK(res && ((unsigned char*)res)[0]==7 && ((unsigned char*)res)[1]==0xFF,
                  "its point, and its gap as the type's fill");
            delete[] (char*)res;
            db.close();
        }

        // An unknown future version is refused rather than guessed at
        {
            SERIES v9;
            memset(&v9,0,sizeof(v9));
            v9.version = 9;
            v9.timestamp = t0; v9.interval = 10;
            v9.typecode = (uint32_t)BS_UNSIGNED | (1u << 8);
            BSeries tmp; tmp.data_directory = dir;
            v9.checksum = tmp.getChecksum(&v9);
            snprintf(path,sizeof(path),"%s/780",dir);
            FILE *w=fopen(path,"wb"); fwrite(&v9,sizeof(v9),1,w); fclose(w);
            tmp.close();
        }
        {
            BSeries db; db.data_directory=dir;
            int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; void*res=NULL;
            CHECK(db.read(780,t0,t0+30,&n,&r,&spp,&fpt,&ds,&res)!=NO_ERROR,"an unknown header version is refused");
            delete[] (char*)res;
            db.close();
        }
    }

    printf("[F2] the null fill survives without a config\n");
    {
        // The bug this version was added for: a custom fill used to live only in
        // the definitions file, so the same bytes meant different things depending
        // on whether that file was present.
        uint32_t t0 = 1700000000;
        {
            BSeries db; db.data_directory=dir;
            db.defineSeries(900,900,5,BS_UNSIGNED,1,"door",0x02);
            CHECK(db.createSeriesFile(900,5,BS_UNSIGNED,1,t0)==NO_ERROR,"create with a custom fill");
            unsigned char a=0,b=1;
            db.write(900,&a,1,t0);
            db.write(900,&b,1,t0+5);
            db.write(900,&b,1,t0+15);
            db.close();
        }
        {
            // no definitions installed at all this time
            BSeries db; db.data_directory=dir;
            int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; void*res=NULL;
            CHECK(db.read(900,t0,t0+20,&n,&r,&spp,&fpt,&ds,&res)==NO_ERROR,"read it back with no config");
            unsigned char *o=(unsigned char*)res;
            CHECK(res && n==4 && o[2]==0x02,"the gap is still written as 0x02");
            delete[] (char*)res;

            // The header, not a definition, is what says so now
            SERIES header; int64_t size=0;
            CHECK(db.seriesInfo(900,&header,&size)==NO_ERROR,"header readable");
            CHECK(header.version==SERIES_VERSION_PROFILED,"written as version 4");
            CHECK(db.resolveNullFill(900,&header)==0x02,"and the fill comes from the header");
            CHECK(!db.definitionForKey(900,NULL),"with no definition installed at all");
            db.close();
        }
    }

    printf("[G] definition drift\n");
    {
        uint32_t t0=time(NULL);
        {   BSeries db; db.data_directory=dir; db.default_seconds_per_point=10;
            unsigned char b=5; db.write(888,&b,1,t0); db.close(); }
        BSeries db; db.data_directory=dir;
        db.defineSeries(888,888,3600,BS_FLOAT,4,"drifted");
        int64_t n=0,r=0,spp=0,fpt=0; uint32_t ds=0; uint8_t dt=0; void*res=NULL;
        int rc = db.read(888,t0,t0+30,&n,&r,&spp,&fpt,&ds,&res,&dt);
        CHECK(rc==NO_ERROR,"still reads");
        CHECK(spp==10 && ds==1,"file header wins over the drifted definition");
        delete[] (char*)res;
        db.close();
    }

    printf("[H] per table definitions\n");
    {
        char path[512];
        snprintf(path,sizeof(path),"%s/tables.conf",dir);
        FILE *f = fopen(path,"w");
        fputs("legacy  1-99        10s  uint8\n",f);       // before any table line
        fputs("table network\n",f);
        fputs("ping    1-9999      1s   uint8\n",f);
        fputs("table power\n",f);
        fputs("draw    1-999       10s  uint32\n",f);
        fclose(f);

        SERIES_DEFINITION d;

        BSeries a; a.data_directory = dir;
        CHECK(a.loadDefinitions(path,"default")==1, "the default table gets the leading definitions only");
        CHECK(a.definitionForKey(50,&d) && d.interval==10 && d.datasize==1, "and they are the right ones");
        CHECK(!a.definitionForKey(5000,NULL), "network's definitions are not installed in default");
        a.close();

        BSeries b; b.data_directory = dir;
        CHECK(b.loadDefinitions(path,"network")==1, "network gets its own");
        CHECK(b.definitionForKey(5000,&d) && d.interval==1 && d.datatype==BS_UNSIGNED && d.datasize==1, "1s uint8 there");
        b.close();

        BSeries c; c.data_directory = dir;
        CHECK(c.loadDefinitions(path,"power")==1, "power gets its own");
        CHECK(c.definitionForKey(500,&d) && d.interval==10 && d.datasize==4, "10s uint32 for an overlapping key range");
        c.close();

        BSeries e; e.data_directory = dir;
        CHECK(e.loadDefinitions(path)==3, "no table filter installs every definition");
        e.close();

        // a typo under one table is reported even when another is being loaded
        snprintf(path,sizeof(path),"%s/bad_table.conf",dir);
        f = fopen(path,"w");
        fputs("table network\n",f);
        fputs("ping 1-9999 1s uint8\n",f);
        fputs("table power\n",f);
        fputs("draw 1-999 10s notatype\n",f);
        fclose(f);
        BSeries g; g.data_directory = dir;
        CHECK(g.loadDefinitions(path,"network")<0, "a bad line under another table still fails the load");
        g.close();

        snprintf(path,sizeof(path),"%s/bad_directive.conf",dir);
        f = fopen(path,"w");
        fputs("table\n",f);
        fclose(f);
        BSeries h; h.data_directory = dir;
        CHECK(h.loadDefinitions(path,"default")<0, "a table line with no name is refused");
        h.close();
    }

    return testReport();
}
