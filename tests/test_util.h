#ifndef TEST_UTIL_H
#define TEST_UTIL_H

// Shared scaffolding for the test programs. Each test makes its own temporary
// directory so "make test" needs no arguments and leaves nothing behind.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>

static int test_failures = 0;

#define CHECK(condition,message) do { \
    if(!(condition)){ printf("  FAIL: %s\n",message); test_failures++; } \
    else printf("  ok:   %s\n",message); \
} while(0)

static char test_directory[256];

static const char *testMakeDirectory(){
    snprintf(test_directory,sizeof(test_directory),"/tmp/bseries_test_XXXXXX");
    if(mkdtemp(test_directory) == NULL){
        perror("mkdtemp");
        exit(1);
    }
    return test_directory;
}

static void testRemoveDirectory(){

    DIR *dir = opendir(test_directory);
    if(dir == NULL)
        return;

    struct dirent *entry;
    while((entry = readdir(dir)) != NULL){
        if(strcmp(entry->d_name,".") == 0 || strcmp(entry->d_name,"..") == 0)
            continue;
        char path[512];
        snprintf(path,sizeof(path),"%s/%s",test_directory,entry->d_name);
        remove(path);
    }

    closedir(dir);
    rmdir(test_directory);
}

static int testReport(){
    printf("\n%s (%d failures)\n", test_failures ? "FAILURES" : "ALL PASS", test_failures);
    testRemoveDirectory();
    return test_failures ? 1 : 0;
}

#endif
