#ifndef DEBUG_H
#define DEBUG_H

#include <stdio.h>

// Diagnostics go to stderr, never stdout.
//
// Two reasons, both of which bite in a container. stderr is unbuffered, so a
// message is on its way out the moment it is written rather than sitting in a
// stdio buffer until the process exits or the buffer fills; when stdout is a pipe,
// as it is under docker logs, it is block buffered and a crash loses everything
// still in it. And keeping diagnostics off stdout leaves that stream free for
// anything a caller might actually want to parse.

#define WARN

#ifdef DEBUG
#define _WARN(...)  fprintf(stderr,__VA_ARGS__)
#define _ERROR(...) fprintf(stderr,__VA_ARGS__)
#define _DEBUG(...) fprintf(stderr,__VA_ARGS__)
#endif

#ifdef WARN
#define _WARN(...)  fprintf(stderr,__VA_ARGS__)
#define _ERROR(...) fprintf(stderr,__VA_ARGS__)
#define _DEBUG(...) // fprintf(stderr,__VA_ARGS__)
#endif

#endif // DEBUG_H
