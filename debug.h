#ifndef DEBUG_H
#define DEBUG_H

#define WARN

#ifdef DEBUG
#define _WARN(...)  printf(__VA_ARGS__)
#define _ERROR(...) printf(__VA_ARGS__)
#define _DEBUG(...) printf(__VA_ARGS__)
#endif

#ifdef WARN
#define _WARN(...)  printf(__VA_ARGS__)
#define _ERROR(...) printf(__VA_ARGS__)
#define _DEBUG(...) // printf(__VA_ARGS__)
#endif

#ifdef ERROR
#define warn(...) // printf(__VA_ARGS__)
#define error(...) printf(__VA_ARGS__)
#define debug(...) // printf(__VA_ARGS__)
#endif

#endif // DEBUG_H

