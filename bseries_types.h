#ifndef BSERIES_TYPES_H
#define BSERIES_TYPES_H

#include <stdint.h>
#include <string.h>


/// Datatype classes.
///
/// These values are written into the series header and are therefore part of the
/// on disk format. Never renumber them; append new classes at the end.

#define BS_UNSIGNED     0
#define BS_SIGNED       1
#define BS_FLOAT        2
#define BS_TYPE_INVALID 255


/// A concrete type is the pair (class, width in bytes):
///     uint8   = (BS_UNSIGNED, 1)
///     int32   = (BS_SIGNED,   4)
///     float32 = (BS_FLOAT,    4)
///
/// Series headers pack that pair into a single 32 bit typecode so that the header
/// stayed 20 bytes when the datatype was added. Version 1 headers predate the
/// datatype and store a plain byte width in the same field, see bseries.h.


typedef struct {
    const char *name;
    uint8_t datatype;
    uint8_t datasize;
} BS_TYPE_NAME;


/// The canonical name for a pair is its first entry here, the rest are aliases.

static const BS_TYPE_NAME bs_type_names[] = {
    {"uint8",   BS_UNSIGNED, 1},
    {"uint16",  BS_UNSIGNED, 2},
    {"uint32",  BS_UNSIGNED, 4},
    {"uint64",  BS_UNSIGNED, 8},
    {"int8",    BS_SIGNED,   1},
    {"int16",   BS_SIGNED,   2},
    {"int32",   BS_SIGNED,   4},
    {"int64",   BS_SIGNED,   8},
    {"float32", BS_FLOAT,    4},
    {"float64", BS_FLOAT,    8},

    {"byte",    BS_UNSIGNED, 1},
    {"uchar",   BS_UNSIGNED, 1},
    {"char",    BS_SIGNED,   1},
    {"float",   BS_FLOAT,    4},
    {"double",  BS_FLOAT,    8}
};

#define BS_TYPE_NAME_COUNT (sizeof(bs_type_names)/sizeof(bs_type_names[0]))


/// True if this class and width name a type the database can store.

static inline bool bsTypeValid(uint8_t datatype, uint8_t datasize){

    if(datatype == BS_FLOAT)
        return datasize == 4 || datasize == 8;

    if(datatype == BS_UNSIGNED || datatype == BS_SIGNED)
        return datasize == 1 || datasize == 2 || datasize == 4 || datasize == 8;

    return false;
}


/// "float32" -> (BS_FLOAT,4). Returns false if the name is not recognised.

static inline bool bsTypeFromName(const char *name, uint8_t *datatype, uint8_t *datasize){

    if(name == NULL)
        return false;

    for(unsigned int i = 0; i < BS_TYPE_NAME_COUNT; i++){
        if(strcmp(name,bs_type_names[i].name) == 0){
            *datatype = bs_type_names[i].datatype;
            *datasize = bs_type_names[i].datasize;
            return true;
        }
    }

    return false;
}


/// (BS_FLOAT,4) -> "float32". Returns "invalid" for pairs that are not storable.

static inline const char *bsTypeName(uint8_t datatype, uint8_t datasize){

    for(unsigned int i = 0; i < BS_TYPE_NAME_COUNT; i++){
        if(bs_type_names[i].datatype == datatype && bs_type_names[i].datasize == datasize)
            return bs_type_names[i].name;
    }

    return "invalid";
}


/// The byte a series of this type is filled with to mean "no point was recorded
/// here". Series are dense, so every gap has to be spelled out in the file.
///
/// 0xFF over a float of either width is a NaN, which is exactly the sentinel you
/// want. Over an unsigned integer it is the maximum value, so the top of the range
/// is reserved and cannot be stored. Signed integers have no clean sentinel at
/// all, so they get 0x80 fill (a large negative number) purely because it is a
/// less likely reading than the -1 that 0xFF fill would produce. If any of that
/// collides with real data for your series, set an explicit null fill in the
/// series definition instead.

static inline unsigned char bsTypeNullFill(uint8_t datatype, uint8_t datasize){

    (void)datasize;

    if(datatype == BS_SIGNED)
        return 0x80;

    return 0xFF;
}


#endif // BSERIES_TYPES_H
