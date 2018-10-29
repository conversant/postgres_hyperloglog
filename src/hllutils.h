#include "hyperloglog.h"
#if PG_VERSION_NUM >= 90500
#include "common/pg_lzcompress.h"
#else
#include "utils/pg_lzcompress.h"
#endif

#ifndef HLLUTILS_H
#define HLLUTILS_H
#define POW2(a) (1 << (a))

#if PG_VERSION_NUM >= 90500
typedef struct {
    int32 vl_len_;
    int32 rawsize;
} PGLZ_Header;
#endif

/* ---------------------- function declarations ------------------------ */
void insertion_sort(uint32_t* a, int n);
int dedupe(uint32_t* sparse_data, int idx);
int size_sparse_array(int8_t b);
uint64_t MurmurHash64A (const void * key, int len, unsigned int seed);
void pg_decompress(const PGLZ_Header *source, char *dest);
bool pg_compress(const char *source, int32 slen, PGLZ_Header *dest, const PGLZ_Strategy *strategy);

#endif
