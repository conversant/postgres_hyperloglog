#include "hyperloglog.h"
#if PG_VERSION_NUM >= 90500
#include "common/pg_lzcompress.h"
typedef struct {
    int32 vl_len_;
    int32 rawsize;
} PGLZ_Header;
#else
#include "utils/pg_lzcompress.h"
#endif

#ifndef HLLUTILS_H
#define HLLUTILS_H
#define POW2(a) (1 << (a))

/* ---------------------- function declarations ------------------------ */
void insertion_sort(uint32_t* a, int n);
int dedupe(uint32_t* sparse_data, int idx);
int size_sparse_array(int8_t b);
uint64_t MurmurHash64A (const void * key, int len, unsigned int seed);
void pg_decompress(PGLZ_Header *data, HLLCounter htemp);

#endif
