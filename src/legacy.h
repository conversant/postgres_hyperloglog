#ifndef LEGACY_H
#define LEGACY_H
#include "hyperloglog.h"
// V1 functions
HyperLogLogCounter hyperloglog_compress_V1(HyperLogLogCounter hloglog);
HyperLogLogCounter hyperloglog_compress_dense_V1(HyperLogLogCounter hloglog);
HyperLogLogCounter hyperloglog_compress_sparse_V1(HyperLogLogCounter hloglog);
HyperLogLogCounter hyperloglog_decompress_V1(HyperLogLogCounter hloglog);
HyperLogLogCounter hyperloglog_decompress_dense_V1(HyperLogLogCounter hloglog);
HyperLogLogCounter hyperloglog_decompress_sparse_V1(HyperLogLogCounter hloglog);
#endif
