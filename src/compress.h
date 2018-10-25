#if PG_VERSION_NUM >= 90500
#include "common/pg_lzcompress.h"
typedef struct {
    int32 vl_len_;
    int32 rawsize;
} PGLZ_Header;
#else
#include "utils/pg_lzcompress.h"
#endif