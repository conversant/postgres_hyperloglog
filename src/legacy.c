#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <math.h>
#include <string.h>

#include "postgres.h"
#include "legacy.h"
#include "utils/pg_lzcompress.h"

/* duplicate functions needed to have legacy code work */
static void insertion_sort(uint32_t *a, const size_t n);
HyperLogLogCounter dedupe_V1(HyperLogLogCounter hloglog);
int size_sparse_array_V1(int8_t b);

/* Returns the maximum number of 32 bits ints that can be stored in the sparse array */
int size_sparse_array_V1(int8_t b){
    return  pow(2,b-4) - ceil(sizeof(HyperLogLogCounterData)/4.0);
}

static void insertion_sort(uint32_t *a, const size_t n) {
    size_t i, j;
    uint32_t value;
    for (i = 1; i < n; i++) {
        value = a[i];
        for (j = i; j > 0 && value < a[j - 1]; j--) {
            a[j] = a[j - 1];
        }
        a[j] = value;
    }
}

/* First sorts the sparse data and then removes duplicates resetting the idx so further
 * new values can be added. */
HyperLogLogCounter dedupe_V1 (HyperLogLogCounter hloglog){
    int i,j;
    uint32_t * sparse_data;
    sparse_data = (uint32_t *) hloglog->data;

    for (i=0;i<hloglog->idx - 1;i++){
        if (sparse_data[i] > sparse_data[i+1]){
            insertion_sort((uint32_t *)hloglog->data,hloglog->idx);
            break;
        }
    }

    sparse_data = (uint32_t *) hloglog->data;

    j = 0;
    for (i=0; i < hloglog->idx ; i++){
        if (i == 0){
            j++;
            continue;
        } else if (sparse_data[i] != sparse_data[j - 1]){
            sparse_data[j++] = sparse_data[i];
        }
    }
    hloglog->idx = j;

    memset(&sparse_data[j],0,size_sparse_array_V1(hloglog->b) - j);

    return hloglog;
}



/* ----------------------------------------------------------------------------
   ---------------- OLD FUNCTIONS FOR BACKWARDS COMPATABILITY -----------------
   ---------------------------------------------------------------------------- */
// V1 functions

/* Compress header function */
HyperLogLogCounter hyperloglog_compress_V1(HyperLogLogCounter hloglog){
    /* make sure the data isn't compressed already */
    if (hloglog->b < 0) {
        return hloglog;
    }

    if (hloglog->idx == -1){
        hloglog = hyperloglog_compress_dense_V1(hloglog);
    } else {
        hloglog = hyperloglog_compress_sparse_V1(hloglog);
    }

    return hloglog;
}

/* Compresses dense encoded counters using lz compression */
HyperLogLogCounter hyperloglog_compress_dense_V1(HyperLogLogCounter hloglog){
    PGLZ_Header * dest;
    char entry,*data;
    int i, m;

    /* make sure the dest struct has enough space for an unsuccessful compression
     * and a 4 bytes of overflow since lz might not recognize its over until then
     * preventing segfaults */
    m = (int)pow(2,hloglog->b);
    dest = malloc(m + sizeof(PGLZ_Header) + 4);
    memset(dest,0,m + sizeof(PGLZ_Header) + 4);
    data = malloc(m);

    /* put all registers in a normal array  i.e. remove dense packing so
     * lz compression can work optimally */
    for(i=0; i < m ; i++){
        HLL_DENSE_GET_REGISTER(entry,hloglog->data,i,hloglog->binbits);
        data[i] = entry;
    }

    /* lz_compress the normalized array and copy that data into hloglog->data
     * if any compression was acheived */
    pglz_compress(data,m,dest,PGLZ_strategy_always);
    if (VARSIZE(dest) >= (m * hloglog->binbits /8) ){
    /* free allocated memory and return unaltered array */
        if (dest){
            free(dest);
        }
        if (data){
            free(data);
        }
        return hloglog;
    }
    memcpy(hloglog->data,dest,VARSIZE(dest));

    /* resize the counter to only encompass the compressed data and the struct overhead*/
    SET_VARSIZE(hloglog,sizeof(HyperLogLogCounterData) + VARSIZE(dest) );

    /* invert the b value so it being < 0 can be used as a compression flag */
    hloglog->b = -1 * (hloglog->b);

    /* free allocated memory */
    if (dest){
        free(dest);
    }
    if (data){
        free(data);
    }

    /* return the compressed counter */
    return hloglog;
}

/* Current implementation of sparse compression is rather niave. First duplicates are
 * removed then the counter is resized to store just the current values to minimize disk
 * footprint. There is room here for further optimatization. */
HyperLogLogCounter hyperloglog_compress_sparse_V1(HyperLogLogCounter hloglog){

    dedupe_V1(hloglog);

    SET_VARSIZE(hloglog,sizeof(HyperLogLogCounterData) + hloglog->idx*4);

    /* invert the b value so it being < 0 can be used as a compression flag */
    hloglog->b = -1 * (hloglog->b);

    return hloglog;
}

/* Decompress header function */
HyperLogLogCounter hyperloglog_decompress_V1(HyperLogLogCounter hloglog){
     /* make sure the data is compressed */
    if (hloglog->b > 0) {
        return hloglog;
    }

    if (hloglog->idx == -1){
        hloglog = hyperloglog_decompress_dense_V1(hloglog);
    } else {
        hloglog = hyperloglog_decompress_sparse_V1(hloglog);
    }

    return hloglog;
}

/* Decompresses sparse counters */
HyperLogLogCounter hyperloglog_decompress_dense_V1(HyperLogLogCounter hloglog){
    char * dest;
    int m,i;
    HyperLogLogCounter htemp;

    /* reset b to positive value for calcs and to indicate data is decompressed */
    hloglog->b = -1 * (hloglog->b);

    /* allocate and zero an array large enough to hold all the decompressed bins */
    m = (int) pow(2,hloglog->b);
    dest = malloc(m);
    memset(dest,0,m);

    /* decompress the data */
    pglz_decompress((PGLZ_Header *)hloglog->data,dest);

    /* copy the struct internals but not the data into a counter with enough space
     * for the uncompressed data  */
    htemp = palloc(sizeof(HyperLogLogCounterData) + (int)ceil((m * hloglog->binbits / 8.0)));
    memcpy(htemp,hloglog,sizeof(HyperLogLogCounterData));
    hloglog = htemp;

    /* set the registers to the appropriate value based on the decompressed data */
    for (i=0; i<m; i++){
        HLL_DENSE_SET_REGISTER(hloglog->data,i,dest[i],hloglog->binbits);
    }

    /* set the varsize to the appropriate length  */
    SET_VARSIZE(hloglog,sizeof(HyperLogLogCounterData) + (int)ceil((m * hloglog->binbits / 8.0)) );


    /* free allocated memory */
    if (dest){
        free(dest);
    }

    return hloglog;
}

/* Decompresses sparse counters. Which currently just means allocating more memory and
 * flipping the compression flag */
HyperLogLogCounter hyperloglog_decompress_sparse_V1(HyperLogLogCounter hloglog){
    HyperLogLogCounter htemp;
    size_t length;

    /* reset b to positive value for calcs and to indicate data is decompressed */
    hloglog->b = -1 * (hloglog->b);

    length = pow(2,(hloglog->b-2));

    htemp = palloc0(length);
    memcpy(htemp,hloglog,VARSIZE(hloglog));
    hloglog = htemp;

    SET_VARSIZE(hloglog,length);
    return hloglog;
}

