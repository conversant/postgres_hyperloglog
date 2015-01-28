#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <math.h>
#include <string.h>

#include "postgres.h"
#include "utils/pg_lzcompress.h"

#include "varint.h"
#include "hyperloglog.h"
#include "constants.h"
#include "hllutils.h"

/* Externed Constants */
/* Alpha * m * m constants, for various numbers of 'b'. See constants.h for
 * more information. */
extern const float alpham[17];

/* linear counting thresholds */
extern const int threshold[19];

/* mask */
extern const uint32_t MASK[15][5];

/* error correction data (x-values) */
extern const double rawEstimateData[15][201];

/* error correction data (y-values) */
extern const double biasData[15][201];

/* precomputed inverse powers of 2 */
extern const double PE[64];

/* function declarations for local functions */
static double hyperloglog_estimate_dense(HyperLogLogCounter hloglog);
static double hyperloglog_estimate_sparse(HyperLogLogCounter hloglog);
static double error_estimate(double E,int b);

static HyperLogLogCounter hyperloglog_add_hash_dense(HyperLogLogCounter hloglog, uint64_t hash);
static HyperLogLogCounter hyperloglog_add_hash_sparse(HyperLogLogCounter hloglog, uint64_t hash);
static uint32_t encode_hash(uint64_t hash, HyperLogLogCounter hloglog);
static HyperLogLogCounter sparse_to_dense(HyperLogLogCounter hloglog);

static HyperLogLogCounter hyperloglog_compress_dense(HyperLogLogCounter hloglog);
static HyperLogLogCounter hyperloglog_compress_sparse(HyperLogLogCounter hloglog);
static HyperLogLogCounter hyperloglog_decompress_dense(HyperLogLogCounter hloglog);
static HyperLogLogCounter hyperloglog_decompress_sparse(HyperLogLogCounter hloglog);

static int hyperloglog_get_size_sparse(double ndistinct, float error);

/* function definitions */

/* Allocate HLL estimator that can handle the desired cartinality and precision.
 * 
 * parameters:
 *      ndistinct   - cardinality the estimator should handle
 *      error       - requested error rate (0 - 1, where 0 means 'exact')
 * 
 * returns:
 *      instance of HLL estimator (throws ERROR in case of failure)
 */
HyperLogLogCounter 
hyperloglog_create(double ndistinct, float error) 
{

    float m;
    size_t length;
    HyperLogLogCounter p;
    
    /* target error rate needs to be between 0 and 1 */
    if (error <= 0 || error >= 1)
        elog(ERROR, "invalid error rate requested - only values in (0,1) allowed");

    /* the counter is allocated as part of this memory block  */
    length = hyperloglog_get_size_sparse(ndistinct, error);
    p = (HyperLogLogCounter)palloc0(length);

    /* set the counter struct version */
    p->version = STRUCT_VERSION;

    /* what is the minimum number of bins to achieve the requested error rate?
     *  we'll increase this to the nearest power of two later */
    m = ERROR_CONST / (error * error);

    /* so how many bits do we need to index the bins (round up to nearest power
    *  of two) */
    p->b = (uint8_t)ceil(log2(m));

    /* set the number of bits per bin */
    p->binbits = (uint8_t)ceil(log2(log2(ndistinct)));

    /* set the starting sparse index to 0 since all counters start sparse and
     *  empty */
    p->idx = 0;

    if (p->b < MIN_INDEX_BITS)   /* we want at least 2^4 (=16) bins */
        p->b = MIN_INDEX_BITS;
    else if (p->b > MAX_INDEX_BITS)
        elog(ERROR, "number of index bits exceeds MAX_INDEX_BITS (requested %d)", p->b);

    SET_VARSIZE(p, length);

    return p;

}

/* Performs a simple 'copy' of the counter, i.e. allocates a new counter and copies
 * the state from the supplied one. */
HyperLogLogCounter 
hyperloglog_copy(HyperLogLogCounter counter) 
{
    
    size_t length = VARSIZE(counter);
    HyperLogLogCounter copy = (HyperLogLogCounter)palloc(length);
    
    memcpy(copy, counter, length);
    
    return copy;

}

/* Merges the two estimators. Either modifies the first estimator in place (inplace=true),
 * or creates a new copy and returns that (inplace=false). Modification in place is very
 * handy in aggregates, when we really want to modify the aggregate state in place.
 * 
 * Mering is only possible if the counters share the same parameters (number of bins,
 * bin size, ...). If the counters don't match, this throws an ERROR. */
HyperLogLogCounter 
hyperloglog_merge(HyperLogLogCounter counter1, HyperLogLogCounter counter2, short inplace) 
{

    int i;
    HyperLogLogCounter result;
    uint8_t resultcount, countercount,rho,entry;
    uint32_t * sparse_data, * sparse_data_result,idx;
    
    /* check compatibility first */
    if (counter1->b != counter2->b && -1*counter1->b != counter2->b)
        elog(ERROR, "index size of estimators differs (%d != %d)", counter1->b, counter2->b);
    else if (counter1->binbits != counter2->binbits)
        elog(ERROR, "bin size of estimators differs (%d != %d)", counter1->binbits, counter2->binbits);

    /* shall we create a new estimator, or merge into counter1 */
    if (! inplace)
        result = hyperloglog_copy(counter1);
    else
        result = counter1;

    /* Keep the maximum register value for each bin */
    if (result->idx == -1 && counter2->idx == -1){
        for (i = 0; i < pow(2, result->b); i++){
            HLL_DENSE_GET_REGISTER(resultcount,result->data,i,result->binbits);
            HLL_DENSE_GET_REGISTER(countercount,counter2->data,i,counter2->binbits);
            if (resultcount < countercount) {
                HLL_DENSE_SET_REGISTER(result->data,i,countercount,result->binbits);
            }
        }
    } else if (result->idx == -1) {
        sparse_data = (uint32_t *) counter2->data;
        
        /* First the encoded hash must be converted to idx and rho before it 
         * can be added to the densely encoded result counter */
        for (i=0; i < counter2->idx; i++){
            idx = sparse_data[i];
            
            /* if last bit is 1 then rho is the preceding ~6 bits
             * otherwise rho can be calculated from the leading bits*/
            if (sparse_data[i] & 1) {
                /* grab the binbits before the indicator bit and add that to
                 *  the number of zero bits in p-p' */
                idx = idx >> (32 - result->b);
                rho = ((sparse_data[i] & (int)(pow(2,result->binbits+1) - 2)) >> 1) + (32 - 1 - result->b - result->binbits);
            } else {
                idx = (idx << result->binbits) >> result->binbits;
                idx  = idx >> (32 - (result->binbits+ result->b));
                rho = __builtin_clz(sparse_data[i] << (result->binbits+ result->b)) + 1;
            }
            
            /* keep the highest value */
            HLL_DENSE_GET_REGISTER(entry,result->data,idx,result->binbits);
            if (rho > entry) {
                HLL_DENSE_SET_REGISTER(result->data,idx,rho,result->binbits);
            }
 
         }
    } else if (counter2->idx == -1) {
        /* Toggle the compression flag for result if it is compressed since 
         * sparse_to_dense doesn't require the counter to be resized coming 
         * in but it does copy this value which would then cause the counter 
         * to falsely read as compressed afterwards. This saves an unnecessary
         *  memory allocation in the decompress stage. */

        result = sparse_to_dense(result);
        for (i = 0; i < pow(2, result->b); i++){
            HLL_DENSE_GET_REGISTER(resultcount,result->data,i,result->binbits);
            HLL_DENSE_GET_REGISTER(countercount,counter2->data,i,counter2->binbits);
            if (resultcount < countercount) {
                HLL_DENSE_SET_REGISTER(result->data,i,countercount,result->binbits);
            }
        }
    } else {
        /* decompress if needed */
        
        sparse_data = (uint32_t *) counter2->data;
        sparse_data_result = (uint32_t *) result->data;
        
        /* Add encoded hashes just like in add_hash_sparse with the same dedupe
         *  before promotion logic. */
        for (i=0; i < counter2->idx; i++){
            sparse_data_result[result->idx++] = sparse_data[i];
            
            if (result->idx > size_sparse_array(result->b)) {
                result->idx = dedupe((uint32_t *)result->data,result->idx);
                if (result->idx > size_sparse_array(result->b) * (7/8)) {
                    result = sparse_to_dense(result);
                    result = hyperloglog_merge(result,counter2,1);            
                    return result;
                }
            }
        }

    }

    return result;

}


/* Computes size of the structure, depending on the requested error rate and
 * ndistinct. */
int 
hyperloglog_get_size(double ndistinct, float error) 
{

    int b;
    float m;
    
    if (error <= 0 || error >= 1)
        elog(ERROR, "invalid error rate requested");
    
    m = ERROR_CONST / (error * error);
    b = (int)ceil(log2(m));
    
    if (b < MIN_INDEX_BITS)
        b = MIN_INDEX_BITS;
    else if (b > MAX_INDEX_BITS)
        elog(ERROR, "number of index bits exceeds MAX_INDEX_BITS (requested %d)",b);
    
    /* The size is the sum of the struct overhead and the bytes the used to 
     * store the buckets. Which is the product of the number of buckets and
     *  the (bits per bucket)/ 8 where 8 is the amount of bits per byte.
     *  
     *  size_in_bytes = struct_overhead + num_buckets*(bits_per_bucket/8)
     *
     * */  
    return sizeof(HyperLogLogCounterData) + (int)ceil((pow(2, b) * ceil(log2(log2(ndistinct))) / 8.0));

}

/* Returns the number of bytes to allocate for the initial sparse encoded 
 * counter. Since palloc allocates blocks that are 2^x bytes big if we were
 * to naively chose our sparse size to be a power of 2 and then add in the 
 * approriate struct overhead we would have a significant amount of wasted 
 * space. So its best to allocate a power of 2 and take the struct overhead 
 * out of the maximum number of sparse encoded hashes. */
static int 
hyperloglog_get_size_sparse(double ndistinct, float error) 
{

    int b;
    float m;
    if (error <= 0 || error >= 1)
        elog(ERROR, "invalid error rate requested");
    
    m = ERROR_CONST / (error * error);
    b = (int)ceil(log2(m));
    
    if (b < MIN_INDEX_BITS)
        b = MIN_INDEX_BITS;
    else if (b > MAX_INDEX_BITS)
        elog(ERROR, "number of index bits exceeds MAX_INDEX_BITS (requested %d)",b);
    
    return (int)pow(2,b-2);
}

/* Hyperloglog estimate header function */
double 
hyperloglog_estimate(HyperLogLogCounter hloglog) 
{
    double E = 0;
    
    if (hloglog->idx == -1){
        E = hyperloglog_estimate_dense(hloglog);
    } else {
        E = hyperloglog_estimate_sparse(hloglog);
    }

    return E;
}
/*
 * Computes the HLL estimate, as described in the paper.
 * 
 * In short it does these steps:
 * 
 * 1) sums the data in counters (1/2^m[i])
 * 2) computes the raw estimate E
 * 3) corrects the estimate for low values
 * 
 */
static double 
hyperloglog_estimate_dense(HyperLogLogCounter hloglog) 
{

    double H = 0, E = 0;
    int j, V=0;
    uint8_t entry;
    int m = (int)ceil(pow(2,hloglog->b));

    /* compute the sum for the harmonic mean */
    for (j = 0; j < m; j++){
        HLL_DENSE_GET_REGISTER(entry,hloglog->data,j,hloglog->binbits);
        H += PE[entry];
    }

    /* multiple by constants to turn the mean into an estimate */
    E = alpham[hloglog->b] / H;

    /* correct for hyperloglog's low cardinality bias by either using linear
     *  counting or error estimation */
    if (E <= (5.0 * m)) {
	
	    /* account for hloglog low cardinality bias */    
	    E = E - error_estimate(E,hloglog->b);

        /* search for empty registers for linear counting */
        for (j = 0; j < m; j++){
            HLL_DENSE_GET_REGISTER(entry,hloglog->data,j,hloglog->binbits);
            if (entry == 0){
                V += 1;
            }
        }

        /* Don't use linear counting if there are no empty registers since we 
         * don't to divide by 0 */
        if (V != 0){
            H = m * log(m / (float)V);
        } else {
            H = E;
        }

        /* if the estimated cardinality is below the threshold for a specific 
         * accuracy return the linear counting result otherwise use the error
         * corrected version */
        if (H <= threshold[hloglog->b]) {
            E = H;
        }

    }

    return E;

}

/* estimates the error from hyperloglog's low cardinality bias and by taking
 * the nearest neighbor interpolation of the nearest 6 points */
static double 
error_estimate(double E,int b)
{
    double avg=0;
    int i, idx=-1, max=0;

    /* get the number of interpoloation points for that precision */
    if (b > 5 ) {
        max = MAX_INTERPOLATION_POINTS;
    } else if (b == 5) {
        max = PRECISION_5_MAX_INTERPOLATION_POINTS;
    } else if (b == 4) {
        max = PRECISION_4_MAX_INTERPOLATION_POINTS;
    }

    /* find the index of the first interpolation point greater than the 
     * uncorrected estimate */
    for (i = 0; i < max; i++){
        if (E < rawEstimateData[b-4][i]){
            idx = i;
            break;
        }
    }

    /* take an average of the 6 nearest points working around cases where 
     * idx + 3 > max or idx -2 < 0  */
    if (idx == -1){
        avg = (double)(biasData[b-4][max - 1] + biasData[b-4][max - 2] + biasData[b-4][max -3] + biasData[b-4][max - 4] + biasData[b-4][max - 5] + biasData[b-4][max - 6]) / 6;
    } else if ( idx > 3 && idx < (max - 4)){
        avg = (double)(biasData[b-4][idx + 3] +  biasData[b-4][idx + 2] + biasData[b-4][idx + 1] + biasData[b-4][idx] + biasData[b-4][idx - 1] + biasData[b-4][idx - 2]) / 6;
    } else if ( idx < 4) {
        avg = (double)(biasData[b-4][5] +  biasData[b-4][4] + biasData[b-4][3] + biasData[b-4][2] + biasData[b-4][1] + biasData[b-4][0]) / 6;
    } else if ( idx > (max - 5)){
        avg = (double)(biasData[b-4][max - 1] + biasData[b-4][max - 2] + biasData[b-4][max -3] + biasData[b-4][max - 4] + biasData[b-4][max - 5] + biasData[b-4][max - 6]) / 6;
    }
    return avg;
}

/* Evaluates the stored encoded hashes using linear counting */
static double 
hyperloglog_estimate_sparse(HyperLogLogCounter hloglog)
{
    int i,V,m = pow(2,(32 - 1 - hloglog->binbits));
    uint32_t * sparse_data;

    /* sort the values so we can ignore duplicates */
    hloglog->idx = dedupe((uint32_t *)hloglog->data,hloglog->idx);
    sparse_data = (uint32_t *) hloglog->data;
    
    /* count number of unique values */
    V = 0;
    for (i=0; i < hloglog->idx ; i++){
        if (i == 0){
            V++;
        } else if (sparse_data[i] != sparse_data[i-1]){
            V++;
        }
    }

    /* Instead of counting empty counters like we do in dense estimation with 
     * linear counting we need to count the number of unique values and 
     * subtract that from the total possible unique counters since we are using
     * a sparse representation of this space. */
    return  m * log(m / (double)(m-V));

}

/* Add element header function */
HyperLogLogCounter 
hyperloglog_add_element(HyperLogLogCounter hloglog, const char * element, int elen) 
{

    uint64_t hash;

    /* compute the hash */
    hash = MurmurHash64A(element, elen, HASH_SEED);    

    /* add the hash to the estimator */
    if (hloglog->idx == -1 ){
        hloglog = hyperloglog_add_hash_dense(hloglog, hash);
    } else {
        hloglog = hyperloglog_add_hash_sparse(hloglog, hash);
    }

    return hloglog;
}

/* Add the appropriate values to a dense encoded counter for a given hash */
static HyperLogLogCounter 
hyperloglog_add_hash_dense(HyperLogLogCounter hloglog, uint64_t hash) 
{

    uint64_t idx;
    uint8_t rho,entry,addn;

    /* get idx (keep only the first 'b' bits) */
    idx  = hash >> (HASH_LENGTH - hloglog->b);

    /* rho needs to be independent from 'idx' */
    rho = __builtin_clzll(hash << hloglog->b) + 1;

    /* We only have (64 - hloglog->b) bits leftover after the index bits
     * however the chance that we need more is 2^-(64 - hloglog->b) which
     * is very small. So we only compute more when needed. To do this we
     * rehash the original hash and take the rho of the new hash and add it
     * to the (64 - hloglog->b) bits. We can repeat this for rho up to 255.
     * We can't go any higher since integer values >255 take more than 1 byte
     * which is currently supported nor really necessary due to 2^(2^8) ~ 
     * 1.16E77 a number so large its not feasible to have that many unique 
     * elements. */
    if (rho == HASH_LENGTH){
	    addn = HASH_LENGTH;
	    rho = (HASH_LENGTH - hloglog->b);
	    while (addn == HASH_LENGTH && rho < pow(2,hloglog->binbits)){
		    hash = MurmurHash64A((const char * )&hash, HASH_LENGTH/8, HASH_SEED);
            /* zero length runs should be 1 so counter gets set */
		    addn = __builtin_clzll(hash) + 1;
		    rho += addn;
	    }
    }

    /* keep the highest value */
    HLL_DENSE_GET_REGISTER(entry,hloglog->data,idx,hloglog->binbits);
    if (rho > entry) {
        HLL_DENSE_SET_REGISTER(hloglog->data,idx,rho,hloglog->binbits);
    }
    
    return hloglog;

}

/* Add the encoded hash to the array of sparse values and promote to dense 
 * encoding if size threshold is exceeded */
static HyperLogLogCounter 
hyperloglog_add_hash_sparse(HyperLogLogCounter hloglog, uint64_t hash) 
{
    uint32_t encoded_hash;
    uint32_t * bigdata;

    bigdata = (uint32_t *) hloglog->data;
    encoded_hash = encode_hash(hash,hloglog);

    bigdata[hloglog->idx++] = encoded_hash;

    /* If the size threshold is exceeded first we dedupe the array to guard 
     * against situations where many duplicate elements are input into the 
     * counter which would needlessly promote it to dense encoding sacrificing
     * accuracy at a lower cardinality than desired. However if the dedupe 
     * doesn't provide a reasonable reduction its should still be promoted. */
    if (hloglog->idx > size_sparse_array(hloglog->b)){
        hloglog->idx = dedupe((uint32_t *)hloglog->data,hloglog->idx);
        if (hloglog->idx > size_sparse_array(hloglog->b)*7/8){
            hloglog = sparse_to_dense(hloglog);
        }
    }
    
    return hloglog;
}

/* Encode the 64 bit hash to a 32 bit summary for sparse encoding and later
 *  conversion to dense encoding. The encoding scheme is as follows:
 * 
 * We take an index of 25 bits (as opposed to the default 14) to allow for
 * greater accuracy when sparsely encoded. If the bits 14 - 25 contain a 1 
 * (so rho is less than 11) then we add a 0 to the of the encode as a flag 
 * for this. However if these bits are all 0's then we need to add rho to the
 * end of this so we can sucesfully switch to dense encoding when the time 
 * comes. We indicate this scenario with a trailing 1 bit. It is worth noting we
 * actually add rho - 11 since we can infer that extra 11 from the fact that 
 * bits 14-25 are 0.
 *
 * Scenario 1 (where rho is in bits 14-25):
 * six 0 bits | first 25 bits of hash | 0 = 32 bit encoded number
 *
 * Scenario 2 (where rho is outside of bits 14-25):
 * first 25 bits of hash | rho - 11 (6 bits total) | 1 = 32 bit encoded number
*/
static uint32_t 
encode_hash(uint64_t hash, HyperLogLogCounter hloglog)
{
    uint32_t encode = 0;
    uint64_t idx;
    uint8_t rho,addn;

    /* which stream is this (keep only the first p' bits) */
    idx  = hash >> (HASH_LENGTH - (32 - 1 - hloglog->binbits));

    /* check p-p' bits for significant digit */
    if (idx & MASK[hloglog->b - 4][hloglog->binbits - 4]){
        encode = idx << 1;
    } else {
        encode = idx << hloglog->binbits;
        rho = __builtin_clzll(hash<<(32- 1 - hloglog->binbits)) + 1;
        if (rho == HASH_LENGTH){
            addn = HASH_LENGTH;
            rho = (HASH_LENGTH - (32- 1 - hloglog->binbits));
            while (addn == HASH_LENGTH && rho < pow(2,hloglog->binbits)){
                hash = MurmurHash64A((const char * )&hash, HASH_LENGTH/8, HASH_SEED);
                /*zero length runs should be 1 so counter gets set */
                addn = __builtin_clzll(hash) + 1;
                rho += addn;
            }
        }
        encode = encode + rho;
        encode = (encode << 1);
        encode = encode + 1;
    }

    return encode;
}

/* Promote a counter from sparse encoding to dense encoding */
static HyperLogLogCounter 
sparse_to_dense(HyperLogLogCounter hloglog)
{
    HyperLogLogCounter htemp;
    uint32_t * sparse_data;
    uint32_t idx;
    uint8_t rho,entry;
    int i, m = pow(2,hloglog->b);

    if (hloglog->idx == -1){
        return hloglog;
    }

    /* Sparse encoded counters are smaller than dense so new space needs to be
     *  alloced */
    sparse_data = malloc(hloglog->idx*sizeof(uint32_t));
    memmove(sparse_data,&hloglog->data,hloglog->idx*sizeof(uint32_t));
    htemp = palloc0(sizeof(HyperLogLogCounterData) + (int)ceil((m * hloglog->binbits / 8.0)));
    memcpy(htemp,hloglog,sizeof(HyperLogLogCounterData));
    hloglog = htemp;
    memset(hloglog->data,0,(int)ceil((m * hloglog->binbits / 8.0)));

    for (i=0; i < hloglog->idx; i++){
        idx = sparse_data[i];

        /* if last bit is 1 then rho is the preceding ~6 bits
         * otherwise rho can be calculated from the leading bits*/
        if (sparse_data[i] & 1) {
            /* grab the binbits before the indicator bit and add that to the 
             * number of zero bits in p-p' */
            idx = idx >> (32 - hloglog->b);
            rho = ((sparse_data[i] & (int)(pow(2,hloglog->binbits+1) - 2)) >> 1) + (32 - 1 - hloglog->b - hloglog->binbits);
        } else {
            idx = (idx << hloglog->binbits) >> hloglog->binbits;
            idx  = idx >> (32 - (hloglog->binbits+ hloglog->b));
            rho = __builtin_clz(sparse_data[i] << (hloglog->binbits+ hloglog->b)) + 1;
        }
        
        /* keep the highest value */
        HLL_DENSE_GET_REGISTER(entry,hloglog->data,idx,hloglog->binbits);
        if (rho > entry) {
            HLL_DENSE_SET_REGISTER(hloglog->data,idx,rho,hloglog->binbits);
        }

    }
    
    if (sparse_data){
        free(sparse_data);
    }
    
    SET_VARSIZE(hloglog,sizeof(HyperLogLogCounterData) + (int)ceil((m * hloglog->binbits / 8.0)) );    

    hloglog->idx = -1;

    return hloglog;
}

/* Just reset the counter (set all the counters to 0). We do this by
 * zeroing the data array */
void 
hyperloglog_reset_internal(HyperLogLogCounter hloglog) 
{

    memset(hloglog->data, 0, VARSIZE(hloglog) - sizeof(HyperLogLogCounterData) );

}

/* check the equality by comparing the register values not the cardinalities */
int 
hyperloglog_is_equal(HyperLogLogCounter counter1, HyperLogLogCounter counter2)
{
   
    uint8_t entry1,entry2;
    HyperLogLogCounter counter1copy,counter2copy;
    uint32_t * sparse_data1, *sparse_data2;
    int i, m = (int)ceil(pow(2,counter1->b)); 

    /* check compatibility first */
    if (counter1->b != counter2->b)
        elog(ERROR, "index size (bit length) of estimators differs (%d != %d)", counter1->b, counter2->b);
    else if (counter1->binbits != counter2->binbits)
        elog(ERROR, "bin size of estimators differs (%d != %d)", counter1->binbits, counter2->binbits);

    /* compare registers returning false on any difference */
    if (counter1->idx == -1 && counter2->idx == -1){
        for (i = 0; i < m; i++){
            HLL_DENSE_GET_REGISTER(entry1,counter1->data,i,counter1->binbits);
            HLL_DENSE_GET_REGISTER(entry2,counter2->data,i,counter2->binbits);
            if (entry1 != entry2){
                return 0;
            }
        }
    } else if (counter1->idx == -1) {
        counter2copy = sparse_to_dense(counter2);
        for (i = 0; i < m; i++){
            HLL_DENSE_GET_REGISTER(entry1,counter1->data,i,counter1->binbits);
            HLL_DENSE_GET_REGISTER(entry2,counter2copy->data,i,counter2copy->binbits);
            if (entry1 != entry2){
                return 0;
            }
        }
    } else if (counter2->idx == -1) {
        counter1copy = sparse_to_dense(counter1);
        for (i = 0; i < m; i++){
            HLL_DENSE_GET_REGISTER(entry1,counter1copy->data,i,counter1copy->binbits);
            HLL_DENSE_GET_REGISTER(entry2,counter2->data,i,counter2->binbits);
            if (entry1 != entry2){
                return 0;
            }
        }
    } else {
        counter1copy = hyperloglog_copy(counter1);
        counter2copy = hyperloglog_copy(counter2);
        sparse_data1 = (uint32_t *) counter1copy->data;
        sparse_data2 = (uint32_t *) counter2copy->data;

        counter1copy->idx = dedupe((uint32_t *)counter1copy->data,counter1copy->idx);
        counter2copy->idx = dedupe((uint32_t *)counter2copy->data,counter2copy->idx);
 
        if (counter1copy->idx != counter2copy->idx){
            return 0;
        }

        for (i=0; i < counter1copy->idx ; i++){
            if (sparse_data1[i] != sparse_data2[i]){
                return 0;
            }
        }
    
    }

    return 1;
}

/* Compress header function */
HyperLogLogCounter 
hyperloglog_compress(HyperLogLogCounter hloglog)
{
    /* make sure the data isn't compressed already */
    if (hloglog->b < 0) {
        return hloglog;
    }

    if (hloglog->idx == -1){
        hloglog = hyperloglog_compress_dense(hloglog);
    } else {
        hloglog = hyperloglog_compress_sparse(hloglog);
    }
    
    return hloglog;
}

/* Compresses dense encoded counters using lz compression */
static HyperLogLogCounter 
hyperloglog_compress_dense(HyperLogLogCounter hloglog)
{
    PGLZ_Header * dest;
    char entry,*data;    
    int i, m;

    /* make sure the dest struct has enough space for an unsuccessful 
     * compression and a 4 bytes of overflow since lz might not recognize its
     * over until then preventing segfaults */
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

    /* resize the counter to only encompass the compressed data and the struct
     *  overhead*/
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

/* Current implementation of sparse compression is rather niave. First 
 * duplicates are removed then the counter is resized to store just the current
 * values to minimize disk footprint. There is room here for further 
 * optimatization. */
static HyperLogLogCounter 
hyperloglog_compress_sparse(HyperLogLogCounter hloglog)
{
    uint32_t out;
    uint8_t * encodes;

    /* add 80 (320 bytes) to allow for cases where group-varint doesn't reduce
     * size */
    uint32_t sparse_size = size_sparse_array(hloglog->b) + 80;

    encodes = malloc(sparse_size*sizeof(uint32_t));

    hloglog->idx = dedupe((uint32_t *)hloglog->data,hloglog->idx);

    out = group_encode_sorted((uint32_t *)hloglog->data,hloglog->idx,encodes);

    if (out < hloglog->idx*4){
        memcpy(hloglog->data,encodes,out);
        SET_VARSIZE(hloglog,sizeof(HyperLogLogCounterData) + out);
        hloglog->b = -1 * (hloglog->b);
    } else {
        SET_VARSIZE(hloglog,sizeof(HyperLogLogCounterData) + hloglog->idx*4);
        hloglog->b = ( -1 * (hloglog->b + 18));
    }

    if (encodes){
        free(encodes);
    }

    return hloglog;
}

/* Decompress header function */
HyperLogLogCounter 
hyperloglog_decompress(HyperLogLogCounter hloglog)
{
     /* make sure the data is compressed */
    if (hloglog->b > 0) {
        return hloglog;
    }
    
    if (hloglog->idx == -1){
        hloglog = hyperloglog_decompress_dense(hloglog);
    } else {
        hloglog = hyperloglog_decompress_sparse(hloglog);
    }
    
    return hloglog;
}

/* Decompresses sparse counters */
static HyperLogLogCounter 
hyperloglog_decompress_dense(HyperLogLogCounter hloglog)
{
    char * dest;
    int m,i;
    HyperLogLogCounter htemp;    

    /* reset b to positive value for calcs and to indicate data is decompressed
     *  */
    hloglog->b = -1 * (hloglog->b);

    /* allocate and zero an array large enough to hold all the decompressed 
     * bins */
    m = (int) pow(2,hloglog->b);
    dest = malloc(m);
    memset(dest,0,m);

    /* decompress the data */
    pglz_decompress((PGLZ_Header *)hloglog->data,dest);

    /* copy the struct internals but not the data into a counter with enough 
     * space for the uncompressed data  */
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
static HyperLogLogCounter 
hyperloglog_decompress_sparse(HyperLogLogCounter hloglog)
{
    HyperLogLogCounter htemp;
    size_t length;

    /* reset b to positive value for calcs and to indicate data is decompressed */
    hloglog->b = -1 * (hloglog->b);

    if (hloglog->b > 18){
        hloglog->b = hloglog->b - 18;
        
        length = pow(2,(hloglog->b-2));
        htemp = palloc0(length);
        memcpy(htemp,hloglog,VARSIZE(hloglog));
        hloglog = htemp;

        SET_VARSIZE(hloglog,length);
    } else {
        length = pow(2,(hloglog->b-2));
        htemp = palloc0(length);
        memcpy(htemp,hloglog,sizeof(HyperLogLogCounterData));
        group_decode_sorted((uint8_t *)hloglog->data,hloglog->idx,(uint32_t *) htemp->data);
        hloglog = htemp;

        SET_VARSIZE(hloglog,length);
    }
    
    return hloglog;
}

