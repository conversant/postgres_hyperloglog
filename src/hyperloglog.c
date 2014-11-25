#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <math.h>
#include <string.h>

#include "postgres.h"
#include "libpq/md5.h"

#include "hyperloglog.h"

/* we're using md5, which produces 16B (128-bit) values */
#define HASH_LENGTH 16

/* array macros to save wasted array space */
#define HLL_BITS 6 /* Enough to count up to 63 leading zeroes. */
#define HLL_REGISTER_MAX ((1<<HLL_BITS)-1)

#define HLL_DENSE_GET_REGISTER(target,p,regnum) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*HLL_BITS/8; \
    unsigned long _fb = regnum*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long b0 = _p[_byte]; \
    unsigned long b1 = _p[_byte+1]; \
    target = ((b0 >> _fb) | (b1 << _fb8)) & HLL_REGISTER_MAX; \
} while(0)

/* Set the value of the register at position 'regnum' to 'val'.
 * 'p' is an array of unsigned bytes. */
#define HLL_DENSE_SET_REGISTER(p,regnum,val) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*HLL_BITS/8; \
    unsigned long _fb = regnum*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long _v = val; \
    _p[_byte] &= ~(HLL_REGISTER_MAX << _fb); \
    _p[_byte] |= _v << _fb; \
    _p[_byte+1] &= ~(HLL_REGISTER_MAX >> _fb8); \
    _p[_byte+1] |= _v >> _fb8; \
} while(0)


/* Alpha * m * m constants, for various numbers of 'b'.
 * 
 * According to hyperloglog_create the 'b' values are between 4 and 16,
 * so the array has 16 non-zero items matching indexes 4, 5, ..., 16.
 * This makes it very easy to access the constants.
 */
static float alpham[17] = {0, 0, 0, 0, 172.288 , 713.728, 2904.064,11718.991761634348, 47072.71267120224, 188686.82445861166, 755541.746198293, 3023758.3915552306, 12098218.894406674, 48399248.750978045, 193609743.86875492, 774464475.7234259, 3097908905.9095263};

/* linear counting thresholds */
static int threshold[19] = {0,0,0,0,10,20,40,80,220,400,900,1800,3100,6500,11500,20000,50000,120000,350000};

/* precomputed inverse powers of 2 */
double PE[64] = { 1.,
        0.5,
        0.25,
        0.125,
        0.0625,
        0.03125,
        0.015625,
        0.0078125,
        0.00390625,
        0.001953125,
        0.0009765625,
        0.00048828125,
        0.000244140625,
        0.0001220703125,
        0.00006103515625,
        0.000030517578125,
        0.0000152587890625,
        0.00000762939453125,
        0.000003814697265625,
        0.0000019073486328125,
        0.00000095367431640625,
        0.000000476837158203125,
        0.0000002384185791015625,
        0.00000011920928955078125,
        0.000000059604644775390625,
        0.000000029802322387695312,
        0.000000014901161193847656,
        0.000000007450580596923828,
        0.000000003725290298461914,
        0.000000001862645149230957,
        0.0000000009313225746154785,
        0.00000000046566128730773926,
        0.00000000023283064365386963,
        0.00000000011641532182693481,
        0.00000000005820766091346741,
        0.000000000029103830456733704,
        0.000000000014551915228366852,
        0.000000000007275957614183426,
        0.000000000003637978807091713,
        0.0000000000018189894035458565,
        0.0000000000009094947017729282,
        0.0000000000004547473508864641,
        0.00000000000022737367544323206,
        0.00000000000011368683772161603,
        0.000000000000056843418860808015,
        0.000000000000028421709430404007,
        0.000000000000014210854715202004,
        0.000000000000007105427357601002,
        0.000000000000003552713678800501,
        0.0000000000000017763568394002505,
        0.0000000000000008881784197001252,
        0.0000000000000004440892098500626,
        0.0000000000000002220446049250313,
        0.00000000000000011102230246251565,
        0.00000000000000005551115123125783,
        0.000000000000000027755575615628914,
        0.000000000000000013877787807814457,
        0.000000000000000006938893903907228,
        0.000000000000000003469446951953614,
        0.000000000000000001734723475976807,
        0.0000000000000000008673617379884035,
        0.00000000000000000043368086899420177,
        0.00000000000000000021684043449710089,
        0.00000000000000000010842021724855044,
        0.00000000000000000005421010862427522};

uint8_t hyperloglog_get_max_bit(uint64_t buffer, int bitfrom, int nbits);
double hyperloglog_estimate(HyperLogLogCounter hloglog);

void hyperloglog_add_hash(HyperLogLogCounter hloglog, uint64_t hash);
void hyperloglog_reset_internal(HyperLogLogCounter hloglog);

/* Allocate HLL estimator that can handle the desired cartinality and precision.
 *
 * TODO The ndistinct is not currently used to determine size of the bin (number of
 * bits used to store the counter) - it's always 1B=8bits, for now, to make it
 * easier to work with. See the header file (hyperloglog.h) for discussion of this.
 * 
 * parameters:
 *      ndistinct   - cardinality the estimator should handle
 *      error       - requested error rate (0 - 1, where 0 means 'exact')
 * 
 * returns:
 *      instance of HLL estimator (throws ERROR in case of failure)
 */
HyperLogLogCounter hyperloglog_create(double ndistinct, float error) {

    float m;
    size_t length = hyperloglog_get_size(ndistinct, error);

    /* the bitmap is allocated as part of this memory block (-1 as one bin is already in) */
    HyperLogLogCounter p = (HyperLogLogCounter)palloc(length);

    /* target error rate needs to be between 0 and 1 */
    if (error <= 0 || error >= 1)
        elog(ERROR, "invalid error rate requested - only values in (0,1) allowed");

    /* what is the minimum number of bins to achieve the requested error rate? we'll
     * increase this to the nearest power of two later */
    m = 1.0816 / (error * error);

    /* so how many bits do we need to index the bins (nearest power of two) */
    p->b = (int)ceil(log2(m));

    /* TODO Is there actually a good reason to limit the number precision to 16 bits? We're
    * using MD5, so we have 128 bits available ... It'll require more memory - 16 bits is 65k
    * bins, requiring 65kB of  memory, which indeed is a lot. But why not to allow that if
    * that's what was requested? */

    if (p->b < 4)   /* we want at least 2^4 (=16) bins */
        p->b = 4;
    else if (p->b > 16)
        elog(ERROR, "number of index bits exceeds 16 (requested %d)", p->b);

    memset(p->data, 0, (int)pow(2, p->b));

    /* use 1B for a counter by default */
    p->binbits = 8;

    SET_VARSIZE(p, length);

    return p;

}

/* Performs a simple 'copy' of the counter, i.e. allocates a new counter and copies
 * the state from the supplied one. */
HyperLogLogCounter hyperloglog_copy(HyperLogLogCounter counter) {
    
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
HyperLogLogCounter hyperloglog_merge(HyperLogLogCounter counter1, HyperLogLogCounter counter2, short inplace) {

    int i;
    HyperLogLogCounter result;
    uint8_t resultcount, countercount;

    /* check compatibility first */
    if (counter1->b != counter2->b)
        elog(ERROR, "index size of estimators differs (%d != %d)", counter1->b, counter2->b);
    else if (counter1->binbits != counter2->binbits)
        elog(ERROR, "bin size of estimators differs (%d != %d)", counter1->binbits, counter2->binbits);

    /* shall we create a new estimator, or merge into counter1 */
    if (! inplace)
        result = hyperloglog_copy(counter1);
    else
        result = counter1;

    /* copy the state of the estimator */
    for (i = 0; i < pow(2, result->b); i++){
        HLL_DENSE_GET_REGISTER(resultcount,result->data,i);
        HLL_DENSE_GET_REGISTER(countercount,counter2->data,i);
        if (resultcount < countercount) {
            HLL_DENSE_SET_REGISTER(result->data,i,countercount);
        }
    }
    

    return result;

}


/* Computes size of the structure, depending on the requested error rate.
 * 
 * TODO The ndistinct is not currently used to determine size of the bin.
 */
int hyperloglog_get_size(double ndistinct, float error) {

  int b;
  float m;

  if (error <= 0 || error >= 1)
      elog(ERROR, "invalid error rate requested");

  m = 1.0816 / (error * error);
  b = (int)ceil(log2(m));

  if (b < 4)
      b = 4;
  else if (b > 16)
      elog(ERROR, "number of bits in HyperLogLog exceeds 16");

  /* the size is the sum of the struct overhead and data with it 
   * rounded up to nearest multiple of 4 bytes */  
  return sizeof(HyperLogLogCounterData) + (((int) ceil( pow(2, b) * ceil(log2(log2(ndistinct))) / 32.0) + 3) &  ~0x03);

}

/* searches for the leftmost 1 (aka 'rho' in the algorithm) */
uint8_t hyperloglog_get_max_bit(uint64_t buffer, int bitfrom, int nbits) {
    int i,j=0;
    for (i=bitfrom; i<nbits; i++){
        j++;
        if ( buffer & (1ULL << (nbits - 1-i)) )
            return j;
    }
    return (nbits - bitfrom);

}

/*
 * Computes the HLL estimate, as described in the paper.
 * 
 * In short it does these steps:
 * 
 * 1) sums the data in counters (1/2^m[i])
 * 2) computes the raw estimate E
 * 3) corrects the estimate for low/high values
 * 
 */
double hyperloglog_estimate(HyperLogLogCounter hloglog) {

    double H = 0, E = 0;
    int j;
    uint8_t entry;
    int m = (int)ceil(pow(2,hloglog->b));

    /* compute the sum for the indicator function */
    for (j = 0; j < m; j++){
        HLL_DENSE_GET_REGISTER(entry,hloglog->data,j);
        H += PE[entry];
    }

    /* and finally the estimate itself */
    E = alpham[hloglog->b] / H;

    if (E <= (5.0 * m)) {

        /* search for empty registers for linear counting */
        int V = 0;
        for (j = 0; j < m; j++){
            HLL_DENSE_GET_REGISTER(entry,hloglog->data,j);
            if (entry == 0){
                V += 1;
            }
        }

        /* Don't use linear counting if there are no empty registers */
        if (V != 0){
            H = m * log(m / (float)V);
        } else {
            H = E;
        }

        /* if the estimated cardinality is below the threshold for a specific accuracy
         * return the linear counting result otherwise use the error corrected version */
        if (H <= threshold[hloglog->b]) {
            E = H;
        }

    }

    return E;


}


void hyperloglog_add_element(HyperLogLogCounter hloglog, const char * element, int elen) {

    /* get the hash */
    unsigned char hash[HASH_LENGTH];

    /* compute the hash using the salt */
    pg_md5_binary(element, elen, hash);

    /* add the hash to the estimator */
    hyperloglog_add_hash(hloglog, hash);

}


void hyperloglog_add_hash(HyperLogLogCounter hloglog, uint64_t hash) {

    /* get the hash */
    uint64_t idx;
    uint8_t rho,entry;

    /* which stream is this (keep only the first 'b' bits) */
    memcpy(&idx, &hash, sizeof(uint64_t));
    idx  = idx >> (64 - hloglog->b);

    /* needs to be independent from 'idx' */
    rho = hyperloglog_get_max_bit(hash, hloglog->b, 64); /* 64-bit hash */

    /* keep the highest value */
    HLL_DENSE_GET_REGISTER(entry,hloglog->data,idx);
    if (rho > entry) {
        HLL_DENSE_SET_REGISTER(hloglog->data,idx,rho);
    }
    

}

/* Just reset the counter (set all the counters to 0). */
void hyperloglog_reset_internal(HyperLogLogCounter hloglog) {

    memset(hloglog->data, 0, hloglog->m);

}
