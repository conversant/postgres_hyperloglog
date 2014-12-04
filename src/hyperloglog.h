#include "postgres.h"

/* This is an implementation of HyperLogLog algorithm as described in the
 * paper "HyperLogLog: the analysis of near-optimal cardinality estimation
 * algorithm", published by Flajolet, Fusy, Gandouet and Meunier in 2007.
 * Generally it is an improved version of LogLog algorithm with the last
 * step modified, to combine the parts using harmonic means.
 *
 * Several improvements have been included that are described in "HyperLogLog 
 * in Practice: Algorithmic Engineering of a State of The Art Cardinality 
 * Estimation Algorithm", published by Stefan Heulem, Marc Nunkesse and 
 * Alexander Hall.
 */
typedef struct HyperLogLogCounterData {
    
    /* length of the structure (varlena) */
    char vl_len_[4];
    
    /* Number bits used to index the buckets - this is determined depending
     * on the requested error rate - see hyperloglog_create() for details. */
    uint8_t b; /* bits for bin index */
    
    /* number of bits for a single bucket */
    uint8_t binbits;
    
    /* largest observed 'rho' for each of the 'm' buckets (uses the very same trick
     * as in the varlena type in include/c.h where additional memory is palloc'ed and
     * treated as part of the data array ) */
    char data[1];
    
} HyperLogLogCounterData;

typedef HyperLogLogCounterData * HyperLogLogCounter;

/* creates an optimal bitmap able to count a multiset with the expected
 * cardinality and the given error rate. */
HyperLogLogCounter hyperloglog_create(double ndistinct, float error);

/* Helper function to return the size of a fully populated counter with
 * the given parameters. */
int hyperloglog_get_size(double ndistinct, float error);

/* Compares the bucket values of two counters to test for equality */
int hyperloglog_is_equal(HyperLogLogCounter counter1, HyperLogLogCounter counter2);

/* Returns a copy of the counter */
HyperLogLogCounter hyperloglog_copy(HyperLogLogCounter counter);

/* Merges two counters into one. The final counter can either be a modified counter1 or
 * completely new copy. */
HyperLogLogCounter hyperloglog_merge(HyperLogLogCounter counter1, HyperLogLogCounter counter2, short inplace);

/* add element existence */
void hyperloglog_add_element(HyperLogLogCounter hloglog, const char * element, int elen);

/* get an estimate from the hyperloglog counter */
double hyperloglog_estimate(HyperLogLogCounter hloglog);

/* reset a counter */
void hyperloglog_reset_internal(HyperLogLogCounter hloglog);
