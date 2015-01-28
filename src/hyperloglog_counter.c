/* File contains functions that interact directly with the postgres api */
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/lsyscache.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"

#include "hyperloglog.h"
#include "upgrade.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* PG_GETARG macros for HLLCounter's that does version checking */
#define PG_GETARG_HLL_P(n) pg_check_hll_version((HLLCounter) PG_GETARG_BYTEA_P(n))
#define PG_GETARG_HLL_P_COPY(n) pg_check_hll_version((HLLCounter) PG_GETARG_BYTEA_P_COPY(n))

/* shoot for 2^64 distinct items and 0.8125% error rate by default */
#define DEFAULT_NDISTINCT   1ULL << 63 
#define DEFAULT_ERROR       0.008125

/* Use the PG_FUNCTION_INFO_V! macro to pass functions to postgres */
PG_FUNCTION_INFO_V1(hyperloglog_add_item);
PG_FUNCTION_INFO_V1(hyperloglog_add_item_agg);
PG_FUNCTION_INFO_V1(hyperloglog_add_item_agg_error);
PG_FUNCTION_INFO_V1(hyperloglog_add_item_agg_default);

PG_FUNCTION_INFO_V1(hyperloglog_merge_simple);
PG_FUNCTION_INFO_V1(hyperloglog_merge_agg);
PG_FUNCTION_INFO_V1(hyperloglog_get_estimate);

PG_FUNCTION_INFO_V1(hyperloglog_init_default);
PG_FUNCTION_INFO_V1(hyperloglog_init_error);
PG_FUNCTION_INFO_V1(hyperloglog_init);
PG_FUNCTION_INFO_V1(hyperloglog_size_default);
PG_FUNCTION_INFO_V1(hyperloglog_size_error);
PG_FUNCTION_INFO_V1(hyperloglog_size);
PG_FUNCTION_INFO_V1(hyperloglog_reset);
PG_FUNCTION_INFO_V1(hyperloglog_length);

PG_FUNCTION_INFO_V1(hyperloglog_in);
PG_FUNCTION_INFO_V1(hyperloglog_out);
PG_FUNCTION_INFO_V1(hyperloglog_rect);
PG_FUNCTION_INFO_V1(hyperloglog_send);

PG_FUNCTION_INFO_V1(hyperloglog_comp);
PG_FUNCTION_INFO_V1(hyperloglog_decomp);
PG_FUNCTION_INFO_V1(hyperloglog_update);
PG_FUNCTION_INFO_V1(hyperloglog_info);
PG_FUNCTION_INFO_V1(hyperloglog_info_noargs);


PG_FUNCTION_INFO_V1(hyperloglog_equal);
PG_FUNCTION_INFO_V1(hyperloglog_not_equal);
PG_FUNCTION_INFO_V1(hyperloglog_union);
PG_FUNCTION_INFO_V1(hyperloglog_intersection);
PG_FUNCTION_INFO_V1(hyperloglog_compliment);
PG_FUNCTION_INFO_V1(hyperloglog_symmetric_diff);

/* ------------- function declarations for local functions --------------- */
Datum hyperloglog_add_item(PG_FUNCTION_ARGS);
Datum hyperloglog_add_item_agg(PG_FUNCTION_ARGS);
Datum hyperloglog_add_item_agg_error(PG_FUNCTION_ARGS);
Datum hyperloglog_add_item_agg_default(PG_FUNCTION_ARGS);

Datum hyperloglog_get_estimate(PG_FUNCTION_ARGS);
Datum hyperloglog_merge_simple(PG_FUNCTION_ARGS);
Datum hyperloglog_merge_agg(PG_FUNCTION_ARGS);

Datum hyperloglog_size_default(PG_FUNCTION_ARGS);
Datum hyperloglog_size_error(PG_FUNCTION_ARGS);
Datum hyperloglog_size(PG_FUNCTION_ARGS);
Datum hyperloglog_init_default(PG_FUNCTION_ARGS);
Datum hyperloglog_init_error(PG_FUNCTION_ARGS);
Datum hyperloglog_init(PG_FUNCTION_ARGS);
Datum hyperloglog_reset(PG_FUNCTION_ARGS);
Datum hyperloglog_length(PG_FUNCTION_ARGS);

Datum hyperloglog_in(PG_FUNCTION_ARGS);
Datum hyperloglog_out(PG_FUNCTION_ARGS);
Datum hyperloglog_recv(PG_FUNCTION_ARGS);
Datum hyperloglog_send(PG_FUNCTION_ARGS);

Datum hyperloglog_comp(PG_FUNCTION_ARGS);
Datum hyperloglog_decomp(PG_FUNCTION_ARGS);
Datum hyperloglog_update(PG_FUNCTION_ARGS);
Datum hyperloglog_info(PG_FUNCTION_ARGS);
Datum hyperloglog_info_noargs(PG_FUNCTION_ARGS);

Datum hyperloglog_equal(PG_FUNCTION_ARGS);
Datum hyperloglog_not_equal(PG_FUNCTION_ARGS);
Datum hyperloglog_union(PG_FUNCTION_ARGS);
Datum hyperloglog_intersection(PG_FUNCTION_ARGS);
Datum hyperloglog_compliment(PG_FUNCTION_ARGS);
Datum hyperloglog_symmetric_diff(PG_FUNCTION_ARGS);

static HLLCounter pg_check_hll_version(HLLCounter hloglog);

/* ---------------------- function definitions --------------------------- */
static HLLCounter 
pg_check_hll_version(HLLCounter hloglog)
{
    if (hloglog->version != STRUCT_VERSION){
        elog(ERROR,"ERROR: The stored counter is version %u while the library is version %u. Please change library version or use upgrade function to upgrade the counter",hloglog->version,STRUCT_VERSION);
    }
    return hloglog;
}

Datum
hyperloglog_add_item(PG_FUNCTION_ARGS)
{

    HLLCounter hyperloglog;

    /* requires the estimator to be already created */
    if (PG_ARGISNULL(0))
        elog(ERROR, "hyperloglog counter must not be NULL");

    /* if the element is not NULL, add it to the estimator (i.e. skip NULLs)
     * */
    if (! PG_ARGISNULL(1)) {

        Oid         element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
        Datum       element = PG_GETARG_DATUM(1);
        int16       typlen;
        bool        typbyval;
        char        typalign;

        /* estimator (we know it's not a NULL value) */
        hyperloglog = PG_GETARG_HLL_P(0);

        /* TODO The requests for type info shouldn't be a problem (thanks to
         * lsyscache), but if it turns out to have a noticeable impact it's
         * possible to cache that between the calls (in the estimator).
         *
         * I have noticed no measurable effect from either option. */

        /* get type information for the second parameter (anyelement item) */
        get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

	    /* decompress if needed */
        if(hyperloglog->b < 0){
            hyperloglog = hll_decompress(hyperloglog);
        }	

        /* it this a varlena type, passed by reference or by value ? */
        if (typlen == -1) {
            /* varlena */
            hyperloglog = hll_add_element(hyperloglog, VARDATA(element), VARSIZE(element) - VARHDRSZ);
        } else if (typbyval) {
            /* fixed-length, passed by value */
            hyperloglog = hll_add_element(hyperloglog, (char*)&element, typlen);
        } else {
            /* fixed-length, passed by reference */
            hyperloglog = hll_add_element(hyperloglog, (char*)element, typlen);
        }

    }

    PG_RETURN_VOID();

}

Datum
hyperloglog_add_item_agg(PG_FUNCTION_ARGS)
{

    HLLCounter hyperloglog;
    double ndistinct;
    float errorRate; /* required error rate */

    /* info for anyelement */
    Oid         element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
    Datum       element = PG_GETARG_DATUM(1);
    int16       typlen;
    bool        typbyval;
    char        typalign;

    /* Create a new estimator (with requested error rate and ndistinct) or
     * reuse the existing one.  Return null if both counter and element args
     * are null. This prevents excess empty counter creation */
    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)){
        PG_RETURN_NULL();
    } else if (PG_ARGISNULL(0) && !PG_ARGISNULL(1)) {

        errorRate = PG_GETARG_FLOAT4(2);
	    ndistinct = PG_GETARG_FLOAT8(3);

        /* error rate between 0 and 1 (not 0) */
        if ((errorRate <= 0) || (errorRate > 1))
            elog(ERROR, "error rate has to be between 0 and 1");

        hyperloglog = hll_create(ndistinct, errorRate);

    } else { /* existing estimator */
        hyperloglog = PG_GETARG_HLL_P(0);
    }

    /* add the item to the estimator (skip NULLs) */
    if (! PG_ARGISNULL(1)) {

    	/* TODO The requests for type info shouldn't be a problem (thanks to
		 * lsyscache), but if it turns out to have a noticeable impact it's
		 * possible to cache that between the calls (in the estimator).
		 *
		 * I have noticed no measurable effect from either option. */
        
        /* get type information for the second parameter (anyelement item) */
        get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

	    /* decompress if needed */
        if(hyperloglog->b < 0){
            hyperloglog = hll_decompress(hyperloglog);
        }	

        /* it this a varlena type, passed by reference or by value ? */
        if (typlen == -1) {
            /* varlena */
            hyperloglog = hll_add_element(hyperloglog, VARDATA(element), VARSIZE(element) - VARHDRSZ);
        } else if (typbyval) {
            /* fixed-length, passed by value */
            hyperloglog = hll_add_element(hyperloglog, (char*)&element, typlen);
        } else {
            /* fixed-length, passed by reference */
            hyperloglog = hll_add_element(hyperloglog, (char*)element, typlen);
        }
    }

    /* return the updated bytea */
    PG_RETURN_BYTEA_P(hyperloglog);

}

Datum
hyperloglog_add_item_agg_error(PG_FUNCTION_ARGS)
{

    HLLCounter hyperloglog;
    float errorRate; /* required error rate */

    /* info for anyelement */
    Oid         element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
    Datum       element = PG_GETARG_DATUM(1);
    int16       typlen;
    bool        typbyval;
    char        typalign;

    /* Create a new estimator (with requested error rate) or reuse the
     * existing one. Return null if both counter and element args are null.
     * This prevents excess empty counter creation */
    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)){
        PG_RETURN_NULL();
    } else if (PG_ARGISNULL(0)) {

        errorRate = PG_GETARG_FLOAT4(2);

        /* error rate between 0 and 1 (not 0) */
        if ((errorRate <= 0) || (errorRate > 1))
            elog(ERROR, "error rate has to be between 0 and 1");

        hyperloglog = hll_create(DEFAULT_NDISTINCT, errorRate);

    } else { /* existing estimator */
        hyperloglog = PG_GETARG_HLL_P(0);
    }

    /* add the item to the estimator (skip NULLs) */
    if (! PG_ARGISNULL(1)) {

    	/* TODO The requests for type info shouldn't be a problem (thanks to
		 * lsyscache), but if it turns out to have a noticeable impact it's
		 * possible to cache that between the calls (in the estimator).
		 *
		 * I have noticed no measurable effect from either option. */
        
        /* get type information for the second parameter (anyelement item) */
        get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

	    /* decompress if needed */
        if(hyperloglog->b < 0){
            hyperloglog = hll_decompress(hyperloglog);
        }	

        /* it this a varlena type, passed by reference or by value ? */
        if (typlen == -1) {
            /* varlena */
            hyperloglog = hll_add_element(hyperloglog, VARDATA(element), VARSIZE(element) - VARHDRSZ);
        } else if (typbyval) {
            /* fixed-length, passed by value */
            hyperloglog = hll_add_element(hyperloglog, (char*)&element, typlen);
        } else {
            /* fixed-length, passed by reference */
            hyperloglog = hll_add_element(hyperloglog, (char*)element, typlen);
        }
    }

    /* return the updated bytea */
    PG_RETURN_BYTEA_P(hyperloglog);

}

Datum
hyperloglog_add_item_agg_default(PG_FUNCTION_ARGS)
{

    HLLCounter hyperloglog;

    /* info for anyelement */
    Oid         element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
    Datum       element = PG_GETARG_DATUM(1);
    int16       typlen;
    bool        typbyval;
    char        typalign;
    
    /* Create a new estimator (with default error rate and ndistinct) or reuse
     * the existing one. Return null if both counter and element args are null.
	 * This prevents excess empty counter creation */
    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)){
        PG_RETURN_NULL();
    } else if (PG_ARGISNULL(0)) {
        hyperloglog = hll_create(DEFAULT_NDISTINCT, DEFAULT_ERROR);
    } else {
        hyperloglog = PG_GETARG_HLL_P(0);
    }

    /* add the item to the estimator (skip NULLs) */
    if (! PG_ARGISNULL(1)) {

    	/* TODO The requests for type info shouldn't be a problem (thanks to
		 * lsyscache), but if it turns out to have a noticeable impact it's
		 * possible to cache that between the calls (in the estimator).
		 *
		 * I have noticed no measurable effect from either option. */

        /* get type information for the second parameter (anyelement item) */
        get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

	    /* decompress if needed */
        if(hyperloglog->b < 0){
            hyperloglog = hll_decompress(hyperloglog);
        }	

        /* it this a varlena type, passed by reference or by value ? */
        if (typlen == -1) {
            /* varlena */
            hyperloglog = hll_add_element(hyperloglog, VARDATA(element), VARSIZE(element) - VARHDRSZ);
        } else if (typbyval) {
            /* fixed-length, passed by value */
            hyperloglog = hll_add_element(hyperloglog, (char*)&element, typlen);
        } else {
            /* fixed-length, passed by reference */
            hyperloglog = hll_add_element(hyperloglog, (char*)element, typlen);
        }

    }

    /* return the updated bytea */
    PG_RETURN_BYTEA_P(hyperloglog);

}

Datum
hyperloglog_merge_simple(PG_FUNCTION_ARGS)
{

    HLLCounter counter1 = NULL;
    HLLCounter counter2 = NULL;

    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    } else if (PG_ARGISNULL(0)) {
	    counter2 = PG_GETARG_HLL_P(1);
    } else if (PG_ARGISNULL(1)) {
	    counter1 = PG_GETARG_HLL_P(0);
    } else {
	    counter1 = PG_GETARG_HLL_P_COPY(0);
	    counter2 = PG_GETARG_HLL_P_COPY(1);

	    /* decompress if needed */
        if(counter1->b < 0){
            counter1 = hll_decompress(counter1);
        }
	    if(counter2->b < 0){
            counter2 = hll_decompress(counter2);
        }
        
        counter1 = hll_merge(counter1, counter2, false);
    }

    PG_RETURN_BYTEA_P(counter1);

}

Datum
hyperloglog_merge_agg(PG_FUNCTION_ARGS)
{

    HLLCounter counter1;
    HLLCounter counter2;

    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)){
	    /* if both counters are null return null */
    	PG_RETURN_NULL();

    } else if (PG_ARGISNULL(0)) {
        /* if first counter is null just copy the second estimator into the
         * first one */
        counter1 = PG_GETARG_HLL_P(1);

    } else if (PG_ARGISNULL(1)) {
	    /* if second counter is null just return the the first estimator */
    	counter1 = PG_GETARG_HLL_P(0);

    } else {

        /* ok, we already have the estimator - merge the second one into it */
        counter1 = PG_GETARG_HLL_P(0);
    	counter2 = PG_GETARG_HLL_P(1);

	    /* decompress is handled inside the merge function since its not
	     * always necessary */
        if(counter1->b < 0){
            counter1 = hll_decompress(counter1);
        }
        if(counter2->b < 0){
            counter2 = hll_decompress(counter2);
        }

        /* perform the merge (in place) */
        counter1 = hll_merge(counter1, counter2, true);

    }

    /* return the updated bytea */
    PG_RETURN_BYTEA_P(counter1);


}


Datum
hyperloglog_get_estimate(PG_FUNCTION_ARGS)
{

    double estimate;
    HLLCounter hyperloglog = PG_GETARG_HLL_P_COPY(0);
    
    /* decompress if needed */
    if(hyperloglog->b < 0){
        hyperloglog = hll_decompress(hyperloglog);
    }    

    estimate = hll_estimate(hyperloglog);

    /* return the updated bytea */
    PG_RETURN_FLOAT8(estimate);

}

Datum
hyperloglog_init_default(PG_FUNCTION_ARGS)
{
      HLLCounter hyperloglog;

      hyperloglog = hll_create(DEFAULT_NDISTINCT, DEFAULT_ERROR);

      PG_RETURN_BYTEA_P(hyperloglog);
}

Datum
hyperloglog_init_error(PG_FUNCTION_ARGS)
{
      HLLCounter hyperloglog;

      float errorRate; /* required error rate */

      errorRate = PG_GETARG_FLOAT4(0);

      /* error rate between 0 and 1 (not 0) */
      if ((errorRate <= 0) || (errorRate > 1)) {
          elog(ERROR, "error rate has to be between 0 and 1");
      }

      hyperloglog = hll_create(DEFAULT_NDISTINCT, errorRate);

      PG_RETURN_BYTEA_P(hyperloglog);
}

Datum
hyperloglog_init(PG_FUNCTION_ARGS)
{
      HLLCounter hyperloglog;

      double ndistinct; 
      float errorRate; /* required error rate */

      ndistinct = PG_GETARG_FLOAT8(1);
      errorRate = PG_GETARG_FLOAT4(0);

      /* error rate between 0 and 1 (not 0) */
      if ((errorRate <= 0) || (errorRate > 1)) {
          elog(ERROR, "error rate has to be between 0 and 1");
      }

      hyperloglog = hll_create(ndistinct, errorRate);

      PG_RETURN_BYTEA_P(hyperloglog);
}

Datum
hyperloglog_size_default(PG_FUNCTION_ARGS)
{
      PG_RETURN_INT32(hll_get_size(DEFAULT_NDISTINCT, DEFAULT_ERROR));
}

Datum
hyperloglog_size_error(PG_FUNCTION_ARGS)
{
      float errorRate; /* required error rate */

      errorRate = PG_GETARG_FLOAT4(0);

      /* error rate between 0 and 1 (not 0) */
      if ((errorRate <= 0) || (errorRate > 1)) {
          elog(ERROR, "error rate has to be between 0 and 1");
      }

      PG_RETURN_INT32(hll_get_size(DEFAULT_NDISTINCT, errorRate));
}

Datum
hyperloglog_size(PG_FUNCTION_ARGS)
{
      double ndistinct; 
      float errorRate; /* required error rate */

      ndistinct = PG_GETARG_FLOAT8(1);
      errorRate = PG_GETARG_FLOAT4(0);

      /* error rate between 0 and 1 (not 0) */
      if ((errorRate <= 0) || (errorRate > 1)) {
          elog(ERROR, "error rate has to be between 0 and 1");
      }

      PG_RETURN_INT32(hll_get_size(ndistinct, errorRate));
}

Datum
hyperloglog_length(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(VARSIZE(PG_GETARG_HLL_P(0)));
}

Datum
hyperloglog_reset(PG_FUNCTION_ARGS)
{
	hll_reset_internal((PG_GETARG_HLL_P(0)));
	PG_RETURN_VOID();
}


/*
 *		byteain			- converts from printable representation of byte array
 *
 *		Non-printable characters must be passed as '\nnn' (octal) and are
 *		converted to internal form.  '\' must be passed as '\\'.
 *		ereport(ERROR, ...) if bad form.
 *
 *		BUGS:
 *				The input is scanned twice.
 *				The error checking of input is minimal.
 */
Datum
hyperloglog_in(PG_FUNCTION_ARGS)
{
	Datum dd = DirectFunctionCall1(byteain, PG_GETARG_DATUM(0));
	return dd;
}

/*
 *		byteaout		- converts to printable representation of byte array
 *
 *		In the traditional escaped format, non-printable characters are
 *		printed as '\nnn' (octal) and '\' as '\\'.
 */
Datum
hyperloglog_out(PG_FUNCTION_ARGS)
{
	Datum dd = DirectFunctionCall1(byteaout, PG_GETARG_DATUM(0));
	return dd;
	
}

/*
 *		bytearecv			- converts external binary format to bytea
 */
Datum
hyperloglog_recv(PG_FUNCTION_ARGS)
{
    Datum dd = DirectFunctionCall1(bytearecv, PG_GETARG_DATUM(0));
    return dd;
}

/*
 *		byteasend			- converts bytea to binary format
 *
 * This is a special case: just copy the input...
 */
Datum
hyperloglog_send(PG_FUNCTION_ARGS)
{
    Datum dd = PG_GETARG_DATUM(0);
    bytea* bp = DatumGetByteaP(dd);
    StringInfoData buf;
    pq_begintypsend(&buf);
    pq_sendbytes(&buf, VARDATA(bp), VARSIZE(bp) - VARHDRSZ);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
hyperloglog_comp(PG_FUNCTION_ARGS)
{
    HLLCounter hyperloglog;

    if (PG_ARGISNULL(0) ){
        PG_RETURN_NULL();
    }

    hyperloglog =  PG_GETARG_HLL_P_COPY(0);

    hyperloglog = hll_compress(hyperloglog);
    
    PG_RETURN_BYTEA_P(hyperloglog);
}

Datum
hyperloglog_decomp(PG_FUNCTION_ARGS)
{
    HLLCounter hyperloglog;

    if (PG_ARGISNULL(0) ){
        PG_RETURN_NULL();
    }

    hyperloglog =  PG_GETARG_HLL_P_COPY(0);

    hyperloglog = hll_decompress(hyperloglog);

    PG_RETURN_BYTEA_P(hyperloglog);
}


Datum
hyperloglog_update(PG_FUNCTION_ARGS)
{
    HLLCounter hyperloglog;

    if (PG_ARGISNULL(0) ){
        PG_RETURN_NULL();
    }

    hyperloglog = (HLLCounter)PG_GETARG_BYTEA_P(0);

    if (hyperloglog->b < 0){
        hyperloglog = hll_decompress(hyperloglog);
    }
    
    hyperloglog = hll_upgrade(hyperloglog);

    PG_RETURN_BYTEA_P(hyperloglog);
}


Datum
hyperloglog_info(PG_FUNCTION_ARGS)
{
    HLLCounter hyperloglog;
    char out[500], comp[4], enc[7];

    if (PG_ARGISNULL(0) ){
        PG_RETURN_NULL();
    }

    hyperloglog =  (HLLCounter)PG_GETARG_BYTEA_P(0);
    
    if (hyperloglog->b < 0){
        snprintf(comp,4,"yes");
    } else {
        snprintf(comp,4,"no");
    }

    if (hyperloglog->idx == -1){
        snprintf(enc,7,"dense");
    } else {
        snprintf(enc,7,"sparse");
    }

    snprintf(out,500,"Counter Summary\nstruct version: %d\nsize on disk (bytes): %d\nbits per bin: %d\nindex bits: %d\nnumber of bins: %d\ncompressed?: %s\nencoding: %s\n--------------------------",hyperloglog->version,VARSIZE(hyperloglog),hyperloglog->binbits,abs(hyperloglog->b),(int)pow(2,abs(hyperloglog->b)),comp,enc);

    PG_RETURN_TEXT_P(cstring_to_text(out));
}

Datum
hyperloglog_info_noargs(PG_FUNCTION_ARGS)
{
    char out[500];

    snprintf(out,500,"Current struct version %d\nDefault error rate %f\nDefault ndistinct %llu",STRUCT_VERSION,DEFAULT_ERROR,DEFAULT_NDISTINCT);

    PG_RETURN_TEXT_P(cstring_to_text(out));
}

    
/* set operations */
Datum
hyperloglog_equal(PG_FUNCTION_ARGS)
{

    HLLCounter counter1;
    HLLCounter counter2;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    } else {
	    counter1 = PG_GETARG_HLL_P_COPY(0);
	    counter2 = PG_GETARG_HLL_P_COPY(1);

	    /* decompress if needed */
        if(counter1->b < 0){
            counter1 = hll_decompress(counter1);
        }
	    if(counter2->b < 0){
            counter2 = hll_decompress(counter2);
        }

        PG_RETURN_BOOL(hll_is_equal(counter1, counter2));
    }

}

Datum
hyperloglog_not_equal(PG_FUNCTION_ARGS)
{

    HLLCounter counter1;
    HLLCounter counter2;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    } else {
	    counter1 = PG_GETARG_HLL_P_COPY(0);
	    counter2 = PG_GETARG_HLL_P_COPY(1);

	    /* decompress if needed */
        if(counter1->b < 0){
            counter1 = hll_decompress(counter1);
        }
	    if(counter2->b < 0){
            counter2 = hll_decompress(counter2);
        }

        PG_RETURN_BOOL(!hll_is_equal(counter1, counter2));
    }

}

Datum
hyperloglog_union(PG_FUNCTION_ARGS)
{

    HLLCounter counter1;
    HLLCounter counter2;

    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    } else if (PG_ARGISNULL(0)) {
	    counter2 = PG_GETARG_HLL_P_COPY(1);

	    /* decompress if needed */
        if(counter2->b < 0){
            counter2 = hll_decompress(counter2);
        }
	
        PG_RETURN_FLOAT8(hll_estimate(counter2));
    } else if (PG_ARGISNULL(1)) {
	    counter1 = PG_GETARG_HLL_P_COPY(0);

	    /* decompress if needed */
        if(counter1->b < 0){
            counter1 = hll_decompress(counter1);
        }

        PG_RETURN_FLOAT8(hll_estimate(counter1));
    } else {
	    counter1 = PG_GETARG_HLL_P_COPY(0);
	    counter2 = PG_GETARG_HLL_P_COPY(1);

	    /* decompress if needed */
        if(counter1->b < 0){
            counter1 = hll_decompress(counter1);
        }
	    if(counter2->b < 0){
            counter2 = hll_decompress(counter2);
        }

        PG_RETURN_FLOAT8(hll_estimate(hll_merge(counter1, counter2,false)));
    }

}

Datum
hyperloglog_intersection(PG_FUNCTION_ARGS)
{

    HLLCounter counter1;
    HLLCounter counter2;
    double A, B , AUB;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    } else {
	    counter1 = PG_GETARG_HLL_P_COPY(0);
	    counter2 = PG_GETARG_HLL_P_COPY(1);

	    /* decompress if needed */
        if(counter1->b < 0){
            counter1 = hll_decompress(counter1);
        }
	    if(counter2->b < 0){
            counter2 = hll_decompress(counter2);
        }

        A = hll_estimate(counter1);
        B = hll_estimate(counter2);
        AUB = hll_estimate(hll_merge(counter1, counter2,false));
        PG_RETURN_FLOAT8(A + B - AUB);
    }

}

Datum
hyperloglog_compliment(PG_FUNCTION_ARGS)
{

    HLLCounter counter1;
    HLLCounter counter2;
    double B, AUB;

    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    } else if (PG_ARGISNULL(0)) {
	    counter2 = PG_GETARG_HLL_P_COPY(1);

	    /* decompress if needed */
        if(counter2->b < 0){
            counter2 = hll_decompress(counter2);
        }
	
        PG_RETURN_FLOAT8(hll_estimate(counter2));
    } else if (PG_ARGISNULL(1)) {
	    counter1 = PG_GETARG_HLL_P_COPY(0);

	    /* decompress if needed */
        if(counter1->b < 0){
            counter1 = hll_decompress(counter1);
        }

        PG_RETURN_FLOAT8(hll_estimate(counter1));
    } else {
	    counter1 = PG_GETARG_HLL_P_COPY(0);
	    counter2 = PG_GETARG_HLL_P_COPY(1);

	    /* decompress if needed */
        if(counter1->b < 0){
            counter1 = hll_decompress(counter1);
        }
    	if(counter2->b < 0){
            counter2 = hll_decompress(counter2);
        }

        B = hll_estimate(counter2);
        AUB = hll_estimate(hll_merge(counter1, counter2,false));
        PG_RETURN_FLOAT8(AUB - B);
    }

}

Datum
hyperloglog_symmetric_diff(PG_FUNCTION_ARGS)
{

    HLLCounter counter1;
    HLLCounter counter2;
    double A, B , AUB;

    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    } else if (PG_ARGISNULL(0)) {
	    counter2 = PG_GETARG_HLL_P_COPY(1);

	    /* decompress if needed */
        if(counter2->b < 0){
            counter2 = hll_decompress(counter2);
        }
	
        PG_RETURN_FLOAT8(hll_estimate(counter2));
    } else if (PG_ARGISNULL(1)) {
	    counter1 = PG_GETARG_HLL_P_COPY(0);

	    /* decompress if needed */
        if(counter1->b < 0){
            counter1 = hll_decompress(counter1);
        }

        PG_RETURN_FLOAT8(hll_estimate(counter1));
    } else {
	    counter1 = PG_GETARG_HLL_P_COPY(0);
	    counter2 = PG_GETARG_HLL_P_COPY(1);

	    /* decompress if needed */
        if(counter1->b < 0){
            counter1 = hll_decompress(counter1);
        }
	    if(counter2->b < 0){
            counter2 = hll_decompress(counter2);
        } 

        A = hll_estimate(counter1);
        B = hll_estimate(counter2);
        AUB = hll_estimate(hll_merge(counter1, counter2,false));
        PG_RETURN_FLOAT8(2*AUB - A - B);
    }

}

