-- HyperLogLog 

-- HyperLogLog counter (shell type)
CREATE TYPE hyperloglog_estimator;

/* input/output functions */
CREATE FUNCTION hyperloglog_in(value cstring) RETURNS hyperloglog_estimator
     AS '$libdir/hyperloglog_counter', 'hyperloglog_in'
     LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hyperloglog_out(counter hyperloglog_estimator) RETURNS cstring
     AS '$libdir/hyperloglog_counter', 'hyperloglog_out'
     LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hyperloglog_recv(internal) RETURNS hyperloglog_estimator
     AS '$libdir/hyperloglog_counter', 'hyperloglog_out'
     LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hyperloglog_send(hyperloglog_estimator) RETURNS bytea
     AS '$libdir/hyperloglog_counter', 'hyperloglog_out'
     LANGUAGE C STRICT IMMUTABLE;

-- actual LogLog counter data type
CREATE TYPE hyperloglog_estimator (
    INPUT = hyperloglog_in,
    OUTPUT = hyperloglog_out,
    STORAGE = EXTENDED,
    ALIGNMENT = int4,
    INTERNALLENGTH = VARIABLE,
    SEND = hyperloglog_send,
    RECEIVE = hyperloglog_recv
);

-- allow cast from bytea to hyperloglog_estimator
CREATE CAST (bytea as hyperloglog_estimator) WITHOUT FUNCTION;

-- get estimator size for the requested number of bitmaps / key size
CREATE FUNCTION hyperloglog_size(error_rate real) RETURNS int
     AS 'MODULE_PATHNAME', 'hyperloglog_size'
     LANGUAGE C IMMUTABLE;

-- creates a new LogLog estimator with a given number of bitmaps / key size
-- an estimator with 32 bitmaps and keysize 3 usually gives reasonable results
CREATE FUNCTION hyperloglog_init(error_rate real) RETURNS hyperloglog_estimator
     AS 'MODULE_PATHNAME', 'hyperloglog_init'
     LANGUAGE C IMMUTABLE;

-- merges the second estimator into the first one
CREATE FUNCTION hyperloglog_merge(estimator1 hyperloglog_estimator, estimator2 hyperloglog_estimator) RETURNS hyperloglog_estimator
     AS 'MODULE_PATHNAME', 'hyperloglog_merge_simple'
     LANGUAGE C IMMUTABLE;

-- merges the second estimator into the first one
CREATE FUNCTION hyperloglog_merge_agg(estimator1 hyperloglog_estimator, estimator2 hyperloglog_estimator) RETURNS hyperloglog_estimator
     AS 'MODULE_PATHNAME', 'hyperloglog_merge_agg'
     LANGUAGE C IMMUTABLE;

-- add an item to the estimator
CREATE FUNCTION hyperloglog_add_item(counter hyperloglog_estimator, item anyelement) RETURNS void
     AS 'MODULE_PATHNAME', 'hyperloglog_add_item'
     LANGUAGE C IMMUTABLE;

-- get current estimate of the distinct values (as a real number)
CREATE FUNCTION hyperloglog_get_estimate(counter hyperloglog_estimator) RETURNS double precision
     AS 'MODULE_PATHNAME', 'hyperloglog_get_estimate'
     LANGUAGE C STRICT IMMUTABLE;

-- reset the estimator (start counting from the beginning)
CREATE FUNCTION hyperloglog_reset(counter hyperloglog_estimator) RETURNS void
     AS 'MODULE_PATHNAME', 'hyperloglog_reset'
     LANGUAGE C STRICT IMMUTABLE;

-- length of the estimator (about the same as hyperloglog_size with existing estimator)
CREATE FUNCTION length(counter hyperloglog_estimator) RETURNS int
     AS 'MODULE_PATHNAME', 'hyperloglog_length'
     LANGUAGE C STRICT IMMUTABLE;

/* functions for aggregate functions */

CREATE FUNCTION hyperloglog_add_item_agg(counter hyperloglog_estimator, item anyelement, error_rate real) RETURNS hyperloglog_estimator
     AS 'MODULE_PATHNAME', 'hyperloglog_add_item_agg'
     LANGUAGE C IMMUTABLE;

CREATE FUNCTION hyperloglog_add_item_agg2(counter hyperloglog_estimator, item anyelement) RETURNS hyperloglog_estimator
     AS 'MODULE_PATHNAME', 'hyperloglog_add_item_agg2'
     LANGUAGE C IMMUTABLE;

-- LogLog based aggregate (item, error rate)
CREATE AGGREGATE hyperloglog_distinct(anyelement, real)
(
    sfunc = hyperloglog_add_item_agg,
    stype = hyperloglog_estimator,
    prefunc = hyperloglog_merge_agg,
    finalfunc = hyperloglog_get_estimate
);

-- LogLog based aggregate (item)
CREATE AGGREGATE hyperloglog_distinct(anyelement)
(
    sfunc = hyperloglog_add_item_agg2,
    stype = hyperloglog_estimator,
    prefunc = hyperloglog_merge_agg,
    finalfunc = hyperloglog_get_estimate
);

-- build the counter(s), but does not perform the final estimation (i.e. can be used to pre-aggregate data)
CREATE AGGREGATE hyperloglog_accum(anyelement, real)
(
    sfunc = hyperloglog_add_item_agg,
    prefunc = hyperloglog_merge_agg,
    stype = hyperloglog_estimator
);

CREATE AGGREGATE hyperloglog_accum(anyelement)
(
    sfunc = hyperloglog_add_item_agg2,
    prefunc = hyperloglog_merge_agg,
    stype = hyperloglog_estimator
);

-- merges all the counters into just a single one (e.g. after running hyperloglog_accum)
CREATE AGGREGATE hyperloglog_merge(hyperloglog_estimator)
(
    sfunc = hyperloglog_merge_agg,
    prefunc = hyperloglog_merge_agg,
    stype = hyperloglog_estimator
);

-- evaluates the estimate (for an estimator)
CREATE OPERATOR # (
    PROCEDURE = hyperloglog_get_estimate,
    RIGHTARG = hyperloglog_estimator
);

-- merges two estimators into a new one
CREATE OPERATOR || (
    PROCEDURE = hyperloglog_merge,
    LEFTARG  = hyperloglog_estimator,
    RIGHTARG = hyperloglog_estimator,
    COMMUTATOR = ||
);
