BEGIN;
    SET client_min_messages TO WARNING;

    SELECT hyperloglog_size() = 12304 counter_size;

    CREATE TEMP TABLE test_temp AS
    SELECT
        hyperloglog_accum(i) v_counter,
        hyperloglog_accum(i::text) v_counter2,
        hyperloglog_accum(i%100) v_counter3
    FROM
        generate_series(1,100000) s(i);

    SELECT length(v_counter) BETWEEN 8000 AND 10000 v_counter_length from test_temp;

    SELECT length(v_counter2) BETWEEN 8000 AND 10000 v_counter2_length from test_temp;

    SELECT length(v_counter3) BETWEEN 300 AND 420 v_counter3_length from test_temp;

    SELECT hyperloglog_get_estimate(v_counter) BETWEEN 98000 AND 102000 v_counter_estimate from test_temp;

    SELECT hyperloglog_get_estimate(v_counter2) BETWEEN 98000 AND 102000 v_counter2_estimate from test_temp;

    SELECT hyperloglog_get_estimate(v_counter3) BETWEEN 99 AND 101  v_counter3_estimate from test_temp;

    SELECT hyperloglog_distinct(id) BETWEEN 99 AND 101 distinct_estimate_int_sparse FROM generate_series(1,100) s(id);    

    SELECT hyperloglog_distinct(id::text) BETWEEN 99 AND 101 distinct_estimate_text_sparse FROM generate_series(1,100) s(id);

    SELECT hyperloglog_distinct(id) BETWEEN 98000 AND 102000 distinct_estimate_int_dense FROM generate_series(1,100000) s(id);

    SELECT hyperloglog_distinct(id::text) BETWEEN 98000 AND 102000 distinct_estimate_text_dense FROM generate_series(1,100000) s(id);

    SELECT sum(a.counters) BETWEEN 39200 AND 40800 sum FROM (select m.i, hyperloglog_accum(m.j) counters from (select i, random() j from generate_series(1,100) s(i) cross join (select j from  generate_series(1,400) s(j)) j) m group by i) a;

    SELECT hyperloglog_get_estimate(hyperloglog_merge(hyperloglog_accum(i),hyperloglog_accum(i))) BETWEEN 99 AND 101 merge_sparse from generate_series(1,100) s(i);

    SELECT hyperloglog_get_estimate(hyperloglog_merge(hyperloglog_accum(i),hyperloglog_accum(i))) BETWEEN 9800 AND 10200 merge_dense from generate_series(1,10000) s(i);

    SELECT hyperloglog_get_estimate(hyperloglog_merge(hyperloglog_accum(i%5),hyperloglog_accum(i))) BETWEEN 9800 AND 10200 merge_mixed1 from generate_series(1,10000) s(i);

    SELECT hyperloglog_get_estimate(hyperloglog_merge(hyperloglog_accum(i),hyperloglog_accum(i%5))) BETWEEN 9800 AND 10200 merge_mixed2 from generate_series(1,10000) s(i);

    SELECT hyperloglog_equal(hyperloglog_accum(i),hyperloglog_accum(i)) sparse_equality from generate_series(1,100) s(i);

    SELECT hyperloglog_equal(hyperloglog_accum(i),hyperloglog_accum(i)) sparse_equality2 from generate_series(1,1000) s(i);
    
    SELECT hyperloglog_equal(hyperloglog_accum(i),hyperloglog_accum(i)) dense_equality from generate_series(1,10000) s(i);

    SELECT hyperloglog_not_equal(hyperloglog_accum(i),hyperloglog_accum(i*-1)) sparse_inequality from generate_series(1,100) s(i);

    SELECT hyperloglog_not_equal(hyperloglog_accum(i),hyperloglog_accum(i*-1)) sparse_inequality2 from generate_series(1,1000) s(i);

    SELECT hyperloglog_not_equal(hyperloglog_accum(i),hyperloglog_accum(i*-1)) dense_inequality from generate_series(1,10000) s(i);

    SELECT hyperloglog_intersection(hyperloglog_accum(i),hyperloglog_accum(i*-1)) BETWEEN -1 AND 1 no_intersection_sparse from generate_series(1,100) s(i);

    SELECT hyperloglog_intersection(hyperloglog_accum(i),hyperloglog_accum(i*-1)) BETWEEN -2000 AND 2000 no_intersection_dense from generate_series(1,10000) s(i);

    SELECT hyperloglog_intersection(hyperloglog_accum(i),hyperloglog_accum(i*-1)) BETWEEN 19 AND 21 intersection_sparse from generate_series(-10,100) s(i);

    SELECT hyperloglog_intersection(hyperloglog_accum(i),hyperloglog_accum(i*-1)) BETWEEN 1800 AND 2200 intersection_dense from generate_series(-1000,10000) s(i);

    SELECT hyperloglog_intersection(hyperloglog_accum(i),hyperloglog_accum(i%100)) BETWEEN 90 AND 110 intersection_mixed from generate_series(1,10000) s(i);

    SELECT hyperloglog_union(hyperloglog_accum(i),hyperloglog_accum(i)) BETWEEN 99 AND 101 union_sparse from generate_series(1,100) s(i);

    SELECT hyperloglog_union(hyperloglog_accum(i),hyperloglog_accum(i)) BETWEEN 9800 AND 10200 union_dense from generate_series(1,10000) s(i);

    SELECT hyperloglog_union(hyperloglog_accum(i%5),hyperloglog_accum(i)) BETWEEN 9800 AND 10200 union_mixed1 from generate_series(1,10000) s(i);

    SELECT hyperloglog_union(hyperloglog_accum(i),hyperloglog_accum(i%5)) BETWEEN 9800 AND 10200 union_mixed2 from generate_series(1,10000) s(i);

    SELECT hyperloglog_compliment(hyperloglog_accum(i),hyperloglog_accum(i%100)) BETWEEN 9800 AND 10000 compliment from generate_series(1,10000) s(i);

    SELECT hyperloglog_compliment(hyperloglog_accum(i%100),hyperloglog_accum(i)) = 0  compliment from generate_series(1,10000) s(i);

    SELECT hyperloglog_symmetric_diff(hyperloglog_accum(i),hyperloglog_accum(i*-1)) BETWEEN 17000 AND 19000  symmetric_diff from generate_series(-1000,10000) s(i);

    SELECT #(hyperloglog_accum(i)) BETWEEN 99 AND 101 hashtag_operator from generate_series(1,100) s(i);

    SELECT #(hyperloglog_accum(i) || hyperloglog_accum(i)) BETWEEN 99 AND 101 concat_operator from generate_series(1,100) s(i);

    SELECT hyperloglog_accum(i) = hyperloglog_accum(i) equal_operator from generate_series(1,100) s(i);

    SELECT hyperloglog_accum(i) <> hyperloglog_accum(i::text) notequal_operator from generate_series(1,100) s(i);

    SELECT hyperloglog_accum(i) > hyperloglog_accum(i%5) greater_than_operator from generate_series(1,100) s(i);

    SELECT hyperloglog_accum(i%5) < hyperloglog_accum(i) less_than_operator from generate_series(1,100) s(i);

    SELECT hyperloglog_accum(i) >= hyperloglog_accum(i) greater_than_equal_operator_equal from generate_series(1,100) s(i);

    SELECT hyperloglog_accum(i) >= hyperloglog_accum(i%5) greater_than_equal_operator_greater from generate_series(1,100) s(i);

    SELECT hyperloglog_accum(i) <= hyperloglog_accum(i) less_than_operator_equal from generate_series(1,100) s(i);

    SELECT hyperloglog_accum(i%5) <= hyperloglog_accum(i) less_than_equal_operator_less from generate_series(1,100) s(i);

ROLLBACK;
