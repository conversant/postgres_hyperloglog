MODULE_big = hyperloglog_counter
OBJS = src/hyperloglog_counter.o src/hyperloglog.o src/upgrade.o src/hllutils.o src/encoding.o

EXTENSION = hyperloglog_counter
DATA = sql/greenplum.sql sql/postgres.sql
MODULES = hyperloglog_counter
 
TEST_VERSION := $(shell psql -tAc "select case when lower(version()) like '%greenplum%' then 'gp' else 'pg' end")
OUT_DIR = test/expected
SQL_DIR = test/sql
PSQL = psql
PSQLOPTS  = -X --echo-all -P null=NULL
PGOPTIONS = --client-min-messages=warning

GLOBAL_BASE_TEST = set_ops operators
ifeq ($(TEST_VERSION),gp)
  BASE_TEST = gp_base $(GLOBAL_BASE_TEST) gp_persistence gp_update gp_aggs gp_compression
else
  BASE_TEST = base $(GLOBAL_BASE_TEST) update aggs compression
endif
 
TEST         = $(foreach test,$(BASE_TEST),$(SQL_DIR)/$(test).out)
TESTS        = $(foreach test,$(BASE_TEST),$(SQL_DIR)/$(test).sql)
REGRESS      = $(patsubst $(SQL_DIR)%,%,$(TESTS))
REGRESS_OPTS = -X --echo-all -P null=NULL
 
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: hyperloglog_counter.so

hyperloglog_counter.so: $(OBJS)

%.o : src/%.c

tests: clean_test $(TEST)
	@find $(SQL_DIR) -maxdepth 1 -name '*.diff' -print >> failures
	@find $(SQL_DIR) -maxdepth 1 -name '*.out' -print >> test_cases
	@if test -s failures; then \
        echo ERROR: `cat failures | wc -l` / `cat test_cases | wc -l` tests failed; \
        echo; \
                rm -f failures test_cases; \
        exit 1; \
    else \
                echo `cat test_cases | wc -l` / `cat test_cases | wc -l` tests passed; \
                rm -f failures test_cases; \
    fi
 
%.out:
	@echo $*
	@if test -f ../testdata/$*.csv; then \
      PGOPTIONS=$(PGOPTIONS) $(PSQL) $(PSQLOPTS) -f $*.sql < ../testdata/$*.csv > $*.out 2>&1; \
    else \
      PGOPTIONS=$(PGOPTIONS) $(PSQL) $(PSQLOPTS) -f $*.sql >> $*.out 2>&1; \
    fi
	@diff -u $*.ref $*.out >> $*.diff || status=1
	@if test -s $*.diff; then \
        echo " .. FAIL"; \
    else \
        echo " .. PASS"; \
        rm -f $*.diff; \
    fi

clean_test:
	rm -f $(SQL_DIR)/*.out $(SQL_DIR)/*.diff failures test_cases
