# cstore_fdw/Makefile
#
# Copyright (c) 2014 Citus Data, Inc.
#

MODULE_big = vec_agg

PG_CPPFLAGS = --std=c99
OBJS = vec_agg.o vec_funcs.o numeric.o

EXTENSION = vec_agg
DATA = vec_agg--1.0.sql

#REGRESS =
#EXTRA_CLEAN =

ifeq ($(enable_coverage),yes)
	PG_CPPFLAGS += --coverage
	SHLIB_LINK  += --coverage
	EXTRA_CLEAN += *.gcno
endif

#
# Users need to specify their Postgres installation path through pg_config. For
# example: /usr/local/pgsql/bin/pg_config or /usr/lib/postgresql/9.3/bin/pg_config
#

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)