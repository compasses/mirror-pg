MODULE_big = fastdecimal
OBJS = numerichelper.o fastdecimal.o

EXTENSION = fastdecimal
SQLFILES := fastdecimal--1.0.sql

DATA = fastdecimal--1.0.sql

MODULES = fastdecimal

CFLAGS =`pg_config --includedir-server`

TESTS = $(wildcard test/sql/*.sql)

REGRESS = decimal64test

REGRESS_OPTS = --inputdir=test --outputdir=test --load-extension=fastdecimal

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/fastdecimal
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
