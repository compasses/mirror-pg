
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION vec_agg" to load this file. \quit


-- We declare transition functions here. Note that these functions' declarations
-- and their definitions don't actually match. We manually set the arguments to
-- pass to these functions in vectorized_aggregates.c.

CREATE FUNCTION int4_sum_vec(bigint, int)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION int8_sum_vec(numeric, bigint)
RETURNS numeric
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION int4_avg_accum_vec(bigint[], int)
RETURNS bigint[]
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION int8_avg_accum_vec(numeric[], bigint)
RETURNS numeric[]
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION numeric_avg_accum_vec(numeric[], bigint)
RETURNS numeric[]
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION int8inc_vec(bigint)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION int8inc_any_vec(bigint, "any")
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION float4pl_vec(real, real)
RETURNS real
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION float8pl_vec(double precision, double precision)
RETURNS double precision
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION float8_accum_vec(double precision[], double precision)
RETURNS double precision[]
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION float4_accum_vec(double precision[], real)
RETURNS double precision[]
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
