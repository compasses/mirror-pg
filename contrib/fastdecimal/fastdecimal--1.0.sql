
------------------
-- decimal64 --
------------------

CREATE TYPE DECIMAL64;

CREATE FUNCTION decimal64in(cstring, oid, int4)
RETURNS decimal64
AS 'fastdecimal', 'decimal64in'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64out(decimal64)
RETURNS cstring
AS 'fastdecimal', 'decimal64out'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64recv(internal)
RETURNS decimal64
AS 'fastdecimal', 'decimal64recv'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64send(decimal64)
RETURNS bytea
AS 'fastdecimal', 'decimal64send'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64typmodin(_cstring)
RETURNS INT4
AS 'fastdecimal', 'decimal64typmodin'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64typmodout(INT4)
RETURNS cstring
AS 'fastdecimal', 'decimal64typmodout'
LANGUAGE C IMMUTABLE STRICT;


CREATE TYPE decimal64 (
    INPUT          = decimal64in,
    OUTPUT         = decimal64out,
    RECEIVE        = decimal64recv,
    SEND           = decimal64send,
	TYPMOD_IN      = decimal64typmodin,
	TYPMOD_OUT     = decimal64typmodout,
    INTERNALLENGTH = 8,
	ALIGNMENT      = 'double',
    STORAGE        = plain,
    CATEGORY       = 'N',
    PREFERRED      = false,
    COLLATABLE     = false,
	PASSEDBYVALUE -- But not always.. XXX fix that.
);

-- decimal64, NUMERIC
CREATE FUNCTION decimal64eq(decimal64, decimal64)
RETURNS bool
AS 'fastdecimal', 'decimal64eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64ne(decimal64, decimal64)
RETURNS bool
AS 'fastdecimal', 'decimal64ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64lt(decimal64, decimal64)
RETURNS bool
AS 'fastdecimal', 'decimal64lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64le(decimal64, decimal64)
RETURNS bool
AS 'fastdecimal', 'decimal64le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64gt(decimal64, decimal64)
RETURNS bool
AS 'fastdecimal', 'decimal64gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64ge(decimal64, decimal64)
RETURNS bool
AS 'fastdecimal', 'decimal64ge'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64um(decimal64)
RETURNS decimal64
AS 'fastdecimal', 'decimal64um'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64pl(decimal64, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'decimal64pl'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64mi(decimal64, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'decimal64mi'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64mul(decimal64, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'decimal64mul'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64div(decimal64, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'decimal64div'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION abs(decimal64)
RETURNS decimal64
AS 'fastdecimal', 'decimal64abs'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64larger(decimal64, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'decimal64larger'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64smaller(decimal64, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'decimal64smaller'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_cmp(decimal64, decimal64)
RETURNS INT4
AS 'fastdecimal', 'decimal64_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_hash(decimal64)
RETURNS INT4
AS 'fastdecimal', 'decimal64_hash'
LANGUAGE C IMMUTABLE STRICT;

--
-- Operators.
--

-- decimal64 op decimal64
CREATE OPERATOR = (
    LEFTARG    = decimal64,
    RIGHTARG   = decimal64,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = decimal64eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = decimal64,
    RIGHTARG   = decimal64,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = decimal64ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = decimal64,
    RIGHTARG   = decimal64,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = decimal64lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = decimal64,
    RIGHTARG   = decimal64,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = decimal64le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = decimal64,
    RIGHTARG   = decimal64,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = decimal64ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = decimal64,
    RIGHTARG   = decimal64,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = decimal64gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR + (
    LEFTARG    = decimal64,
    RIGHTARG   = decimal64,
    COMMUTATOR = +,
    PROCEDURE  = decimal64pl
);

CREATE OPERATOR - (
    LEFTARG    = decimal64,
    RIGHTARG   = decimal64,
    PROCEDURE  = decimal64mi
);

CREATE OPERATOR - (
    RIGHTARG   = decimal64,
    PROCEDURE  = decimal64um
);

CREATE OPERATOR * (
    LEFTARG    = decimal64,
    RIGHTARG   = decimal64,
    COMMUTATOR = *,
    PROCEDURE  = decimal64mul
);

CREATE OPERATOR / (
    LEFTARG    = decimal64,
    RIGHTARG   = decimal64,
    PROCEDURE  = decimal64div
);

CREATE OPERATOR CLASS decimal64_ops
DEFAULT FOR TYPE decimal64 USING btree AS
    OPERATOR    1   <  (decimal64, decimal64),
    OPERATOR    2   <= (decimal64, decimal64),
    OPERATOR    3   =  (decimal64, decimal64),
    OPERATOR    4   >= (decimal64, decimal64),
    OPERATOR    5   >  (decimal64, decimal64),
    FUNCTION    1   decimal64_cmp(decimal64, decimal64);

CREATE OPERATOR CLASS decimal64_ops
DEFAULT FOR TYPE decimal64 USING hash AS
    OPERATOR    1   =  (decimal64, decimal64),
    FUNCTION    1   decimal64_hash(decimal64);

-- decimal64, NUMERIC
CREATE FUNCTION decimal64_numeric_cmp(decimal64, NUMERIC)
RETURNS INT4
AS 'fastdecimal', 'decimal64_numeric_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_decimal64_cmp(NUMERIC, decimal64)
RETURNS INT4
AS 'fastdecimal', 'numeric_decimal64_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_numeric_eq(decimal64, NUMERIC)
RETURNS bool
AS 'fastdecimal', 'decimal64_numeric_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_numeric_ne(decimal64, NUMERIC)
RETURNS bool
AS 'fastdecimal', 'decimal64_numeric_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_numeric_lt(decimal64, NUMERIC)
RETURNS bool
AS 'fastdecimal', 'decimal64_numeric_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_numeric_le(decimal64, NUMERIC)
RETURNS bool
AS 'fastdecimal', 'decimal64_numeric_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_numeric_gt(decimal64, NUMERIC)
RETURNS bool
AS 'fastdecimal', 'decimal64_numeric_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_numeric_ge(decimal64, NUMERIC)
RETURNS bool
AS 'fastdecimal', 'decimal64_numeric_ge'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR = (
    LEFTARG    = decimal64,
    RIGHTARG   = NUMERIC,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = decimal64_numeric_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = decimal64,
    RIGHTARG   = NUMERIC,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = decimal64_numeric_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = decimal64,
    RIGHTARG   = NUMERIC,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = decimal64_numeric_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = decimal64,
    RIGHTARG   = NUMERIC,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = decimal64_numeric_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = decimal64,
    RIGHTARG   = NUMERIC,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = decimal64_numeric_ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = decimal64,
    RIGHTARG   = NUMERIC,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = decimal64_numeric_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR CLASS decimal64_numeric_ops
FOR TYPE decimal64 USING btree AS
    OPERATOR    1   <  (decimal64, NUMERIC),
    OPERATOR    2   <= (decimal64, NUMERIC),
    OPERATOR    3   =  (decimal64, NUMERIC),
    OPERATOR    4   >= (decimal64, NUMERIC),
    OPERATOR    5   >  (decimal64, NUMERIC),
    FUNCTION    1   decimal64_numeric_cmp(decimal64, NUMERIC);

CREATE OPERATOR CLASS decimal64_numeric_ops
FOR TYPE decimal64 USING hash AS
    OPERATOR    1   =  (decimal64, NUMERIC),
    FUNCTION    1   decimal64_hash(decimal64);

-- NUMERIC, decimal64
CREATE FUNCTION numeric_decimal64_eq(NUMERIC, decimal64)
RETURNS bool
AS 'fastdecimal', 'numeric_decimal64_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_decimal64_ne(NUMERIC, decimal64)
RETURNS bool
AS 'fastdecimal', 'numeric_decimal64_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_decimal64_lt(NUMERIC, decimal64)
RETURNS bool
AS 'fastdecimal', 'numeric_decimal64_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_decimal64_le(NUMERIC, decimal64)
RETURNS bool
AS 'fastdecimal', 'numeric_decimal64_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_decimal64_gt(NUMERIC, decimal64)
RETURNS bool
AS 'fastdecimal', 'numeric_decimal64_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_decimal64_ge(NUMERIC, decimal64)
RETURNS bool
AS 'fastdecimal', 'numeric_decimal64_ge'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR = (
    LEFTARG    = NUMERIC,
    RIGHTARG   = decimal64,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = numeric_decimal64_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = NUMERIC,
    RIGHTARG   = decimal64,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = numeric_decimal64_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = NUMERIC,
    RIGHTARG   = decimal64,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = numeric_decimal64_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = NUMERIC,
    RIGHTARG   = decimal64,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = numeric_decimal64_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = NUMERIC,
    RIGHTARG   = decimal64,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = numeric_decimal64_ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = NUMERIC,
    RIGHTARG   = decimal64,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = numeric_decimal64_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR CLASS numeric_decimal64_ops
FOR TYPE decimal64 USING btree AS
    OPERATOR    1   <  (NUMERIC, decimal64) FOR SEARCH,
    OPERATOR    2   <= (NUMERIC, decimal64) FOR SEARCH,
    OPERATOR    3   =  (NUMERIC, decimal64) FOR SEARCH,
    OPERATOR    4   >= (NUMERIC, decimal64) FOR SEARCH,
    OPERATOR    5   >  (NUMERIC, decimal64) FOR SEARCH,
    FUNCTION    1   numeric_decimal64_cmp(NUMERIC, decimal64);

CREATE OPERATOR CLASS numeric_decimal64_ops
FOR TYPE decimal64 USING hash AS
    OPERATOR    1   =  (NUMERIC, decimal64),
    FUNCTION    1   decimal64_hash(decimal64);

--
-- Cross type operators with int4
--

-- decimal64, INT4
CREATE FUNCTION decimal64_int4_cmp(decimal64, INT4)
RETURNS INT4
AS 'fastdecimal', 'decimal64_int4_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int4_eq(decimal64, INT4)
RETURNS bool
AS 'fastdecimal', 'decimal64_int4_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int4_ne(decimal64, INT4)
RETURNS bool
AS 'fastdecimal', 'decimal64_int4_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int4_lt(decimal64, INT4)
RETURNS bool
AS 'fastdecimal', 'decimal64_int4_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int4_le(decimal64, INT4)
RETURNS bool
AS 'fastdecimal', 'decimal64_int4_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int4_gt(decimal64, INT4)
RETURNS bool
AS 'fastdecimal', 'decimal64_int4_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int4_ge(decimal64, INT4)
RETURNS bool
AS 'fastdecimal', 'decimal64_int4_ge'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64int4pl(decimal64, INT4)
RETURNS decimal64
AS 'fastdecimal', 'decimal64int4pl'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64int4mi(decimal64, INT4)
RETURNS decimal64
AS 'fastdecimal', 'decimal64int4mi'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64int4mul(decimal64, INT4)
RETURNS decimal64
AS 'fastdecimal', 'decimal64int4mul'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64int4div(decimal64, INT4)
RETURNS decimal64
AS 'fastdecimal', 'decimal64int4div'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR = (
    LEFTARG    = decimal64,
    RIGHTARG   = INT4,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = decimal64_int4_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = decimal64,
    RIGHTARG   = INT4,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = decimal64_int4_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = decimal64,
    RIGHTARG   = INT4,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = decimal64_int4_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = decimal64,
    RIGHTARG   = INT4,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = decimal64_int4_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = decimal64,
    RIGHTARG   = INT4,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = decimal64_int4_ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = decimal64,
    RIGHTARG   = INT4,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = decimal64_int4_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR + (
    LEFTARG    = decimal64,
    RIGHTARG   = INT4,
    COMMUTATOR = +,
    PROCEDURE  = decimal64int4pl
);

CREATE OPERATOR - (
    LEFTARG    = decimal64,
    RIGHTARG   = INT4,
    PROCEDURE  = decimal64int4mi
);

CREATE OPERATOR * (
    LEFTARG    = decimal64,
    RIGHTARG   = INT4,
    COMMUTATOR = *,
    PROCEDURE  = decimal64int4mul
);

CREATE OPERATOR / (
    LEFTARG    = decimal64,
    RIGHTARG   = INT4,
    PROCEDURE  = decimal64int4div
);

CREATE OPERATOR CLASS decimal64_int4_ops
FOR TYPE decimal64 USING btree AS
    OPERATOR    1   <  (decimal64, INT4),
    OPERATOR    2   <= (decimal64, INT4),
    OPERATOR    3   =  (decimal64, INT4),
    OPERATOR    4   >= (decimal64, INT4),
    OPERATOR    5   >  (decimal64, INT4),
    FUNCTION    1   decimal64_int4_cmp(decimal64, INT4);

CREATE OPERATOR CLASS decimal64_int4_ops
FOR TYPE decimal64 USING hash AS
    OPERATOR    1   =  (decimal64, INT4),
    FUNCTION    1   decimal64_hash(decimal64);

-- INT4, decimal64
CREATE FUNCTION int4_decimal64_cmp(INT4, decimal64)
RETURNS INT4
AS 'fastdecimal', 'int4_decimal64_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_decimal64_eq(INT4, decimal64)
RETURNS bool
AS 'fastdecimal', 'int4_decimal64_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_decimal64_ne(INT4, decimal64)
RETURNS bool
AS 'fastdecimal', 'int4_decimal64_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_decimal64_lt(INT4, decimal64)
RETURNS bool
AS 'fastdecimal', 'int4_decimal64_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_decimal64_le(INT4, decimal64)
RETURNS bool
AS 'fastdecimal', 'int4_decimal64_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_decimal64_gt(INT4, decimal64)
RETURNS bool
AS 'fastdecimal', 'int4_decimal64_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_decimal64_ge(INT4, decimal64)
RETURNS bool
AS 'fastdecimal', 'int4_decimal64_ge'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4decimal64pl(INT4, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'int4decimal64pl'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4decimal64mi(INT4, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'int4decimal64mi'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4decimal64mul(INT4, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'int4decimal64mul'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4decimal64div(INT4, decimal64)
RETURNS DOUBLE PRECISION
AS 'fastdecimal', 'int4decimal64div'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR = (
    LEFTARG    = INT4,
    RIGHTARG   = decimal64,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = int4_decimal64_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = INT4,
    RIGHTARG   = decimal64,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = int4_decimal64_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = INT4,
    RIGHTARG   = decimal64,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = int4_decimal64_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = INT4,
    RIGHTARG   = decimal64,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = int4_decimal64_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = INT4,
    RIGHTARG   = decimal64,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = int4_decimal64_ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = INT4,
    RIGHTARG   = decimal64,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = int4_decimal64_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR + (
    LEFTARG    = INT4,
    RIGHTARG   = decimal64,
    COMMUTATOR = +,
    PROCEDURE  = int4decimal64pl
);

CREATE OPERATOR - (
    LEFTARG    = INT4,
    RIGHTARG   = decimal64,
    PROCEDURE  = int4decimal64mi
);

CREATE OPERATOR * (
    LEFTARG    = INT4,
    RIGHTARG   = decimal64,
    COMMUTATOR = *,
    PROCEDURE  = int4decimal64mul
);

CREATE OPERATOR / (
    LEFTARG    = INT4,
    RIGHTARG   = decimal64,
    PROCEDURE  = int4decimal64div
);

CREATE OPERATOR CLASS int4_decimal64_ops
FOR TYPE decimal64 USING btree AS
    OPERATOR    1   <  (INT4, decimal64),
    OPERATOR    2   <= (INT4, decimal64),
    OPERATOR    3   =  (INT4, decimal64),
    OPERATOR    4   >= (INT4, decimal64),
    OPERATOR    5   >  (INT4, decimal64),
    FUNCTION    1   int4_decimal64_cmp(INT4, decimal64);

CREATE OPERATOR CLASS int4_decimal64_ops
FOR TYPE decimal64 USING hash AS
    OPERATOR    1   =  (INT4, decimal64),
    FUNCTION    1   decimal64_hash(decimal64);

--
-- Cross type operators with int2
--
-- decimal64, INT2
CREATE FUNCTION decimal64_int2_cmp(decimal64, INT2)
RETURNS INT4
AS 'fastdecimal', 'decimal64_int2_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int2_eq(decimal64, INT2)
RETURNS bool
AS 'fastdecimal', 'decimal64_int2_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int2_ne(decimal64, INT2)
RETURNS bool
AS 'fastdecimal', 'decimal64_int2_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int2_lt(decimal64, INT2)
RETURNS bool
AS 'fastdecimal', 'decimal64_int2_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int2_le(decimal64, INT2)
RETURNS bool
AS 'fastdecimal', 'decimal64_int2_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int2_gt(decimal64, INT2)
RETURNS bool
AS 'fastdecimal', 'decimal64_int2_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_int2_ge(decimal64, INT2)
RETURNS bool
AS 'fastdecimal', 'decimal64_int2_ge'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64int2pl(decimal64, INT2)
RETURNS decimal64
AS 'fastdecimal', 'decimal64int2pl'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64int2mi(decimal64, INT2)
RETURNS decimal64
AS 'fastdecimal', 'decimal64int2mi'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64int2mul(decimal64, INT2)
RETURNS decimal64
AS 'fastdecimal', 'decimal64int2mul'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64int2div(decimal64, INT2)
RETURNS decimal64
AS 'fastdecimal', 'decimal64int2div'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR = (
    LEFTARG    = decimal64,
    RIGHTARG   = INT2,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = decimal64_int2_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = decimal64,
    RIGHTARG   = INT2,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = decimal64_int2_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = decimal64,
    RIGHTARG   = INT2,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = decimal64_int2_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = decimal64,
    RIGHTARG   = INT2,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = decimal64_int2_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = decimal64,
    RIGHTARG   = INT2,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = decimal64_int2_ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = decimal64,
    RIGHTARG   = INT2,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = decimal64_int2_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR + (
    LEFTARG    = decimal64,
    RIGHTARG   = INT2,
    COMMUTATOR = +,
    PROCEDURE  = decimal64int2pl
);

CREATE OPERATOR - (
    LEFTARG    = decimal64,
    RIGHTARG   = INT2,
    PROCEDURE  = decimal64int2mi
);

CREATE OPERATOR * (
    LEFTARG    = decimal64,
    RIGHTARG   = INT2,
    COMMUTATOR = *,
    PROCEDURE  = decimal64int2mul
);

CREATE OPERATOR / (
    LEFTARG    = decimal64,
    RIGHTARG   = INT2,
    PROCEDURE  = decimal64int2div
);

CREATE OPERATOR CLASS decimal64_int2_ops
FOR TYPE decimal64 USING btree AS
    OPERATOR    1   <  (decimal64, INT2),
    OPERATOR    2   <= (decimal64, INT2),
    OPERATOR    3   =  (decimal64, INT2),
    OPERATOR    4   >= (decimal64, INT2),
    OPERATOR    5   >  (decimal64, INT2),
    FUNCTION    1   decimal64_int2_cmp(decimal64, INT2);

CREATE OPERATOR CLASS decimal64_int2_ops
FOR TYPE decimal64 USING hash AS
    OPERATOR    1   =  (decimal64, INT2),
    FUNCTION    1   decimal64_hash(decimal64);

-- INT2, decimal64
CREATE FUNCTION int2_decimal64_cmp(INT2, decimal64)
RETURNS INT4
AS 'fastdecimal', 'int2_decimal64_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_decimal64_eq(INT2, decimal64)
RETURNS bool
AS 'fastdecimal', 'int2_decimal64_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_decimal64_ne(INT2, decimal64)
RETURNS bool
AS 'fastdecimal', 'int2_decimal64_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_decimal64_lt(INT2, decimal64)
RETURNS bool
AS 'fastdecimal', 'int2_decimal64_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_decimal64_le(INT2, decimal64)
RETURNS bool
AS 'fastdecimal', 'int2_decimal64_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_decimal64_gt(INT2, decimal64)
RETURNS bool
AS 'fastdecimal', 'int2_decimal64_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_decimal64_ge(INT2, decimal64)
RETURNS bool
AS 'fastdecimal', 'int2_decimal64_ge'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2decimal64pl(INT2, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'int2decimal64pl'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2decimal64mi(INT2, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'int2decimal64mi'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2decimal64mul(INT2, decimal64)
RETURNS decimal64
AS 'fastdecimal', 'int2decimal64mul'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2decimal64div(INT2, decimal64)
RETURNS DOUBLE PRECISION
AS 'fastdecimal', 'int2decimal64div'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR = (
    LEFTARG    = INT2,
    RIGHTARG   = decimal64,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = int2_decimal64_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = INT2,
    RIGHTARG   = decimal64,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = int2_decimal64_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = INT2,
    RIGHTARG   = decimal64,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = int2_decimal64_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = INT2,
    RIGHTARG   = decimal64,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = int2_decimal64_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = INT2,
    RIGHTARG   = decimal64,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = int2_decimal64_ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = INT2,
    RIGHTARG   = decimal64,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = int2_decimal64_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR + (
    LEFTARG    = INT2,
    RIGHTARG   = decimal64,
    COMMUTATOR = +,
    PROCEDURE  = int2decimal64pl
);

CREATE OPERATOR - (
    LEFTARG    = INT2,
    RIGHTARG   = decimal64,
    PROCEDURE  = int2decimal64mi
);

CREATE OPERATOR * (
    LEFTARG    = INT2,
    RIGHTARG   = decimal64,
    COMMUTATOR = *,
    PROCEDURE  = int2decimal64mul
);

CREATE OPERATOR / (
    LEFTARG    = INT2,
    RIGHTARG   = decimal64,
    PROCEDURE  = int2decimal64div
);

CREATE OPERATOR CLASS int2_decimal64_ops
FOR TYPE decimal64 USING btree AS
    OPERATOR    1   <  (INT2, decimal64),
    OPERATOR    2   <= (INT2, decimal64),
    OPERATOR    3   =  (INT2, decimal64),
    OPERATOR    4   >= (INT2, decimal64),
    OPERATOR    5   >  (INT2, decimal64),
    FUNCTION    1   int2_decimal64_cmp(INT2, decimal64);

CREATE OPERATOR CLASS int2_decimal64_ops
FOR TYPE decimal64 USING hash AS
    OPERATOR    1   =  (INT2, decimal64),
    FUNCTION    1   decimal64_hash(decimal64);

--
-- Casts
--

CREATE FUNCTION decimal64int4scale(decimal64, INT4)
RETURNS decimal64
AS 'fastdecimal', 'decimal64int4scale'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4decimal64(INT4)
RETURNS decimal64
AS 'fastdecimal', 'int4decimal64'
LANGUAGE C IMMUTABLE STRICT;


CREATE FUNCTION decimal64int4(decimal64)
RETURNS INT4
AS 'fastdecimal', 'decimal64int4'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int8decimal64(int8)
RETURNS decimal64
AS '$libdir/fastdecimal', 'int8decimal64' 
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64int8(decimal64) 
RETURNS int8
AS '$libdir/fastdecimal', 'decimal64int8'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2decimal64(INT2)
RETURNS decimal64
AS 'fastdecimal', 'int2decimal64'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64int2(decimal64)
RETURNS INT2
AS 'fastdecimal', 'decimal64int2'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64tod(decimal64)
RETURNS DOUBLE PRECISION
AS 'fastdecimal', 'decimal64tod'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION dtodecimal64(DOUBLE PRECISION)
RETURNS decimal64
AS 'fastdecimal', 'dtodecimal64'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64tof(decimal64)
RETURNS REAL
AS 'fastdecimal', 'decimal64tof'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ftodecimal64(REAL)
RETURNS decimal64
AS 'fastdecimal', 'ftodecimal64'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION decimal64_numeric(decimal64)
RETURNS NUMERIC
AS 'fastdecimal', 'decimal64_numeric'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_decimal64(NUMERIC)
RETURNS decimal64
AS 'fastdecimal', 'numeric_decimal64'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (decimal64 AS decimal64)
	WITH FUNCTION decimal64int4scale (decimal64, INT4) AS ASSIGNMENT;

CREATE CAST (INT4 AS decimal64)
	WITH FUNCTION int4decimal64 (INT4) AS IMPLICIT;

CREATE CAST (decimal64 AS INT4)
	WITH FUNCTION decimal64int4 (decimal64) AS ASSIGNMENT;

CREATE CAST (INT2 AS decimal64)
	WITH FUNCTION int2decimal64 (INT2) AS IMPLICIT;

CREATE CAST (decimal64 AS INT2)
	WITH FUNCTION decimal64int2 (decimal64) AS ASSIGNMENT;

CREATE CAST (decimal64 AS DOUBLE PRECISION)
	WITH FUNCTION decimal64tod (decimal64) AS IMPLICIT;

CREATE CAST (DOUBLE PRECISION AS decimal64)
	WITH FUNCTION dtodecimal64 (DOUBLE PRECISION) AS ASSIGNMENT; -- XXX? or Implicit?

CREATE CAST (decimal64 AS REAL)
	WITH FUNCTION decimal64tof (decimal64) AS IMPLICIT;

CREATE CAST (REAL AS decimal64)
	WITH FUNCTION ftodecimal64 (REAL) AS ASSIGNMENT; -- XXX or Implicit?

CREATE CAST (decimal64 AS NUMERIC)
	WITH FUNCTION decimal64_numeric (decimal64) AS IMPLICIT;

CREATE CAST (NUMERIC AS decimal64)
	WITH FUNCTION numeric_decimal64 (NUMERIC) AS ASSIGNMENT;

CREATE CAST(int8 AS decimal64)
    WITH FUNCTION int8decimal64(int8);

CREATE CAST(decimal64 AS int8)
    WITH FUNCTION decimal64int8(decimal64);

-- Aggregate Support


CREATE FUNCTION decimal64_avg_accum(INTERNAL, decimal64)
RETURNS INTERNAL
AS 'fastdecimal', 'decimal64_avg_accum'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION decimal64_sum(INTERNAL)
RETURNS numeric
AS 'fastdecimal', 'decimal64_sum'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION decimal64_avg(INTERNAL)
RETURNS numeric
AS 'fastdecimal', 'decimal64_avg'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION decimal64aggstatecombine(INTERNAL, INTERNAL)
RETURNS INTERNAL
AS 'fastdecimal', 'decimal64aggstatecombine'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION decimal64aggstateserialize(INTERNAL)
RETURNS BYTEA
AS 'fastdecimal', 'decimal64aggstateserialize'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION decimal64aggstatedeserialize(BYTEA, INTERNAL)
RETURNS INTERNAL
AS 'fastdecimal', 'decimal64aggstatedeserialize'
LANGUAGE C IMMUTABLE;

CREATE AGGREGATE min(decimal64) (
    SFUNC = decimal64smaller,
    STYPE = decimal64,
    SORTOP = <
);

CREATE AGGREGATE max(decimal64) (
    SFUNC = decimal64larger,
    STYPE = decimal64,
    SORTOP = >
);

CREATE AGGREGATE sum(decimal64) (
    SFUNC = decimal64_avg_accum,
	FINALFUNC = decimal64_sum,
    SERIALfUNC = decimal64aggstateserialize,
    DESERIALFUNC = decimal64aggstatedeserialize,
    COMBINEFUNC = decimal64aggstatecombine,
    STYPE = INTERNAL
);

CREATE AGGREGATE avg(decimal64) (
    SFUNC = decimal64_avg_accum,
	FINALFUNC = decimal64_avg,
    SERIALfUNC = decimal64aggstateserialize,
    DESERIALFUNC = decimal64aggstatedeserialize,
    COMBINEFUNC = decimal64aggstatecombine,
    STYPE = INTERNAL
);


