/*-------------------------------------------------------------------------
 *
 * decimal64.c
 *		  Fixed Decimal numeric type extension
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  decimal64.c
 *
 * The research leading to these results has received funding from the European
 * Union’s Seventh Framework Programme (FP7/2007-2015) under grant agreement
 * n° 318633
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <limits.h>
#include <math.h>

#include "funcapi.h"
#include "access/hash.h"
#include "catalog/pg_type.h"
#include "executor/execHHashagg.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/execnodes.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "fastdecimal.h"


#define MAXINT8LEN		25

#define MAXDECIMAL64_DIGITS 16

/*
 * The store multiplier length of scale for decimal64, should not change this value
 */
#define DECIMAL64SCALE_MULTIPLIER 100LL

#define ENCODE_NEG_SCALE(v, scale) (v = (v * DECIMAL64SCALE_MULTIPLIER - scale))
#define ENCODE_POS_SCALE(v, scale) (v = (v * DECIMAL64SCALE_MULTIPLIER + scale))

#define SAMESIGN(a,b)	(((a) < 0) == ((b) < 0))

#define ENCODE_SCALE(v, scale) \
	do { \
		if (v < 0) \
			(v = (v * DECIMAL64SCALE_MULTIPLIER - scale)); \
		else \
			(v = (v * DECIMAL64SCALE_MULTIPLIER + scale)); \
	} while(0)

#define DECODE_SCALE(v, scale)	\
	do { \
		if (v < 0) \
		{ \
			int128 t = (int128) -v; \
			(scale = (t - (t/DECIMAL64SCALE_MULTIPLIER) * DECIMAL64SCALE_MULTIPLIER)); \
			t /= DECIMAL64SCALE_MULTIPLIER; \
			v = -t;\
		} \
		else \
		{ \
			(scale = (v - (v/DECIMAL64SCALE_MULTIPLIER) * DECIMAL64SCALE_MULTIPLIER)); \
			v /=DECIMAL64SCALE_MULTIPLIER; \
		} \
	} while(0)

static int64 MULTI_ARRAY[MAXDECIMAL64_DIGITS + 1] = {
	1,
	10,
	100,
	1000,
	10000,
	100000,
	1000000,
	10000000,
	100000000,
	1000000000,
	10000000000,
	100000000000,
	1000000000000,
	10000000000000,
	100000000000000,
	1000000000000000,
	10000000000000000,
};

/* Compiler must have a working 128 int type */
typedef __int128 int128;

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(decimal64in);
PG_FUNCTION_INFO_V1(decimal64typmodin);
PG_FUNCTION_INFO_V1(decimal64typmodout);
PG_FUNCTION_INFO_V1(decimal64out);
PG_FUNCTION_INFO_V1(decimal64recv);
PG_FUNCTION_INFO_V1(decimal64send);

PG_FUNCTION_INFO_V1(decimal64eq);
PG_FUNCTION_INFO_V1(decimal64ne);
PG_FUNCTION_INFO_V1(decimal64lt);
PG_FUNCTION_INFO_V1(decimal64gt);
PG_FUNCTION_INFO_V1(decimal64le);
PG_FUNCTION_INFO_V1(decimal64ge);
PG_FUNCTION_INFO_V1(decimal64_cmp);

PG_FUNCTION_INFO_V1(decimal64_int2_eq);
PG_FUNCTION_INFO_V1(decimal64_int2_ne);
PG_FUNCTION_INFO_V1(decimal64_int2_lt);
PG_FUNCTION_INFO_V1(decimal64_int2_gt);
PG_FUNCTION_INFO_V1(decimal64_int2_le);
PG_FUNCTION_INFO_V1(decimal64_int2_ge);
PG_FUNCTION_INFO_V1(decimal64_int2_cmp);

PG_FUNCTION_INFO_V1(int2_decimal64_eq);
PG_FUNCTION_INFO_V1(int2_decimal64_ne);
PG_FUNCTION_INFO_V1(int2_decimal64_lt);
PG_FUNCTION_INFO_V1(int2_decimal64_gt);
PG_FUNCTION_INFO_V1(int2_decimal64_le);
PG_FUNCTION_INFO_V1(int2_decimal64_ge);
PG_FUNCTION_INFO_V1(int2_decimal64_cmp);

PG_FUNCTION_INFO_V1(decimal64_int4_eq);
PG_FUNCTION_INFO_V1(decimal64_int4_ne);
PG_FUNCTION_INFO_V1(decimal64_int4_lt);
PG_FUNCTION_INFO_V1(decimal64_int4_gt);
PG_FUNCTION_INFO_V1(decimal64_int4_le);
PG_FUNCTION_INFO_V1(decimal64_int4_ge);
PG_FUNCTION_INFO_V1(decimal64_int4_cmp);

PG_FUNCTION_INFO_V1(int4_decimal64_eq);
PG_FUNCTION_INFO_V1(int4_decimal64_ne);
PG_FUNCTION_INFO_V1(int4_decimal64_lt);
PG_FUNCTION_INFO_V1(int4_decimal64_gt);
PG_FUNCTION_INFO_V1(int4_decimal64_le);
PG_FUNCTION_INFO_V1(int4_decimal64_ge);
PG_FUNCTION_INFO_V1(int4_decimal64_cmp);

PG_FUNCTION_INFO_V1(decimal64_numeric_cmp);
PG_FUNCTION_INFO_V1(decimal64_numeric_eq);
PG_FUNCTION_INFO_V1(decimal64_numeric_ne);
PG_FUNCTION_INFO_V1(decimal64_numeric_lt);
PG_FUNCTION_INFO_V1(decimal64_numeric_gt);
PG_FUNCTION_INFO_V1(decimal64_numeric_le);
PG_FUNCTION_INFO_V1(decimal64_numeric_ge);
PG_FUNCTION_INFO_V1(numeric_decimal64_cmp);
PG_FUNCTION_INFO_V1(numeric_decimal64_eq);
PG_FUNCTION_INFO_V1(numeric_decimal64_ne);
PG_FUNCTION_INFO_V1(numeric_decimal64_lt);
PG_FUNCTION_INFO_V1(numeric_decimal64_gt);
PG_FUNCTION_INFO_V1(numeric_decimal64_le);
PG_FUNCTION_INFO_V1(numeric_decimal64_ge);
PG_FUNCTION_INFO_V1(decimal64_hash);
PG_FUNCTION_INFO_V1(decimal64um);
PG_FUNCTION_INFO_V1(decimal64up);
PG_FUNCTION_INFO_V1(decimal64pl);
PG_FUNCTION_INFO_V1(decimal64mi);
PG_FUNCTION_INFO_V1(decimal64mul);
PG_FUNCTION_INFO_V1(decimal64div);
PG_FUNCTION_INFO_V1(decimal64abs);
PG_FUNCTION_INFO_V1(decimal64larger);
PG_FUNCTION_INFO_V1(decimal64smaller);
PG_FUNCTION_INFO_V1(decimal64int4pl);
PG_FUNCTION_INFO_V1(decimal64int4mi);
PG_FUNCTION_INFO_V1(decimal64int4mul);
PG_FUNCTION_INFO_V1(decimal64int4div);
PG_FUNCTION_INFO_V1(decimal64int4scale);
PG_FUNCTION_INFO_V1(int4decimal64pl);
PG_FUNCTION_INFO_V1(int4decimal64mi);
PG_FUNCTION_INFO_V1(int4decimal64mul);
PG_FUNCTION_INFO_V1(int4decimal64div);
PG_FUNCTION_INFO_V1(decimal64int2pl);
PG_FUNCTION_INFO_V1(decimal64int2mi);
PG_FUNCTION_INFO_V1(decimal64int2mul);
PG_FUNCTION_INFO_V1(decimal64int2div);
PG_FUNCTION_INFO_V1(int2decimal64pl);
PG_FUNCTION_INFO_V1(int2decimal64mi);
PG_FUNCTION_INFO_V1(int2decimal64mul);
PG_FUNCTION_INFO_V1(int2decimal64div);
PG_FUNCTION_INFO_V1(int4decimal64);
PG_FUNCTION_INFO_V1(decimal64int4);
PG_FUNCTION_INFO_V1(decimal64int8);
PG_FUNCTION_INFO_V1(int8decimal64);
PG_FUNCTION_INFO_V1(int2decimal64);
PG_FUNCTION_INFO_V1(decimal64int2);
PG_FUNCTION_INFO_V1(decimal64tod);
PG_FUNCTION_INFO_V1(dtodecimal64);
PG_FUNCTION_INFO_V1(decimal64tof);
PG_FUNCTION_INFO_V1(ftodecimal64);
PG_FUNCTION_INFO_V1(numeric_decimal64);
PG_FUNCTION_INFO_V1(decimal64_numeric);
PG_FUNCTION_INFO_V1(decimal64_avg_accum);
PG_FUNCTION_INFO_V1(decimal64_avg);
PG_FUNCTION_INFO_V1(decimal64_sum);
PG_FUNCTION_INFO_V1(decimal64aggstatecombine);
PG_FUNCTION_INFO_V1(decimal64aggstateserialize);
PG_FUNCTION_INFO_V1(decimal64aggstatedeserialize);


/*
 * Used for decimal64
 */
typedef int64 decimal64;

typedef struct Decimal64AggState
{
	bool		calcSumX2;		/* if true, calculate sumX2 */
	int64		N;				/* count of processed numbers */
	int128		sumX;			/* sum of processed numbers */
	int128		sumX2;			/* sum of squares of processed numbers */
	int			scale;			/* need scale for decimal64 accum */
} Decimal64AggState;

static char *pg_int64tostr(char *str, int64 value);
static char *pg_int64tostr_zeropad(char *str, int64 value, int64 padding);
static decimal64 scanin_applytypmod(const char *str, int32 typmod);

static Decimal64AggState *makeDecimal64AggState(FunctionCallInfo fcinfo, bool calcSumX2);
static void decimal64_accum(Decimal64AggState *state, int128 newval);
static int decimal64_accumpl(int128 *sum, int sumScale, int128 val, int valScale);

 /*
 * pg_int64tostr
 *		Converts 'value' into a decimal string representation of the number.
 *
 * Caller must ensure that 'str' points to enough memory to hold the result
 * (at least 21 bytes, counting a leading sign and trailing NUL).
 * Return value is a pointer to the new NUL terminated end of string.
 */
static char *
pg_int64tostr(char *str, int64 value)
{
	char *start;
	char *end;

	/*
	 * Handle negative numbers in a special way. We can't just append a '-'
	 * prefix and reverse the sign as on two's complement machines negative
	 * numbers can be 1 further from 0 than positive numbers, we do it this way
	 * so we properly handle the smallest possible value.
	 */
	if (value < 0)
	{
		*str++ = '-';

		/* mark the position we must reverse the string from. */
		start = str;

		/* Compute the result string backwards. */
		do
		{
			int64		remainder;
			int64		oldval = value;

			value /= 10;
			remainder = oldval - value * 10;
			*str++ = '0' + -remainder;
		} while (value != 0);
	}
	else
	{
		/* mark the position we must reverse the string from. */
		start = str;
		do
		{
			int64		remainder;
			int64		oldval = value;

			value /= 10;
			remainder = oldval - value * 10;
			*str++ = '0' + remainder;
		} while (value != 0);
	}

	/* Add trailing NUL byte, and back up 'str' to the last character. */
	end = str;
	*str-- = '\0';

	/* Reverse string. */
	while (start < str)
	{
		char		swap = *start;
		*start++ = *str;
		*str-- = swap;
	}
	return end;
}

/*
 * pg_int64tostr_zeropad
 *		Converts 'value' into a decimal string representation of the number.
 *		'padding' specifies the minimum width of the number. Any extra space
 *		is filled up by prefixing the number with zeros. The return value is a
 *		pointer to the NUL terminated end of the string.
 *
 * Note: Callers should ensure that 'padding' is above zero.
 * Note: This function is optimized for the case where the number is not too
 *		 big to fit inside of the specified padding.
 * Note: Caller must ensure that 'str' points to enough memory to hold the
		 result (at least 21 bytes, counting a leading sign and trailing NUL,
		 or padding + 1 bytes, whichever is larger).
 */
static char *
pg_int64tostr_zeropad(char *str, int64 value, int64 padding)
{
	char	   *start = str;
	char	   *end = &str[padding];
	int64 		num = value;

	Assert(padding > 0);

	/*
	 * Handle negative numbers in a special way. We can't just append a '-'
	 * prefix and reverse the sign as on two's complement machines negative
	 * numbers can be 1 further from 0 than positive numbers, we do it this way
	 * so we properly handle the smallest possible value.
	 */
	if (num < 0)
	{
		*start++ = '-';
		padding--;

		/*
		 * Build the number starting at the end. Here remainder will be a
		 * negative number, we must reverse this sign on this before adding
		 * '0' in order to get the correct ASCII digit
		 */
		while (padding--)
		{
			int64		remainder;
			int64		oldval = num;

			num /= 10;
			remainder = oldval - num * 10;
			start[padding] = '0' + -remainder;
		}
	}
	else
	{
		/* build the number starting at the end */
		while (padding--)
		{
			int64		remainder;
			int64		oldval = num;

			num /= 10;
			remainder = oldval - num * 10;
			start[padding] = '0' + remainder;
		}
	}

	/*
	 * If padding was not high enough to fit this number then num won't have
	 * been divided down to zero. We'd better have another go, this time we
	 * know there won't be any zero padding required so we can just enlist the
	 * help of pg_int64tostr()
	 */
	if (num != 0)
		return pg_int64tostr(str, value);

	*end = '\0';
	return end;
}

/*
 * decimal642str
 *		Prints the decimal64 'val' to buffer as a string.
 *		Returns a pointer to the end of the written string.
 */
static char *
decimal642str(decimal64 val, char *buffer)
{
	char	   *ptr = buffer;
	int 		scale = 0;
	int64		multiplier;

	DECODE_SCALE(val, scale);

	if (scale > MAXDECIMAL64_DIGITS)
		ereport(ERROR,
		(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("DECIMAL64 scale %d cannot be greater than %d",
				scale, MAXDECIMAL64_DIGITS)));
	
	multiplier = MULTI_ARRAY[scale];
	int64		integralpart = val / multiplier;
	int64		fractionalpart = val % multiplier;

	if (val < 0)
	{
		fractionalpart = -fractionalpart;

		/*
		 * Handle special case for negative numbers where the intergral part
		 * is zero. pg_int64tostr() won't prefix with "-0" in this case, so
		 * we'll do it manually
		 */
		if (integralpart == 0)
			*ptr++ = '-';
	}

	ptr = pg_int64tostr(ptr, integralpart);

	if (fractionalpart > 0 || scale > 0)
	{
		*ptr++ = '.';
		ptr = pg_int64tostr_zeropad(ptr, fractionalpart, scale);
	}

	return ptr;
}

/*
 * scandecimal64 --- try to parse a string into a decimal64 and do the type modifier
 */
static decimal64
scanin_applytypmod(const char *str, int32 typmod)
{
	const char *ptr = str;
	decimal64		integralpart = 0;
	int64		fractionalpart = 0;
	bool		negative;
	int			vprecision = 0;
	int64 		multiplier = 1;
	int			vscale = 0;
	int			precisionlimit;
	int			scalelimit;
	int			maxdigits;

	if (typmod < (int32) (VARHDRSZ))
	{
		precisionlimit = scalelimit = MAXDECIMAL64_DIGITS;
		maxdigits = MAXDECIMAL64_DIGITS;
	}
	else
	{
		typmod -= VARHDRSZ;
		precisionlimit = (typmod >> 16) & 0xffff;
		scalelimit = typmod & 0xffff;
		maxdigits = precisionlimit - scalelimit;
	}

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		negative = true;
		ptr++;
	}
	else
	{
		negative = false;
		if (*ptr == '+')
			ptr++;
	}

	/* scan the precision part */
	while (isdigit((unsigned char) *ptr))
	{
		int64 tmp = integralpart * 10 + (*ptr++ - '0');
		vprecision++;
		integralpart = tmp;
	}

	/* modifer scale limit */
	if (integralpart == 0)
	{
		vprecision = 0; /* less than 1 case */
	}

	if (vprecision > maxdigits)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				errmsg("decimal64 field overflow"),
				errdetail("A field with max precision %d, max scale %d must round to an absolute value less than %s%d.",
						precisionlimit, scalelimit,
						/* Display 10^0 as 1 */
						maxdigits ? "10^" : "",
						maxdigits ? maxdigits : 1
						)));

	if (scalelimit == MAXDECIMAL64_DIGITS && maxdigits >= vprecision)
		scalelimit = maxdigits - vprecision;
	
	/* process the part after the decimal point */
	if (*ptr == '.')
	{
		int carry = 0;
		ptr++;

		while (isdigit((unsigned char) *ptr) && scalelimit > 0)
		{
			scalelimit --;
			fractionalpart = fractionalpart * 10 +  (*ptr++ - '0');
			vscale++;
		}

		if (isdigit((unsigned char) *ptr))
		{
			if ((*ptr - '0') >= 5)
				carry = 1;
		}

		fractionalpart += carry;
	}

	/* compact the fractional zero */
	while (true && fractionalpart > 0)
	{
		if (fractionalpart % 10 == 0)
		{
			fractionalpart /= 10;
			vscale--;
		}
		else
		{
			break;
		}
	}

	/* consume any remaining space chars */
	while (isspace((unsigned char) *ptr) || isdigit((unsigned char) *ptr))
		ptr++;

	if (*ptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			   errmsg("value \"%s\" is out of range for type decimal64", str)));

	multiplier = MULTI_ARRAY[vscale];

	if (negative)
	{
		integralpart *= -1;
		integralpart *= multiplier;
		integralpart -= fractionalpart;
		ENCODE_NEG_SCALE(integralpart, vscale);
		return integralpart;
	}
	else
	{
		integralpart *= multiplier;
		integralpart += fractionalpart;
		ENCODE_POS_SCALE(integralpart, vscale);
		return integralpart;
	}
}

/*
 * decimal64in()
 */
Datum
decimal64in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	decimal64	result = scanin_applytypmod(str, typmod);

	PG_RETURN_INT64(result);
}


Datum
decimal64typmodin(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *tl;
	int			n;
	int32		typmod;

	tl = ArrayGetIntegerTypmods(ta, &n);

	if (n == 2)
	{
		if (tl[0] < 1 || tl[0] > MAXDECIMAL64_DIGITS)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("DECIMAL64 precision %d must be between 1 and %d",
							tl[0], MAXDECIMAL64_DIGITS)));
		if (tl[1] < 0 || tl[1] > tl[0])
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("DECIMAL64 scale %d must be between 0 and precision %d",
					tl[1], tl[0])));
		typmod = ((tl[0] << 16) | tl[1]) + VARHDRSZ;
	}
	else if (n == 1)
	{
		if (tl[0] < 1 || tl[0] > MAXDECIMAL64_DIGITS)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("DECIMAL64 precision %d must be between 1 and %d",
							tl[0], MAXDECIMAL64_DIGITS)));
		/* scale defaults to zero */
		typmod = (tl[0] << 16) + VARHDRSZ;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("invalid DECIMAL64 type modifier")));
		typmod = 0;				/* keep compiler quiet */
	}

	PG_RETURN_INT32(typmod);
}

Datum
decimal64typmodout(PG_FUNCTION_ARGS)
{
	int32		typmod = PG_GETARG_INT32(0);
	char	   *res = (char *) palloc(64);

	if (typmod >= 0)
		snprintf(res, 64, "(%d,%d)",
				 ((typmod - VARHDRSZ) >> 16) & 0xffff,
				 (typmod - VARHDRSZ) & 0xffff);
	else
		*res = '\0';

	PG_RETURN_CSTRING(res);
}


/*
 * decimal64out()
 */
Datum
decimal64out(PG_FUNCTION_ARGS)
{
	decimal64	val = PG_GETARG_INT64(0);
	char		buf[MAXINT8LEN + 1];
	char	   *end = decimal642str(val, buf);
	PG_RETURN_CSTRING(pnstrdup(buf, end - buf));
}

/*
 *		decimal64recv			- converts external binary format to int8
 */
Datum
decimal64recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_INT64(pq_getmsgint64(buf));
}

/*
 *		decimal64send			- converts int8 to binary format
 */
Datum
decimal64send(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, arg1);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/*----------------------------------------------------------
 *	Relational operators for decimal64s, including cross-data-type comparisons.
 *---------------------------------------------------------*/

/*
 * Compare two decimal64 values, each represent by int64 
 */
static int
decimal64cmp(decimal64 val1, decimal64 val2)
{
	int scale1;
	int scale2;
	int128 v1, v2;

	if (val1 == val2)
		return 0;
	
	DECODE_SCALE(val1, scale1);
	DECODE_SCALE(val2, scale2);
	v2 = val2;
	v1 = val1;

	if (scale1 > scale2)
	{
		int leftscale = scale1 - scale2;
		v2 *= MULTI_ARRAY[leftscale];
	}
	else if (scale2 > scale1)
	{
		int leftscale = scale2 - scale1;
		v1 *= MULTI_ARRAY[leftscale];
	}
	
	if (v1 < v2)
		return -1;
	else if (v1 > v2)
		return 1;
	else
		return 0;
}

Datum
decimal64eq(PG_FUNCTION_ARGS)
{
	decimal64		val1 = PG_GETARG_INT64(0);
	decimal64		val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(val1 == val2);
}

Datum
decimal64ne(PG_FUNCTION_ARGS)
{
	decimal64		val1 = PG_GETARG_INT64(0);
	decimal64		val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(val1 != val2);
}

Datum
decimal64lt(PG_FUNCTION_ARGS)
{
	int result = decimal64cmp(PG_GETARG_INT64(0), PG_GETARG_INT64(1));

	PG_RETURN_BOOL(result == -1);
}

Datum
decimal64gt(PG_FUNCTION_ARGS)
{
	int result = decimal64cmp(PG_GETARG_INT64(0), PG_GETARG_INT64(1));

	PG_RETURN_BOOL(result == 1);
}

Datum
decimal64le(PG_FUNCTION_ARGS)
{
	int result = decimal64cmp(PG_GETARG_INT64(0), PG_GETARG_INT64(1));

	PG_RETURN_BOOL(result == -1 || result == 0);
}

Datum
decimal64ge(PG_FUNCTION_ARGS)
{
	int result = decimal64cmp(PG_GETARG_INT64(0), PG_GETARG_INT64(1));

	PG_RETURN_BOOL(result == 1 || result == 0);
}

Datum
decimal64_cmp(PG_FUNCTION_ARGS)
{
	return decimal64cmp(PG_GETARG_INT64(0), PG_GETARG_INT64(1));
}

/* int2, decimal64 */
Datum
decimal64_int2_eq(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT16(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == 0);
}

Datum
decimal64_int2_ne(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT16(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) != 0);
}

Datum
decimal64_int2_lt(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT16(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == -1);
}

Datum
decimal64_int2_gt(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT16(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == 1);
}

Datum
decimal64_int2_le(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT16(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) <=0);
}

Datum
decimal64_int2_ge(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT16(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) >= 0);
}

Datum
decimal64_int2_cmp(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT16(1) * DECIMAL64SCALE_MULTIPLIER;

	return decimal64cmp(val1, val2);
}

Datum
int2_decimal64_eq(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT16(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == 0);
}

Datum
int2_decimal64_ne(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT16(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) != 0);
}

Datum
int2_decimal64_lt(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT16(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == -1);
}

Datum
int2_decimal64_gt(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT16(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == 1);
}

Datum
int2_decimal64_le(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT16(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) <= 0);
}

Datum
int2_decimal64_ge(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT16(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) >= 0);
}

Datum
int2_decimal64_cmp(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT16(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	return decimal64cmp(val1, val2);
}

/* decimal64, int4 */
Datum
decimal64_int4_eq(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT32(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == 0);
}

Datum
decimal64_int4_ne(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT32(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) != 0);
}

Datum
decimal64_int4_lt(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT32(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == -1);
}

Datum
decimal64_int4_gt(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT32(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == 1);
}

Datum
decimal64_int4_le(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT32(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) <= 0);
}

Datum
decimal64_int4_ge(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT32(1) * DECIMAL64SCALE_MULTIPLIER;

	PG_RETURN_BOOL(decimal64cmp(val1, val2) >= 0);
}

Datum
decimal64_int4_cmp(PG_FUNCTION_ARGS)
{
	decimal64	val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT32(1) * DECIMAL64SCALE_MULTIPLIER;
	return decimal64cmp(val1, val2);
}

Datum
int4_decimal64_eq(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT32(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == 0);
}

Datum
int4_decimal64_ne(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT32(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) != 0);
}

Datum
int4_decimal64_lt(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT32(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == -1);
}

Datum
int4_decimal64_gt(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT32(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) == 1);
}

Datum
int4_decimal64_le(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT32(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) <= 0);
}

Datum
int4_decimal64_ge(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT32(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(decimal64cmp(val1, val2) >= 0);
}

Datum
int4_decimal64_cmp(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT32(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	val2 = PG_GETARG_INT64(1);

	return decimal64cmp(val1, val2);
}

Datum
decimal64_numeric_cmp(PG_FUNCTION_ARGS)
{
	decimal64	arg1 = PG_GETARG_INT64(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	Datum		val1;

	val1 = DirectFunctionCall1(decimal64_numeric, Int64GetDatum(arg1));

	PG_RETURN_INT32(DirectFunctionCall2(numeric_cmp, val1, val2));
}

Datum
decimal64_numeric_eq(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(decimal64_numeric_cmp, val1,
											   val2));

	PG_RETURN_BOOL(result == 0);
}

Datum
decimal64_numeric_ne(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(decimal64_numeric_cmp, val1,
											   val2));
	PG_RETURN_BOOL(result != 0);
}

Datum
decimal64_numeric_lt(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(decimal64_numeric_cmp, val1,
											   val2));

	PG_RETURN_BOOL(result < 0);
}

Datum
decimal64_numeric_gt(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(decimal64_numeric_cmp, val1,
											   val2));

	PG_RETURN_BOOL(result > 0);
}

Datum
decimal64_numeric_le(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(decimal64_numeric_cmp, val1,
											   val2));

	PG_RETURN_BOOL(result <= 0);
}

Datum
decimal64_numeric_ge(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(decimal64_numeric_cmp, val1,
											   val2));

	PG_RETURN_BOOL(result >= 0);
}

Datum
numeric_decimal64_cmp(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	decimal64	arg2 = PG_GETARG_INT64(1);
	Datum		val2;

	val2 = DirectFunctionCall1(decimal64_numeric, Int64GetDatum(arg2));

	PG_RETURN_INT32(DirectFunctionCall2(numeric_cmp, val1, val2));
}

Datum
numeric_decimal64_eq(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(numeric_decimal64_cmp, val1,
											   val2));

	PG_RETURN_BOOL(result == 0);
}

Datum
numeric_decimal64_ne(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(numeric_decimal64_cmp, val1,
											   val2));

	PG_RETURN_BOOL(result != 0);
}

Datum
numeric_decimal64_lt(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(numeric_decimal64_cmp, val1,
											   val2));

	PG_RETURN_BOOL(result < 0);
}

Datum
numeric_decimal64_gt(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(numeric_decimal64_cmp, val1,
											   val2));

	PG_RETURN_BOOL(result > 0);
}

Datum
numeric_decimal64_le(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(numeric_decimal64_cmp, val1,
											   val2));

	PG_RETURN_BOOL(result <= 0);
}

Datum
numeric_decimal64_ge(PG_FUNCTION_ARGS)
{
	Datum		val1 = PG_GETARG_DATUM(0);
	Datum		val2 = PG_GETARG_DATUM(1);
	int32		result;

	result = DatumGetInt32(DirectFunctionCall2(numeric_decimal64_cmp, val1,
											   val2));

	PG_RETURN_BOOL(result >= 0);
}

Datum
decimal64_hash(PG_FUNCTION_ARGS)
{
	int64		val = PG_GETARG_INT64(0);
	Datum		result;

	result = hash_any((unsigned char *) &val, sizeof(int64));
	PG_RETURN_DATUM(result);
}

/*----------------------------------------------------------
 *	Arithmetic operators on decimal64.
 *---------------------------------------------------------*/

Datum
decimal64um(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT64(0);
	int64		result;

	result = -arg;
	/* overflow check (needed for INT64_MIN) */
	if (arg != 0 && SAMESIGN(result, arg))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64(result);
}

Datum
decimal64up(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT64(0);

	PG_RETURN_INT64(arg);
}

/* implment plus for decimal64, also be used for subtraction */
static int128
decimal64plus(decimal64 v1, decimal64 v2)
{
	int scale1, scale2, scale;
	int128 result;

	DECODE_SCALE(v1, scale1);
	DECODE_SCALE(v2, scale2);
	scale = scale1;

	if (scale1 > scale2)
	{
		int left = scale1 - scale2;
		int64 multi = MULTI_ARRAY[left];
		v2 *= multi;
		scale = scale1;
	}
	else if (scale2 > scale1)
	{
		int left = scale2 - scale1;
		int64 multi = MULTI_ARRAY[left];
		v1 *= multi;
		scale = scale2;
	}

	result = (int128) v1 + v2;
	ENCODE_SCALE(result, scale);
	return result;
}

static int
decimal64_accumpl(int128 *sum, int sumScale, int128 val, int valScale)
{
	if (sumScale > valScale)
	{
		*sum += val * MULTI_ARRAY[sumScale - valScale];
		return sumScale;
	}
	else if (valScale > sumScale)
	{
		int128 result = *sum * MULTI_ARRAY[valScale - sumScale];
		*sum = result + val;
		return valScale;
	}
	else
	{
		*sum += val;
		return valScale;
	}
}

Datum
decimal64pl(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		arg2 = PG_GETARG_INT64(1);

	int128 result = decimal64plus(arg1, arg2);

	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64(result);
}

Datum
decimal64mi(PG_FUNCTION_ARGS)
{
	decimal64		arg1 = PG_GETARG_INT64(0);
	decimal64		arg2 = PG_GETARG_INT64(1);
	
	int128 result = decimal64plus(arg1, -arg2);

	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64(result);
}

static int
getdigitsnumber(int128 val)
{
	int result = 0;
	while (val > 0)
	{
		val /= 10;
		result ++;
	}

	return result;
}

/* return valid decimal64 value, use the scale specific, may do rounding for the val */
static decimal64
int128todecimal64(int128 val, int scale)
{
	bool isneg = false;
	
	if (val < 0)
	{
		isneg = true;
		val = -val;
	}

	if (scale == 0)
	{
		ENCODE_SCALE(val, 0);

		if ((int64) val != val)
			ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				errmsg("decimal64 out of range")));
		
		return (int64) val;
	}
	else
	{
		int precision;
		int leftscale;
		precision = getdigitsnumber(val) - scale;

		if (precision > MAXDECIMAL64_DIGITS)
		{
			ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				errmsg("decimal64 out of range")));
		}
		
		/* in order rounding, need check next digit */
		leftscale = precision + scale - MAXDECIMAL64_DIGITS - 1;
		/* truncate the val */
		if (leftscale > 0)
		{
			int128 new = val / MULTI_ARRAY[leftscale];
			int newscale = scale - (leftscale + 1);

			if (new % 10 >= 5)
				new = new / 10 + (isneg ? -1 : 1);
			else
				new /= 10;
			
			val = new;
			ENCODE_SCALE(val, newscale);
		}
		else
			ENCODE_SCALE(val, scale);
	}

	if (isneg)
		val = -val;

	if ((int64) val != val)
		ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			errmsg("decimal64 out of range")));
	
	return val;
}

static decimal64
decimal64multi(decimal64 v1, decimal64 v2)
{
	int scale1, scale2, scale;
	int128 result;

	DECODE_SCALE(v1, scale1);
	DECODE_SCALE(v2, scale2);
	result = (int128) v1 * v2;
	scale = scale1 + scale2;
	return int128todecimal64(result, scale);
}

Datum
decimal64mul(PG_FUNCTION_ARGS)
{
	decimal64		arg1 = PG_GETARG_INT64(0);
	decimal64		arg2 = PG_GETARG_INT64(1);

	PG_RETURN_INT64(decimal64multi(arg1, arg2));
}

static decimal64
decimal64divid(decimal64 dividend, decimal64 divisor)
{
	int scale1, scale2, scale;
	int128 arg1, arg2, result;

	DECODE_SCALE(dividend, scale1);
	DECODE_SCALE(divisor, scale2);
	Assert(divisor != 0);

	arg1 = dividend;
	arg2 = divisor;

	if (scale1 > scale2)
	{
		int left = scale1 - scale2;
		int64 multi = MULTI_ARRAY[left];
		arg2 = (int128) divisor * multi;
	}
	else if (scale2 > scale1)
	{
		int left = scale2 - scale1;
		int64 multi = MULTI_ARRAY[left];
		arg1 = (int128) dividend * multi;
	}

	scale = scale1 + scale2;

	/* extend to max precision */
	if (scale < MAXDECIMAL64_DIGITS)
		scale = MAXDECIMAL64_DIGITS;

	arg1 *= MULTI_ARRAY[scale];
	result = arg1 / arg2;

	return int128todecimal64(result, scale);
}

Datum
decimal64div(PG_FUNCTION_ARGS)
{
	decimal64		dividend = PG_GETARG_INT64(0);
	decimal64		divisor = PG_GETARG_INT64(1);
	int128		result;

	if (divisor == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	result = decimal64divid(dividend, divisor);

	if (result != ((int64) result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64((int64) result);
}

/* decimal64abs()
 * Absolute value
 */
Datum
decimal64abs(PG_FUNCTION_ARGS)
{
	decimal64		arg1 = PG_GETARG_INT64(0);
	decimal64		result;

	result = (arg1 < 0) ? -arg1 : arg1;
	/* overflow check (needed for INT64_MIN) */
	if (result < 0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));
	PG_RETURN_INT64(result);
}


Datum
decimal64larger(PG_FUNCTION_ARGS)
{
	decimal64	arg1 = PG_GETARG_INT64(0);
	decimal64	arg2 = PG_GETARG_INT64(1);
	int64		result;

	result = ((decimal64cmp(arg1, arg2) > 0) ? arg1 : arg2);

	PG_RETURN_INT64(result);
}

Datum
decimal64smaller(PG_FUNCTION_ARGS)
{
	decimal64	arg1 = PG_GETARG_INT64(0);
	decimal64	arg2 = PG_GETARG_INT64(1);
	int64		result;

	result = ((decimal64cmp(arg1, arg2) < 0) ? arg1 : arg2);

	PG_RETURN_INT64(result);
}

Datum
decimal64int4pl(PG_FUNCTION_ARGS)
{
	decimal64	arg1 = PG_GETARG_INT64(0);
	int64		adder = PG_GETARG_INT32(1) * DECIMAL64SCALE_MULTIPLIER;
	int128		result = decimal64plus(arg1, adder);

	if (result != (int64)result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64((int64)result);
}

Datum
decimal64int4mi(PG_FUNCTION_ARGS)
{
	decimal64	arg1 = PG_GETARG_INT64(0);
	int64		subtractor = PG_GETARG_INT32(1) * DECIMAL64SCALE_MULTIPLIER;
	int128		result = decimal64plus(arg1, -subtractor);

	if (result != (int64)result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64((int64)result);
}

Datum
decimal64int4mul(PG_FUNCTION_ARGS)
{
	decimal64	arg1 = PG_GETARG_INT64(0);
	int32		arg2 = PG_GETARG_INT32(1) * DECIMAL64SCALE_MULTIPLIER;
	int128		result = decimal64multi(arg1, arg2);

	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64((int64)result);
}

Datum
decimal64int4div(PG_FUNCTION_ARGS)
{
	decimal64	arg1 = PG_GETARG_INT64(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int64		result;

	if (arg2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/*
	 * INT64_MIN / -1 is problematic, since the result can't be represented on
	 * a two's-complement machine.  Some machines produce INT64_MIN, some
	 * produce zero, some throw an exception.  We can dodge the problem by
	 * recognizing that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
		result = -arg1;
		/* overflow check (needed for INT64_MIN) */
		if (arg1 != 0 && SAMESIGN(result, arg1))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("decimal64 out of range")));

		PG_RETURN_INT64(result);
	}

	result = decimal64divid(arg1, arg2);

	PG_RETURN_INT64(result);
}

Datum
int4decimal64pl(PG_FUNCTION_ARGS)
{
	int64		adder = PG_GETARG_INT32(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	arg2 = PG_GETARG_INT64(1);
	int128		result = decimal64plus(adder, arg2);
	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64((int64)result);
}

Datum
int4decimal64mi(PG_FUNCTION_ARGS)
{
	int64		subtractor = PG_GETARG_INT32(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	arg2 = PG_GETARG_INT64(1);
	int128		result = decimal64plus(subtractor, -arg2);
	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64((int64)result);
}

Datum
int4decimal64mul(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	arg2 = PG_GETARG_INT64(1);
	int128		result = decimal64multi(arg1, arg2);

	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64((int64)result);
}

static float8
decimal64todouble(decimal64 val)
{
	int scale;
	DECODE_SCALE(val, scale);

	return (float8)val/MULTI_ARRAY[scale];
}

Datum
int4decimal64div(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	arg2 = PG_GETARG_INT64(1);
	decimal64	result;

	if (arg2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	result = decimal64divid(arg1, arg2);

	/* No overflow is possible */
	PG_RETURN_FLOAT8(decimal64todouble(result));
}

Datum
decimal64int2pl(PG_FUNCTION_ARGS)
{
	decimal64	arg1 = PG_GETARG_INT64(0);
	int64		adder = PG_GETARG_INT16(1) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	result = decimal64plus(arg1, adder);
	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64(result);
}

Datum
decimal64int2mi(PG_FUNCTION_ARGS)
{
	decimal64	arg1 = PG_GETARG_INT64(0);
	int64		subtractor = PG_GETARG_INT16(1) * DECIMAL64SCALE_MULTIPLIER;
	
	int128		result = decimal64plus(arg1, -subtractor);
	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64(result);
}

Datum
decimal64int2mul(PG_FUNCTION_ARGS)
{
	decimal64	arg1 = PG_GETARG_INT64(0);
	int16		arg2 = PG_GETARG_INT16(1) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	result = decimal64multi(arg1, arg2);
	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64(result);
}

Datum
decimal64int2div(PG_FUNCTION_ARGS)
{
	decimal64	arg1 = PG_GETARG_INT64(0);
	int16		arg2 = PG_GETARG_INT16(1);
	decimal64	result;

	if (arg2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/*
	 * INT64_MIN / -1 is problematic, since the result can't be represented on
	 * a two's-complement machine.  Some machines produce INT64_MIN, some
	 * produce zero, some throw an exception.  We can dodge the problem by
	 * recognizing that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
		result = -arg1;
		/* overflow check (needed for INT64_MIN) */
		if (arg1 != 0 && SAMESIGN(result, arg1))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("decimal64 out of range")));

		PG_RETURN_INT64(result);
	}

	result = decimal64divid(arg1, arg2);

	PG_RETURN_INT64(result);
}

Datum
int2decimal64pl(PG_FUNCTION_ARGS)
{
	int64		adder = PG_GETARG_INT16(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	arg2 = PG_GETARG_INT64(1);
	int128		result = decimal64plus(adder, arg2);

	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64((int64) result);
}

Datum
int2decimal64mi(PG_FUNCTION_ARGS)
{
	int64		subtractor = PG_GETARG_INT16(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	arg2 = PG_GETARG_INT64(1);

	int128		result = decimal64plus(subtractor, -arg2);

	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));

	PG_RETURN_INT64((int64) result);
}

Datum
int2decimal64mul(PG_FUNCTION_ARGS)
{
	int64		multiplier = PG_GETARG_INT16(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	arg2 = PG_GETARG_INT64(1);
	int64		result = decimal64multi(multiplier, arg2);
	
	if (result != (int64) result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("decimal64 out of range")));


	PG_RETURN_INT64(result);
}

Datum
int2decimal64div(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT16(0) * DECIMAL64SCALE_MULTIPLIER;
	decimal64	arg2 = PG_GETARG_INT64(1);
	int64		result;

	if (arg2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	result = decimal64divid(arg1, arg2);

	/* No overflow is possible */
	PG_RETURN_INT64(result);
}

/*----------------------------------------------------------
 *	Conversion operators.
 *---------------------------------------------------------*/

/*
 * decimal64 serves as casting function for decimal64 to decimal64.
 * The only serves to generate an error if the decimal is too big for the
 * specified typmod.
 */
Datum
decimal64int4scale(PG_FUNCTION_ARGS)
{
	decimal64	num = PG_GETARG_INT64(0);
	int32		typmod = PG_GETARG_INT32(1);
	Datum		result;

	/* no need to check typmod if it's -1 */
	if (typmod != -1)
	{
		result = DirectFunctionCall1(decimal64out, num);
		result = DirectFunctionCall3(decimal64in, result, 0, typmod);
		
		PG_RETURN_INT64(result);
	}

	PG_RETURN_INT64(num);
}

Datum
int4decimal64(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT32(0);

	PG_RETURN_INT64(arg * DECIMAL64SCALE_MULTIPLIER);
}

Datum
decimal64int4(PG_FUNCTION_ARGS)
{
	decimal64	arg = PG_GETARG_INT64(0);
	float8 result = decimal64todouble(arg);

	if ((int32) result != result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range for decimal64")));

	PG_RETURN_INT32((int32) result);
}

Datum
int8decimal64(PG_FUNCTION_ARGS)
{
	int64	arg = PG_GETARG_INT64(0);
	int128  result = (int128) arg * DECIMAL64SCALE_MULTIPLIER;

	if (result != (int64) result)
		ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			errmsg("int8 out of range for decimal64")));
	
	PG_RETURN_INT64((int64) result);
}

Datum
decimal64int8(PG_FUNCTION_ARGS)
{
	decimal64 arg = PG_GETARG_INT64(0);
	float8 	result = decimal64todouble(arg);

	if ((int64) result != result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range for decimal64")));

	PG_RETURN_INT64((int64) result);
}


Datum
int2decimal64(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT16(0);

	PG_RETURN_INT64(arg * DECIMAL64SCALE_MULTIPLIER);
}

Datum
decimal64int2(PG_FUNCTION_ARGS)
{
	decimal64	arg = PG_GETARG_INT64(0);
	float8 		result = decimal64todouble(arg);

	if ((int16) result != result)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));

	PG_RETURN_INT16((int16) result);
}

Datum
decimal64tod(PG_FUNCTION_ARGS)
{
	decimal64	arg = PG_GETARG_INT64(0);
	float8		result = decimal64todouble(arg);

	PG_RETURN_FLOAT8(result);
}

/* dtodecimal64()
 * Convert float8 to decimal64
 */
Datum
dtodecimal64(PG_FUNCTION_ARGS)
{
	float8	arg = PG_GETARG_FLOAT8(0);
	Datum result;
	result = DirectFunctionCall1(float8out, Float8GetDatum(arg));
	result = DirectFunctionCall3(decimal64in, result, 0, -1);
	PG_RETURN_INT64(result);
}

Datum
decimal64tof(PG_FUNCTION_ARGS)
{
	decimal64	arg = PG_GETARG_INT64(0);
	float4		result = (float4) decimal64todouble(arg);

	PG_RETURN_FLOAT4(result);
}

/* ftodecimal64()
 * Convert float4 to decimal64.
 */
Datum
ftodecimal64(PG_FUNCTION_ARGS)
{
	float4		arg = PG_GETARG_FLOAT4(0);
	Datum		result;
	result = DirectFunctionCall1(float4out, Float4GetDatum(arg));
	result = DirectFunctionCall3(decimal64in, result, 0, -1);
	PG_RETURN_INT64(result);
}

Datum
decimal64_numeric(PG_FUNCTION_ARGS)
{
	decimal64	num = PG_GETARG_INT64(0);
	char	   *tmp;
	Datum		result;

	tmp = DatumGetCString(DirectFunctionCall1(decimal64out,
											  Int64GetDatum(num)));

	result = DirectFunctionCall3(numeric_in, CStringGetDatum(tmp), 0, -1);

	pfree(tmp);

	PG_RETURN_DATUM(result);
}

Datum
numeric_decimal64(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	char	   *tmp;
	Datum		result;

	if (numeric_is_nan(num))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot convert NaN to decimal64")));

	tmp = DatumGetCString(DirectFunctionCall1(numeric_out,
											  NumericGetDatum(num)));

	result = DirectFunctionCall3(decimal64in, CStringGetDatum(tmp), 0, -1);

	pfree(tmp);

	PG_RETURN_DATUM(result);
}


/* Aggregate Support */

/*
 * Prepare state data for a 128-bit aggregate function that needs to compute
 * sum, count and optionally sum of squares of the input.
 */
static Decimal64AggState *
makeDecimal64AggState(FunctionCallInfo fcinfo, bool calcSumX2)
{
	Decimal64AggState *state;
	MemoryContext agg_context;
	MemoryContext old_context;
	int agg_type;

	if (!(agg_type = AggCheckCallContext(fcinfo, &agg_context)))
		elog(ERROR, "aggregate function called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	AggState *aggstate = (AggState *)fcinfo->context;
	if (agg_type == AGG_CONTEXT_AGGREGATE && aggstate->hhashtable)
	{
		state = (Decimal64AggState *) mpool_alloc(aggstate->hhashtable->group_buf, sizeof(Decimal64AggState));
		MemSet(state, 0, sizeof(Decimal64AggState));
	}
	else
	{
		state = (Decimal64AggState *) palloc0(sizeof(Decimal64AggState));
	}
	state->calcSumX2 = calcSumX2;

	MemoryContextSwitchTo(old_context);

	return state;
}

static void
decimal64_accum(Decimal64AggState *state, int128 newval)
{
	int scale;
	DECODE_SCALE(newval, scale);

	if (state->N++ > 0)
	{
		/* could not overflow */
		if (scale != state->scale)
		{
			/* raise scale */
			state->scale = decimal64_accumpl(&state->sumX, state->scale, newval, scale);
		}
		else
		{
			state->sumX += newval;
		}
	}
	else
	{
		state->sumX = newval;
		state->scale = scale;
	}
}

Datum
decimal64_avg_accum(PG_FUNCTION_ARGS)
{
	Decimal64AggState *state;
	state = PG_ARGISNULL(0) ? NULL : (Decimal64AggState *) PG_GETARG_POINTER(0);

	if (state == NULL)
		state = makeDecimal64AggState(fcinfo, false);
	
	if (!PG_ARGISNULL(1))
		decimal64_accum(state, (int128) PG_GETARG_INT64(1));
	
	PG_RETURN_POINTER(state);
}

Datum
decimal64_avg(PG_FUNCTION_ARGS)
{
	Decimal64AggState *state;
	NumericVar	result;
	Datum		tmp,
				scale,
				countd,
				sumd;

	state = PG_ARGISNULL(0) ? NULL : (Decimal64AggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	init_var(&result);
	decimal_to_numericvar(state->sumX, &result);

	if (state->scale > 0)
	{
		countd 	= DirectFunctionCall1(int8_numeric,
								 Int64GetDatumFast(state->N));
		sumd 	= NumericGetDatum(decimal_make_result(&result));
		scale 	= DirectFunctionCall1(int8_numeric,
								 Int64GetDatumFast(MULTI_ARRAY[state->scale]));
		tmp  	= DirectFunctionCall2(numeric_div, sumd, scale);
		free_var(&result);

		PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, tmp, countd));
	}
	else
	{
		countd = DirectFunctionCall1(int8_numeric,
								 Int64GetDatumFast(state->N));
		sumd = NumericGetDatum(decimal_make_result(&result));

		free_var(&result);
		PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumd, countd));
	}
}

Datum
decimal64_sum(PG_FUNCTION_ARGS)
{
	Decimal64AggState *state;
	Numeric		res;
	NumericVar	result;
	Datum 		scale, sumd;

	state = PG_ARGISNULL(0) ? NULL : (Decimal64AggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	init_var(&result);
	decimal_to_numericvar(state->sumX, &result);

	if (state->scale > 0)
	{
		scale = DirectFunctionCall1(int8_numeric,
						Int64GetDatumFast(MULTI_ARRAY[state->scale]));
		sumd 	= NumericGetDatum(decimal_make_result(&result));
		
		free_var(&result);
		PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumd, scale));
	}
	else
	{
		res = decimal_make_result(&result);
		free_var(&result);
		PG_RETURN_NUMERIC(res);
	}
}

/*
 * Input / Output / Send / Receive functions for aggrgate states
 */

Datum
decimal64aggstateserialize(PG_FUNCTION_ARGS)
{
	Decimal64AggState *state;
	StringInfoData buf;
	bytea	   *result;
	int128		send;

	/* Ensure we disallow calling when not in aggregate context */
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state = (Decimal64AggState *) PG_GETARG_POINTER(0);

	/* in case null agg */
	if (state == NULL)
	{
		state = makeDecimal64AggState(fcinfo, false);
	}

	pq_begintypsend(&buf);

	/* N */
	pq_sendint64(&buf, state->N);
	/* scale */
	pq_sendint(&buf, state->scale, 4);
	/* sumX, send in two part together */
	send = state->sumX;
	pq_sendint64(&buf, send >> 64);
	pq_sendint64(&buf, (uint64)send);

	result = pq_endtypsend(&buf);

	PG_RETURN_BYTEA_P(result);
}

Datum
decimal64aggstatedeserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	Decimal64AggState *result;
	StringInfoData buf;
	int128			recv;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_P(0);

	/*
	 * Copy the bytea into a StringInfo so that we can "receive" it using the
	 * standard recv-function infrastructure.
	 */
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(sstate), VARSIZE(sstate) - VARHDRSZ);

	result = (Decimal64AggState *) palloc(sizeof(Decimal64AggState));

	/* N */
	result->N = pq_getmsgint64(&buf);
	/* scale */
	result->scale = pq_getmsgint(&buf, 4);
	/* sumX */
	recv = (int128) pq_getmsgint64(&buf) << 64;
	recv = recv | (uint64) pq_getmsgint64(&buf);
	result->sumX = recv;
	pq_getmsgend(&buf);
	pfree(buf.data);

	PG_RETURN_POINTER(result);
}


Datum
decimal64aggstatecombine(PG_FUNCTION_ARGS)
{
	Decimal64AggState *collectstate;
	Decimal64AggState *transstate;

	collectstate = PG_ARGISNULL(0) ? NULL : (Decimal64AggState *)
		PG_GETARG_POINTER(0);

	if (collectstate == NULL)
	{
		collectstate = makeDecimal64AggState(fcinfo, false);
	}

	transstate = PG_ARGISNULL(1) ? NULL : (Decimal64AggState *) PG_GETARG_POINTER(1);

	if (transstate == NULL)
		PG_RETURN_POINTER(collectstate);
	
	collectstate->scale = decimal64_accumpl(&collectstate->sumX, collectstate->scale, 
							transstate->sumX, transstate->scale);
	collectstate->N += transstate->N;

	PG_RETURN_POINTER(collectstate);
}
