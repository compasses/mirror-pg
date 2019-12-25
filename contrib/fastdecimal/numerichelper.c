/*-------------------------------------------------------------------------
 *
 * numerichelper.c
 * for utils to do fastdecimal arithmetic, most port from numeric.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <float.h>
#include <limits.h>
#include <math.h>

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

/*
 * alloc_var() -
 *
 *	Allocate a digit buffer of ndigits digits (plus a spare digit for rounding)
 *  Called when a var may have been previously used.
 */
static void
alloc_var(NumericVar *var, int ndigits)
{
	digitbuf_free(var);
	init_alloc_var(var, ndigits);
}

/*
 * Convert 128 bit integer to numeric.
 */
void
decimal_to_numericvar(int128 val, NumericVar *var)
{
	uint128			uval,
					newuval;
	NumericDigit   *ptr;
	int				ndigits;

	/* int128 can require at most 39 decimal digits; add one for safety */
	alloc_var(var, 40 / DEC_DIGITS);
	if (val < 0)
	{
		var->sign = NUMERIC_NEG;
		uval = -val;
	}
	else
	{
		var->sign = NUMERIC_POS;
		uval = val;
	}
	var->dscale = 0;
	if (val == 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		return;
	}
	ptr = var->digits + var->ndigits;
	ndigits = 0;
	do
	{
		ptr--;
		ndigits++;
		newuval = uval / NBASE;
		*ptr = uval - newuval * NBASE;
		uval = newuval;
	} while (uval);
	var->digits = ptr;
	var->ndigits = ndigits;
	var->weight = ndigits - 1;
}


/*
 * make_result() -
 *
 *	Create the packed db numeric format in palloc()'d memory from
 *	a variable.
 *	CAUTION: we free_var(var) here!
 */
Numeric
decimal_make_result(NumericVar *var)
{
	Numeric		result;
	NumericDigit *digits = var->digits;
	int			weight = var->weight;
	int			sign = var->sign;
	int			n;
	Size		len;

	if (sign == NUMERIC_NAN)
	{
		free_var(var);
		result = (Numeric) palloc(NUMERIC_HDRSZ_SHORT);

		SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
		result->choice.n_header = NUMERIC_NAN;
		/* the header word is all we need */
		return result;
	}

	n = var->ndigits;

	/* truncate leading zeroes */
	while (n > 0 && *digits == 0)
	{
		digits++;
		weight--;
		n--;
	}
	/* truncate trailing zeroes */
	while (n > 0 && digits[n - 1] == 0)
		n--;

	/* If zero result, force to weight=0 and positive sign */
	if (n == 0)
	{
		weight = 0;
		sign = NUMERIC_POS;
	}

	/* Build the result */
	if (NUMERIC_CAN_BE_SHORT(var->dscale, weight))
	{
		len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
		result = (Numeric) palloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_short.n_header =
			(sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK)
			 : NUMERIC_SHORT)
			| (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT)
			| (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0)
			| (weight & NUMERIC_SHORT_WEIGHT_MASK);
	}
	else
	{
		len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
		result = (Numeric) palloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_long.n_sign_dscale =
			sign | (var->dscale & NUMERIC_DSCALE_MASK);
		result->choice.n_long.n_weight = weight;
	}

	memcpy(NUMERIC_DIGITS(result), digits, n * sizeof(NumericDigit));
	Assert(NUMERIC_NDIGITS(result) == n);

	/* Check for overflow of int16 fields */
	if (NUMERIC_WEIGHT(result) != weight ||
		NUMERIC_DSCALE(result) != var->dscale)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numeric format")));

	free_var(var);
	return result;
}




