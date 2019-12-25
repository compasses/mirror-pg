#include "postgres.h"
#include "vec_agg.h"

#include <ctype.h>
#include <float.h>
#include <limits.h>
#include <math.h>

#include "access/hash.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/numeric.h"

Datum int4_sum_vec(PG_FUNCTION_ARGS);
Datum int8_sum_vec(PG_FUNCTION_ARGS);
Datum int4_avg_accum_vec(PG_FUNCTION_ARGS);
Datum int8_avg_accum_vec(PG_FUNCTION_ARGS);
Datum numeric_avg_accum_vec(PG_FUNCTION_ARGS);
Datum int8inc_vec(PG_FUNCTION_ARGS);
Datum int8inc_any_vec(PG_FUNCTION_ARGS);
Datum float4pl_vec(PG_FUNCTION_ARGS);
Datum float8pl_vec(PG_FUNCTION_ARGS);
Datum float4_accum_vec(PG_FUNCTION_ARGS);
Datum float8_accum_vec(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(int4_sum_vec);
PG_FUNCTION_INFO_V1(int8_sum_vec);
PG_FUNCTION_INFO_V1(int4_avg_accum_vec);
PG_FUNCTION_INFO_V1(int8_avg_accum_vec);
PG_FUNCTION_INFO_V1(numeric_avg_accum_vec);
PG_FUNCTION_INFO_V1(int8inc_vec);
PG_FUNCTION_INFO_V1(int8inc_any_vec);
PG_FUNCTION_INFO_V1(float4pl_vec);
PG_FUNCTION_INFO_V1(float8pl_vec);
PG_FUNCTION_INFO_V1(float4_accum_vec);
PG_FUNCTION_INFO_V1(float8_accum_vec);

/*
 * Routines for avg(int2) and avg(int4).  The transition datatype
 * is a two-element int8 array, holding count and sum.
 */
typedef struct Int8TransTypeData
{
	int64		count;
	int64		sum;
} Int8TransTypeData;

/* ----------------------------------------------------------------------
 *
 * Aggregate functions
 *
 * The transition datatype for all these aggregates is declared as INTERNAL.
 * Actually, it's a pointer to a NumericAggState allocated in the aggregate
 * context.  The digit buffers for the NumericVars will be there too.
 *
 * ----------------------------------------------------------------------
 */

typedef int16 NumericDigit;

typedef struct NumericVar
{
	int			ndigits;		/* # of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			sign;			/* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
	int			dscale;			/* display scale */
	NumericDigit *buf;			/* start of palloc'd space for digits[] */
	NumericDigit *digits;		/* base-NBASE digits */
} NumericVar;

typedef struct NumericAggState
{
	bool		calcSumX2;		/* if true, calculate sumX2 */
	MemoryContext agg_context;	/* context we're calculating in */
	int64		N;				/* count of processed numbers */
	NumericVar	sumX;			/* sum of processed numbers */
	NumericVar	sumX2;			/* sum of squares of processed numbers */
	int			maxScale;		/* maximum scale seen so far */
	int64		maxScaleCount;	/* number of values seen with maximum scale */
	int64		NaNcount;		/* count of NaN values (not included in N!) */
} NumericAggState;


extern NumericAggState *
makeNumericAggState(FunctionCallInfo fcinfo, bool calcSumX2);
extern void
do_numeric_accum(NumericAggState *state, Numeric newval);

static float8 *
check_float8_array(ArrayType *transarray, const char *caller, int n)
{
	/*
	 * We expect the input to be an N-element float array; verify that. We
	 * don't need to use deconstruct_array() since the array data is just
	 * going to look like a C array of N float8 values.
	 */
	if (ARR_NDIM(transarray) != 1 ||
		ARR_DIMS(transarray)[0] != n ||
		ARR_HASNULL(transarray) ||
		ARR_ELEMTYPE(transarray) != FLOAT8OID)
	{
		elog(ERROR, "%s: expected %d-element float8 array", caller, n);
	}

	return (float8 *) ARR_DATA_PTR(transarray);
}


Datum
int4_sum_vec(PG_FUNCTION_ARGS)
{
	ColBatchData *columnData = (ColBatchData *) PG_GETARG_POINTER(1);
	uint32 rowCount = *((uint32 *) PG_GETARG_POINTER(2));
	int64 newValue = 0;
	uint32 i = 0;

	if (PG_ARGISNULL(0))
	{
		newValue = 0;
	}
	else
	{
		newValue = PG_GETARG_INT64(0);
	}

	for (i = 0; i < rowCount; i++)
	{
		Datum value = columnData->datums[i];
		bool exists = columnData->exists[i];

		if (exists)
		{
			newValue = newValue + (int64) DatumGetInt32(value);
		}
	}

	PG_RETURN_INT64(newValue);
}


Datum
int8_sum_vec(PG_FUNCTION_ARGS)
{
	ColBatchData *columnData = (ColBatchData *) PG_GETARG_POINTER(1);
	uint32 rowCount = *((uint32 *) PG_GETARG_POINTER(2));
	Datum newValue;
	uint32 i = 0;

	if (PG_ARGISNULL(0))
	{
		newValue = DirectFunctionCall1(int8_numeric, Int64GetDatum(0));
	}
	else
	{
		newValue = PG_GETARG_DATUM(0);
	}

	for (i = 0; i < rowCount; i++)
	{
		Datum value = columnData->datums[i];
		bool exists = columnData->exists[i];

		if (exists)
		{
			newValue = DirectFunctionCall2(numeric_add,
										   newValue,
										   DirectFunctionCall1(int8_numeric, value));
		}
	}

	PG_RETURN_DATUM(newValue);
}


Datum
int4_avg_accum_vec(PG_FUNCTION_ARGS)
{
	ArrayType *transarray = PG_GETARG_ARRAYTYPE_P(0);
	ColBatchData *columnData = (ColBatchData *) PG_GETARG_POINTER(1);
	uint32 rowCount = *((uint32 *) PG_GETARG_POINTER(2));

	int64 newValue = 0;
	uint32 i = 0;
	uint32 realCount = 0;
	Int8TransTypeData *transdata = NULL;

	if (ARR_HASNULL(transarray) || 
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
	{
		elog(ERROR, "expected 2-element int8 array");
	}

	for (i = 0; i < rowCount; i++)
	{
		Datum value = columnData->datums[i];
		bool exists = columnData->exists[i];
		
		if (exists)
		{
			newValue = newValue + (int64) DatumGetInt32(value);
			realCount++;
		}
	}

	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
	transdata->count = transdata->count + realCount;
	transdata->sum = transdata->sum + newValue;
	PG_RETURN_ARRAYTYPE_P(transarray);
}

/*
 * Transition function for int8 input when we don't need sumX2.
 */
Datum
numeric_avg_accum_vec(PG_FUNCTION_ARGS)
{
	ColBatchData *columnData = (ColBatchData *) PG_GETARG_POINTER(1);
	uint32 rowCount = *((uint32 *) PG_GETARG_POINTER(2));
	NumericAggState *state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);
	uint32 i = 0;

	/* Create the state data on the first call */
	if (state == NULL)
		state = makeNumericAggState(fcinfo, false);

	for (i = 0; i < rowCount; i++)
	{
		Datum value = columnData->datums[i];
		bool exists = columnData->exists[i];

		if (exists)
		{
			Numeric		newval;
			newval = DatumGetNumeric(value);
			do_numeric_accum(state, newval);
		}
	}

	PG_RETURN_POINTER(state);
}

/*
 * Transition function for int8 input when we don't need sumX2.
 */
Datum
int8_avg_accum_vec(PG_FUNCTION_ARGS)
{
	ColBatchData *columnData = (ColBatchData *) PG_GETARG_POINTER(1);
	uint32 rowCount = *((uint32 *) PG_GETARG_POINTER(2));
	NumericAggState *state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);
	uint32 i = 0;

	/* Create the state data on the first call */
	if (state == NULL)
		state = makeNumericAggState(fcinfo, false);

	for (i = 0; i < rowCount; i++)
	{
		Datum value = columnData->datums[i];
		bool exists = columnData->exists[i];

		if (exists)
		{
			Numeric		newval;
			newval = DatumGetNumeric(DirectFunctionCall1(int8_numeric, value));
			do_numeric_accum(state, newval);
		}
	}

	PG_RETURN_POINTER(state);
}


Datum
int8inc_vec(PG_FUNCTION_ARGS)
{
	int64 arg = PG_GETARG_INT64(0);
	uint32 rowCount = *((uint32 *) PG_GETARG_POINTER(2));
	int64 result = arg + (int64) rowCount;

	/* Overflow check */
	if (result < arg)
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("bigint out of range")));
	}

	PG_RETURN_INT64(result);
}


Datum
int8inc_any_vec(PG_FUNCTION_ARGS)
{
	int64 arg = PG_GETARG_INT64(0);
	ColBatchData *columnData = (ColBatchData *) PG_GETARG_POINTER(1);
	uint32 rowCount = *((uint32 *) PG_GETARG_POINTER(2));
	uint32 i = 0;
	int64 result = arg;

	for (i = 0; i < rowCount; i++)
	{
		bool exists = columnData->exists[i];
		
		if (exists)
		{
			result++;
			if (result < arg)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("bigint out of range")));
		}
	}
	
	PG_RETURN_INT64(result);
}


Datum
float4pl_vec(PG_FUNCTION_ARGS)
{
    ColBatchData *columnData = (ColBatchData *) PG_GETARG_POINTER(1);
	uint32 rowCount = *((uint32 *) PG_GETARG_POINTER(2));
	float4 newValue = 0.0;
	uint32 i = 0;

	if (PG_ARGISNULL(0))
	{
		newValue = 0.0;
	}
	else
	{
		newValue = PG_GETARG_FLOAT4(0);
	}

	for (i = 0; i < rowCount; i++)
	{
		if (columnData->exists[i])
		{
			newValue = newValue + DatumGetFloat4(columnData->datums[i]);
		}
	}

	PG_RETURN_FLOAT4(newValue);
}


Datum
float8pl_vec(PG_FUNCTION_ARGS)
{
    ColBatchData *columnData = (ColBatchData *) PG_GETARG_POINTER(1);
	uint32 rowCount = *((uint32 *) PG_GETARG_POINTER(2));
	float8 newValue = 0.0;
	uint32 i = 0;

	if (PG_ARGISNULL(0))
	{
		newValue = 0.0;
	}
	else
	{
		newValue = PG_GETARG_FLOAT8(0);
	}

	for (i = 0; i < rowCount; i++)
	{
		if (columnData->exists[i])
		{
			newValue = newValue + DatumGetFloat8(columnData->datums[i]);
		}
	}

	PG_RETURN_FLOAT8(newValue);
}


Datum
float8_accum_vec(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);

	ColBatchData *columnData = (ColBatchData *) PG_GETARG_POINTER(1);
	uint32 rowCount = *((uint32 *) PG_GETARG_POINTER(2));

	uint32 i = 0;
	float8 *transvalues = NULL;
	float8 N = 0.0;
	float8 sumX = 0.0;

	transvalues = check_float8_array(transarray, "float8_accum_vec", 3);
	N = transvalues[0];
	sumX = transvalues[1];

	for (i = 0; i < rowCount; i++)
	{	
		if (columnData->exists[i])
		{
			sumX = sumX + DatumGetFloat8(columnData->datums[i]);
			N++;
		}
	}
	transvalues[0] = N;
	transvalues[1] = sumX;

	PG_RETURN_ARRAYTYPE_P(transarray);
}


Datum
float4_accum_vec(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	ColBatchData *columnData = (ColBatchData *) PG_GETARG_POINTER(1);
	uint32 rowCount = *((uint32 *) PG_GETARG_POINTER(2));

	uint32 i = 0;
	float8 *transvalues = NULL;
	float8 N = 0.0;
	float8 sumX = 0.0;

	transvalues = check_float8_array(transarray, "float8_accum_vec", 3);
	N = transvalues[0];
	sumX = transvalues[1];

	for (i = 0; i < rowCount; i++)
	{	
		if (columnData->exists[i])
		{
			sumX = sumX + (float8) DatumGetFloat4(columnData->datums[i]);
			N++;
		}
	}

	transvalues[0] = N;
	transvalues[1] = sumX;

	PG_RETURN_ARRAYTYPE_P(transarray);
}
