#ifndef VECTORIZED_AGGREGATES_H
#define VECTORIZED_AGGREGATES_H

#include "executor/execdesc.h"
#include "nodes/parsenodes.h"

typedef struct ColBatchData {
	bool *exists;
	Datum *datums;
} ColBatchData;

#define MAX_BUFFER_PAGE_LIST_SIZE 50

typedef struct ColBatch
{
	uint32 colCount;
	uint32 curBatchSize;
	uint32 maxSize;
	uint32 bufferCount;
	Buffer bufferList[MAX_BUFFER_PAGE_LIST_SIZE];
	uint32 maxColIndex;
	bool	colReturn;
	bool	done;
	ColBatchData **colBatchData;
} ColBatch;



extern void vectorized_ExecutorRun(QueryDesc *queryDesc,
					 ScanDirection direction, long count);

extern TupleTableSlot *ExecProcNodeVectorized(PlanState *node);

extern TupleTableSlot *ExecAggVectorized(AggState *node);

#endif
