/*-------------------------------------------------------------------------
 *
 * fastdecimal.h
 * common facilities, most functions port from numeric.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <float.h>
#include <limits.h>
#include <math.h>

#define	NUMERIC_LOCAL_NDIG	36		/* number of 'digits' in local digits[] */
#define DEC_DIGITS	4			    /* decimal digits per NBASE digit */
#define NBASE		10000
typedef int16 NumericDigit;

/*
 * Interpretation of high bits.
 */

#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_POS			0x0000
#define NUMERIC_NEG			0x4000
#define NUMERIC_SHORT		0x8000
#define NUMERIC_NAN			0xC000
#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_NAN(n)		(NUMERIC_FLAGBITS(n) == NUMERIC_NAN)
#define NUMERIC_IS_SHORT(n)		(NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)

#define NUMERIC_HDRSZ	(VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

/*
 * Short format definitions.
 */

#define NUMERIC_SHORT_SIGN_MASK			0x2000
#define NUMERIC_SHORT_DSCALE_MASK		0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT		7
#define NUMERIC_SHORT_DSCALE_MAX		\
	(NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK	0x0040
#define NUMERIC_SHORT_WEIGHT_MASK		0x003F
#define NUMERIC_SHORT_WEIGHT_MAX		NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN		(-(NUMERIC_SHORT_WEIGHT_MASK+1))

/*
 * Extract sign, display scale, weight.
 */

#define NUMERIC_DSCALE_MASK			0x3FFF

#define NUMERIC_SIGN(n) \
	(NUMERIC_IS_SHORT(n) ? \
		(((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? \
		NUMERIC_NEG : NUMERIC_POS) : NUMERIC_FLAGBITS(n))
#define NUMERIC_DSCALE(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) \
		>> NUMERIC_SHORT_DSCALE_SHIFT \
	: ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	(((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
		~NUMERIC_SHORT_WEIGHT_MASK : 0) \
	 | ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) \
	: ((n)->choice.n_long.n_weight))

#define digitbuf_free(v)	\
	do { \
		if ((v)->buf != (v)->ndb)	\
		{							\
		 	pfree((v)->buf); 		\
			(v)->buf = (v)->ndb;	\
		}	\
	} while (0)

#define free_var(v)	\
				digitbuf_free((v));

#define	NUMERIC_LOCAL_NDIG	36		/* number of 'digits' in local digits[] */
#define NUMERIC_LOCAL_NMAX	(NUMERIC_LOCAL_NDIG - 2)
#define	NUMERIC_LOCAL_DTXT	128		/* number of char in local text */
#define NUMERIC_LOCAL_DMAX	(NUMERIC_LOCAL_DTXT - 2)



#define digitbuf_alloc(ndigits)  \
	((NumericDigit *) palloc((ndigits) * sizeof(NumericDigit)))

/*
 * init_alloc_var() -
 *
 *	Init a var and allocate digit buffer of ndigits digits (plus a spare
 *  digit for rounding).
 *  Called when first using a var.
 */
#define	init_alloc_var(v, n) \
	do 	{	\
		(v)->buf = (v)->ndb;	\
		(v)->ndigits = (n);	\
		if ((n) > NUMERIC_LOCAL_NMAX)	\
			(v)->buf = digitbuf_alloc((n) + 1);	\
		(v)->buf[0] = 0;	\
		(v)->digits = (v)->buf + 1;	\
	} while (0)

/*
 * If the flag bits are NUMERIC_SHORT or NUMERIC_NAN, we want the short header;
 * otherwise, we want the long one.  Instead of testing against each value, we
 * can just look at the high bit, for a slight efficiency gain.
 */
#define NUMERIC_HEADER_IS_SHORT(n)	(((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
	(VARHDRSZ + sizeof(uint16) + \
	 (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

#define NUMERIC_DIGITS(num) (NUMERIC_HEADER_IS_SHORT(num) ? \
	(num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_NDIGITS(num) \
	((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))
#define NUMERIC_CAN_BE_SHORT(scale,weight) \
	((scale) <= NUMERIC_SHORT_DSCALE_MAX && \
	(weight) <= NUMERIC_SHORT_WEIGHT_MAX && \
	(weight) >= NUMERIC_SHORT_WEIGHT_MIN)

typedef struct NumericVar
{
	int			ndigits;		/* # of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			sign;			/* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
	int			dscale;			/* display scale */
	NumericDigit *buf;			/* start of space for digits[] */
	NumericDigit *digits;		/* base-NBASE digits */
	NumericDigit ndb[NUMERIC_LOCAL_NDIG];	/* local space for digits[] */
} NumericVar;

struct NumericShort
{
	uint16		n_header;		/* Sign + display scale + weight */
	NumericDigit n_data[1];		/* Digits */
};

struct NumericLong
{
	uint16		n_sign_dscale;	/* Sign + display scale */
	int16		n_weight;		/* Weight of 1st digit	*/
	NumericDigit n_data[1];		/* Digits */
};

union NumericChoice
{
	uint16		n_header;		/* Header word */
	struct NumericLong n_long;	/* Long form (4-byte header) */
	struct NumericShort n_short;	/* Short form (2-byte header) */
};

struct NumericData
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	union NumericChoice choice; /* choice of format */
};


#define quick_init_var(v) \
	do { \
		(v)->buf = (v)->ndb;	\
		(v)->digits = NULL; 	\
	} while (0)


#define init_var(v) \
	do { \
		quick_init_var((v));	\
		(v)->ndigits = (v)->weight = (v)->sign = (v)->dscale = 0; \
	} while (0)

extern void decimal_to_numericvar(int128 val, NumericVar *var);

extern Numeric decimal_make_result(NumericVar *var);