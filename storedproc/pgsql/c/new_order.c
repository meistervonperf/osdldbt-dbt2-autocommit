/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003-2008 Mark Wong & Open Source Development Labs, Inc.
 *               2016      Takashi Horikawa
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.8.2.
 */

#include <sys/types.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h> /* for OIDs */
#include <executor/spi.h> /* this should include most necessary APIs */
#include <executor/executor.h>  /* for GetAttributeByName() */
#include <utils/builtins.h>
#include <funcapi.h> /* for returning set of rows in order_status */
#include "access/xact.h"

#include "dbt2common.h"

/*
 * New Order transaction SQL statements.
 */

#define NEW_ORDER_1 statements[0].plan
#define NEW_ORDER_2 statements[1].plan
#define NEW_ORDER_3 statements[2].plan
#define NEW_ORDER_4 statements[3].plan
#define NEW_ORDER_5 statements[4].plan
#define NEW_ORDER_6 statements[5].plan
#define NEW_ORDER_7 statements[6].plan
#define NEW_ORDER_8 statements[7].plan
#define NEW_ORDER_9 statements[8].plan
#define NEW_ORDER_10 statements[9].plan
#define NEW_ORDER_BODY statements[10].plan

const char s_dist[10][11] = {
	"s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05",
	"s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10"
};

static cached_statement statements[] =
{

	{ /* NEW_ORDER_1 */
	"SELECT w_tax\n" \
	"FROM warehouse\n" \
	"WHERE w_id = $1",
	1, { INT4OID }
	},

	{ /* NEW_ORDER_2 */
	"SELECT d_tax, d_next_o_id\n" \
	"FROM district \n" \
	"WHERE d_w_id = $1\n" \
	"  AND d_id = $2\n" \
	"FOR UPDATE",
	2, { INT4OID, INT4OID }
	},

	{ /* NEW_ORDER_3 */
	"UPDATE district\n" \
	"SET d_next_o_id = d_next_o_id + 1\n" \
	"WHERE d_w_id = $1\n" \
	"  AND d_id = $2",
	2, { INT4OID, INT4OID }
	},

	{ /* NEW_ORDER_4 */
	"SELECT c_discount, c_last, c_credit\n" \
	"FROM customer\n" \
	"WHERE c_w_id = $1\n" \
	"  AND c_d_id = $2\n" \
	"  AND c_id = $3",
	3, { INT4OID, INT4OID, INT4OID }
	},

	{ /* NEW_ORDER_5 */
	"INSERT INTO new_order (no_o_id, no_w_id, no_d_id)\n" \
	"VALUES ($1, $2, $3)",
	3, { INT4OID, INT4OID, INT4OID}
	},

	{ /* NEW_ORDER_6 */
	"INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d,\n" \
	"		    o_carrier_id, o_ol_cnt, o_all_local)\n" \
	"VALUES ($1, $2, $3, $4, current_timestamp, NULL, $5, $6)",
	6, { INT4OID, INT4OID, INT4OID, INT4OID, INT4OID, INT4OID}
	},

	{ /* NEW_ORDER_7 */
	"SELECT i_price, i_name, i_data\n" \
	"FROM item\n" \
	"WHERE i_id = $1",
	1, { INT4OID }
	},

	{ /* NEW_ORDER_8 */
	"SELECT s_quantity, $1, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = $2\n" \
	"  AND s_w_id = $3",
	3, { TEXTOID, INT4OID, INT4OID}
	},

	{ /* NEW_ORDER_9 */
	"UPDATE stock\n" \
	"SET s_quantity = s_quantity - $1\n" \
	"WHERE s_i_id = $2\n" \
	"  AND s_w_id = $3",
	3, { INT4OID, INT4OID, INT4OID}
	},

	{ /* NEW_ORDER_10 */
	"INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number,\n" \
	"			ol_i_id, ol_supply_w_id, ol_delivery_d,\n" \
	"			ol_quantity, ol_amount, ol_dist_info)\n" \
	"VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $9)",
	9, { INT4OID, INT4OID, INT4OID, INT4OID, INT4OID, INT4OID, INT4OID, FLOAT4OID, TEXTOID }
	},

	{ /* NEW_ORDER_body */
	  "SELECT new_order_body(\n" \
	  "$1, $2, $3, $4, $5,\n" \
	  "$6, $7, $8, $9, $10,\n" \
	  "$11, $12, $13, $14, $15,\n" \
	  "$16, $17, $18, $19, $20)",
	  20, { INT4OID, INT4OID, INT4OID, INT4OID, INT4OID,
		0, 0, 0, 0, 0, /* will be replaced with the new_order_info's OID */
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0 }
	},

	{ NULL }
};

/* Prototypes to prevent potential gcc warnings. */
Datum new_order(PG_FUNCTION_ARGS);
Datum make_new_order_info(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(new_order);
PG_FUNCTION_INFO_V1(new_order_body);
PG_FUNCTION_INFO_V1(make_new_order_info);

Datum new_order(PG_FUNCTION_ARGS)
{
	/* Input variable. */
	int32 o_ol_cnt = PG_GETARG_INT32(4);

	int i;

	Portal new_order_portal;
	Datum args[20];
	char  nulls[20] = {' ', ' ', ' ', ' ', ' ',   ' ', ' ', ' ', ' ',  ' ',
			   ' ', ' ', ' ', ' ', ' ',   ' ', ' ', ' ', ' ',  ' '
	};

	if (statements[10].argtypes[5] == 0)
	{
		/* get tupdesc, tuple manipulating variables, from the type name */
		TupleDesc tupdesc = RelationNameGetTupleDesc("new_order_info");

		for (i = 5; i < 20; i++)
			statements[10].argtypes[i] = tupdesc->tdtypeid;
	}

	BeginTransactionBlock();	/* TBLOCK_STARTED -> TBLOCK_BEGIN      */ 
	CommitTransactionCommand();	/* TBLOCK_BEGIN   -> TBLOCK_INPROGRESS */ 
	SPI_connect();

	plan_queries(statements);
	
	for (i = 0; i < 20; i++) {
		args[i] = PG_GETARG_DATUM(i);
	}

	new_order_portal = SPI_cursor_open(NULL, NEW_ORDER_BODY, args, nulls, false);
	SPI_cursor_fetch(new_order_portal, true, o_ol_cnt);
	if (SPI_processed != o_ol_cnt) {
		SPI_cursor_close(new_order_portal);
		/* Item doesn't exist, rollback transaction. */
		UserAbortTransactionBlock();	/* TBLOCK_INPROGRESS -> TBLOCK_ABORT_PENDING */
		SPI_finish();
		PG_RETURN_INT32(2);
	}

	SPI_cursor_close(new_order_portal);
	SPI_finish();
	EndTransactionBlock();	/* TBLOCK_INPROGRESS -> TBLOCK_END */ 
	PG_RETURN_INT32(0);
}


Datum new_order_body(PG_FUNCTION_ARGS)
{
	/* Input variables. */
	int32 w_id = PG_GETARG_INT32(0);
	int32 d_id = PG_GETARG_INT32(1);
	int32 c_id = PG_GETARG_INT32(2);
	int32 o_all_local = PG_GETARG_INT32(3);
	int32 o_ol_cnt = PG_GETARG_INT32(4);

	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;
	int32 count = 0;

	static int32 ol_i_id[15];
	int32 ol_supply_w_id[15];
	int32 ol_quantity[15];
	static int32 quantity[15];

	int i, j;

	int ret;

	char *w_tax = NULL;

	char *d_tax = NULL;
	int32 d_next_o_id;

	char *c_discount = NULL;
	char *c_last = NULL;
	char *c_credit = NULL;

	float order_amount = 0.0;

	char *i_price[15];
	char *i_name[15];
	char *i_data[15];

	float ol_amount[15];
	char *s_quantity[15];
	char *my_s_dist[15];
	char *s_data[15];

	Datum args[9];
	char  nulls[9] = {' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ' };



    FuncCallContext     *funcctx;
    int                  call_cntr;
    int                  max_calls;
    //    TupleDesc            tupdesc;
    AttInMetadata       *attinmeta;

     /* stuff done only on the first call of the function */
     if (SRF_IS_FIRSTCALL())
     {
        MemoryContext   oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
	//	/* total number of tuples to be returned */
	//	funcctx->max_calls = PG_GETARG_UINT32(0);

        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));

        /*
         * generate attribute metadata needed later to produce tuples from raw
         * C strings
         */
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;



	/* Loop through the last set of parameters. */
	for (i = 0, j = 5; i < 15; i++) {
		bool isnull;
		HeapTupleHeader  t = PG_GETARG_HEAPTUPLEHEADER(j++);
		ol_i_id[i] = DatumGetInt32(GetAttributeByName(t, "ol_i_id", &isnull));
		ol_supply_w_id[i] = DatumGetInt32(GetAttributeByName(t,
				"ol_supply_w_id", &isnull));
		ol_quantity[i] = DatumGetInt32(GetAttributeByName(t, "ol_quantity",
				&isnull));
	}

	elog(DEBUG1, "%d w_id = %d", (int) getpid(), w_id);
	elog(DEBUG1, "%d d_id = %d", (int) getpid(), d_id);
	elog(DEBUG1, "%d c_id = %d", (int) getpid(), c_id);
	elog(DEBUG1, "%d o_all_local = %d", (int) getpid(), o_all_local);
	elog(DEBUG1, "%d o_ol_cnt = %d", (int) getpid(), o_ol_cnt);

	elog(DEBUG1, "%d ##  ol_i_id  ol_supply_w_id  ol_quantity",
		(int) getpid());
	elog(DEBUG1, "%d --  -------  --------------  -----------",
		(int) getpid());
	for (i = 0; i < o_ol_cnt; i++) {
		elog(DEBUG1, "%d %2d  %7d  %14d  %11d", (int) getpid(),
				i + 1, ol_i_id[i], ol_supply_w_id[i], ol_quantity[i]);
	}

	SPI_connect();

	plan_queries(statements);

	args[0] = Int32GetDatum(w_id);
	ret = SPI_execute_plan(NEW_ORDER_1, args, nulls, true, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		w_tax = SPI_getvalue(tuple, tupdesc, 1);
		elog(DEBUG1, "%d w_tax = %s", (int) getpid(), w_tax);
	} else {
		elog(ERROR, "NEW_ORDER_1 failed");
		SPI_finish();
		//		PG_RETURN_INT32(10);
		goto error;
	}

	args[0] = Int32GetDatum(w_id);
	args[1] = Int32GetDatum(d_id);
	ret = SPI_execute_plan(NEW_ORDER_2, args, nulls, false, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		d_tax = SPI_getvalue(tuple, tupdesc, 1);
		d_next_o_id = atoi(SPI_getvalue(tuple, tupdesc, 2));
		elog(DEBUG1, "%d d_tax = %s", (int) getpid(), d_tax);
		elog(DEBUG1, "%d d_next_o_id = %d", (int) getpid(),
			d_next_o_id);
	} else {
		elog(ERROR, "NEW_ORDER_2 failed");
		SPI_finish();
		//		PG_RETURN_INT32(11);
		goto error;
	}

	args[0] = Int32GetDatum(w_id);
	args[1] = Int32GetDatum(d_id);
	ret = SPI_execute_plan(NEW_ORDER_3, args, nulls, false, 0);
	if (ret != SPI_OK_UPDATE) {
		elog(ERROR, "NEW_ORDER_3 failed");
		SPI_finish();
		//		PG_RETURN_INT32(12);
		goto error;
	}

	args[0] = Int32GetDatum(w_id);
	args[1] = Int32GetDatum(d_id);
	args[2] = Int32GetDatum(c_id);
	ret = SPI_execute_plan(NEW_ORDER_4, args, nulls, false, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		c_discount = SPI_getvalue(tuple, tupdesc, 1);
		c_last = SPI_getvalue(tuple, tupdesc, 2);
		c_credit = SPI_getvalue(tuple, tupdesc, 3);
		elog(DEBUG1, "%d c_discount = %s", (int) getpid(), c_discount);
		elog(DEBUG1, "%d c_last = %s", (int) getpid(), c_last);
		elog(DEBUG1, "%d c_credit = %s", (int) getpid(), c_credit);
	} else {
		elog(ERROR, "NEW_ORDER_4 failed");
		SPI_finish();
		//		PG_RETURN_INT32(13);
		goto error;
	}

	args[0] = Int32GetDatum(d_next_o_id);
	args[1] = Int32GetDatum(w_id);
	args[2] = Int32GetDatum(d_id);
	ret = SPI_execute_plan(NEW_ORDER_5, args, nulls, false, 0);
	if (ret != SPI_OK_INSERT) {
		elog(ERROR, "NEW_ORDER_5 failed %d", ret);
		SPI_finish();
		//		PG_RETURN_INT32(14);
		goto error;
	}

	args[0] = Int32GetDatum(d_next_o_id);
	args[1] = Int32GetDatum(d_id);
	args[2] = Int32GetDatum(w_id);
	args[3] = Int32GetDatum(c_id);
	args[4] = Int32GetDatum(o_ol_cnt);
	args[5] = Int32GetDatum(o_all_local);
	ret = SPI_execute_plan(NEW_ORDER_6, args, nulls, false, 0);
	if (ret != SPI_OK_INSERT) {
		elog(ERROR, "NEW_ORDER_6 failed");
		SPI_finish();
		//		PG_RETURN_INT32(15);
		goto error;
	}

	for (i = 0; i < o_ol_cnt; i++) {
		int decr_quantity;

		args[0] = Int32GetDatum(ol_i_id[i]);
		ret = SPI_execute_plan(NEW_ORDER_7, args, nulls, true, 0);
		/*
		 * Shouldn't have to check if ol_i_id is 0, but if the row
		 * doesn't exist, the query still passes.
		 */
		if (ol_i_id[i] != 0 && ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			i_price[i] = SPI_getvalue(tuple, tupdesc, 1);
			i_name[i] = SPI_getvalue(tuple, tupdesc, 2);
			i_data[i] = SPI_getvalue(tuple, tupdesc, 3);
			elog(DEBUG1, "%d i_price[%d] = %s", (int) getpid(), i,
				i_price[i]);
			elog(DEBUG1, "%d i_name[%d] = %s", (int) getpid(), i,
				i_name[i]);
			elog(DEBUG1, "%d i_data[%d] = %s", (int) getpid(), i,
				i_data[i]);
			count++;
		} else {
			SPI_finish();	/* Item doesn't exist. */
			goto error;
		}

		ol_amount[i] = atof(i_price[i]) * (float) ol_quantity[i];

		args[0] = CStringGetTextDatum(s_dist[d_id - 1]);
		args[1] = Int32GetDatum(ol_i_id[i]);
		args[2] = Int32GetDatum(w_id);
		ret = SPI_execute_plan(NEW_ORDER_8, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			s_quantity[i] = SPI_getvalue(tuple, tupdesc, 1);
			quantity[i] = atoi(s_quantity[i]);
			my_s_dist[i] = SPI_getvalue(tuple, tupdesc, 2);
			s_data[i] = SPI_getvalue(tuple, tupdesc, 3);
			elog(DEBUG1, "%d s_quantity[%d] = %s", (int) getpid(),
				i, s_quantity[i]);
			elog(DEBUG1, "%d my_s_dist[%d] = %s", (int) getpid(),
				i, my_s_dist[i]);
			elog(DEBUG1, "%d s_data[%d] = %s", (int) getpid(), i,
				s_data[i]);
		} else {
			elog(ERROR, "NEW_ORDER_8 failed");
			SPI_finish();
			//			PG_RETURN_INT32(16);
			goto error;
		}
		order_amount += ol_amount[i];

		if (atoi(s_quantity[i]) > ol_quantity[i] + 10) {
			decr_quantity = ol_quantity[i];
		} else {
			decr_quantity = ol_quantity[i] - 91;
		}
		args[0] = Int32GetDatum(decr_quantity);
		args[1] = Int32GetDatum(ol_i_id[i]);
		args[2] = Int32GetDatum(w_id);
		ret = SPI_execute_plan(NEW_ORDER_9, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			elog(ERROR, "NEW_ORDER_9 failed");
			SPI_finish();
			//			PG_RETURN_INT32(17);
			goto error;
		}

		args[0] = Int32GetDatum(d_next_o_id);
		args[1] = Int32GetDatum(d_id);
		args[2] = Int32GetDatum(w_id);
		args[3] = Int32GetDatum(i + 1);
		args[4] = Int32GetDatum(ol_i_id[i]);
		args[5] = Int32GetDatum(ol_supply_w_id[i]);
		args[6] = Int32GetDatum(ol_quantity[i]);
		args[7] = Float4GetDatum(ol_amount[i]);
		args[8] = CStringGetTextDatum(my_s_dist[i]);
		ret = SPI_execute_plan(NEW_ORDER_10, args, nulls, false, 0);
		if (ret != SPI_OK_INSERT) {
			elog(ERROR, "NEW_ORDER_10 failed");
			SPI_finish();
			//			PG_RETURN_INT32(18);
			goto error;
		}
	}

	SPI_finish();



     error:
	funcctx->max_calls = count;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;

    if (call_cntr < max_calls)    /* do when there is more left to send */
    {
        char       **values;
        HeapTuple    tuple;
        Datum        result;

        /*
         * Prepare a values array for building the returned tuple.
         * This should be an array of C strings which will
         * be processed later by the type input functions.
         */
        values = (char **) palloc(2 * sizeof(char *));
        values[0] = (char *) palloc(16 * sizeof(char));
        values[1] = (char *) palloc(16 * sizeof(char));

	snprintf(values[0], 16, "%d", ol_i_id[call_cntr]);
        snprintf(values[1], 16, "%d", quantity[call_cntr]);

        /* build a tuple */
        tuple = BuildTupleFromCStrings(attinmeta, values);

        /* make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);

        /* clean up (this is not really necessary) */
        pfree(values[0]);
        pfree(values[1]);
        pfree(values);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else    /* do when there is no more left */
    {
        SRF_RETURN_DONE(funcctx);
    }
}

Datum make_new_order_info(PG_FUNCTION_ARGS)
{
	/* result Datum */
	Datum result;
	char** cstr_values;
	HeapTuple result_tuple;

	/* tuple manipulating variables */
	TupleDesc tupdesc;
	AttInMetadata *attinmeta;

	/* loop variables. */
	int i;

	/* get tupdesc from the type name */
	tupdesc = RelationNameGetTupleDesc("new_order_info");

	/*
	 * generate attribute metadata needed later to produce tuples
	 * from raw C strings
	 */
	attinmeta = TupleDescGetAttInMetadata(tupdesc);

	cstr_values = (char **) palloc(3 * sizeof(char *));
	for(i = 0; i < 3; i++) {
		cstr_values[i] = (char*) palloc(16 * sizeof(char)); /* 16 bytes */
		snprintf(cstr_values[i], 16, "%d", PG_GETARG_INT32(i));
	}

	/* build a tuple */
	result_tuple = BuildTupleFromCStrings(attinmeta, cstr_values);

	/* make the tuple into a datum */
	result = HeapTupleGetDatum(result_tuple);
	return result;
}
