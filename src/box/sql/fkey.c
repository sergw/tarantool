/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * This file contains code used by the compiler to add foreign key
 * support to compiled SQL statements.
 */
#include "coll.h"
#include "sqliteInt.h"
#include "box/fkey.h"
#include "box/schema.h"
#include "box/session.h"
#include "tarantoolInt.h"
#include "vdbeInt.h"

#ifndef SQLITE_OMIT_TRIGGER

/*
 * Deferred and Immediate FKs
 * --------------------------
 *
 * Foreign keys in SQLite come in two flavours: deferred and immediate.
 * If an immediate foreign key constraint is violated,
 * SQLITE_CONSTRAINT_FOREIGNKEY is returned and the current
 * statement transaction rolled back. If a
 * deferred foreign key constraint is violated, no action is taken
 * immediately. However if the application attempts to commit the
 * transaction before fixing the constraint violation, the attempt fails.
 *
 * Deferred constraints are implemented using a simple counter associated
 * with the database handle. The counter is set to zero each time a
 * database transaction is opened. Each time a statement is executed
 * that causes a foreign key violation, the counter is incremented. Each
 * time a statement is executed that removes an existing violation from
 * the database, the counter is decremented. When the transaction is
 * committed, the commit fails if the current value of the counter is
 * greater than zero. This scheme has two big drawbacks:
 *
 *   * When a commit fails due to a deferred foreign key constraint,
 *     there is no way to tell which foreign constraint is not satisfied,
 *     or which row it is not satisfied for.
 *
 *   * If the database contains foreign key violations when the
 *     transaction is opened, this may cause the mechanism to malfunction.
 *
 * Despite these problems, this approach is adopted as it seems simpler
 * than the alternatives.
 *
 * INSERT operations:
 *
 *   I.1) For each FK for which the table is the child table, search
 *        the parent table for a match. If none is found increment the
 *        constraint counter.
 *
 *   I.2) For each FK for which the table is the parent table,
 *        search the child table for rows that correspond to the new
 *        row in the parent table. Decrement the counter for each row
 *        found (as the constraint is now satisfied).
 *
 * DELETE operations:
 *
 *   D.1) For each FK for which the table is the child table,
 *        search the parent table for a row that corresponds to the
 *        deleted row in the child table. If such a row is not found,
 *        decrement the counter.
 *
 *   D.2) For each FK for which the table is the parent table, search
 *        the child table for rows that correspond to the deleted row
 *        in the parent table. For each found increment the counter.
 *
 * UPDATE operations:
 *
 *   An UPDATE command requires that all 4 steps above are taken, but only
 *   for FK constraints for which the affected columns are actually
 *   modified (values must be compared at runtime).
 *
 * Note that I.1 and D.1 are very similar operations, as are I.2 and D.2.
 * This simplifies the implementation a bit.
 *
 * For the purposes of immediate FK constraints, the OR REPLACE conflict
 * resolution is considered to delete rows before the new row is inserted.
 * If a delete caused by OR REPLACE violates an FK constraint, an exception
 * is thrown, even if the FK constraint would be satisfied after the new
 * row is inserted.
 *
 * Immediate constraints are usually handled similarly. The only difference
 * is that the counter used is stored as part of each individual statement
 * object (struct Vdbe). If, after the statement has run, its immediate
 * constraint counter is greater than zero,
 * it returns SQLITE_CONSTRAINT_FOREIGNKEY
 * and the statement transaction is rolled back. An exception is an INSERT
 * statement that inserts a single row only (no triggers). In this case,
 * instead of using a counter, an exception is thrown immediately if the
 * INSERT violates a foreign key constraint. This is necessary as such
 * an INSERT does not open a statement transaction.
 *
 * TODO: How should dropping a table be handled? How should renaming a
 * table be handled?
 *
 *
 * Query API Notes
 * ---------------
 *
 * Before coding an UPDATE or DELETE row operation, the code-generator
 * for those two operations needs to know whether or not the operation
 * requires any FK processing and, if so, which columns of the original
 * row are required by the FK processing VDBE code (i.e. if FKs were
 * implemented using triggers, which of the old.* columns would be
 * accessed). No information is required by the code-generator before
 * coding an INSERT operation. The functions used by the UPDATE/DELETE
 * generation code to query for this information are:
 *
 *   fkey_is_required() - Test to see if FK processing is required.
 *   fkey_old_mask()  - Query for the set of required old.* columns.
 *
 *
 * Externally accessible module functions
 * --------------------------------------
 *
 *   sqlite3FkCheck()    - Check for foreign key violations.
 *   sqlite3FkActions()  - Code triggers for ON UPDATE/ON DELETE actions.
 *
 * VDBE Calling Convention
 * -----------------------
 *
 * Example:
 *
 *   For the following INSERT statement:
 *
 *     CREATE TABLE t1(a, b INTEGER PRIMARY KEY, c);
 *     INSERT INTO t1 VALUES(1, 2, 3.1);
 *
 *   Register (x):        2    (type integer)
 *   Register (x+1):      1    (type integer)
 *   Register (x+2):      NULL (type NULL)
 *   Register (x+3):      3.1  (type real)
 */

/**
 * This function is called when a row is inserted into or deleted
 * from the child table of foreign key constraint. If an SQL UPDATE
 * is executed on the child table of fkey, this function is invoked
 * twice for each row affected - once to "delete" the old row, and
 * then again to "insert" the new row.
 *
 * Each time it is called, this function generates VDBE code to
 * locate the row in the parent table that corresponds to the row
 * being inserted into or deleted from the child table. If the
 * parent row can be found, no special action is taken. Otherwise,
 * if the parent row can *not* be found in the parent table:
 *
 *   Operation | FK type   | Action taken
 *   ------------------------------------------------------------
 *   INSERT      immediate   Increment the "immediate constraint counter".
 *
 *   DELETE      immediate   Decrement the "immediate constraint counter".
 *
 *   INSERT      deferred    Increment the "deferred constraint counter".
 *
 *   DELETE      deferred    Decrement the "deferred constraint counter".
 *
 * These operations are identified in the comment at the top of
 * this file (fkey.c) as "I.1" and "D.1".
 *
 * @param parse_context Current parsing context.
 * @param parent Parent table of FK constraint.
 * @param fk_def FK constraint definition.
 * @param referenced_idx Id of referenced index.
 * @param reg_data Address of array containing child table row.
 * @param incr_count Increment constraint counter by this value.
 * @param is_ignore If true, pretend parent contains all NULLs.
 */
static void
fkey_lookup_parent(struct Parse *parse_context, struct space *parent,
		   struct fkey_def *fk_def, uint32_t referenced_idx,
		   int reg_data, int incr_count, bool is_ignore)
{
	struct Vdbe *v = sqlite3GetVdbe(parse_context);
	int cursor = parse_context->nTab - 1;
	int ok_label = sqlite3VdbeMakeLabel(v);
	/*
	 * If incr_count is less than zero, then check at runtime
	 * if there are any outstanding constraints to resolve.
	 * If there are not, there is no need to check if deleting
	 * this row resolves any outstanding violations.
	 *
	 * Check if any of the key columns in the child table row
	 * are NULL. If any are, then the constraint is considered
	 * satisfied. No need to search for a matching row in the
	 * parent table.
	 */
	if (incr_count < 0)
		sqlite3VdbeAddOp2(v, OP_FkIfZero, fk_def->is_deferred,
				  ok_label);

	for (uint32_t i = 0; i < fk_def->field_count; i++) {
		int iReg = fk_def->links[i].child_field + reg_data + 1;
		sqlite3VdbeAddOp2(v, OP_IsNull, iReg, ok_label);
	}
	if (is_ignore == 0) {
		uint32_t field_count = fk_def->field_count;
		int temp_regs = sqlite3GetTempRange(parse_context, field_count);
		int rec_reg = sqlite3GetTempReg(parse_context);
		uint32_t id =
			SQLITE_PAGENO_FROM_SPACEID_AND_INDEXID(fk_def->parent_id,
							       referenced_idx);
		vdbe_emit_open_cursor(parse_context, cursor, id, parent);
		for (uint32_t i = 0; i < field_count; ++i) {
			sqlite3VdbeAddOp2(v, OP_Copy,
					  fk_def->links[i].child_field + 1 +
					  reg_data, temp_regs + i);
		}
		/*
		 * If the parent table is the same as the child
		 * table, and we are about to increment the
		 * constraint-counter (i.e. this is an INSERT operation),
		 * then check if the row being inserted matches itself.
		 * If so, do not increment the constraint-counter.
		 *
		 * If any of the parent-key values are NULL, then
		 * the row cannot match itself. So set JUMPIFNULL
		 * to make sure we do the OP_Found if any of the
		 * parent-key values are NULL (at this point it
		 * is known that none of the child key values are).
		 */
		if (parent->def->id == fk_def->child_id && incr_count == 1) {
			int jump = sqlite3VdbeCurrentAddr(v) + field_count + 1;
			for (uint32_t i = 0; i < field_count; i++) {
				int child_col = fk_def->links[i].child_field +
						1 + reg_data;
				int parent_col = fk_def->links[i].parent_field +
						 1 + reg_data;
				sqlite3VdbeAddOp3(v, OP_Ne, child_col, jump,
						  parent_col);
				sqlite3VdbeChangeP5(v, SQLITE_JUMPIFNULL);
			}
			sqlite3VdbeGoto(v, ok_label);
		}
		struct index *idx = space_index(parent, referenced_idx);
		assert(idx != NULL);
		sqlite3VdbeAddOp4(v, OP_MakeRecord, temp_regs, field_count,
				  rec_reg, sql_index_affinity_str(v->db,
								 idx->def),
				  P4_DYNAMIC);
		sqlite3VdbeAddOp4Int(v, OP_Found, cursor, ok_label, rec_reg, 0);
		sqlite3ReleaseTempReg(parse_context, rec_reg);
		sqlite3ReleaseTempRange(parse_context, temp_regs, field_count);
	}
	struct session *user_session = current_session();
	if (!fk_def->is_deferred &&
	    !(user_session->sql_flags & SQLITE_DeferFKs) &&
	    !parse_context->pToplevel && !parse_context->isMultiWrite) {
		/*
		 * If this is an INSERT statement that will
		 * insert exactly one row into the table, raise
		 * a constraint immediately instead of incrementing
		 * a counter. This is necessary as the VM code is being
		 * generated for will not open a statement transaction.
		 */
		assert(incr_count == 1);
		sqlite3HaltConstraint(parse_context, SQLITE_CONSTRAINT_FOREIGNKEY,
				      ON_CONFLICT_ACTION_ABORT, 0, P4_STATIC,
				      P5_ConstraintFK);
	} else {
		if (incr_count > 0 && !fk_def->is_deferred)
			sqlite3MayAbort(parse_context);
		sqlite3VdbeAddOp2(v, OP_FkCounter, fk_def->is_deferred,
				  incr_count);
	}
	sqlite3VdbeResolveLabel(v, ok_label);
	sqlite3VdbeAddOp1(v, OP_Close, cursor);
}

/*
 * Return an Expr object that refers to a memory register corresponding
 * to column iCol of table pTab.
 *
 * regBase is the first of an array of register that contains the data
 * for pTab.  regBase+1 holds the first column.
 * regBase+2 holds the second column, and so forth.
 */
static Expr *
exprTableRegister(Parse * pParse,	/* Parsing and code generating context */
		  Table * pTab,	/* The table whose content is at r[regBase]... */
		  int regBase,	/* Contents of table pTab */
		  i16 iCol	/* Which column of pTab is desired */
    )
{
	Expr *pExpr;
	sqlite3 *db = pParse->db;

	pExpr = sqlite3Expr(db, TK_REGISTER, 0);
	if (pExpr) {
		if (iCol >= 0 && iCol != pTab->iPKey) {
			pExpr->iTable = regBase + iCol + 1;
			char affinity = pTab->def->fields[iCol].affinity;
			pExpr->affinity = affinity;
			pExpr = sqlite3ExprAddCollateString(pParse, pExpr,
							    "binary");
		} else {
			pExpr->iTable = regBase;
			pExpr->affinity = AFFINITY_INTEGER;
		}
	}
	return pExpr;
}

/**
 * Return an Expr object that refers to column of space_def which
 * has cursor cursor.
 * @param db The database connection.
 * @param def space definition.
 * @param cursor The open cursor on the table.
 * @param column The column that is wanted.
 * @retval not NULL on success.
 * @retval NULL on error.
 */
static Expr *
exprTableColumn(sqlite3 * db, struct space_def *def, int cursor, i16 column)
{
	Expr *pExpr = sqlite3Expr(db, TK_COLUMN, 0);
	if (pExpr) {
		pExpr->space_def = def;
		pExpr->iTable = cursor;
		pExpr->iColumn = column;
	}
	return pExpr;
}

/*
 * This function is called to generate code executed when a row is deleted
 * from the parent table of foreign key constraint pFKey and, if pFKey is
 * deferred, when a row is inserted into the same table. When generating
 * code for an SQL UPDATE operation, this function may be called twice -
 * once to "delete" the old row and once to "insert" the new row.
 *
 * Parameter nIncr is passed -1 when inserting a row (as this may decrease
 * the number of FK violations in the db) or +1 when deleting one (as this
 * may increase the number of FK constraint problems).
 *
 * The code generated by this function scans through the rows in the child
 * table that correspond to the parent table row being deleted or inserted.
 * For each child row found, one of the following actions is taken:
 *
 *   Operation | FK type   | Action taken
 *   --------------------------------------------------------------------------
 *   DELETE      immediate   Increment the "immediate constraint counter".
 *                           Or, if the ON (UPDATE|DELETE) action is RESTRICT,
 *                           throw a "FOREIGN KEY constraint failed" exception.
 *
 *   INSERT      immediate   Decrement the "immediate constraint counter".
 *
 *   DELETE      deferred    Increment the "deferred constraint counter".
 *                           Or, if the ON (UPDATE|DELETE) action is RESTRICT,
 *                           throw a "FOREIGN KEY constraint failed" exception.
 *
 *   INSERT      deferred    Decrement the "deferred constraint counter".
 *
 * These operations are identified in the comment at the top of this file
 * (fkey.c) as "I.2" and "D.2".
 */
static void
fkScanChildren(Parse * pParse,	/* Parse context */
	       SrcList * pSrc,	/* The child table to be scanned */
	       Table * pTab,	/* The parent table */
	       struct fkey_def *fkey,	/* The foreign key linking pSrc to pTab */
	       int regData,	/* Parent row data starts here */
	       int nIncr	/* Amount to increment deferred counter by */
    )
{
	sqlite3 *db = pParse->db;	/* Database handle */
	Expr *pWhere = 0;	/* WHERE clause to scan with */
	NameContext sNameContext;	/* Context used to resolve WHERE clause */
	WhereInfo *pWInfo;	/* Context used by sqlite3WhereXXX() */
	int iFkIfZero = 0;	/* Address of OP_FkIfZero */
	Vdbe *v = sqlite3GetVdbe(pParse);

	if (nIncr < 0) {
		iFkIfZero =
		    sqlite3VdbeAddOp2(v, OP_FkIfZero, fkey->is_deferred, 0);
		VdbeCoverage(v);
	}

	struct space *child_space = space_by_id(fkey->child_id);
	assert(child_space != NULL);
	/* Create an Expr object representing an SQL expression like:
	 *
	 *   <parent-key1> = <child-key1> AND <parent-key2> = <child-key2> ...
	 *
	 * The collation sequence used for the comparison should be that of
	 * the parent key columns. The affinity of the parent key column should
	 * be applied to each child key value before the comparison takes place.
	 */
	for (uint32_t i = 0; i < fkey->field_count; i++) {
		Expr *pLeft;	/* Value from parent table row */
		Expr *pRight;	/* Column ref to child table */
		Expr *pEq;	/* Expression (pLeft = pRight) */
		i16 iCol;	/* Index of column in child table */
		const char *zCol;	/* Name of column in child table */

		iCol = fkey->links[i].parent_field;
		pLeft = exprTableRegister(pParse, pTab, regData, iCol);
		iCol = fkey->links[i].child_field;
		assert(iCol >= 0);
		zCol = child_space->def->fields[iCol].name;
		pRight = sqlite3Expr(db, TK_ID, zCol);
		pEq = sqlite3PExpr(pParse, TK_EQ, pLeft, pRight);
		pWhere = sqlite3ExprAnd(db, pWhere, pEq);
	}

	/* If the child table is the same as the parent table, then add terms
	 * to the WHERE clause that prevent this entry from being scanned.
	 * The added WHERE clause terms are like this:
	 *
	 *     NOT( $current_a==a AND $current_b==b AND ... )
	 *     The primary key is (a,b,...)
	 */
	if (pTab->def->id == fkey->child_id && nIncr > 0) {
		Expr *pNe;	/* Expression (pLeft != pRight) */
		Expr *pLeft;	/* Value from parent table row */
		Expr *pRight;	/* Column ref to child table */

		Expr *pEq, *pAll = 0;
		for (uint32_t i = 0; i < fkey->field_count; i++) {
			i16 iCol = fkey->links[i].parent_field;
			assert(iCol >= 0);
			pLeft = exprTableRegister(pParse, pTab, regData, iCol);
			pRight = exprTableColumn(db, pTab->def,
						 pSrc->a[0].iCursor, iCol);
			pEq = sqlite3PExpr(pParse, TK_EQ, pLeft, pRight);
			pAll = sqlite3ExprAnd(db, pAll, pEq);
		}
		pNe = sqlite3PExpr(pParse, TK_NOT, pAll, 0);
		pWhere = sqlite3ExprAnd(db, pWhere, pNe);
	}

	/* Resolve the references in the WHERE clause. */
	memset(&sNameContext, 0, sizeof(NameContext));
	sNameContext.pSrcList = pSrc;
	sNameContext.pParse = pParse;
	sqlite3ResolveExprNames(&sNameContext, pWhere);

	/* Create VDBE to loop through the entries in pSrc that match the WHERE
	 * clause. For each row found, increment either the deferred or immediate
	 * foreign key constraint counter.
	 */
	pWInfo = sqlite3WhereBegin(pParse, pSrc, pWhere, 0, 0, 0, 0);
	sqlite3VdbeAddOp2(v, OP_FkCounter, fkey->is_deferred, nIncr);
	if (pWInfo) {
		sqlite3WhereEnd(pWInfo);
	}

	/* Clean up the WHERE clause constructed above. */
	sql_expr_delete(db, pWhere, false);
	if (iFkIfZero)
		sqlite3VdbeJumpHere(v, iFkIfZero);
}

/**
 * The second argument points to an fkey object representing
 * a foreign key. An UPDATE statement against child table is
 * currently being processed. For each column of the table that
 * is actually updated, the corresponding element in the changes
 * array is zero or greater (if a column is unmodified the
 * corresponding element is set to -1).
 *
 * @param fkey FK constraint definition.
 * @param changes Array indicating modified columns.
 * @retval true, if any of the columns that are part of the child
 *         key for FK constraint are modified.
 */
static bool
fkey_child_is_modified(const struct fkey_def *fkey, int *changes)
{
	for (uint32_t i = 0; i < fkey->field_count; ++i) {
		uint32_t child_key = fkey->links[i].child_field;
		if (changes[child_key] >= 0)
			return true;
	}
	return false;
}

/**
 * Works the same as fkey_child_is_modified(), but checks are
 * provided on parent table.
 *
 * @param fkey FK constraint definition.
 * @param changes Array indicating modified columns.
 * @retval true, if any of the columns that are part of the parent
 *         key for FK constraint are modified.
 */
static bool
fkey_parent_is_modified(const struct fkey_def *fkey, int *changes)
{
	for (uint32_t i = 0; i < fkey->field_count; i++) {
		uint32_t parent_key = fkey->links[i].parent_field;
		if (changes[parent_key] >= 0)
			return true;
	}
	return false;
}

/**
 * Return true if the parser passed as the first argument is
 * used to code a trigger that is really a "SET NULL" action.
 */
static bool
fkey_action_is_set_null(struct Parse *parse_context, const struct fkey *fkey)
{
	struct Parse *top_parse = sqlite3ParseToplevel(parse_context);
	if (top_parse->pTriggerPrg) {
		struct sql_trigger *trigger = top_parse->pTriggerPrg->trigger;
		if ((trigger == fkey->on_delete_trigger &&
		     fkey->def->on_delete == FKEY_ACTION_SET_NULL) ||
		    (trigger == fkey->on_update_trigger &&
		     fkey->def->on_update == FKEY_ACTION_SET_NULL)) {
			return true;
		}
	}
	return false;
}

/*
 * This function is called when inserting, deleting or updating a row of
 * table pTab to generate VDBE code to perform foreign key constraint
 * processing for the operation.
 *
 * For a DELETE operation, parameter regOld is passed the index of the
 * first register in an array of (pTab->nCol+1) registers containing the
 * PK of the row being deleted, followed by each of the column values
 * of the row being deleted, from left to right. Parameter regNew is passed
 * zero in this case.
 *
 * For an INSERT operation, regOld is passed zero and regNew is passed the
 * first register of an array of (pTab->nCol+1) registers containing the new
 * row data.
 *
 * For an UPDATE operation, this function is called twice. Once before
 * the original record is deleted from the table using the calling convention
 * described for DELETE. Then again after the original record is deleted
 * but before the new record is inserted using the INSERT convention.
 */
void
sqlite3FkCheck(Parse * pParse,	/* Parse context */
	       Table * pTab,	/* Row is being deleted from this table */
	       int regOld,	/* Previous row data is stored here */
	       int regNew,	/* New row data is stored here */
	       int *aChange	/* Array indicating UPDATEd columns (or 0) */
    )
{
	sqlite3 *db = pParse->db;	/* Database handle */
	struct session *user_session = current_session();

	/* Exactly one of regOld and regNew should be non-zero. */
	assert((regOld == 0) != (regNew == 0));

	/* If foreign-keys are disabled, this function is a no-op. */
	if ((user_session->sql_flags & SQLITE_ForeignKeys) == 0)
		return;

	/*
	 * Loop through all the foreign key constraints for which
	 * pTab is the child table.
	 */
	struct space *space = space_by_id(pTab->def->id);
	assert(space != NULL);
	for (struct fkey *fk = space->child_fkey; fk != NULL;
	     fk = fk->fkey_child_next) {
		struct fkey_def *fk_def = fk->def;
		int bIgnore = 0;
		if (aChange != NULL && space->def->id != fk_def->parent_id &&
		    !fkey_child_is_modified(fk_def, aChange))
			continue;
		pParse->nTab++;
		struct space *parent = space_by_id(fk_def->parent_id);
		assert(parent != NULL);
		if (regOld != 0) {
			/* A row is being removed from the child table. Search for the parent.
			 * If the parent does not exist, removing the child row resolves an
			 * outstanding foreign key constraint violation.
			 */
			fkey_lookup_parent(pParse, parent, fk_def, fk->index_id,
					   regOld, -1, bIgnore);
		}
		if (regNew != 0 && !fkey_action_is_set_null(pParse, fk)) {
			/* A row is being added to the child table. If a parent row cannot
			 * be found, adding the child row has violated the FK constraint.
			 *
			 * If this operation is being performed as part of a trigger program
			 * that is actually a "SET NULL" action belonging to this very
			 * foreign key, then omit this scan altogether. As all child key
			 * values are guaranteed to be NULL, it is not possible for adding
			 * this row to cause an FK violation.
			 */
			fkey_lookup_parent(pParse, parent, fk_def, fk->index_id,
					   regNew, +1, bIgnore);
		}
	}
	/*
	 * Loop through all the foreign key constraints that
	 * refer to this table.
	 */
	for (struct fkey *fk = space->parent_fkey; fk != NULL;
	     fk = fk->fkey_parent_next) {
		struct fkey_def *fk_def = fk->def;
		if (aChange != NULL &&
		    !fkey_parent_is_modified(fk_def, aChange))
			continue;
		if (!fk_def->is_deferred &&
		    !(user_session->sql_flags & SQLITE_DeferFKs) &&
		    !pParse->pToplevel && !pParse->isMultiWrite) {
			assert(regOld == 0 && regNew != 0);
			/* Inserting a single row into a parent table cannot cause (or fix)
			 * an immediate foreign key violation. So do nothing in this case.
			 */
			continue;
		}

		/* Create a SrcList structure containing the child table.  We need the
		 * child table as a SrcList for sqlite3WhereBegin()
		 */
		struct SrcList *pSrc = sqlite3SrcListAppend(db, 0, 0);
		if (pSrc != NULL) {
			struct SrcList_item *pItem = pSrc->a;
			struct space *child = space_by_id(fk->def->child_id);
			assert(child != NULL);
			struct Table *tab =
				sqlite3HashFind(&db->pSchema->tblHash,
						child->def->name);
			pItem->pTab = tab;
			pItem->zName = sqlite3DbStrDup(db, child->def->name);
			pItem->pTab->nTabRef++;
			pItem->iCursor = pParse->nTab++;

			if (regNew != 0) {
				fkScanChildren(pParse, pSrc, pTab, fk->def,
					       regNew, -1);
			}
			if (regOld != 0) {
				enum fkey_action action = fk_def->on_update;
				fkScanChildren(pParse, pSrc, pTab, fk->def,
					       regOld, 1);
				/* If this is a deferred FK constraint, or a CASCADE or SET NULL
				 * action applies, then any foreign key violations caused by
				 * removing the parent key will be rectified by the action trigger.
				 * So do not set the "may-abort" flag in this case.
				 *
				 * Note 1: If the FK is declared "ON UPDATE CASCADE", then the
				 * may-abort flag will eventually be set on this statement anyway
				 * (when this function is called as part of processing the UPDATE
				 * within the action trigger).
				 *
				 * Note 2: At first glance it may seem like SQLite could simply omit
				 * all OP_FkCounter related scans when either CASCADE or SET NULL
				 * applies. The trouble starts if the CASCADE or SET NULL action
				 * trigger causes other triggers or action rules attached to the
				 * child table to fire. In these cases the fk constraint counters
				 * might be set incorrectly if any OP_FkCounter related scans are
				 * omitted.
				 */
				if (!fk_def->is_deferred &&
				    action != FKEY_ACTION_CASCADE &&
				    action != FKEY_ACTION_SET_NULL) {
					sqlite3MayAbort(pParse);
				}
			}
			sqlite3SrcListDelete(db, pSrc);
		}
	}
}

#define COLUMN_MASK(x) (((x)>31) ? 0xffffffff : ((u32)1<<(x)))

uint32_t
fkey_old_mask(uint32_t space_id)
{
	uint32_t mask = 0;
	struct session *user_session = current_session();
	if (user_session->sql_flags & SQLITE_ForeignKeys) {
		struct space *space = space_by_id(space_id);
		for (struct fkey *fk = space->child_fkey; fk != NULL;
		     fk = fk->fkey_child_next) {
			struct fkey_def *def = fk->def;
			for (uint32_t i = 0; i < def->field_count; ++i)
				mask |=COLUMN_MASK(def->links[i].child_field);
		}
		for (struct fkey *fk = space->parent_fkey; fk != NULL;
		     fk = fk->fkey_parent_next) {
			struct fkey_def *def = fk->def;
			for (uint32_t i = 0; i < def->field_count; ++i)
				mask |= COLUMN_MASK(def->links[i].parent_field);
		}
	}
	return mask;
}

bool
fkey_is_required(uint32_t space_id, int *changes)
{
	struct session *user_session = current_session();
	if (user_session->sql_flags & SQLITE_ForeignKeys) {
		struct space *space = space_by_id(space_id);
		if (changes == NULL) {
			/*
			 * A DELETE operation. FK processing is
			 * required if space is child or parent.
			 */
			return space->parent_fkey != NULL ||
			       space->child_fkey != NULL;
		} else {
			/*
			 * This is an UPDATE. FK processing is
			 * only required if the operation modifies
			 * one or more child or parent key columns.
			 */
			for (struct fkey *p = space->child_fkey; p != NULL;
			     p = p->fkey_child_next) {
				if (fkey_child_is_modified(p->def, changes))
					return true;
			}
			for (struct fkey *p = space->parent_fkey; p != NULL;
			     p = p->fkey_parent_next) {
				if (fkey_parent_is_modified(p->def, changes))
					return true;
			}
		}
	}
	return false;
}

/**
 * This function is called when an UPDATE or DELETE operation is
 * being compiled on table pTab, which is the parent table of
 * foreign-key pFKey.
 * If the current operation is an UPDATE, then the pChanges
 * parameter is passed a pointer to the list of columns being
 * modified. If it is a DELETE, pChanges is passed a NULL pointer.
 *
 * It returns a pointer to a sql_trigger structure containing a
 * trigger equivalent to the ON UPDATE or ON DELETE action
 * specified by pFKey.
 * If the action is "NO ACTION" or "RESTRICT", then a NULL pointer
 * is returned (these actions require no special handling by the
 * triggers sub-system, code for them is created by
 * fkScanChildren()).
 *
 * For example, if pFKey is the foreign key and pTab is table "p"
 * in the following schema:
 *
 *   CREATE TABLE p(pk PRIMARY KEY);
 *   CREATE TABLE c(ck REFERENCES p ON DELETE CASCADE);
 *
 * then the returned trigger structure is equivalent to:
 *
 *   CREATE TRIGGER ... DELETE ON p BEGIN
 *     DELETE FROM c WHERE ck = old.pk;
 *   END;
 *
 * The returned pointer is cached as part of the foreign key
 * object. It is eventually freed along with the rest of the
 * foreign key object by sqlite3FkDelete().
 *
 * @param pParse Parse context.
 * @param pTab Table being updated or deleted from.
 * @param pFKey Foreign key to get action for.
 * @param pChanges Change-list for UPDATE, NULL for DELETE.
 *
 * @retval not NULL on success.
 * @retval NULL on failure.
 */
static struct sql_trigger *
fkActionTrigger(struct Parse *pParse, struct Table *pTab, struct fkey *fkey,
		struct ExprList *pChanges)
{
	sqlite3 *db = pParse->db;	/* Database handle */
	struct session *user_session = current_session();
	bool is_update = pChanges != NULL;
	struct fkey_def *fk_def = fkey->def;
	enum fkey_action action = is_update ? fk_def->on_update :
					      fk_def->on_delete;
	if (action == FKEY_ACTION_RESTRICT &&
	    (user_session->sql_flags & SQLITE_DeferFKs))
		return NULL;
	struct sql_trigger *trigger = is_update ? fkey->on_update_trigger :
						  fkey->on_delete_trigger;
	if (action != FKEY_NO_ACTION && trigger == NULL) {
		TriggerStep *pStep = 0;	/* First (only) step of trigger program */
		Expr *pWhere = 0;	/* WHERE clause of trigger step */
		ExprList *pList = 0;	/* Changes list if ON UPDATE CASCADE */
		Select *pSelect = 0;	/* If RESTRICT, "SELECT RAISE(...)" */
		Expr *pWhen = 0;	/* WHEN clause for the trigger */
		struct space *child_space = space_by_id(fk_def->child_id);
		assert(child_space != NULL);
		for (uint32_t i = 0; i < fk_def->field_count; ++i) {
			Token tOld = { "old", 3, false };	/* Literal "old" token */
			Token tNew = { "new", 3, false };	/* Literal "new" token */
			Token tFromCol;	/* Name of column in child table */
			Token tToCol;	/* Name of column in parent table */
			int iFromCol;	/* Idx of column in child table */
			Expr *pEq;	/* tFromCol = OLD.tToCol */

			iFromCol = fk_def->links[i].child_field;
			sqlite3TokenInit(&tToCol,
					 pTab->def->fields[fk_def->links[i].parent_field].name);

			sqlite3TokenInit(&tFromCol,
					 child_space->def->fields[iFromCol].name);

			/* Create the expression "OLD.zToCol = zFromCol". It is important
			 * that the "OLD.zToCol" term is on the LHS of the = operator, so
			 * that the affinity and collation sequence associated with the
			 * parent table are used for the comparison.
			 */
			pEq = sqlite3PExpr(pParse, TK_EQ,
					   sqlite3PExpr(pParse, TK_DOT,
							sqlite3ExprAlloc(db,
									 TK_ID,
									 &tOld,
									 0),
							sqlite3ExprAlloc(db,
									 TK_ID,
									 &tToCol,
									 0)),
					   sqlite3ExprAlloc(db, TK_ID,
							    &tFromCol, 0)
			    );
			pWhere = sqlite3ExprAnd(db, pWhere, pEq);

			/* For ON UPDATE, construct the next term of the WHEN clause.
			 * The final WHEN clause will be like this:
			 *
			 *    WHEN NOT(old.col1 = new.col1 AND ... AND old.colN = new.colN)
			 */
			if (pChanges) {
				pEq = sqlite3PExpr(pParse, TK_EQ,
						   sqlite3PExpr(pParse, TK_DOT,
								sqlite3ExprAlloc
								(db, TK_ID,
								 &tOld, 0),
								sqlite3ExprAlloc
								(db, TK_ID,
								 &tToCol, 0)),
						   sqlite3PExpr(pParse, TK_DOT,
								sqlite3ExprAlloc
								(db, TK_ID,
								 &tNew, 0),
								sqlite3ExprAlloc
								(db, TK_ID,
								 &tToCol, 0))
				    );
				pWhen = sqlite3ExprAnd(db, pWhen, pEq);
			}

			if (action != FKEY_ACTION_RESTRICT
			    && (action != FKEY_ACTION_CASCADE || pChanges)) {
				Expr *pNew;
				if (action == FKEY_ACTION_CASCADE) {
					pNew = sqlite3PExpr(pParse, TK_DOT,
							    sqlite3ExprAlloc(db,
									     TK_ID,
									     &tNew,
									     0),
							    sqlite3ExprAlloc(db,
									     TK_ID,
									     &tToCol,
									     0));
				} else if (action == FKEY_ACTION_SET_DEFAULT) {
					uint32_t space_id = fk_def->child_id;
					Expr *pDflt =
						space_column_default_expr(
							space_id, (uint32_t)iFromCol);
					if (pDflt) {
						pNew =
						    sqlite3ExprDup(db, pDflt,
								   0);
					} else {
						pNew =
						    sqlite3ExprAlloc(db,
								     TK_NULL, 0,
								     0);
					}
				} else {
					pNew =
					    sqlite3ExprAlloc(db, TK_NULL, 0, 0);
				}
				pList =
				    sql_expr_list_append(pParse->db, pList,
							 pNew);
				sqlite3ExprListSetName(pParse, pList, &tFromCol,
						       0);
			}
		}

		const char *zFrom = child_space->def->name;
		uint32_t nFrom = sqlite3Strlen30(zFrom);

		if (action == FKEY_ACTION_RESTRICT) {
			Token tFrom;
			Expr *pRaise;

			tFrom.z = zFrom;
			tFrom.n = nFrom;
			pRaise =
			    sqlite3Expr(db, TK_RAISE,
					"FOREIGN KEY constraint failed");
			if (pRaise) {
				pRaise->affinity = ON_CONFLICT_ACTION_ABORT;
			}
			pSelect = sqlite3SelectNew(pParse,
						   sql_expr_list_append(pParse->db,
									NULL,
									pRaise),
						   sqlite3SrcListAppend(db, 0,
									&tFrom),
						   pWhere, 0, 0, 0, 0, 0, 0);
			pWhere = 0;
		}

		/* Disable lookaside memory allocation */
		db->lookaside.bDisable++;
		size_t trigger_size = sizeof(struct sql_trigger) +
				      sizeof(TriggerStep) + nFrom + 1;
		trigger =
			(struct sql_trigger *)sqlite3DbMallocZero(db,
								  trigger_size);
		if (trigger != NULL) {
			pStep = trigger->step_list = (TriggerStep *)&trigger[1];
			pStep->zTarget = (char *)&pStep[1];
			memcpy((char *)pStep->zTarget, zFrom, nFrom);

			pStep->pWhere =
			    sqlite3ExprDup(db, pWhere, EXPRDUP_REDUCE);
			pStep->pExprList =
			    sql_expr_list_dup(db, pList, EXPRDUP_REDUCE);
			pStep->pSelect =
			    sqlite3SelectDup(db, pSelect, EXPRDUP_REDUCE);
			if (pWhen) {
				pWhen = sqlite3PExpr(pParse, TK_NOT, pWhen, 0);
				trigger->pWhen =
				    sqlite3ExprDup(db, pWhen, EXPRDUP_REDUCE);
			}
		}

		/* Re-enable the lookaside buffer, if it was disabled earlier. */
		db->lookaside.bDisable--;

		sql_expr_delete(db, pWhere, false);
		sql_expr_delete(db, pWhen, false);
		sql_expr_list_delete(db, pList);
		sql_select_delete(db, pSelect);
		if (db->mallocFailed == 1) {
			fkey_trigger_delete(db, trigger);
			return 0;
		}
		assert(pStep != 0);

		switch (action) {
		case FKEY_ACTION_RESTRICT:
			pStep->op = TK_SELECT;
			break;
		case FKEY_ACTION_CASCADE:
			if (pChanges == NULL) {
				pStep->op = TK_DELETE;
				break;
			}
			FALLTHROUGH;
		default:
			pStep->op = TK_UPDATE;
		}

		pStep->trigger = trigger;
		if (is_update)
			fkey->on_update_trigger = trigger;
		else
			fkey->on_delete_trigger = trigger;
		trigger->op = (pChanges ? TK_UPDATE : TK_DELETE);
	}

	return trigger;
}

/*
 * This function is called when deleting or updating a row to implement
 * any required CASCADE, SET NULL or SET DEFAULT actions.
 */
void
sqlite3FkActions(Parse * pParse,	/* Parse context */
		 Table * pTab,	/* Table being updated or deleted from */
		 ExprList * pChanges,	/* Change-list for UPDATE, NULL for DELETE */
		 int regOld,	/* Address of array containing old row */
		 int *aChange	/* Array indicating UPDATEd columns (or 0) */
    )
{
	struct session *user_session = current_session();
	/* If foreign-key support is enabled, iterate through all FKs that
	 * refer to table pTab. If there is an action associated with the FK
	 * for this operation (either update or delete), invoke the associated
	 * trigger sub-program.
	 */
	if (user_session->sql_flags & SQLITE_ForeignKeys) {
		struct space *space = space_by_id(pTab->def->id);
		assert(space != NULL);
		for (struct fkey *fkey = space->parent_fkey; fkey != NULL;
		     fkey = fkey->fkey_parent_next) {
			if (aChange == 0 ||
			    fkey_parent_is_modified(fkey->def, aChange)) {
				struct sql_trigger *pAct =
					fkActionTrigger(pParse, pTab, fkey,
							pChanges);
				if (pAct == NULL)
					continue;
				vdbe_code_row_trigger_direct(pParse, pAct, pTab,
							     regOld,
							     ON_CONFLICT_ACTION_ABORT,
							     0);
			}
		}
	}
}

#endif				/* ifndef SQLITE_OMIT_TRIGGER */
