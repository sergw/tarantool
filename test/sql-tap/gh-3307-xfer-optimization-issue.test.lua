#!/usr/bin/env tarantool
test = require("sqltester")
test:plan(46)

local bfr, aftr

local function do_xfer_test(test_number, return_code)
	test_name = string.format("xfer-optimization-1.%d", test_number)
	test:do_test(
		test_name,
		function()
			return {aftr - bfr}
		end, {
			-- <test_name>
			return_code
			-- <test_name>
		})
end

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.1",
	[[
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER UNIQUE);
		INSERT INTO t1 VALUES (1, 1), (2, 2), (3, 3);
		CREATE TABLE t2(a INTEGER PRIMARY KEY, b INTEGER UNIQUE);
		INSERT INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.1>
		0
		-- <xfer-optimization-1.1>
	})

aftr = box.sql.debug().sql_xfer_count

test:do_execsql_test(
	"xfer-optimization-1.2",
	[[
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.2>
		1, 1, 2, 2, 3, 3
		-- <xfer-optimization-1.2>
	})

do_xfer_test(3, 1)

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.4",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(id INTEGER PRIMARY KEY, b INTEGER);
		CREATE TABLE t2(id INTEGER PRIMARY KEY, b INTEGER);
		CREATE INDEX i1 ON t1(b);
		CREATE INDEX i2 ON t2(b);
		INSERT INTO t1 VALUES (1, 1), (2, 2), (3, 3);
		INSERT INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.4>
		0
		-- <xfer-optimization-1.4>
	})

aftr = box.sql.debug().sql_xfer_count

test:do_execsql_test(
	"xfer-optimization-1.5",
	[[
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.5>
		1, 1, 2, 2, 3, 3
		-- <xfer-optimization-1.5>
	})

do_xfer_test(6, 1)

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.7",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, c INTEGER);
		INSERT INTO t1 VALUES (1, 1, 2), (2, 2, 3), (3, 3, 4);
		CREATE TABLE t2(a INTEGER PRIMARY KEY, b INTEGER);
		INSERT INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.7>
		1, "table T2 has 2 columns but 3 values were supplied"
		-- <xfer-optimization-1.7>
	})

aftr = box.sql.debug().sql_xfer_count

test:do_execsql_test(
	"xfer-optimization-1.8",
	[[
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.8>

		-- <xfer-optimization-1.8>
	})

do_xfer_test(9, 0)

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.10",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER);
		INSERT INTO t1 VALUES (1, 1), (2, 2), (3, 3);
		CREATE TABLE t2(a INTEGER PRIMARY KEY, b INTEGER);
		INSERT INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.10>
		0
		-- <xfer-optimization-1.10>
	})

aftr = box.sql.debug().sql_xfer_count

test:do_execsql_test(
	"xfer-optimization-1.11",
	[[
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.11>
		1, 1, 2, 2, 3, 3
		-- <xfer-optimization-1.11>
	})

do_xfer_test(12, 1);

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.13",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER);
		INSERT INTO t1 VALUES (1, 1), (2, 2), (3, 2);
		CREATE TABLE t2(b INTEGER, a INTEGER PRIMARY KEY);
		INSERT INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.13>
		1, "Duplicate key exists in unique index 'sqlite_autoindex_T2_1' in space 'T2'"
		-- <xfer-optimization-1.13>
	})

aftr = box.sql.debug().sql_xfer_count

test:do_execsql_test(
	"xfer-optimization-1.14",
	[[
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.14>

		-- <xfer-optimization-1.14>
	})

do_xfer_test(15, 0)

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.16",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER);
		INSERT INTO t1 VALUES (1, 1), (2, 2), (3, 2);
		CREATE TABLE t2(b INTEGER PRIMARY KEY, a INTEGER);
		INSERT INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.16>
		0
		-- <xfer-optimization-1.16>
	})

aftr = box.sql.debug().sql_xfer_count

test:do_execsql_test(
	"xfer-optimization-1.17",
	[[
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.17>
		1, 1, 2, 2, 3, 2
		-- <xfer-optimization-1.17>
	})

do_xfer_test(18, 1)

-- The following tests are supposed to test if xfer-optimization is actually
-- used in the given cases (if the conflict actually occurs):
-- 	1.0) insert w/o explicit confl. action & w/o index replace action
-- 	1.1) insert w/o explicit confl. action & w/ index replace action &
--		empty dest_table
-- 	1.2) insert w/o explicit confl. action & w/ index replace action &
--		non-empty dest_table
-- 	2) insert with abort
-- 	3.0) insert with rollback (into empty table)
-- 	3.1) insert with rollback (into non-empty table)
-- 	4) insert with replace
-- 	5) insert with fail
-- 	6) insert with ignore


-- 1.0) insert w/o explicit confl. action & w/o index replace action
------------------------------------------------------------------------------

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.19",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
		CREATE TABLE t2(a INTEGER PRIMARY KEY, b);
		INSERT INTO t1 VALUES (1, 1), (3, 3), (5, 5);
		INSERT INTO t2 VALUES (2, 2), (3, 4);
		BEGIN;
			INSERT INTO t2 VALUES (4, 4);
			INSERT INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.19>
		1, "Duplicate key exists in unique index 'sqlite_autoindex_T2_1' in space 'T2'"
		-- <xfer-optimization-1.19>
	})

test:do_execsql_test(
	"xfer-optimization-1.20",
	[[
			INSERT INTO t2 VALUES (10, 10);
		COMMIT;
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.20>
		2, 2, 3, 4, 4, 4, 10, 10
		-- <xfer-optimization-1.20>
	})

aftr = box.sql.debug().sql_xfer_count

do_xfer_test(21, 0)

-- 1.1) insert w/o explicit confl. action & w/
--      index replace action & empty dest_table
------------------------------------------------------------------------------

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.22",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b);
		CREATE TABLE t2(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b);
		CREATE TABLE t3(id INT PRIMARY KEY);
		INSERT INTO t1 VALUES (1, 1), (3, 3), (5, 5);
		BEGIN;
			INSERT INTO t3 VALUES (1);
			INSERT INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.22>
		0
		-- <xfer-optimization-1.22>
	})

test:do_execsql_test(
	"xfer-optimization-1.23",
	[[
			INSERT INTO t2 VALUES (10, 10);
		COMMIT;
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.23>
		1, 1, 3, 3, 5, 5, 10, 10
		-- <xfer-optimization-1.23>
	})

aftr = box.sql.debug().sql_xfer_count

test:do_execsql_test(
	"xfer-optimization-1.24",
	[[
		SELECT * FROM t3;
	]], {
		-- <xfer-optimization-1.24>
		1
		-- <xfer-optimization-1.24>
	})

do_xfer_test(25, 1)

-- 1.2) insert w/o explicit confl. action & w/
-- index replace action & non-empty dest_table
------------------------------------------------------------------------------

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.26",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		DROP TABLE t3;
		CREATE TABLE t1(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b);
		CREATE TABLE t2(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b);
		INSERT INTO t1 VALUES (1, 1), (3, 3), (5, 5);
		INSERT INTO t2 VALUES (2, 2), (3, 4);
		BEGIN;
			INSERT INTO t2 VALUES (4, 4);
			INSERT INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.26>
		0
		-- <xfer-optimization-1.26>
	})

test:do_execsql_test(
	"xfer-optimization-1.27",
	[[
			INSERT INTO t2 VALUES (10, 10);
		COMMIT;
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.27>
		1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 10, 10
		-- <xfer-optimization-1.27>
	})

aftr = box.sql.debug().sql_xfer_count

do_xfer_test(28, 0)

-- 2) insert with abort
------------------------------------------------------------------------------

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.29",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
		CREATE TABLE t2(a INTEGER PRIMARY KEY, b);
		INSERT INTO t1 VALUES (1, 1), (3, 3), (5, 5);
		INSERT INTO t2 VALUES (2, 2), (3, 4);
		BEGIN;
			INSERT INTO t2 VALUES (4, 4);
			INSERT OR ABORT INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.29>
		1, "Duplicate key exists in unique index 'sqlite_autoindex_T2_1' in space 'T2'"
		-- <xfer-optimization-1.29>
	})

test:do_execsql_test(
	"xfer-optimization-1.30",
	[[
			INSERT INTO t2 VALUES (10, 10);
		COMMIT;
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.30>
		2, 2, 3, 4, 4, 4, 10, 10
		-- <xfer-optimization-1.30>
	})

aftr = box.sql.debug().sql_xfer_count

do_xfer_test(31, 1)

-- 3.0) insert with rollback (into empty table)
------------------------------------------------------------------------------

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.32",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
		CREATE TABLE t2(a INTEGER PRIMARY KEY, b);
		INSERT INTO t1 VALUES (1, 1), (3, 3), (5, 5);
		BEGIN;
			INSERT OR ROLLBACK INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.32>
		0
		-- <xfer-optimization-1.32>
	})

test:do_execsql_test(
	"xfer-optimization-1.33",
	[[
			INSERT INTO t2 VALUES (10, 10);
		COMMIT;
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.33>
		1, 1, 3, 3, 5, 5, 10, 10
		-- <xfer-optimization-1.33>
	})

aftr = box.sql.debug().sql_xfer_count

do_xfer_test(34, 1)

-- 3.1) insert with rollback (into non-empty table)
------------------------------------------------------------------------------

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.35",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
		CREATE TABLE t2(a INTEGER PRIMARY KEY, b);
		INSERT INTO t1 VALUES (1, 1), (3, 3), (5, 5);
		INSERT INTO t2 VALUES (2, 2), (3, 4);
		BEGIN;
			INSERT INTO t2 VALUES (4, 4);
			INSERT OR ROLLBACK INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.35>
		1, "UNIQUE constraint failed: T2.A"
		-- <xfer-optimization-1.35>
	})

test:do_execsql_test(
	"xfer-optimization-1.36",
	[[
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.36>
		2, 2, 3, 4
		-- <xfer-optimization-1.36>
	})

aftr = box.sql.debug().sql_xfer_count

do_xfer_test(37, 0)

-- 4) insert with replace
------------------------------------------------------------------------------

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.38",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
		CREATE TABLE t2(a INTEGER PRIMARY KEY, b);
		INSERT INTO t1 VALUES (1, 1), (3, 3), (5, 5);
		INSERT INTO t2 VALUES (2, 2), (3, 4);
		BEGIN;
			INSERT INTO t2 VALUES (4, 4);
			INSERT OR REPLACE INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.38>
		0
		-- <xfer-optimization-1.38>
	})

test:do_execsql_test(
	"xfer-optimization-1.39",
	[[
			INSERT INTO t2 VALUES (10, 10);
		COMMIT;
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.39>
		1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 10, 10
		-- <xfer-optimization-1.39>
	})

aftr = box.sql.debug().sql_xfer_count

do_xfer_test(40, 0)

-- 5) insert with fail
------------------------------------------------------------------------------

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.41",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
		CREATE TABLE t2(a INTEGER PRIMARY KEY, b);
		INSERT INTO t1 VALUES (1, 1), (3, 3), (5, 5);
		INSERT INTO t2 VALUES (2, 2), (3, 4);
		BEGIN;
			INSERT INTO t2 VALUES (4, 4);
			INSERT OR FAIL INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.41>
		1, "Duplicate key exists in unique index 'sqlite_autoindex_T2_1' in space 'T2'"
		-- <xfer-optimization-1.41>
	})

test:do_execsql_test(
	"xfer-optimization-1.42",
	[[
			INSERT INTO t2 VALUES (10, 10);
		COMMIT;
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.42>
		1, 1, 2, 2, 3, 4, 4, 4, 10, 10
		-- <xfer-optimization-1.42>
	})

aftr = box.sql.debug().sql_xfer_count

do_xfer_test(43, 0)

-- 6) insert with ignore
------------------------------------------------------------------------------

bfr = box.sql.debug().sql_xfer_count

test:do_catchsql_test(
	"xfer-optimization-1.44",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
		CREATE TABLE t2(a INTEGER PRIMARY KEY, b);
		INSERT INTO t1 VALUES (1, 1), (3, 3), (5, 5);
		INSERT INTO t2 VALUES (2, 2), (3, 4);
		BEGIN;
			INSERT INTO t2 VALUES (4, 4);
			INSERT OR IGNORE INTO t2 SELECT * FROM t1;
	]], {
		-- <xfer-optimization-1.44>
		0
		-- <xfer-optimization-1.44>
	})

test:do_execsql_test(
	"xfer-optimization-1.45",
	[[
			INSERT INTO t2 VALUES (10, 10);
		COMMIT;
		SELECT * FROM t2;
	]], {
		-- <xfer-optimization-1.45>
		1, 1, 2, 2, 3, 4, 4, 4, 5, 5, 10, 10
		-- <xfer-optimization-1.45>
	})

aftr = box.sql.debug().sql_xfer_count

do_xfer_test(46, 0)

test:finish_test()
