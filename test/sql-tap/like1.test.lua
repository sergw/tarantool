#!/usr/bin/env tarantool
test = require("sqltester")
test:plan(16)

test:do_catchsql_test(
	"like-test-1.1",
	[[
		CREATE TABLE t2 (column1 INTEGER,
				     column2 VARCHAR(100),
				     column3 BLOB,
				     column4 FLOAT,
				     PRIMARY KEY (column1, column2));
		INSERT INTO t2 VALUES (1, 'AB', X'4142', 5.5);
		INSERT INTO t2 VALUES (1, 'CD', X'2020', 1E4);
		INSERT INTO t2 VALUES (2, 'AB', X'2020', 12.34567);
		INSERT INTO t2 VALUES (-1000, '', X'', 0.0);
		CREATE TABLE t1 (a INT PRIMARY KEY, str VARCHAR(100));
		INSERT INTO t1 VALUES (1, 'ab');
		INSERT INTO t1 VALUES (2, 'abCDF');
		INSERT INTO t1 VALUES (3, 'CDF');
		CREATE TABLE t (s1 CHAR(2) PRIMARY KEY, s2 CHAR(2));
		INSERT INTO t VALUES ('AB', 'AB');
	]], {
		-- <like-test-1.1>
		0
		-- <like-test-1.1>
	})

test:do_execsql_test(
	"like-test-1.2",
	[[
		SELECT column1, column2, column1 * column4 FROM
			t2 WHERE column2 LIKE '_B';
	]], {
		-- <like-test-1.2>
		1, 'AB', 5.5, 2, 'AB', 24.69134
		-- <like-test-1.2>
	})

test:do_execsql_test(
	"like-test-1.3",
	[[
		SELECT column1, column2 FROM t2 WHERE column2 LIKE '%B';
	]], {
             -- <like-test-1.3>
             1, 'AB', 2, 'AB'
             -- <like-test-1.3>
	})

test:do_execsql_test(
	"like-test-1.4",
	[[
		SELECT column1, column2 FROM t2 WHERE column2 LIKE 'A__';
	]], {
             -- <like-test-1.4>

             -- <like-test-1.4>
	})

test:do_execsql_test(
	"like-test-1.5",
	[[
		SELECT column1, column2 FROM t2 WHERE column2 LIKE 'A_';
	]], {
             -- <like-test-1.5>
             1, 'AB', 2, 'AB'
             -- <like-test-1.5>
	})

test:do_execsql_test(
	"like-test-1.6",
	[[
		SELECT column1, column2 FROM t2 WHERE column2 LIKE 'A';
	]], {
             -- <like-test-1.6>

             -- <like-test-1.6>
	})

test:do_execsql_test(
	"like-test-1.7",
	[[
		SELECT column1, column2 FROM t2 WHERE column2 LIKE '_';
	]], {
             -- <like-test-1.7>

             -- <like-test-1.7>
	})

test:do_execsql_test(
	"like-test-1.8",
	[[
		SELECT * FROM t WHERE s1 LIKE '%A';
	]], {
             -- <like-test-1.8>

             -- <like-test-1.8>
	})

test:do_execsql_test(
	"like-test-1.9",
	[[
		SELECT * FROM t WHERE s1 LIKE '%C';
	]], {
             -- <like-test-1.9>

             -- <like-test-1.9>
	})

test:do_execsql_test(
	"like-test-1.10",
	[[
		SELECT * FROM t1 WHERE str LIKE '%df';
	]], {
             -- <like-test-1.10>
             2, 'abCDF', 3, 'CDF'
             -- <like-test-1.10>
	})

test:do_execsql_test(
	"like-test-1.11",
	[[
		SELECT * FROM t1 WHERE str LIKE 'a_';
	]], {
             -- <like-test-1.11>
             1, 'ab'
             -- <like-test-1.11>
	})

test:do_execsql_test(
	"like-test-1.12",
	[[
		SELECT column1, column2 FROM t2 WHERE column2 LIKE '__';
	]], {
             -- <like-test-1.12>
             1, 'AB', 1, 'CD', 2, 'AB'
             -- <like-test-1.12>
	})

test:do_execsql_test(
	"like-test-1.13",
	[[
		SELECT str FROM t1 WHERE str LIKE 'ab%';
	]], {
		-- <like-test-1.13>
		'ab', 'abCDF'
		-- <like-test-1.13>
	})

test:do_execsql_test(
	"like-test-1.14",
	[[
		SELECT str FROM t1 WHERE str LIKE 'abC%';
	]], {
		-- <like-test-1.14>
		'abCDF'
		-- <like-test-1.14>
	})

test:do_execsql_test(
	"like-test-1.15",
	[[
		SELECT str FROM t1 WHERE str LIKE 'a_%';
	]], {
		-- <like-test-1.15>
		'ab', 'abCDF'
		-- <like-test-1.15>
	})

test:do_execsql_test(
	"like-test-1.16",
	[[
		DROP TABLE t1;
		DROP TABLE t2;
		DROP TABLE t;
	]], {
             -- <like-test-1.16>

             -- <like-test-1.16>
	})

test:finish_test()
