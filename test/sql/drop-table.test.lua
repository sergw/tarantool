test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.sql.execute('pragma sql_default_engine=\''..engine..'\'')

-- box.cfg()

-- create space
box.sql.execute("CREATE TABLE zzzoobar (c1 INT, c2 INT PRIMARY KEY, c3 TEXT, c4 INT)")

-- Debug
-- box.sql.execute("PRAGMA vdbe_debug=ON ; INSERT INTO zzzoobar VALUES (111, 222, 'c3', 444)")

box.sql.execute("CREATE INDEX zb ON zzzoobar(c1, c3)")

-- Dummy entry
box.sql.execute("INSERT INTO zzzoobar VALUES (111, 222, 'c3', 444)")

box.sql.execute("DROP TABLE zzzoobar")

-- Table does not exist anymore. Should error here.
box.sql.execute("INSERT INTO zzzoobar VALUES (111, 222, 'c3', 444)")

-- Cleanup
-- DROP TABLE should do the job

-- Debug
-- require("console").start()
