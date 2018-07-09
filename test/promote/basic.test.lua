test_run = require('test_run').new()

test_run:create_cluster(CLUSTER, 'promote')
test_run:wait_fullmesh(CLUSTER)

--
-- Check the promote actually allows to switch the master.
--
test_run:switch('box1')
-- Box1 is a master.
box.cfg.read_only

test_run:switch('box2')
-- Box2 is a slave.
box.cfg.read_only
-- And can not do DDL/DML.
box.schema.create_space('test') -- Fail.
box.ctl.promote()
-- Now the slave has become a master.
box.cfg.read_only
-- And can do DDL/DML.
s = box.schema.create_space('test')
s:drop()

test_run:switch('box1')
-- In turn, the old master is a slave now.
box.cfg.read_only
-- For him any DDL/DML is forbidden.
box.schema.create_space('test2')

--
-- Check promotion history.
--
test_run:switch('box2')
promotion_history()

test_run:switch('default')

test_run:drop_cluster(CLUSTER)
