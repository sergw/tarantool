remote = require('net.box')
test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.sql.execute('pragma sql_default_engine=\''..engine..'\'')

-- gh-3010: COLLATE after LIMIT should throw an error

-- All of these tests should throw error "near "COLLATE": syntax error"
box.sql.execute("SELECT 1 LIMIT 1 COLLATE BINARY;")
box.sql.execute("SELECT 1 LIMIT 1 COLLATE BINARY OFFSET 1;")
box.sql.execute("SELECT 1 LIMIT 1 OFFSET 1 COLLATE BINARY;")
box.sql.execute("SELECT 1 LIMIT 1, 1 COLLATE BINARY;")
box.sql.execute("SELECT 1 LIMIT 1 COLLATE BINARY, 1;")


box.schema.user.grant('guest','read,write,execute', 'universe')
cn = remote.connect(box.cfg.listen)

cn:execute('select 1 limit ? collate not_exist', {1})

cn:close()
box.schema.user.revoke('guest', 'read,write,execute', 'universe')
