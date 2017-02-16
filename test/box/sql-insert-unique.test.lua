test_run = require('test_run').new()

test_run:cmd("setopt delimiter ';;'")

ffi = require "ffi"
ffi.cdef[[
    int sql_schema_put(int, int, const char**);
    void free(void *);
]]

-- Manually feed in data in sqlite_master row format.
-- Populate schema objects, make it possible to query
-- Tarantool spaces with SQL.
function sql_schema_put(idb, ...)
    local argc = select('#', ...)
    local argv, cargv = {}, ffi.new('const char*[?]', argc+1)
    for i = 0,argc-1 do
        local v = tostring(select(i+1, ...))
        argv[i] = v
        cargv[i] = v
    end
    cargv[argc] = nil
    local rc = ffi.C.sql_schema_put(idb, argc, cargv);
    local err_msg
    if cargv[0] ~= nil then
        err_msg = ffi.string(cargv[0])
        ffi.C.free(ffi.cast('void *', cargv[0]))
    end
    return rc, err_msg
end

function sql_pageno(space_id, index_id)
    return space_id * 32 + index_id
end

test_run:cmd("setopt delimiter ''");;

-- box.cfg()

-- create space
zoobar = box.schema.space.create("zoobar")
_ = zoobar:create_index("primary",{parts={2,"number"}})
_ = zoobar:create_index("secondary", {parts={1, 'number',  4, 'number'}})

zoobar_pageno = sql_pageno(zoobar.id, zoobar.index.primary.id)
zoobar2_pageno = sql_pageno(zoobar.id, zoobar.index.secondary.id)

sql_schema_put(0, "zoobar"                   , zoobar_pageno , "CREATE TABLE zoobar (c1, c2 PRIMARY KEY, c3, c4) WITHOUT ROWID")
sql_schema_put(0, "sqlite_autoindex_zoobar_1", zoobar_pageno , "")
sql_schema_put(0, "zoobar2"                  , zoobar2_pageno, "CREATE UNIQUE INDEX zoobar2 ON zoobar(c1, c4)")

-- For debug
-- box.sql.execute("PRAGMA vdbe_debug=ON ; INSERT INTO zoobar VALUES (111, 222, 'c3', 444)")

-- Seed entry
box.sql.execute("INSERT INTO zoobar VALUES (111, 222, 'c3', 444)")

-- PK must be unique
box.sql.execute("INSERT INTO zoobar VALUES (112, 222, 'c3', 444)")

-- Unique index must be respected
box.sql.execute("INSERT INTO zoobar VALUES (111, 223, 'c3', 444)")

-- cleanup
zoobar:drop()

-- For debug
-- require("console").start()