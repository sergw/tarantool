#!/usr/bin/env tarantool

local INSTANCE_ID = string.match(arg[0], "%d")
local SOCKET_DIR = require('fio').cwd()
local read_only = INSTANCE_ID ~= '1'
local function instance_uri(instance_id)
    return SOCKET_DIR..'/promote'..instance_id..'.sock';
end
local uuid_prefix = '4d71c17c-8c50-11e8-9eb6-529269fb145'
local uuid_to_name = {}
for i = 1, 4 do
    local uuid = uuid_prefix..tostring(i)
    uuid_to_name[uuid] = 'box'..tostring(i)
end
require('console').listen(os.getenv('ADMIN'))
box.cfg({
    listen = instance_uri(INSTANCE_ID),
    replication = {instance_uri(1), instance_uri(2),
                   instance_uri(3), instance_uri(4)},
    read_only = read_only,
    replication_connect_timeout = 0.1,
    replication_timeout = 0.1,
    instance_uuid = uuid_prefix..tostring(INSTANCE_ID),
})

function promotion_history()
    local ret = {}
    local round_uuid_i = 0
    local prev_round_uuid
    for i, t in box.space._promotion:pairs() do
        t = setmetatable(t:tomap({names_only = true}), {__serialize = 'map'})
        if t.round_uuid ~= prev_round_uuid then
            prev_round_uuid = t.round_uuid
            round_uuid_i = round_uuid_i + 1
        end
        t.round_uuid = 'round_'..round_uuid_i
        t.source_uuid = uuid_to_name[t.source_uuid]
        t.ts = nil
        if t.value == box.NULL then
            t.value = nil
        end
        table.insert(ret, t)
    end
    return ret
end

box.once("bootstrap", function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)
