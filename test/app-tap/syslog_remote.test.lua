#!/usr/bin/env tarantool

local tap = require('tap')
local socket = require('socket')
local os = require('os')
local test = tap.test("syslog_remote")
local log = require('log')
local errno = require('errno')

local addr = '127.0.0.1'
local port = 1000 + math.random(32768)

test:plan(1)
local function start_server()
    test:diag("Starting server %s:%u", addr, port)
    local sc = socket('AF_INET', 'SOCK_DGRAM', 'udp')
    local attempt = 0
    while attempt < 10 do
        if not sc:bind (addr, port) then
	    port = 1000 + math.random(32768)
	    attempt = attempt + 1
	else
	    break
	    end
    end
    sc:bind(addr, port)
    `return sc
end

local function find_log_str()
	local opt = string.format("syslog:server=%s:%u,identity=tarantool", addr, port)
	local res = false
	box.cfg{log = opt, log_level = 5}
	log.info('Test syslog destination')
	while (sc:readable(1)) do
		local buf = sc:recv(1000)
		print (buf)
		res = buf:match('Test syslog destination')
	end
		test:ok(res, "syslog_remote")
end

sc = start_server()
test:test('syslog_remote',find_log_str(test))
sc:close()
os.exit(test:check() == true)
