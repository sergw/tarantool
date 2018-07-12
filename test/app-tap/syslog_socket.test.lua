#!/usr/bin/env tarantool

local tap = require('tap')
local socket = require('socket')
local os = require('os')
local test = tap.test("syslog_unix")
local log = require('log')
local fio = require('fio')

path = fio.pathjoin(fio.cwd(), 'log_unix_socket_test.sock')

test:plan(1)
local function start_server()
    test:diag("Create unix socket at %s", path)
    local unix_socket = socket('AF_UNIX', 'SOCK_DGRAM',0)
    unix_socket:bind('unix/', path)
    socket.tcp_connect('unix', path)
    return unix_socket
end

local function find_log_str (test, socket)
	local buf
	local opt = string.format("syslog:server=unix:%s,identity=tarantool", path)
	print (opt)
	box.cfg{log = opt, log_level = 5}
	log.info("Test remote syslog destination")
	buf = socket:read("Test remote syslog destination", 30)
	if buf ~= nil then
		test:ok(buf:match('Test remote syslog destination'), "syslog_unix")
	else
		test:fail("syslog_unix")
	end
end

sock = start_server()

test:test('syslog_unix',find_log_str(test, sock))
sock:close()
os.remove(path)
os.exit(test:check() == true)
