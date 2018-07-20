/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
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
#include "box/lua/misc.h"

#include "fiber.h" /* fiber->gc() */
#include <small/region.h>
#include "lua/utils.h"
#include "lua/msgpack.h"

#include "box/box.h"
#include "box/port.h"
#include "box/lua/tuple.h"

static uint32_t CTID_STRUCT_SPACE_POINTER = 0;

/** {{{ Miscellaneous utils **/

char *
lbox_encode_tuple_on_gc(lua_State *L, int idx, size_t *p_len)
{
	struct region *gc = &fiber()->gc;
	size_t used = region_used(gc);
	struct mpstream stream;
	mpstream_init(&stream, gc, region_reserve_cb, region_alloc_cb,
			luamp_error, L);
	luamp_encode_tuple(L, luaL_msgpack_default, &stream, idx);
	mpstream_flush(&stream);
	*p_len = region_used(gc) - used;
	return (char *) region_join_xc(gc, *p_len);
}

struct space *
lua_checkephemeralspace(struct lua_State *L, int idx)
{
	uint32_t ctypeid = 0;
	void *data = luaL_checkcdata(L, idx, &ctypeid);
	if (ctypeid != CTID_STRUCT_SPACE_POINTER) {
		luaL_error(L, "Invalid argument #%d (space expected, got %s)",
			   idx, lua_typename(L, ctypeid));
		return NULL;
	}
	return *(struct space **) data;
}

/* }}} */

/** {{{ Lua/C implementation of index:select(): used only by Vinyl **/

static inline void
lbox_port_to_table(lua_State *L, struct port *port_base)
{
	struct port_tuple *port = port_tuple(port_base);
	lua_createtable(L, port->size, 0);
	struct port_tuple_entry *entry = port->first;
	for (int i = 0 ; i < port->size; i++) {
		luaT_pushtuple(L, entry->tuple);
		lua_rawseti(L, -2, i + 1);
		entry = entry->next;
	}
}

static int
lbox_select(lua_State *L)
{
	if (lua_gettop(L) != 6 || !lua_isnumber(L, 1) || !lua_isnumber(L, 2) ||
		!lua_isnumber(L, 3) || !lua_isnumber(L, 4) || !lua_isnumber(L, 5)) {
		return luaL_error(L, "Usage index:select(iterator, offset, "
				  "limit, key)");
	}

	uint32_t space_id = lua_tonumber(L, 1);
	uint32_t index_id = lua_tonumber(L, 2);
	int iterator = lua_tonumber(L, 3);
	uint32_t offset = lua_tonumber(L, 4);
	uint32_t limit = lua_tonumber(L, 5);

	size_t key_len;
	const char *key = lbox_encode_tuple_on_gc(L, 6, &key_len);

	struct port port;
	if (box_select(space_id, index_id, iterator, offset, limit,
		       key, key + key_len, &port) != 0) {
		return luaT_error(L);
	}

	/*
	 * Lua may raise an exception during allocating table or pushing
	 * tuples. In this case `port' definitely will leak. It is possible to
	 * wrap lbox_port_to_table() to pcall(), but it was too expensive
	 * for this binding according to our benchmarks (~5% decrease).
	 * However, we tried to simulate this situation and LuaJIT finalizers
	 * table always crashed the first (can't be fixed with pcall).
	 * https://github.com/tarantool/tarantool/issues/1182
	 */
	lbox_port_to_table(L, &port);
	port_destroy(&port);
	return 1; /* lua table with tuples */
}

/* }}} */

void
box_lua_misc_init(struct lua_State *L)
{
	int rc = luaL_cdef(L, "struct space;");
	assert(rc == 0);
	(void) rc;
	CTID_STRUCT_SPACE_POINTER = luaL_ctypeid(L, "struct space *");
	assert(CTID_STRUCT_SPACE_POINTER != 0);

	static const struct luaL_Reg boxlib_internal[] = {
		{"select", lbox_select},
		{NULL, NULL}
	};

	luaL_register(L, "box.internal", boxlib_internal);
	lua_pop(L, 1);
}
