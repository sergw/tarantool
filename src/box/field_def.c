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

#include "field_def.h"
#include "trivia/util.h"
#include "key_def.h"
#include "box/tuple_format.h"
#include "box/schema_def.h"
#include "box/identifier.h"
// #include "sql.h"

const char *field_type_strs[] = {
	/* [FIELD_TYPE_ANY]      = */ "any",
	/* [FIELD_TYPE_UNSIGNED] = */ "unsigned",
	/* [FIELD_TYPE_STRING]   = */ "string",
	/* [FIELD_TYPE_NUMBER]   = */ "number",
	/* [FIELD_TYPE_INTEGER]  = */ "integer",
	/* [FIELD_TYPE_BOOLEAN]  = */ "boolean",
	/* [FIELD_TYPE_SCALAR]   = */ "scalar",
	/* [FIELD_TYPE_ARRAY]    = */ "array",
	/* [FIELD_TYPE_MAP]      = */ "map",
};

const char *on_conflict_action_strs[] = {
	/* [ON_CONFLICT_ACTION_NONE]     = */ "none",
	/* [ON_CONFLICT_ACTION_ROLLBACK] = */ "rollback",
	/* [ON_CONFLICT_ACTION_ABORT]    = */ "abort",
	/* [ON_CONFLICT_ACTION_FAIL]     = */ "fail",
	/* [ON_CONFLICT_ACTION_IGNORE]   = */ "ignore",
	/* [ON_CONFLICT_ACTION_REPLACE]  = */ "replace",
	/* [ON_CONFLICT_ACTION_DEFAULT]  = */ "default"
};

static int64_t
field_type_by_name_wrapper(const char *str, uint32_t len)
{
	return field_type_by_name(str, len);
}

/**
 * Table of a field types compatibility.
 * For an i row and j column the value is true, if the i type
 * values can be stored in the j type.
 */
static const bool field_type_compatibility[] = {
	   /*   ANY   UNSIGNED  STRING   NUMBER  INTEGER  BOOLEAN   SCALAR   ARRAY     MAP */
/*   ANY    */ true,   false,   false,   false,   false,   false,   false,   false,   false,
/* UNSIGNED */ true,   true,    false,   true,    true,    false,   true,    false,   false,
/*  STRING  */ true,   false,   true,    false,   false,   false,   true,    false,   false,
/*  NUMBER  */ true,   false,   false,   true,    false,   false,   true,    false,   false,
/*  INTEGER */ true,   false,   false,   true,    true,    false,   true,    false,   false,
/*  BOOLEAN */ true,   false,   false,   false,   false,   true,    true,    false,   false,
/*  SCALAR  */ true,   false,   false,   false,   false,   false,   true,    false,   false,
/*   ARRAY  */ true,   false,   false,   false,   false,   false,   false,   true,    false,
/*    MAP   */ true,   false,   false,   false,   false,   false,   false,   false,   true,
};

bool
field_type1_contains_type2(enum field_type type1, enum field_type type2)
{
	int idx = type2 * field_type_MAX + type1;
	return field_type_compatibility[idx];
}

const struct opt_def field_def_reg[] = {
	OPT_DEF_ENUM("type", field_type, struct field_def, type,
		     field_type_by_name_wrapper),
	OPT_DEF("name", OPT_STRPTR, struct field_def, name),
	OPT_DEF("is_nullable", OPT_BOOL, struct field_def, is_nullable),
	OPT_DEF_ENUM("nullable_action", on_conflict_action, struct field_def,
		     nullable_action, NULL),
	OPT_DEF("collation", OPT_UINT32, struct field_def, coll_id),
	OPT_DEF("default", OPT_STRPTR, struct field_def, default_value),
	OPT_DEF("affinity", OPT_UINT32, struct field_def, affinity),
	OPT_END,
};

const struct field_def field_def_default = {
	.type = FIELD_TYPE_ANY,
	.affinity = AFFINITY_UNDEFINED,
	.name = NULL,
	.is_nullable = false,
	.nullable_action = ON_CONFLICT_ACTION_DEFAULT,
	.coll_id = COLL_NONE,
	.default_value = NULL,
	.default_value_expr = NULL
};

enum field_type
field_type_by_name(const char *name, size_t len)
{
	enum field_type field_type = strnindex(field_type_strs, name, len,
					       field_type_MAX);
	if (field_type != field_type_MAX)
		return field_type;
	/* 'num' and 'str' in _index are deprecated since Tarantool 1.7 */
	if (strncasecmp(name, "num", len) == 0)
		return FIELD_TYPE_UNSIGNED;
	else if (strncasecmp(name, "str", len) == 0)
		return FIELD_TYPE_STRING;
	else if (len == 1 && name[0] == '*')
		return FIELD_TYPE_ANY;
	return field_type_MAX;
}

int
field_def_decode(struct field_def *field, const char **data,
		 const char *space_name, uint32_t name_len,
		 uint32_t errcode, uint32_t fieldno, struct region *region)
{
	if (mp_typeof(**data) != MP_MAP) {
		diag_set(ClientError, errcode, tt_cstr(space_name, name_len),
			  tt_sprintf("field %d is not map",
				     fieldno + TUPLE_INDEX_BASE));
		return -1;
	}
	int count = mp_decode_map(data);
	*field = field_def_default;
	bool is_action_missing = true;
	uint32_t action_literal_len = strlen("nullable_action");
	for (int i = 0; i < count; ++i) {
		if (mp_typeof(**data) != MP_STR) {
			diag_set(ClientError, errcode,
				  tt_cstr(space_name, name_len),
				  tt_sprintf("field %d format is not map"\
					     " with string keys",
					     fieldno + TUPLE_INDEX_BASE));
			return -1;
		}
		uint32_t key_len;
		const char *key = mp_decode_str(data, &key_len);
		if (opts_parse_key(field, field_def_reg, key, key_len, data,
				   ER_WRONG_SPACE_FORMAT,
				   fieldno + TUPLE_INDEX_BASE, region,
				   true) != 0)
			return -1;
		if (is_action_missing &&
		    key_len == action_literal_len &&
		    memcmp(key, "nullable_action", action_literal_len) == 0)
			is_action_missing = false;
	}
	if (is_action_missing) {
		field->nullable_action = field->is_nullable ?
			ON_CONFLICT_ACTION_NONE
			: ON_CONFLICT_ACTION_DEFAULT;
	}
	if (field->name == NULL) {
		diag_set(ClientError, errcode, tt_cstr(space_name, name_len),
			  tt_sprintf("field %d name is not specified",
				     fieldno + TUPLE_INDEX_BASE));
		return -1;
	}
	size_t field_name_len = strlen(field->name);
	if (field_name_len > BOX_NAME_MAX) {
		diag_set(ClientError, errcode, tt_cstr(space_name, name_len),
			  tt_sprintf("field %d name is too long",
				     fieldno + TUPLE_INDEX_BASE));
		return -1;
	}
	if(identifier_check(field->name, field_name_len))
		return -1;
	if (field->type == field_type_MAX) {
		diag_set(ClientError, errcode, tt_cstr(space_name, name_len),
			  tt_sprintf("field %d has unknown field type",
				     fieldno + TUPLE_INDEX_BASE));
		return -1;
	}
	if (field->nullable_action == on_conflict_action_MAX) {
		diag_set(ClientError, errcode, tt_cstr(space_name, name_len),
			  tt_sprintf("field %d has unknown field on conflict "
				     "nullable action",
				     fieldno + TUPLE_INDEX_BASE));
		return -1;
	}
	if (!((field->is_nullable && field->nullable_action ==
	       ON_CONFLICT_ACTION_NONE)
	      || (!field->is_nullable
		  && field->nullable_action != ON_CONFLICT_ACTION_NONE))) {
		diag_set(ClientError, errcode, tt_cstr(space_name, name_len),
			  tt_sprintf("field %d has conflicting nullability and "
				     "nullable action properties", fieldno +
				     TUPLE_INDEX_BASE));
		return -1;
	}
	if (field->coll_id != COLL_NONE &&
	    field->type != FIELD_TYPE_STRING &&
	    field->type != FIELD_TYPE_SCALAR &&
	    field->type != FIELD_TYPE_ANY) {
		diag_set(ClientError, errcode, tt_cstr(space_name, name_len),
			  tt_sprintf("collation is reasonable only for "
				     "string, scalar and any fields"));
		return -1;
	}

	// if (field->default_value != NULL &&
	//     sql_expr_compile(sql_get(), field->default_value,
	// 		     strlen(field->default_value),
	// 		     &field->default_value_expr) != 0)
	// 	return -1;
	return 0;
}
