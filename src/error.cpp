#pragma once

enum class Error {
    NO_ERROR = 0,
    QUERY_NOT_SUPPORTED,
    EXPECTED_FIELDS,
    EXPECTED_VALUES,
    EXPECTED_INSERT_INTO,
    EXPECTED_FIELD_DEFS,
    EXPECTED_TABLE_LIST,
    EXPECTED_IDENTIFIER,
    EXPECTED_DATA_TYPE,
    EXPECTED_ORDER_BY_LIST,
    EXPECTED_LEFT_PARANTH,
    EXPECTED_RIGHT_PARANTH,
    EXPECTED_EXPRESSION,
    EXPECTED_EXPRESSION_IN_WHERE_CLAUSE,
    EXPECTED_EXPRESSION_IN_HAVING_CLAUSE,
    EXPECTED_GROUP_BY_LIST,
    NO_EXPRESSION_CONTEXT,
    CANT_HAVE_AGGREGATION,
    CANT_CALL_FUNCTION,
    CANT_CAST_TYPE,
    CANT_HAVE_CASE_EXPRESSION,
    INCORRECT_CASE_EXPRESSION,
    CANT_NEST_AGGREGATION,
    LOGICAL_PLAN_ERROR,
    EXPECTED_ON_TABLE_NAME,
};
