"""Bounded SELECT queries over the operational fields approved for GhostFrog."""

from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import date, datetime, timezone
import json

from mcp.server.fastmcp.exceptions import ToolError
from pglast import ast, parse_sql
from pglast.parser import ParseError
from pglast.stream import RawStream
import psycopg2
from psycopg2 import sql

from ops.ghostfrog_diagnostic_catalog import LOGICAL_RELATIONSHIPS, READABLE_TEXT, RELATIONS


ROLE = "ghostfrog_diagnostics"
MAX_ROWS = 100
MAX_COLUMNS = 40
MAX_QUERY_BYTES = 12_000
MAX_RESULT_BYTES = 48_000
MAX_CELL_CHARS = 512
PROVENANCE = "GhostFrog live PostgreSQL / approved operational columns"
TEXT_TYPES = {"text", "varchar", "bpchar", "name"}
SAFE_TYPES = TEXT_TYPES | {
    "bool", "int2", "int4", "int8", "numeric", "float4", "float8",
    "date", "timestamp", "timestamptz", "interval",
}
SAFE_CASTS = SAFE_TYPES - {"name"}
FUNCTIONS = {
    "abs", "avg", "btrim", "ceil", "count", "date_part", "date_trunc", "dense_rank",
    "extract", "first_value", "floor", "lag", "last_value", "lead", "length", "lower",
    "max", "min", "now", "nullif", "percentile_cont", "percentile_disc", "rank",
    "round", "row_number", "stddev", "stddev_pop", "stddev_samp", "sum", "timezone", "upper",
}
OPERATORS = {
    "=", "<>", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "%",
    "~~", "!~~", "~~*", "!~~*", "~", "!~", "~*", "!~*",
    "BETWEEN", "NOT BETWEEN", "BETWEEN SYMMETRIC", "NOT BETWEEN SYMMETRIC",
}
EXPRESSION_NODES = (
    ast.A_Const, ast.A_Star, ast.Alias, ast.Boolean, ast.BoolExpr, ast.BooleanTest,
    ast.CaseExpr, ast.CaseWhen, ast.CoalesceExpr, ast.ColumnRef, ast.Float,
    ast.GroupingSet, ast.Integer, ast.JoinExpr, ast.MinMaxExpr, ast.NullTest,
    ast.RangeSubselect, ast.ResTarget, ast.String, ast.SubLink, ast.TypeCast, ast.WindowDef,
)
CLOCK_VALUES = {
    "SVFOP_CURRENT_DATE", "SVFOP_CURRENT_TIMESTAMP", "SVFOP_CURRENT_TIMESTAMP_N",
    "SVFOP_LOCALTIMESTAMP", "SVFOP_LOCALTIMESTAMP_N",
}
RELKINDS = {"r": "table", "p": "partitioned table", "v": "view", "m": "materialized view"}
PUBLIC_ITEM_ID = r"^([0-9]{12}|v1[|][0-9]{12}[|][0-9]{1,12})$"
CODED_TEXT = r"^[A-Za-z0-9][A-Za-z0-9_.+-]{0,159}$"
SECRET_PREFIX = r"^(sk[-_]|bearer|gh[pousr]_|github_pat_|xox[baprs]-|AKIA[0-9A-Z]|eyJ[A-Za-z0-9_-])"

COLUMN_CATALOG = """
    SELECT c.relname, c.relkind, a.attname, t.typname,
           pg_catalog.format_type(a.atttypid, a.atttypmod)
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
    JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
    JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
    WHERE n.nspname = 'public' AND tn.nspname = 'pg_catalog'
      AND c.relname = ANY(%s) AND c.relkind IN ('r', 'p', 'v', 'm')
      AND a.attnum > 0 AND NOT a.attisdropped
      AND pg_catalog.has_column_privilege(c.oid, a.attnum, 'SELECT')
    ORDER BY c.relname, a.attnum
"""
FOREIGN_KEYS = """
    SELECT c.relname, array_agg(source_column.attname::text ORDER BY key.ordinality),
           referenced.relname, array_agg(target_column.attname::text ORDER BY key.ordinality)
    FROM pg_catalog.pg_constraint fk
    JOIN pg_catalog.pg_class c ON c.oid = fk.conrelid
    JOIN pg_catalog.pg_class referenced ON referenced.oid = fk.confrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_namespace rn ON rn.oid = referenced.relnamespace
    CROSS JOIN LATERAL unnest(fk.conkey, fk.confkey) WITH ORDINALITY AS key(source, target, ordinality)
    JOIN pg_catalog.pg_attribute source_column ON source_column.attrelid = c.oid AND source_column.attnum = key.source
    JOIN pg_catalog.pg_attribute target_column ON target_column.attrelid = referenced.oid AND target_column.attnum = key.target
    WHERE fk.contype = 'f' AND n.nspname = 'public' AND rn.nspname = 'public'
      AND c.relname = ANY(%s) AND referenced.relname = ANY(%s)
    GROUP BY fk.oid, c.relname, referenced.relname
    ORDER BY c.relname, referenced.relname, fk.oid
"""


class RejectedQuery(ValueError):
    """The requested SQL is outside the diagnostics interface."""


@contextmanager
def _connection():
    try:
        with closing(psycopg2.connect(
            service=ROLE, user=ROLE, host="127.0.0.1", hostaddr="127.0.0.1", connect_timeout=3,
            options="-c search_path=pg_catalog -c timezone=UTC -c statement_timeout=8000 "
                    "-c lock_timeout=1000 -c idle_in_transaction_session_timeout=10000",
        )) as conn:
            conn.set_session(readonly=True)
            yield conn
    except psycopg2.errors.QueryCanceled:
        raise ToolError("PostgreSQL diagnostic query timed out.") from None
    except psycopg2.OperationalError:
        raise ToolError("The GhostFrog diagnostics login or database is unavailable.") from None
    except psycopg2.Error:
        raise ToolError("PostgreSQL rejected the diagnostic query. Check the catalog and SQL syntax.") from None


def _check_name(parts, allowed, message, *, catalog_ok=True):
    names = tuple(part.sval for part in parts)
    if catalog_ok and len(names) == 2 and names[0] == "pg_catalog":
        names = names[1:]
    if len(names) != 1 or names[0] not in allowed:
        raise RejectedQuery(message)


def _validate(node, scope: set[str], sources: set[str]) -> None:
    if isinstance(node, (list, tuple)):
        for child in node:
            _validate(child, scope, sources)
        return
    if not isinstance(node, ast.Node):
        return
    if isinstance(node, ast.SelectStmt):
        if node.intoClause or node.lockingClause:
            raise RejectedQuery("SELECT INTO and row locking are disabled.")
        if node.withClause:
            if node.withClause.recursive:
                raise RejectedQuery("Recursive CTEs are disabled.")
            scope = scope.copy()
            for cte in node.withClause.ctes:
                if not isinstance(cte.ctequery, ast.SelectStmt):
                    raise RejectedQuery("CTEs must contain SELECT statements.")
                _validate(cte.ctequery, scope, sources)
                scope.add(cte.ctename)
    elif isinstance(node, ast.RangeVar):
        if node.catalogname or node.schemaname:
            raise RejectedQuery("Use the unqualified diagnostic relation names from the catalog.")
        if node.relname not in scope:
            if node.relname not in RELATIONS:
                raise RejectedQuery("That relation is outside the diagnostic catalog.")
            sources.add(node.relname)
    elif isinstance(node, ast.FuncCall):
        _check_name(node.funcname, FUNCTIONS, "That function is outside the diagnostic allowlist.")
    elif isinstance(node, ast.TypeName):
        _check_name(node.names, SAFE_CASTS, "That cast is outside the diagnostic allowlist.")
        if node.setof or node.pct_type or node.arrayBounds:
            raise RejectedQuery("Only scalar built-in casts are supported.")
    elif isinstance(node, ast.A_Expr):
        _check_name(node.name, OPERATORS, "That operator is outside the diagnostic allowlist.", catalog_ok=False)
    elif isinstance(node, ast.SortBy):
        if node.useOp:
            raise RejectedQuery("ORDER BY USING is disabled.")
    elif isinstance(node, ast.SQLValueFunction):
        if node.op.name not in CLOCK_VALUES:
            raise RejectedQuery("Only date and time SQL value functions are supported.")
    elif not isinstance(node, EXPRESSION_NODES):
        raise RejectedQuery("The query uses syntax outside the diagnostic SELECT interface.")
    for field in node:
        if not (isinstance(node, ast.SelectStmt) and field == "withClause"):
            _validate(getattr(node, field), scope, sources)


def validate_query(query: str) -> tuple[str, list[str]]:
    if len(query.encode("utf-8")) > MAX_QUERY_BYTES or "\0" in query:
        raise RejectedQuery("SQL exceeds the input limit or contains a null byte.")
    try:
        statements = parse_sql(query)
        if len(statements) != 1 or not isinstance(statements[0].stmt, ast.SelectStmt):
            raise RejectedQuery("Send one read-only SELECT statement.")
        statement = statements[0].stmt
        sources: set[str] = set()
        _validate(statement, set(), sources)
        normalized = RawStream()(statement)
        reparsed = parse_sql(normalized)
        if len(reparsed) != 1 or reparsed[0].stmt != statement:
            raise RejectedQuery("Unable to safely normalize that SELECT.")
        return normalized, sorted(sources)
    except (ParseError, RecursionError, NotImplementedError):
        raise RejectedQuery("Unable to parse that diagnostic SELECT.") from None


def _catalog(conn, names: list[str]) -> dict:
    result: dict = {}
    if not names:
        return result
    allowed = {name: set(RELATIONS[name].split()) for name in names}
    with conn.cursor() as cursor:
        cursor.execute(COLUMN_CATALOG, (names,))
        for name, kind, column, data_type, display_type in cursor:
            if column in allowed[name] and data_type in SAFE_TYPES:
                entry = result.setdefault(name, {"kind": RELKINDS[kind], "columns": {}})
                entry["columns"][column] = (data_type, display_type)
    return result


def _relationship(catalog: dict, shape, kind: str) -> dict | None:
    source, columns, target, target_columns = shape
    if source in catalog and target in catalog:
        if set(columns) <= catalog[source]["columns"].keys() and set(target_columns) <= catalog[target]["columns"].keys():
            return {
                "relation": source, "columns": list(columns), "related_relation": target,
                "related_columns": list(target_columns), "kind": kind,
            }
    return None


def _result(**fields) -> dict:
    return {
        "queried_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "provenance": PROVENANCE,
        **fields,
    }


def catalog() -> dict:
    """Describe available diagnostic relations and safe declared and logical joins."""
    with _connection() as conn:
        available = _catalog(conn, list(RELATIONS))
        names = list(available)
        with conn.cursor() as cursor:
            cursor.execute(FOREIGN_KEYS, (names, names))
            declared = [_relationship(available, shape, "foreign_key") for shape in cursor]
    logical = [_relationship(available, shape, "logical_join") for shape in LOGICAL_RELATIONSHIPS]
    relations = [
        {
            "schema": "public", "name": name, "kind": entry["kind"],
            "columns": [
                {
                    "name": col, "type": kind, "redacts_text": data_type in TEXT_TYPES,
                    **({"text_format": "sanitized product evidence"} if col in READABLE_TEXT.get(name, ()) else {}),
                }
                for col, (data_type, kind) in entry["columns"].items()
            ],
        }
        for name, entry in available.items()
    ]
    return _result(relations=relations, relationships=[item for item in declared + logical if item])


def _projection(name: str, entry: dict):
    columns = {}
    indexed_history_ids = name == "auction_price_history" and "external_id" in entry["columns"]
    for column, (data_type, _) in entry["columns"].items():
        identifier = sql.Identifier(column)
        if (
            data_type not in TEXT_TYPES
            or column in READABLE_TEXT.get(name, ())
            or (indexed_history_ids and column == "external_id")
        ):
            columns[column] = identifier
            continue
        if column in {"external_id", "listing_external_id", "item_id"}:
            columns[column] = sql.SQL("CASE WHEN {col} ~ {pattern} THEN {col} END AS {col}").format(
                col=identifier, pattern=sql.Literal(PUBLIC_ITEM_ID),
            )
            continue
        columns[column] = sql.SQL(
            "CASE WHEN {col} IS NULL THEN NULL WHEN {col} ~ {pattern} AND {col} !~* {secret} "
            "THEN {col} ELSE '<redacted>' END AS {col}"
        ).format(col=identifier, pattern=sql.Literal(CODED_TEXT), secret=sql.Literal(SECRET_PREFIX))

    relation = sql.Identifier("public", name)
    select = sql.SQL("SELECT {} FROM {}").format(sql.SQL(", ").join(columns.values()), relation)
    if indexed_history_ids:
        # Keep the public ID branch indexable on (external_id, recorded_at).
        # Retain malformed and missing IDs as NULL in a separate branch, so
        # totals still include these observations but joins cannot match them.
        public_id = sql.SQL("external_id ~ {}").format(sql.Literal(PUBLIC_ITEM_ID))
        hidden = dict(columns, external_id=sql.SQL("NULL::text AS external_id"))
        select += sql.SQL(" WHERE {safe} UNION ALL SELECT {hidden} FROM {relation} WHERE ({safe}) IS NOT TRUE").format(
            safe=public_id, hidden=sql.SQL(", ").join(hidden.values()), relation=relation,
        )
    return sql.SQL("{} AS ({})").format(sql.Identifier(name), select)


def _json_cell(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat(), False
    if value is None or isinstance(value, (int, bool)):
        return value, False
    # Includes Decimal, interval and non-finite floats.
    text = str(value)
    return text[:MAX_CELL_CHARS], len(text) > MAX_CELL_CHARS


def query(query_text: str, limit: int) -> dict:
    """Run one validated diagnostic SELECT in a read-only transaction."""
    if type(limit) is not int or not 1 <= limit <= MAX_ROWS:
        raise ToolError("The diagnostic row limit must be between 1 and 100.")
    try:
        normalized, sources = validate_query(query_text)
    except RejectedQuery as exc:
        raise ToolError(str(exc)) from None

    with _connection() as conn:
        available = _catalog(conn, sources)
        if len(available) != len(sources):
            raise ToolError("A referenced diagnostic relation is unavailable. Check the catalog.")
        prefix = sql.SQL("WITH {} ").format(sql.SQL(", ").join(
            _projection(name, available[name]) for name in sources
        )) if sources else sql.SQL("")
        statement = prefix + sql.SQL("SELECT * FROM ({}) AS diagnostic_result LIMIT {}").format(
            sql.SQL(normalized), sql.Literal(limit + 1),
        )
        with conn.cursor(name="ghostfrog_diagnostic_result") as cursor:
            cursor.execute(statement)
            rows = cursor.fetchmany(limit + 1)
            columns = [column.name for column in cursor.description]
            if len(columns) > MAX_COLUMNS:
                raise ToolError("Project at most 40 diagnostic result columns.")
            result_rows = []
            size = 0
            rows_truncated = cells_truncated = False
            for row in rows:
                cells = [_json_cell(value) for value in row]
                values = [value for value, _ in cells]
                size += len(json.dumps(values))
                if len(result_rows) == limit or size > MAX_RESULT_BYTES:
                    rows_truncated = True
                    break
                cells_truncated |= any(clipped for _, clipped in cells)
                result_rows.append(values)
    return _result(
        relations=sources, columns=columns, rows=result_rows,
        truncated=rows_truncated or cells_truncated, rows_truncated=rows_truncated,
        cells_truncated=cells_truncated,
    )
